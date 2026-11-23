# Cohort Insights API

A multi-tenant document API. A submitter stores content, a background worker runs it through two simulated stages, and readers poll for the summary and tags. A partner system can also address a document by its own `client_doc_ref`.

The service is Python 3.11, FastAPI, MongoDB, and Redis.

## Run

```bash
docker compose up --build
```

The API listens on `http://localhost:8000`. MongoDB is on `27017`, Redis on `6379`. Configuration is listed in `.env.example`. `GET /health` returns 200 only when both MongoDB and Redis answer a ping.

Local tests need the same two services, then:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

Stage durations and failure rates are environment settings, so the tests run the real pipeline with short sleeps instead of a fake worker.

## API

| Method | Path | Success | Notes |
| --- | --- | --- | --- |
| `POST` | `/documents` | 201, or 200 when the ref already exists | Body: `user_id`, `title`, `content`, optional `client_doc_ref` |
| `PATCH` | `/documents/{document_id}` | 200 | Body: `content`, optional `expected_content_version` |
| `GET` | `/documents/{document_id}` | 200 | Current stage, and summary/tags only when they match this content |
| `GET` | `/users/{user_id}/documents` | 200 | `page`, `page_size` (max 50), optional `status`. Newest first |
| `GET` | `/documents/by-ref/{client_doc_ref}` | 200 | Point lookup on the unique ref index |
| `GET` | `/health` | 200 | 503 when MongoDB or Redis is down |

`POST /documents` returns 201 when a new document id is created. A repeat of an existing `client_doc_ref` returns 200 and the same id. Validation failures are 422. The active-pipeline cap is 429 with `Retry-After`. A lost optimistic version check is 409. An unknown id, an unknown ref, and another user's document are all 404.

Every document route requires `X-User-Id`. I treat that header as injected by a trusted gateway. This service checks identity, it does not authenticate. The header must match `user_id` on submit (403 otherwise). A read or a list for anyone else is 404, so a caller cannot tell a missing document from one they do not own. A malformed id is also 404.

`results_current` is true only when `status` is `completed` and both artifacts survived the version check below. It is computed while building the response. It is not stored.

## Pipeline

`queued → processing → enriching → completed`, or `failed`.

`processing` sleeps 10–20 seconds and writes a deterministic summary of the content. `enriching` sleeps 5–15 seconds, reads that summary, and writes keyword tags plus a `summary-words-N` tag taken from the summary text. Each stage draws its own failure chance, default 0.1, on each attempt.

A worker claims with `findOneAndUpdate`. Two workers cannot own the same stage: the filter matches `queued`, or an `enriching` retry whose claim was cleared, or a claim whose lease has expired. The update sets a new `claim_token`. The stage result is written with a second filter on that token, the status, and the `content_version` captured at claim time. A losing worker matches zero documents and drops its result.

Enriching failure does not clear the summary and does not return the document to `queued`. The document stays on the enriching stage, waits out an exponential backoff, and another claim runs enriching again. `failed_stage` is set only when that stage has used `PIPELINE_MAX_ATTEMPTS` (default 3, including the first try). A reclaimed expired lease counts as another attempt, so the lease (`PIPELINE_CLAIM_LEASE_SECONDS`, default 90) has to be longer than the stage. The process logs a warning when it is not.

The worker runs inside the API process. The claim filter is what makes a second process safe. I would still split it into its own service before running more than one API replica, so a rolling deploy does not scale the simulator with the request path.

## Schema & Staleness Design

A document stores `content_version` (starts at 1) and `content_hash` (SHA-256 of the raw UTF-8 content, no trimming). Summary and tags are subdocuments, not strings:

```json
{
  "summary": {"text": "...", "content_version": 3, "content_hash": "<sha256>"},
  "tags": {
    "values": ["..."],
    "content_version": 3,
    "content_hash": "<sha256>",
    "summary_content_version": 3
  }
}
```

`content_version` on an artifact is the document version it was computed from. `content_hash` is the hash of that content. `summary_content_version` on tags is the summary version they were derived from. Stage records use the same version stamp.

`project_document` is the only path that builds a response, including the list endpoint. It returns a summary only when both the version and the hash equal the document's current values. It returns tags only when they pass that same check and `summary_content_version` equals the summary's version. Otherwise the field is null. A stage record from another version is reported as `pending` with zero attempts. A stage marked succeeded is also reported as `pending` when its artifact did not pass the check, so a reader never sees a successful stage without the result that stage claims to have produced. `failed_stage` is returned only while `status` is `failed`.

That is a comparison, not a stored `is_stale` bit. There is no flag to forget. A reader looking at a stored mix — summary for version 2, tags for version 4, or tags whose hash does not match — cannot see both sides of the mix, because the side that disagrees is omitted. `results_current` is that same comparison plus `status == completed`.

The write path closes the window before the reader runs. `PATCH`, and a repeat `client_doc_ref` that carries different content, do one `findOneAndUpdate` filtered on the current `content_version`. The update increments `content_version`, replaces `content` and `content_hash`, sets `status` to `queued`, clears `claim_token`, and replaces `summary`, `tags`, and `stages` together. The old summary is not left sitting next to the new content. A poll that arrives after this update and before the new run finishes sees the new content, a higher version, `queued`, and null artifacts.

A worker that is still inside the old run cannot put the old result back. Its result write is filtered on the version it claimed and on its claim token. After the version moves, the write matches nothing and is logged and discarded. The same filter is what stops a double claim from publishing two results. If a write ever landed with the wrong stamp anyway, `project_document` would still hide it, because the artifact is stamped with the version it was built for, not with whatever the document says now.

An enriching failure leaves the summary in place. The summary's version and hash still match, so it stays visible, and tags stay null. Processing is not repeated. A processing failure has no summary to show. A later `PATCH` resets both stage records to the new version, so a failure from version 3 is not reported as the outcome of version 4.

Identical bytes are the cache key. Whitespace is significant. The title is not part of the hash, and the summary function does not read the title, so a same-content replay may rename the document without bumping the version.

## Crosswalk

`client_doc_ref` is optional. When it is present it is unique across the collection. Documents submitted without a ref do not store the field.

`GET /documents/by-ref/{client_doc_ref}` is `find_one({"client_doc_ref": ref})`. The index behind it is `client_doc_ref_unique`: a unique index on `{client_doc_ref: 1}` with `partialFilterExpression: {client_doc_ref: {$type: "string"}}`. Missing refs are outside the index, so they do not collide on null. This is a point lookup, not a collection scan.

Repeat submissions of the same ref:

- Same user, same content hash: return the existing document, 200, same `document_id`. No second job. A changed title is saved in place if the version and hash still match.
- Same user, different content: new version of the same document. Same `document_id`, `content_version` incremented, both stages run again, previous artifacts cleared. The partner payload has no revision number, so arrival order wins: both racing writes are applied under the per-user lock, and the later content is the one that runs. A `PATCH` that sends `expected_content_version` is the call that loses with 409 instead of overwriting.
- Different user: 409. The ref is a partner identity, not a per-user nickname, and the second tenant must stop retrying. Reads stay 404 so a read does not reveal the owner.

The unique index is the backstop for two first-time submits of the same ref. The loser gets `DuplicateKeyError`, drops the slot it reserved, and follows the rules above against the row that won.

If the partner later sends a monotonic revision, the update filter should require `client_revision` greater than the stored one and 409 the older write. I would not overload `content_hash` to mean that.

## Cache and the active-job cap

Redis stores two different things.

The result cache key is `ci:result:{content_hash}`, with a TTL (`CACHE_TTL_SECONDS`, default 24 hours). The value is the summary text and the tag list. It is written only after enriching commits for that hash. A submit or a content replacement looks up the hash it is about to store. A hit inserts or updates the document as `completed`, with artifacts stamped for the new version, and does not enter the pipeline. A miss falls through to MongoDB: `find_one` on the `content_hash_status` index plus an `$expr` that the source document's own artifacts match its own version. A hit there refills Redis. If Redis is down, the cache read is a miss and the MongoDB lookup still works; a cache write failure is logged and the request still succeeds.

The key is the hash, not the document id. Replacing content looks up the new hash and cannot read the old content's entry. The old entry stays addressable only by the old hash.

The active-job cap is a sorted set `ci:active:{user_id}` plus a hash `ci:active-meta:{user_id}` of `document_id -> content_version`. A Lua script drops expired scores, then either rejects at `MAX_ACTIVE_JOBS_PER_USER` (default 3) or records the document. `queued`, `processing`, and `enriching` count. `completed` and `failed` do not. A cache hit does not. Replacing an already active document refreshes its member and moves the stored version; it does not take a second slot. Replacing an active document with a cache hit removes the member.

Release deletes the member only when the hash still holds the version being released. A worker finishing version 3 cannot drop the slot after a `PATCH` has moved it to version 4. Admission and release for one user take a short Redis lock, `ci:lock:{user_id}`, so two submits cannot both observe "2 active" and both insert. If the lock cannot be acquired in time the request is 503. If Redis is unreachable, admission counts active documents in MongoDB with the `user_status_created` index and logs that it degraded. That count can race. I would rather fail closed on the cap than fail open.

Leases cover a crash between the Redis add and the MongoDB insert. The member expires. The claim lease covers a dead worker.

## Optimistic concurrency

`PATCH` accepts `expected_content_version`. Inside the per-user lock the service re-reads the document and, when the field is present, requires it to equal the stored version. The write then matches that version. Two patches that both send version 1 produce one 200 and one 409 whose body includes the version that won. Omitting the field means the request applies to whatever version it observes under the lock, so two different edits both land, in arrival order, as version N+1 and N+2.

## Indexes

| Name | Key | Used by |
| --- | --- | --- |
| `user_created` | `{user_id, created_at, _id}` | List, newest first |
| `user_status_created` | `{user_id, status, created_at, _id}` | Status filter, degraded admission count |
| `content_hash_status` | `{content_hash, status}` | Cold result cache |
| `client_doc_ref_unique` | `{client_doc_ref}` unique, partial | By-ref lookup |
| `pipeline_claim` | `{status, not_before, created_at}` | Worker claim |

## At 100×

A single user with 500,000 documents does not make `{user_id, created_at, _id}` the wrong index. Equality on `user_id` is one range, and half a million keys is a normal B-tree. The failure is the read. `skip((page - 1) * page_size)` walks every skipped entry, so a deep page costs work proportional to the offset. `count_documents` walks that same prefix. New submits land on the hot edge, so listing while ingesting keeps shifting the skipped pages. I would keep the index, drop the exact total from the request, and reject large offsets.

I would not shard on `user_id` alone. Every document for that user shares one shard-key value, so the chunk cannot split. A user with 500,000 documents, or a tenant who is a large share of writes, becomes a jumbo chunk pinned to one shard. A hashed `_id` spreads writes and turns every list into a scatter-gather, which is a bad fit for a query that always names one user. A hashed `user_id` still pins that user to one shard, so it does not answer this case. I would range-shard on `{user_id, _id}`. The prefix keeps a normal user's list and active-count on one shard, and `_id` gives the balancer a split point when that user grows. After a split, their list touches more than one shard. That is the point: their load can move, and smaller users stay targeted.

The limiter is one Lua script on a sorted set and a hash, both named only by `user_id`. Redis runs it on one thread. Across many users, 100× submit QPS is many keys, and one Redis still has headroom. It fails when the 100× is one user: admission, refresh, and release serialize on `ci:active:{user_id}`, and the script also rewrites the set to drop expired leases. A missed release then heals only through that hot key. I would switch the request path to a fixed-window `INCR` plus `EXPIRE`, and reconcile the count from the MongoDB active-status index off the request. Workers would refresh a short TTL instead of holding a member for the whole pipeline. In Cluster, the two keys for one user need the same hash tag so the script stays in one slot.

Offset pages do not survive here. `page` becomes `skip`, and skip time grows with the page number. Inserts at the head of a newest-first list also shift later offsets, so a page can repeat or drop a row between calls. I would use a keyset on `(created_at, _id)`, which the list index already orders: the client sends the last row it saw, and the query is the user prefix plus a bound on that pair, with a limit and no skip. Cost stays on the page size, and a newer insert does not move unread rows. I would keep offset only for the first screen, where skip is zero, and I would not return an exact total.

## Assumptions

- Content identity is exact UTF-8 bytes. I do not normalize case or whitespace.
- The simulated summary and tags are pure functions of the content and, for tags, of the summary text. The cache is therefore global. It stores those derived values, not the source document, the title, or the owner.
- MongoDB in Compose is a standalone. I do not use a multi-document transaction. The ref uniqueness constraint is the unique index, and the version check is the document-level update filter. Redis and MongoDB cannot share a transaction anyway.
- One active-job member is one document, across all of its in-flight versions. The version stored next to it is what makes an old worker's release a no-op.

## With more time

- Keyset pagination as the list API, with offset kept only for the first page.
- A partner `client_revision`, and a filter that rejects an older write instead of last-accepted-wins.
- A separate worker process, with queue age, stage failures, claim losses, and admission rejects as metrics.
- The fixed-window limiter described above, still reconciled from MongoDB.
- A replica set, so the Redis admission and the MongoDB insert can at least be ordered with a transaction on the document side.
