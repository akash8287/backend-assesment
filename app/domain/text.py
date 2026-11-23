import re

_TOKEN = re.compile(r"[A-Za-z][A-Za-z0-9']{2,}")
_STOP = frozenset(
    {
        "a",
        "an",
        "the",
        "of",
        "to",
        "and",
        "in",
        "on",
        "for",
        "with",
        "from",
        "by",
        "as",
        "is",
        "are",
        "was",
        "were",
        "be",
        "this",
        "that",
        "it",
        "or",
        "at",
        "its",
    }
)


def build_summary(content: str) -> str:
    compact = " ".join(content.split())
    words = compact.split()
    excerpt = compact[:280]
    return f"Document contains {len(words)} words. Excerpt: {excerpt}"


def build_tags(content: str, summary: str) -> list[str]:
    counts: dict[str, int] = {}
    for token in _TOKEN.findall(content.lower()):
        if token in _STOP:
            continue
        counts[token] = counts.get(token, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    tags = [word for word, _ in ranked[:5]]
    summary_words = len(summary.split())
    tags.append(f"summary-words-{summary_words}")
    return tags
