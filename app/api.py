import re
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from app.domain.constants import USER_ID_PATTERN
from app.domain.projection import project_document
from app.errors import ConflictError, NotFoundError, RateLimitError, UnavailableError
from app.schemas import DocumentCreate, DocumentListResponse, DocumentPatch, DocumentResponse
from app.service import DocumentService

_USER_ID = re.compile(USER_ID_PATTERN)

router = APIRouter()


def get_service(request: Request) -> DocumentService:
    return request.app.state.service


def require_user(x_user_id: Annotated[str | None, Header()] = None) -> str:
    if x_user_id is None or not x_user_id.strip():
        raise HTTPException(status_code=401, detail="X-User-Id header is required")
    if _USER_ID.fullmatch(x_user_id) is None:
        raise HTTPException(status_code=401, detail="X-User-Id header is invalid")
    return x_user_id


def _document(document: dict) -> dict:
    return DocumentResponse.model_validate(project_document(document)).model_dump(mode="json")


@router.post("/documents")
async def submit_document(
    payload: DocumentCreate,
    user_id: Annotated[str, Depends(require_user)],
    service: Annotated[DocumentService, Depends(get_service)],
) -> JSONResponse:
    if payload.user_id != user_id:
        raise HTTPException(status_code=403, detail="user_id does not match X-User-Id")
    try:
        document, created = await service.submit(payload)
    except RateLimitError as exc:
        return _rate_limited(exc)
    except ConflictError as exc:
        return _conflict(exc)
    except UnavailableError as exc:
        return _unavailable(exc)
    return JSONResponse(status_code=201 if created else 200, content=_document(document))


@router.patch("/documents/{document_id}")
async def update_document(
    document_id: str,
    payload: DocumentPatch,
    user_id: Annotated[str, Depends(require_user)],
    service: Annotated[DocumentService, Depends(get_service)],
) -> JSONResponse:
    try:
        document = await service.update_content(
            document_id,
            user_id,
            payload.content,
            payload.expected_content_version,
        )
    except NotFoundError:
        return _not_found()
    except RateLimitError as exc:
        return _rate_limited(exc)
    except ConflictError as exc:
        return _conflict(exc)
    except UnavailableError as exc:
        return _unavailable(exc)
    return JSONResponse(status_code=200, content=_document(document))


@router.get("/documents/by-ref/{client_doc_ref}")
async def get_document_by_ref(
    client_doc_ref: str,
    user_id: Annotated[str, Depends(require_user)],
    service: Annotated[DocumentService, Depends(get_service)],
) -> JSONResponse:
    try:
        document = await service.get_by_ref(client_doc_ref, user_id)
    except NotFoundError:
        return _not_found()
    return JSONResponse(status_code=200, content=_document(document))


@router.get("/documents/{document_id}")
async def get_document(
    document_id: str,
    user_id: Annotated[str, Depends(require_user)],
    service: Annotated[DocumentService, Depends(get_service)],
) -> JSONResponse:
    try:
        document = await service.get(document_id, user_id)
    except NotFoundError:
        return _not_found()
    return JSONResponse(status_code=200, content=_document(document))


@router.get("/users/{user_id}/documents")
async def list_documents(
    user_id: str,
    caller_id: Annotated[str, Depends(require_user)],
    service: Annotated[DocumentService, Depends(get_service)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=50)] = 20,
    status: Annotated[
        Literal["queued", "processing", "enriching", "completed", "failed"] | None,
        Query(),
    ] = None,
) -> JSONResponse:
    if caller_id != user_id:
        return _not_found()
    items, total = await service.list_documents(user_id, status, page, page_size)
    body = DocumentListResponse(
        items=[DocumentResponse.model_validate(project_document(item)) for item in items],
        page=page,
        page_size=page_size,
        total=total,
    )
    return JSONResponse(status_code=200, content=body.model_dump(mode="json"))


def _not_found() -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": "document not found"})


def _conflict(exc: ConflictError) -> JSONResponse:
    body: dict[str, object] = {"detail": exc.detail}
    if exc.content_version is not None:
        body["content_version"] = exc.content_version
    return JSONResponse(status_code=409, content=body)


def _rate_limited(exc: RateLimitError) -> JSONResponse:
    return JSONResponse(
        status_code=429,
        content={"detail": str(exc)},
        headers={"Retry-After": str(exc.retry_after)},
    )


def _unavailable(exc: UnavailableError) -> JSONResponse:
    return JSONResponse(status_code=503, content={"detail": exc.detail})
