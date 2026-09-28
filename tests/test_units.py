import pytest
from pydantic import ValidationError

from app.domain import content_hash
from app.models import DocumentCreate, DocumentUpdate
from app.services.stages import mock_summary, mock_tags


def test_content_hash_is_stable_and_content_sensitive() -> None:
    assert content_hash("hello") == content_hash("hello")
    assert content_hash("hello") != content_hash("hello!")
    assert len(content_hash("x")) == 64


def test_mock_stages_are_deterministic() -> None:
    text = "Rockets rockets engines fuel rockets engines orbit"
    summary = mock_summary(text)
    assert summary == mock_summary(text)
    assert summary.startswith("Summary (7 words):")
    assert mock_tags(summary) == ["rockets", "engines", "fuel", "orbit"]


def test_summary_truncates_long_content() -> None:
    summary = mock_summary(" ".join(f"w{i}" for i in range(100)))
    assert summary.endswith("…")
    assert "w39" in summary and "w40" not in summary


@pytest.mark.parametrize(
    "payload",
    [
        {"user_id": "u", "title": "   ", "content": "c"},  # blank after strip
        {"user_id": "u", "title": "t", "content": ""},
        {"user_id": "bad user", "title": "t", "content": "c"},
        {"user_id": "u", "title": "t", "content": "c", "client_doc_ref": "a/b"},  # would break the by-ref path
        {"user_id": "u", "title": "t", "content": "c", "ref_version": 1},  # ref_version without ref
        {"user_id": "u", "title": "t", "content": "bad\x00byte"},
        {"user_id": "u", "title": "t", "content": "c", "status": "completed"},  # unknown field
        {"user_id": "u", "title": "t" * 301, "content": "c"},
    ],
)
def test_create_validation_rejects(payload: dict) -> None:
    with pytest.raises(ValidationError):
        DocumentCreate(**payload)


def test_create_strips_whitespace() -> None:
    doc = DocumentCreate(user_id="u", title="  t ", content="  c\n")
    assert (doc.title, doc.content) == ("t", "c")


def test_update_rejects_unknown_fields_and_bad_version() -> None:
    with pytest.raises(ValidationError):
        DocumentUpdate(content="c", user_id="someone-else")
    with pytest.raises(ValidationError):
        DocumentUpdate(content="c", expected_version=0)
