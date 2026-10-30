"""A cached ZIP may only ever be extracted into its own cache subdirectory."""

import zipfile
from io import BytesIO
from types import SimpleNamespace

import pytest

from cognee.shared import cache as cache_module
from cognee.shared.cache import StorageAwareCache, _zip_member_path

SIBLING_STATE = "state.json"


def zip_payload(members: dict[str, bytes]) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


class FakeContent:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    async def iter_chunked(self, chunk_size: int):
        yield self._payload


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.headers = {"ETag": '"abc123"'}
        self.content = FakeContent(payload)

    def raise_for_status(self) -> None:
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class FakeSession:
    def __init__(self, payload: bytes) -> None:
        self._response = FakeResponse(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    def get(self, url, timeout=None):
        return self._response


@pytest.fixture
def cache(tmp_path, monkeypatch):
    """A cache manager rooted in the test's own directory, not the user's."""
    monkeypatch.setattr(
        cache_module,
        "get_base_config",
        lambda: SimpleNamespace(cache_root_directory=str(tmp_path)),
    )
    return StorageAwareCache()


def serve_archive(monkeypatch, payload: bytes) -> None:
    monkeypatch.setattr(cache_module, "create_secure_ssl_context", lambda: None)
    monkeypatch.setattr(cache_module.aiohttp, "TCPConnector", lambda ssl=None: None)
    monkeypatch.setattr(
        cache_module.aiohttp, "ClientSession", lambda connector=None: FakeSession(payload)
    )


class TestZipMemberPath:
    """The member-to-path mapping, without touching storage."""

    def test_a_nested_member_stays_inside_the_cache_directory(self):
        assert _zip_member_path("/cache/source-a", "notes/a.txt") == "/cache/source-a/notes/a.txt"

    def test_a_dotted_member_is_normalized(self):
        assert _zip_member_path("/cache/source-a", "./notes/a.txt") == (
            "/cache/source-a/notes/a.txt"
        )

    def test_an_s3_root_keeps_its_double_slash(self):
        assert _zip_member_path("s3://bucket/cognee/cache/source-a", "notes/a.txt") == (
            "s3://bucket/cognee/cache/source-a/notes/a.txt"
        )

    @pytest.mark.parametrize(
        "member_name",
        [
            "../source-b/state.json",
            "notes/../../source-b/state.json",
            "/etc/passwd",
            "C:/Windows/evil.txt",
            "..\\source-b\\state.json",
            "..",
            ".",
            "",
        ],
    )
    def test_a_member_outside_the_cache_directory_is_rejected(self, member_name):
        with pytest.raises(ValueError, match="outside /cache/source-a"):
            _zip_member_path("/cache/source-a", member_name)


class TestDownloadAndExtractZip:
    """The extraction itself, with the HTTP response faked."""

    @pytest.mark.asyncio
    async def test_a_sibling_cache_entry_is_rejected_before_anything_is_written(
        self, cache, tmp_path, monkeypatch
    ):
        serve_archive(monkeypatch, zip_payload({"../source-b/state.json": b"attacker"}))
        sibling = tmp_path / "source-b" / SIBLING_STATE
        sibling.parent.mkdir()
        sibling.write_text("original")

        with pytest.raises(ValueError, match="outside"):
            await cache.download_and_extract_zip("https://example.test/cache.zip", "source-a", "v1")

        assert sibling.read_text() == "original"
        assert not (tmp_path / "source-a" / "version.txt").exists()

    @pytest.mark.asyncio
    async def test_a_traversal_behind_a_real_directory_is_rejected(
        self, cache, tmp_path, monkeypatch
    ):
        serve_archive(monkeypatch, zip_payload({"notes/../../source-b/state.json": b"attacker"}))
        sibling = tmp_path / "source-b" / SIBLING_STATE
        sibling.parent.mkdir()
        sibling.write_text("original")

        with pytest.raises(ValueError, match="outside"):
            await cache.download_and_extract_zip("https://example.test/cache.zip", "source-a", "v1")

        assert sibling.read_text() == "original"
        assert not (tmp_path / "source-a" / "version.txt").exists()

    @pytest.mark.asyncio
    async def test_an_absolute_member_is_rejected(self, cache, tmp_path, monkeypatch):
        serve_archive(monkeypatch, zip_payload({"/tmp/cognee_escape.txt": b"attacker"}))

        with pytest.raises(ValueError, match="outside"):
            await cache.download_and_extract_zip("https://example.test/cache.zip", "source-a", "v1")

        assert not (tmp_path / "cognee_escape.txt").exists()
        assert not (tmp_path / "tmp" / "cognee_escape.txt").exists()

    @pytest.mark.asyncio
    async def test_a_clean_archive_is_extracted_into_its_cache_directory(
        self, cache, tmp_path, monkeypatch
    ):
        serve_archive(
            monkeypatch,
            zip_payload({"notes/a.txt": b"alpha", "b.txt": b"bravo", "./c.txt": b"charlie"}),
        )

        cache_dir = await cache.download_and_extract_zip(
            "https://example.test/cache.zip", "source-a", "v1"
        )

        assert cache_dir == f"{tmp_path}/source-a"
        assert (tmp_path / "source-a" / "notes" / "a.txt").read_bytes() == b"alpha"
        assert (tmp_path / "source-a" / "b.txt").read_bytes() == b"bravo"
        assert (tmp_path / "source-a" / "c.txt").read_bytes() == b"charlie"
        assert (tmp_path / "source-a" / "version.txt").read_text() == "v1"
        assert (tmp_path / "source-a" / "content_id.txt").read_text() == "abc123"
