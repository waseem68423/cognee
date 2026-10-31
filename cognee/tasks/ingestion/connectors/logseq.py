"""Logseq connector: a ``dlt`` source that reads a local Logseq graph, one page per document.

``cognee.remember(<folder>)`` stores a page once: an edited page leaves its old version in memory
and a deleted page is never forgotten. This source gives every page a stable id (its path inside
the graph) and walks the whole selection on every run, so an edit replaces the old document and a
page that left the selection is forgotten. See ``logseq.md`` for what is read, deletion and its
gaps, and privacy.
"""

import hashlib
import os
import re
from collections.abc import Callable, Iterator
from pathlib import Path, PurePosixPath
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    PIPELINE_SCOPE_ATTR,
    STRUCTURE_COLUMN,
)

logger = get_logger("logseq_connector")

# A Logseq block: optional indentation, a "- " bullet, then the block text.
_BLOCK = re.compile(r"^(?P<indent>[ \t]*)-(?:[ \t]+(?P<text>.*))?$")
# A page property or a block property. Logseq separates the key from the value with "::".
_PROPERTY = re.compile(r"^(?P<indent>[ \t]*)(?P<key>[^\s:#][^:]*?)::(?P<value>.*)$")
# A page link "[[Some page]]"; a tag is written "#tag" or "[[#tag]]".
_PAGE_LINK = re.compile(r"\[\[(?P<name>[^\[\]]+)\]\]")
_HASH_TAG = re.compile(r"(?<![\w/])#(?P<name>[^\s#,;.:!?'\"()\[\]{}]+)")
# A block reference "((uuid))" points at a block in this or another page.
_BLOCK_REF = re.compile(r"\(\((?P<uuid>[0-9a-fA-F-]{8,})\)\)")
# Property keys that hold a page's aliases, in Logseq's current and older spelling.
_ALIAS_KEYS = ("alias", "aliases")


class LogseqSourceError(RuntimeError):
    """Base error. Messages carry graph-relative paths only, never absolute paths or text."""


class LogseqGraphUnavailableError(LogseqSourceError):
    """The graph folder is gone at run time, or its root is empty though pages were synced."""


class LogseqListingError(LogseqSourceError):
    """A folder of the graph could not be listed, so the listing is not complete."""


def _page_id(rel_path: str) -> str:
    return f"logseq:{rel_path}"


def _page_properties(text: str) -> dict:
    """The page's ``key:: value`` properties, read from the top of the file before the first block.

    Logseq writes them unindented, one per line, above the first ``- `` bullet. A ``- key:: value``
    line is a *block* property, not a page property, and is not read here.
    """
    properties: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        block = _BLOCK.match(line)
        if block is not None:
            break  # the first block ends the page header
        match = _PROPERTY.match(line)
        if match is None or match.group("indent"):
            break
        properties[match.group("key").strip().lower()] = match.group("value").strip()
    return properties


def _title(text: str, rel_path: str) -> str:
    """The ``title::`` property, else the file name without its extension."""
    title = _page_properties(text).get("title")
    if isinstance(title, str) and title.strip():
        return title.strip()
    return PurePosixPath(rel_path).stem


def _tags(text: str) -> list[str]:
    """``#tag`` and ``[[#tag]]`` names in the page, sorted and without repeats.

    A tag is also a page link, so a page linked as ``[[#project]]`` is tagged ``project``.
    """
    names = set()
    for match in _PAGE_LINK.finditer(text):
        name = match.group("name").strip()
        if name.startswith("#") and len(name) > 1:
            names.add(name[1:].strip())
    for match in _HASH_TAG.finditer(text):
        name = match.group("name").strip()
        if name and not name.startswith("#"):
            names.add(name)
    return sorted(name for name in names if name)


def _links(text: str) -> list[str]:
    """Page names the page links to with ``[[...]]``, sorted and without repeats (tags excluded)."""
    names = {
        match.group("name").strip()
        for match in _PAGE_LINK.finditer(text)
        if not match.group("name").strip().startswith("#")
    }
    return sorted(name for name in names if name)


def _block_refs(text: str) -> list[str]:
    """Block uuids the page references with ``((uuid))``, sorted and without repeats."""
    return sorted({match.group("uuid").lower() for match in _BLOCK_REF.finditer(text)})


def _aliases(text: str) -> list[str]:
    """Alias names from the ``alias::``/``aliases::`` page property, sorted and without repeats."""
    value = ""
    for key in _ALIAS_KEYS:
        value = _page_properties(text).get(key, "") or value
        if value:
            break
    names = {name.strip() for name in _PAGE_LINK.findall(value)} if "[[" in value else set()
    if not names:
        names = {name.strip() for name in re.split(r"[,]", value) if name.strip()}
    return sorted(names)


def _node_set(text: str, rel_path: str) -> list[str]:
    """``logseq:tag:<tag>`` for every tag, ``logseq:page:<name>`` for every page link."""
    nodes = {f"logseq:tag:{tag}" for tag in _tags(text)}
    nodes.update(f"logseq:page:{link}" for link in _links(text))
    nodes.update(f"logseq:alias:{alias}" for alias in _aliases(text))
    return sorted(nodes)


def _ancestors(rel_path: str) -> list[dict]:
    """The page's folders, nearest parent first, as ``cognee_structure`` wants them."""
    folder = PurePosixPath(rel_path).parent
    return [
        {"kind": "folder", "id": parent.as_posix(), "name": parent.name}
        for parent in [folder, *folder.parents]
        if parent.parts
    ]


def _page_row(rel_path: str, text: str) -> dict[str, Any]:
    """One document row. The content is the file as read, page properties included."""
    row: dict[str, Any] = {
        "id": _page_id(rel_path),
        "title": _title(text, rel_path),
        "content": text,
        "url": "",
        "_deleted": False,
    }
    node_set = _node_set(text, rel_path)
    if node_set:
        row[NODE_SET_COLUMN] = node_set
    ancestors = _ancestors(rel_path)
    if ancestors:
        row[STRUCTURE_COLUMN] = {"ancestors": ancestors}
    return row


def _list_pages(graph: Path, folders: list[str] | None) -> list[str]:
    """Graph-relative posix paths of the pages to ingest, sorted.

    Only regular ``*.md`` files (no symlinks, no FIFOs or devices, which would block a read), and
    nothing whose name starts with a dot: not ``.git``/``.logseq``, and not hidden files. Symlinked
    directories are not entered. A folder that cannot be listed raises, because ``os.walk`` would
    skip it silently and the pages inside would then look deleted. A selected folder that exists on
    disk but that the walk never visited under that exact name raises for the same reason. A
    selected folder that does not exist is a removed folder and forgets its pages.
    """

    def refuse(error: OSError) -> None:
        rel = os.path.relpath(error.filename, graph)
        # from None: the OSError text carries the absolute path.
        raise LogseqListingError(
            f"Could not list the graph folder {rel!r} ({type(error).__name__}); "
            "nothing was synced and nothing was forgotten."
        ) from None

    pages = []
    visited = set()
    for directory, subdirs, files in os.walk(graph, onerror=refuse):
        visited.add(Path(directory).relative_to(graph).as_posix())
        subdirs[:] = [name for name in subdirs if not name.startswith(".")]
        for name in files:
            path = os.path.join(directory, name)
            if (
                name.endswith(".md")
                and not name.startswith(".")
                and not os.path.islink(path)
                and os.path.isfile(path)
            ):
                pages.append(Path(path).relative_to(graph).as_posix())
    if folders is not None:
        for folder in folders:
            if folder in visited:
                continue
            if (graph / folder).is_file():
                raise LogseqListingError(
                    f"The selected folder {folder!r} is a file, not a folder; nothing was "
                    "synced and nothing was forgotten."
                )
            if (graph / folder).exists():
                raise LogseqListingError(
                    f"The selected folder {folder!r} exists on disk but the graph has no folder "
                    "with exactly that name (another letter case, another Unicode form or a "
                    "symbolic link); nothing was synced and nothing was forgotten."
                )
        pages = [page for page in pages if any(page.startswith(f"{folder}/") for folder in folders)]
    return sorted(pages)


def _iter_rows(
    graph: Path,
    folders: list[str] | None,
    state: dict,
    stats: dict[str, int],
    check_active: Callable[[], None] | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield a row for every readable page, then a tombstone for every member that is gone.

    The listing is complete before the first row, so an error there yields nothing and leaves
    ``state`` alone. A previous member the listing no longer contains was deleted, moved, or fell
    outside a narrowed ``folders``; all three are forgotten the same way. A page that cannot be read
    keeps the row it already has: it stays a member and is counted in ``failed_read``. A graph root
    with no entries at all is refused when pages were synced before. ``state`` is written after the
    last row.
    """
    if not graph.is_dir():
        raise LogseqGraphUnavailableError(
            "The Logseq graph folder is not available; nothing was synced and nothing was "
            "forgotten. Check that the drive is mounted or point graph_path at the new location."
        )
    listed = _list_pages(graph, folders)
    previous = set(state.get("members", []))
    # Hidden entries count: a real graph has .logseq, so only a root with nothing in it at all is
    # refused. A drive or bind mount that did not come up is an existing empty directory on Linux,
    # and would otherwise look like a graph whose pages were all deleted.
    if previous and not os.listdir(graph):
        raise LogseqGraphUnavailableError(
            "The Logseq graph folder is empty, but earlier runs synced pages from it; nothing was "
            "synced and nothing was forgotten. A drive that did not mount looks exactly like this. "
            "Check the mount, or leave one file or folder in the graph to forget everything on "
            "purpose."
        )
    for folder in folders or []:
        # A folder that is gone with members under it was removed on purpose and forgets them.
        # One with no members and not on disk is most likely a typo and selects nothing.
        if not (graph / folder).exists() and not any(
            page_id.startswith(_page_id(f"{folder}/")) for page_id in previous
        ):
            logger.warning(
                "The selected Logseq folder %r does not exist in the graph and has no synced "
                "pages, so it selects nothing; check the spelling, or remove it from folders if "
                "it was deleted on purpose.",
                folder,
            )
    unreadable = set()
    for rel_path in listed:
        if check_active is not None:
            check_active()  # outside any try: a revoked connection is not a failed page
        try:
            text = (graph / rel_path).read_bytes().decode("utf-8")
        except (OSError, UnicodeDecodeError) as error:
            logger.warning(
                "Logseq page %r could not be read (%s); it keeps what memory already holds.",
                rel_path,
                type(error).__name__,
            )
            unreadable.add(_page_id(rel_path))
            stats["failed"] += 1
            stats["failed_read"] += 1
            continue
        yield _page_row(rel_path, text)
        # After the yield: a row that check_active stopped before it was published is not counted.
        stats["synced"] += 1

    present = {_page_id(rel_path) for rel_path in listed}
    for page_id in sorted(previous - present):
        yield {"id": page_id, "_deleted": True}
        stats["deleted"] += 1
    state.update(v=1, members=sorted((present - unreadable) | (previous & unreadable)))
    logger.info(
        "Logseq: %d page(s) synced, %d forgotten, %d unreadable.",
        stats["synced"],
        stats["deleted"],
        stats["failed_read"],
    )


def _folder_list(folders: list[str] | tuple[str, ...] | None) -> list[str] | None:
    """The selected folders, sorted and without repeats; ``None`` is the whole graph.

    Anything but a non-empty list or tuple of strings is refused. A bare string would be read as
    one folder per letter, and an empty list is neither "everything" nor "nothing" as far as a
    caller can tell. Folders are graph-relative with ``/``. An entry that starts with a dot (hidden
    folders are never read), contains a backslash (one name on POSIX) or has surrounding spaces
    would select nothing, and the next run would forget every page.
    """
    if folders is None:
        return None
    if not isinstance(folders, (list, tuple)) or not folders:
        raise ValueError('folders must be a non-empty list or tuple, like ["pages/Projects"].')
    selected = set()
    for entry in folders:
        path = PurePosixPath(entry) if isinstance(entry, str) else None
        if (
            path is None
            or path.is_absolute()
            or "\\" in entry
            or entry != entry.strip()
            or not path.parts
            or any(part.startswith(".") for part in path.parts)
        ):
            raise ValueError(
                f"folders entry {entry!r} must be a path inside the graph written with /, "
                "without surrounding spaces, whose folder names do not start with a dot."
            )
        selected.add(path.as_posix())
    return sorted(selected)


def logseq_source(
    *,
    graph_path: str | os.PathLike[str],
    resource_name: str | None = None,
    folders: list[str] | tuple[str, ...] | None = None,
    check_active: Callable[[], None] | None = None,
):
    """Return a ``dlt`` resource that yields one document per page of a local Logseq graph.

    Hand the result to ``cognee.remember(...)`` with ``write_disposition="merge"`` and
    ``primary_key="id"``. A run walks the whole selection; a page whose text did not change costs
    no extraction. A normal return can still have skipped pages: read
    ``cognee_sync_stats["failed"]`` (``failed_read``) afterwards.

    Args:
        graph_path: The graph folder, ``~`` expanded and resolved. A ``ValueError`` if it is not an
            existing directory. A run raises if it has disappeared by then, or if its root has no
            entries at all although pages were synced from it before (a drive that did not mount).
        resource_name: Names the staging table and the sync state. The default is derived from the
            resolved graph path, so moving the folder changes it and ingests every page again; pass
            a name to pin it. Two graphs in one dataset need different names.
        folders: Graph-relative folders to read, including their subfolders. ``None`` is the whole
            graph. Pages outside a narrowed selection are forgotten on the next run.
        check_active: Host authorization checkpoint, called before each page is read and before
            each row is published.
    """
    import dlt

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 3:
        raise RuntimeError(
            "Logseq sync requires a Cognee build that reads per-row node sets and "
            "structure (DOCUMENT_SYNC_VERSION >= 3). Upgrade Cognee before syncing."
        )
    graph = Path(graph_path).expanduser().resolve()
    if not graph.is_dir():
        raise ValueError("graph_path must be an existing directory.")
    if resource_name is None:
        resource_name = f"logseq_{hashlib.sha256(str(graph).encode()).hexdigest()[:12]}"
    elif not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError("resource_name must be a non-empty string.")
    selection = _folder_list(folders)
    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def logseq_pages():
        stats.update(synced=0, deleted=0, failed=0, failed_read=0)
        rows = _iter_rows(graph, selection, dlt.current.resource_state(), stats, check_active)
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = logseq_pages()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "logseq")
    setattr(resource, PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource
