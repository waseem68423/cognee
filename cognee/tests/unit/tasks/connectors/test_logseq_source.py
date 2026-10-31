"""The Logseq source reads a graph completely, and forgets a page only on a complete listing.

Every run walks the whole selection, one row per readable ``*.md`` page with its path inside the
graph as id. A previous member the listing no longer contains is tombstoned (deleted, renamed, or
outside a narrowed ``folders``). A graph that is gone, a folder that cannot be listed or a page
that cannot be read never forgets anything. The walker is driven with a plain dict for state; the
``dlt`` resource wrapper is covered by the integration suite, which installs dlt.
"""

import os
from pathlib import Path

import pytest

from cognee.tasks.ingestion.connectors import logseq as logseq_module
from cognee.tasks.ingestion.connectors.logseq import (
    LogseqGraphUnavailableError,
    LogseqListingError,
    _aliases,
    _ancestors,
    _block_refs,
    _folder_list,
    _iter_rows,
    _links,
    _node_set,
    _page_row,
    _tags,
    _title,
)
from cognee.tasks.ingestion.dlt_utils import NODE_SET_COLUMN, STRUCTURE_COLUMN

PLAIN_COLUMNS = {"id", "title", "content", "url", "_deleted"}
EMPTY_STATS = {"synced": 0, "deleted": 0, "failed": 0, "failed_read": 0}
PAGES = {
    "pages/Projects/Engine.md": (
        "title:: Ship the engine\n"
        "alias:: Motor, Power plant\n"
        "tags:: work, q4\n"
        "\n"
        "- The engine ships in Q4 #release\n"
        "  - see [[Roadmap]] and ((a1b2c3d4-0000-0000-0000-000000000000))\n"
        "  - owner:: Ada\n"
    ),
    "pages/Inbox.md": "Call Ada about the engine.\n",
    "journals/2026_10_31.md": "- Met [[Engine]] team #work\n",
}


def _graph(tmp_path: Path, pages: dict) -> Path:
    for rel_path, text in pages.items():
        path = tmp_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return tmp_path


def _rows(graph: Path, state: dict | None = None, folders=None, stats=None):
    stats = EMPTY_STATS.copy() if stats is None else stats
    return list(_iter_rows(graph, folders, state if state is not None else {}, stats)), stats


def test_page_row_has_plain_columns_and_a_stable_id():
    row = _page_row("pages/Inbox.md", "Call Ada.\n")
    assert set(row) >= PLAIN_COLUMNS
    assert row["id"] == "logseq:pages/Inbox.md"
    assert row["_deleted"] is False
    assert row["url"] == ""
    assert row["content"] == "Call Ada.\n"


def test_title_prefers_the_title_property_then_the_file_name():
    assert _title("title:: Ship the engine\n- body\n", "pages/Engine.md") == "Ship the engine"
    assert _title("- no properties\n", "pages/Projects/Engine.md") == "Engine"


def test_page_properties_stop_at_the_first_block():
    # "owner::" is a block property, indented under a bullet, not a page property.
    text = "title:: Ship the engine\n\n- The engine\n  - owner:: Ada\n"
    assert _title(text, "pages/Engine.md") == "Ship the engine"


def test_tags_cover_hash_tags_wiki_tags_and_ignore_urls():
    assert _tags("Ship it #release, ping [[#work]] and see https://a.b/#frag") == [
        "release",
        "work",
    ]


def test_links_and_block_refs_are_collected_without_duplicates():
    text = "[[Roadmap]] [[Roadmap]] [[#tag]] ((a1b2c3d4-0000-0000-0000-000000000000))"
    assert _links(text) == ["Roadmap"]
    assert _block_refs(text) == ["a1b2c3d4-0000-0000-0000-000000000000"]


def test_aliases_read_both_spellings_and_wiki_or_comma_form():
    assert _aliases("alias:: Motor, Power plant\n") == ["Motor", "Power plant"]
    assert _aliases("aliases:: [[Motor]]\n") == ["Motor"]


def test_node_set_carries_tags_links_and_aliases():
    node_set = _node_set(PAGES["pages/Projects/Engine.md"], "pages/Projects/Engine.md")
    assert "logseq:tag:release" in node_set
    assert "logseq:page:Roadmap" in node_set
    assert "logseq:alias:Motor" in node_set
    assert node_set == sorted(node_set)


def test_structure_lists_folders_nearest_parent_first():
    assert _ancestors("pages/Projects/Engine.md") == [
        {"kind": "folder", "id": "pages/Projects", "name": "Projects"},
        {"kind": "folder", "id": "pages", "name": "pages"},
    ]
    assert _ancestors("Inbox.md") == []


def test_a_full_run_syncs_every_page(tmp_path):
    graph = _graph(tmp_path, PAGES)
    rows, stats = _rows(graph)
    assert {row["id"] for row in rows} == {
        "logseq:pages/Projects/Engine.md",
        "logseq:pages/Inbox.md",
        "logseq:journals/2026_10_31.md",
    }
    assert stats == {"synced": 3, "deleted": 0, "failed": 0, "failed_read": 0}
    engine = next(row for row in rows if row["id"].endswith("Engine.md"))
    assert engine[NODE_SET_COLUMN]
    assert engine[STRUCTURE_COLUMN]["ancestors"][0]["id"] == "pages/Projects"


def test_a_deleted_page_is_tombstoned_and_a_read_page_is_not(tmp_path):
    graph = _graph(tmp_path, PAGES)
    state: dict = {}
    _rows(graph, state)
    os.remove(graph / "pages/Inbox.md")

    rows, stats = _rows(graph, state)
    assert {"id": "logseq:pages/Inbox.md", "_deleted": True} in rows
    assert stats["deleted"] == 1
    assert stats["synced"] == 2
    assert "logseq:pages/Inbox.md" not in state["members"]


def test_narrowing_folders_forgets_pages_outside_the_selection(tmp_path):
    graph = _graph(tmp_path, PAGES)
    state: dict = {}
    _rows(graph, state)

    rows, stats = _rows(graph, state, folders=["pages"])
    assert {row["id"] for row in rows if not row["_deleted"]} == {
        "logseq:pages/Projects/Engine.md",
        "logseq:pages/Inbox.md",
    }
    assert stats["deleted"] == 1  # the journal page fell outside the selection


def test_hidden_folders_are_never_read(tmp_path):
    graph = _graph(tmp_path, PAGES)
    (graph / ".logseq").mkdir()
    (graph / ".logseq" / "config.md").write_text("- private\n", encoding="utf-8")
    (graph / ".hidden.md").write_text("- private\n", encoding="utf-8")

    rows, _stats = _rows(graph)
    assert all(".logseq" not in row["id"] and ".hidden" not in row["id"] for row in rows)
    assert len(rows) == 3


def test_an_unreadable_page_keeps_its_membership_and_is_counted(tmp_path):
    graph = _graph(tmp_path, {"Inbox.md": "Call Ada.\n"})
    state: dict = {}
    _rows(graph, state)
    (graph / "Inbox.md").write_bytes(b"\xff\xfe not utf-8")

    rows, stats = _rows(graph, state)
    assert rows == []
    assert stats["failed_read"] == 1
    assert state["members"] == ["logseq:Inbox.md"]


def test_a_missing_graph_raises_and_forgets_nothing(tmp_path):
    state = {"members": ["logseq:Inbox.md"]}
    with pytest.raises(LogseqGraphUnavailableError):
        _rows(tmp_path / "gone", state)
    assert state["members"] == ["logseq:Inbox.md"]


def test_an_empty_graph_root_is_refused_when_pages_were_synced_before(tmp_path):
    state = {"members": ["logseq:Inbox.md"]}
    with pytest.raises(LogseqGraphUnavailableError):
        _rows(tmp_path, state)
    assert state["members"] == ["logseq:Inbox.md"]


def test_a_selected_folder_that_is_a_file_is_refused(tmp_path):
    graph = _graph(tmp_path, {"Inbox.md": "x\n"})
    with pytest.raises(LogseqListingError):
        _rows(graph, folders=["Inbox.md"], state={"members": ["logseq:Inbox.md"]})


def test_an_unlistable_folder_raises_instead_of_looking_deleted(tmp_path):
    graph = _graph(tmp_path, {"pages/Inbox.md": "x\n"})
    state: dict = {}
    _rows(graph, state)

    def refuse(path, names=None, **kwargs):
        raise PermissionError(13, "denied", str(path))

    original = logseq_module.os.walk
    logseq_module.os.walk = lambda root, **kwargs: _refusing_walk(original, refuse, root, **kwargs)
    try:
        with pytest.raises(LogseqListingError, match="nothing was synced"):
            _rows(graph, state)
    finally:
        logseq_module.os.walk = original
    assert state["members"] == ["logseq:pages/Inbox.md"]


def _refusing_walk(original, refuse, root, **kwargs):
    yield from original(root, onerror=refuse, **kwargs)


def test_check_active_is_called_before_every_page(tmp_path):
    graph = _graph(tmp_path, PAGES)
    calls = []
    rows = list(_iter_rows(graph, None, {}, EMPTY_STATS.copy(), lambda: calls.append(1)))
    assert len(rows) == 3
    assert len(calls) >= 3


@pytest.mark.parametrize(
    "folders", ["pages", [], [""], ["/abs"], ["a\\b"], [" pages"], [".hidden"]]
)
def test_folder_list_refuses_ambiguous_entries(folders):
    with pytest.raises(ValueError):
        _folder_list(folders)


def test_folder_list_sorts_and_deduplicates():
    assert _folder_list(["pages/b", "pages/a", "pages/a"]) == ["pages/a", "pages/b"]
    assert _folder_list(None) is None
