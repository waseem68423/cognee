# Logseq SDK connector

A Logseq data-source connector for [cognee](https://github.com/topoteretes/cognee): sync a local
Logseq graph (a folder of Markdown pages) into memory, incrementally, with forget-on-delete.

It exposes a `dlt` source you hand to `cognee.remember(...)`, reusing cognee's existing DLT
ingestion path. Every page becomes one text document that goes through normal chunking and LLM
graph extraction.

It fixes what `cognee.remember(<graph folder>)` does with a graph today: an edited page leaves its
old version in memory, and a deleted page is never forgotten. Here a page's id is its path inside
the graph, so an edit replaces the old document and a page that left the graph is forgotten on the
next sync.

## Requirements

Bundled with the Cognee SDK, no extra install. There is no account, no token and no network
access: the connector reads files from a folder on the machine that runs it.

**SDK only.** The connector has no entry in the integration provider table
(`cognee/modules/integrations/ingestion.py`). That table serves hosts that hold credentials for a
provider's API, and nothing hosted can reach a graph on your disk.

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import logseq_source

source = logseq_source(graph_path="~/Documents/logseq-notes")
await cognee.remember(
    source,
    dataset_name="my_graph",
    primary_key="id",
    write_disposition="merge",  # REQUIRED, the add pipeline defaults to "replace"
    max_rows_per_table=0,
    self_improvement=False,
)

# Pages edited or added since the last run are re-read; deleted pages are forgotten.
await cognee.remember(source, dataset_name="my_graph", write_disposition="merge")

print(source.cognee_sync_stats)  # {"synced": 42, "deleted": 1, "failed": 0, "failed_read": 0}
```

## What is read

| Logseq feature | Becomes |
| --- | --- |
| every `*.md` file under the graph, `pages/` and `journals/` included | one document, `id = logseq:<path>` |
| `title::` page property | document title (else the file name) |
| `alias::` / `aliases::` (comma list or `[[wiki links]]`) | `logseq:alias:<name>` node set entries |
| `#tag`, `[[#tag]]`, `tags::` page property | `logseq:tag:<tag>` node set entries |
| `[[Page name]]` links | `logseq:page:<name>` node set entries |
| `((uuid))` block references | kept in the document text |
| folder of the page | `cognee_structure` ancestors, nearest parent first |
| the file itself | document `content`, page properties included, unmodified |

Block-level properties (`- owner:: Ada`) stay in the text; they are not lifted into metadata, so
nothing about a block is lost or reinterpreted.

## Deletion and its gaps

The connector walks the whole selection on every run and compares it to the members it recorded
last time:

- a page that is **gone, renamed, or outside a narrowed `folders` selection** is tombstoned
  (`_deleted: True`, hard delete) and forgotten;
- a page that **cannot be read** (permissions, invalid UTF-8) keeps what memory already holds and
  is counted in `failed_read`: an unreadable file never deletes anything;
- a graph folder that is **missing**, or **empty although pages were synced before** (a drive that
  did not mount), raises instead of forgetting everything;
- a folder that **cannot be listed** raises, because `os.walk` would otherwise skip it silently and
  its pages would look deleted.

Symlinks inside the graph are not followed, and hidden files and folders (`.logseq`, `.git`) are
never read.

Renaming a page is a delete plus an add, so it costs one extraction for the new page.

## Privacy

The connector reads only the files you point it at and never opens `.logseq/config.edn`, so graph
settings and any credentials kept there are not ingested. Text goes to the configured LLM and
embedding providers like any other cognee ingestion; the same data-handling rules apply.

## Choosing folders

```python
logseq_source(graph_path="~/notes", folders=["pages/Projects"])
```

Pages outside the selection are forgotten on the next run. Folders are graph-relative, written with
`/`, and their names must not start with a dot.
