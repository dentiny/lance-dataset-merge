# lance-dataset-merge

A Rust library for metadata-only concatenation of local Lance datasets. Uses
Lance **14.0.1** with standard Cargo version requirements and a checked-in `Cargo.lock`.

```rust,no_run
use lance::Dataset;
use lance_dataset_merge::merge;

# async fn example() -> lance_core::Result<()> {
let a = Dataset::open("/data/a.lance").await?;
let b = Dataset::open("/data/b.lance").await?;
let output = merge(&[a, b], "/data/combined.lance").await?;
# Ok(())
# }
```

`merge` creates a new dataset manifest referring to the original data files.
It does not copy or rewrite data files or invoke compaction. It retains duplicate
rows, NULLs, and source/fragment order. Use an ordered scanner when order matters.
An empty source list is an error; empty datasets with a schema are valid inputs,
including when every input is empty. The destination must not already exist.

Sources are open `Dataset` handles, so each input uses the version already loaded
in that handle. Schemas must match, including internal field IDs and metadata.
The initial implementation supports homogeneous V2 data files and local filesystem
paths (including local external bases from shallow clones). Deletions, stable row
IDs, row lineage, overlays, dictionary/blob columns, and MemWAL are rejected.
Secondary indices, table config, and table metadata are not inherited. No CLI.

The output depends on the source files remaining at their original locations.
Do not delete or clean up those files while the output is in use. A source version
handle does not pin files against source-side cleanup.

Run `cargo test --locked` to exercise real datasets in temporary local directories.
Tests also compare source file bytes before/after merging and reopen the result
with the standard Lance reader. Tests are run manually; no CI is configured.

The integration suite covers single/repeated sources, multiple fragments, NULLs,
duplicates, empty sources in either position, all-empty sources, snapshot isolation
after an append, shallow clones with colliding base IDs, and concurrent destination
creation. It also verifies that schema mismatches, deletions, stable row IDs, and
existing destinations fail without publishing an output or altering an existing
dataset. Existing directories (even empty ones) are rejected as destinations.

The two implementation stages are intentionally stacked: merge the core library
PR first, then the boundary-test/documentation PR. No source indices are reused;
queries against the merged dataset use ordinary scans until callers build new
indices themselves.
