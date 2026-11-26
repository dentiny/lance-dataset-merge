# lance-dataset-merge

A Rust library for metadata-only concatenation of Lance datasets in object storage. Uses
Lance **14.0.1** with standard Cargo version requirements and a checked-in `Cargo.lock`.

Assumptions:

- **Object storage:** source, destination, and external-base locations are URIs.
  They are passed unchanged to Lance; the library performs no local filesystem
  path resolution or existence checks. Lance checks destination existence through
  its object store. Local datasets are used only as test fixtures.
- **No deletions:** source datasets have no deletion files. Inputs that violate
  this assumption are rejected; merging deletion vectors is outside the scope.
- **One version per source:** every source must have only its initial version 1.
  Updated datasets and old snapshots of datasets with later versions are rejected.

```rust,no_run
use lance::Dataset;
use lance_dataset_merge::Merger;

# async fn example() -> lance_core::Result<()> {
let a = Dataset::open("s3://bucket/a.lance").await?;
let b = Dataset::open("s3://bucket/b.lance").await?;
let sources = [a, b];
let merger = Merger::new(&sources);
let output = merger.merge("s3://bucket/combined.lance").await?;
# Ok(())
# }
```

`Merger::merge` creates a new dataset manifest referring to the original data files.
It does not copy or rewrite data files or invoke compaction. It retains duplicate
rows, NULLs, and source/fragment order. Use an ordered scanner when order matters.
An empty source list is an error; empty datasets with a schema are valid inputs,
including when every input is empty. The destination must not already exist.

`Merger::new(&sources)` borrows open `Dataset` handles at version 1. Schemas must match, including internal field IDs and metadata.
Homogeneous V2 data files and inline stable row IDs are supported. If any source
uses stable row IDs, Lance allocates fresh, unique stable IDs for all output rows.
Source row IDs and per-row version lineage are not preserved. Spilled row lineage
in hidden data columns, overlays, dictionary/blob columns, and MemWAL are rejected.
Secondary indices, table config, and table metadata are not inherited. No CLI.
Enable the required Lance storage backend in the consuming application (for
example, the `aws` feature on its `lance` dependency for S3).

The output depends on the source files remaining at their original locations.
Do not delete or clean up those files while the output is in use. A source version
handle does not pin files against source-side cleanup.

Run `cargo test --locked` to exercise real datasets in temporary local directories.
Tests also compare source file bytes before/after merging and reopen the result
with the standard Lance reader. Tests are run manually; no CI is configured.
