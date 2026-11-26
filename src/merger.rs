use std::collections::HashMap;

use lance::Dataset;
use lance::dataset::{
    CommitBuilder,
    transaction::{Operation, Transaction},
};
use lance_core::Result;
use lance_table::format::BasePath;

use crate::validation;

/// Merges borrowed Lance dataset snapshots without rewriting their data files.
///
/// The source handles remain borrowed for the lifetime of the merger. Constructing
/// a merger performs no I/O; validation and metadata writes happen in [`Self::merge`].
pub struct Merger<'a> {
    sources: &'a [Dataset],
}

impl<'a> Merger<'a> {
    /// Use the versions already loaded in `sources`, including empty datasets.
    /// An empty source list is rejected when [`Self::merge`] is called.
    pub fn new(sources: &'a [Dataset]) -> Self {
        Self { sources }
    }

    /// Concatenate snapshots into a new dataset without copying or rewriting data.
    ///
    /// Each source must have exactly one version: its initial version 1.
    /// Rows, including duplicates, are retained in source/fragment order (use an
    /// ordered scanner if order matters). The destination must not already exist.
    /// Sources must have identical schemas, including field IDs and metadata, and
    /// homogeneous V2 data files. Deletions, spilled row lineage, overlays,
    /// dictionaries, blobs, and MemWAL are unsupported and rejected before writing.
    /// Source row IDs and version lineage are not inherited. If any input uses
    /// stable row IDs, Lance allocates fresh stable IDs for every output row.
    /// Secondary indices, table config, and table metadata are not inherited.
    ///
    /// All locations are object storage URIs passed through to Lance unchanged.
    /// Sources are assumed to have no deletions. Do not move or clean up referenced
    /// objects while the destination is in use.
    ///
    /// ```no_run
    /// # async fn example() -> lance_core::Result<()> {
    /// let left = lance::Dataset::open("s3://bucket/left.lance").await?;
    /// let right = lance::Dataset::open("s3://bucket/right.lance").await?;
    /// let sources = [left, right];
    /// let merger = lance_dataset_merge::Merger::new(&sources);
    /// let merged = merger.merge("s3://bucket/merged.lance").await?;
    /// assert_eq!(merged.version_id(), 1);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn merge(&self, destination: impl AsRef<str>) -> Result<Dataset> {
        let sources = self.sources;
        let destination = destination.as_ref();
        let first = validation::validate_inputs(sources, destination).await?;
        let mut fragments = Vec::new();
        let mut bases = Vec::new();
        let mut base_ids = HashMap::new();
        let mut file_version = None;

        for source in sources {
            let manifest = &source.manifest;
            validation::validate_source(source, first).await?;
            // Resolve inherited bases individually: their IDs are local to each source.
            let root = source.uri().to_owned();
            for fragment in manifest.fragments.iter() {
                validation::validate_fragment(fragment, fragments.len())?;
                let mut fragment = fragment.clone();
                // Let Lance allocate fresh, ordered fragment IDs at commit time.
                fragment.id = 0;
                // Output rows have a new identity; do not inherit source-local IDs.
                fragment.row_id_meta = None;
                fragment.created_at_version_meta = None;
                fragment.last_updated_at_version_meta = None;
                for file in &mut fragment.files {
                    validation::validate_file(file, &mut file_version)?;
                    let (path, is_dataset_root) = match file.base_id {
                        None => (root.clone(), true),
                        Some(id) => {
                            let base = validation::validate_base(source, id)?;
                            (base.path.clone(), base.is_dataset_root)
                        }
                    };
                    let key = (path.clone(), is_dataset_root);
                    let id = match base_ids.get(&key) {
                        Some(id) => *id,
                        None => {
                            let id = validation::validate_base_id(bases.len())?;
                            bases.push(BasePath::new(id, path, None, is_dataset_root));
                            base_ids.insert(key, id);
                            id
                        }
                    };
                    file.base_id = Some(id);
                }
                fragments.push(fragment);
            }
        }
        // Some(empty) is intentional: Lance refuses initial_bases on an existing
        // dataset, so a concurrent creator cannot turn this into an overwrite.
        let operation = Operation::Overwrite {
            fragments,
            schema: first.schema().clone(),
            config_upsert_values: None,
            initial_bases: Some(bases),
        };
        CommitBuilder::new(destination)
            .with_session(first.session())
            .use_stable_row_ids(
                sources
                    .iter()
                    .any(|source| source.manifest.uses_stable_row_ids()),
            )
            .with_skip_auto_cleanup(true)
            .with_max_retries(0)
            .execute(Transaction::new(0, operation, None))
            .await
    }
}
