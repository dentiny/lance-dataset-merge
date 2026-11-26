use std::collections::HashMap;

use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{
    CommitBuilder,
    transaction::{Operation, Transaction},
};
use lance::index::DatasetIndexExt;
use lance_core::datatypes::SchemaCompareOptions;
use lance_core::{Error, Result};
use lance_table::feature_flags::ensure_can_read_manifest;
use lance_table::format::BasePath;

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
    /// Inputs are already-open snapshots; this method never checks out newer versions.
    /// Rows, including duplicates, are retained in source/fragment order (use an
    /// ordered scanner if order matters). The destination must not already exist.
    /// Sources must have identical schemas, including field IDs and metadata, and
    /// homogeneous V2 data files. Deletions, stable row IDs, row lineage, overlays,
    /// dictionaries, blobs, and MemWAL are unsupported and rejected before writing.
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
        let first = sources
            .first()
            .ok_or_else(|| Error::invalid_input("At least one source dataset is required"))?;
        let destination = destination.as_ref();
        match DatasetBuilder::from_uri(destination)
            .with_session(first.session())
            .load()
            .await
        {
            Ok(_) => {
                return Err(Error::invalid_input(format!(
                    "Destination already exists: {destination}"
                )));
            }
            Err(Error::DatasetNotFound { .. } | Error::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        let comparison = SchemaCompareOptions {
            compare_metadata: true,
            compare_dictionary: true,
            compare_field_ids: true,
            ..Default::default()
        };
        let mut fragments = Vec::new();
        let mut bases = Vec::new();
        let mut base_ids = HashMap::new();
        let mut file_version = None;

        for source in sources {
            let manifest = &source.manifest;
            ensure_can_read_manifest(manifest)?;
            source
                .schema()
                .check_compatible(first.schema(), &comparison)?;
            if manifest.uses_stable_row_ids() {
                return Err(Error::invalid_input("Stable row IDs are not supported"));
            }
            if source.schema().has_dictionary_types()
                || source
                    .schema()
                    .fields_pre_order()
                    .any(|field| field.is_blob())
            {
                return Err(Error::invalid_input(
                    "Dictionary and blob columns are not supported",
                ));
            }
            if source.load_indices().await?.iter().any(|index| {
                index
                    .index_details
                    .as_ref()
                    .is_some_and(|details| details.type_url.to_ascii_lowercase().contains("memwal"))
            }) {
                return Err(Error::invalid_input("MemWAL datasets are not supported"));
            }
            // Resolve inherited bases individually: their IDs are local to each source.
            let root = source.uri().to_owned();
            for fragment in manifest.fragments.iter() {
                if fragment.deletion_file.is_some() {
                    return Err(Error::invalid_input(
                        "Sources with deletions are not supported",
                    ));
                }
                if fragment.row_id_meta.is_some()
                    || fragment.created_at_version_meta.is_some()
                    || fragment.last_updated_at_version_meta.is_some()
                    || !fragment.overlays.is_empty()
                {
                    return Err(Error::invalid_input(
                        "Row lineage and overlays are not supported",
                    ));
                }
                if fragments.len() >= u32::MAX as usize {
                    return Err(Error::invalid_input("Fragment ID space exhausted"));
                }
                let mut fragment = fragment.clone();
                // Let Lance allocate fresh, ordered fragment IDs at commit time.
                fragment.id = 0;
                for file in &mut fragment.files {
                    if file.file_major_version < 2 {
                        return Err(Error::invalid_input("Only V2 data files are supported"));
                    }
                    let version = (file.file_major_version, file.file_minor_version);
                    if file_version.is_some_and(|expected| expected != version) {
                        return Err(Error::invalid_input(
                            "Mixed data file versions are not supported",
                        ));
                    }
                    file_version = Some(version);
                    let (path, is_dataset_root) = match file.base_id {
                        None => (root.clone(), true),
                        Some(id) => {
                            let base = manifest.base_paths.get(&id).ok_or_else(|| {
                                Error::invalid_input(format!(
                                    "Source {} references missing base ID {id}",
                                    source.uri()
                                ))
                            })?;
                            (base.path.clone(), base.is_dataset_root)
                        }
                    };
                    let key = (path.clone(), is_dataset_root);
                    let id = match base_ids.get(&key) {
                        Some(id) => *id,
                        None => {
                            let id = u32::try_from(bases.len())
                                .map_err(|_| Error::invalid_input("Base ID space exhausted"))?;
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
            .use_stable_row_ids(false)
            .with_skip_auto_cleanup(true)
            .with_max_retries(0)
            .execute(Transaction::new(0, operation, None))
            .await
    }
}
