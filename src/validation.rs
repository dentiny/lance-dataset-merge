use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::index::DatasetIndexExt;
use lance_core::datatypes::SchemaCompareOptions;
use lance_core::{Error, Result};
use lance_table::feature_flags::ensure_can_read_manifest;
use lance_table::format::{BasePath, DataFile, Fragment};

pub(super) async fn validate_inputs<'a>(
    sources: &'a [Dataset],
    destination: &str,
) -> Result<&'a Dataset> {
    let first = sources
        .first()
        .ok_or_else(|| Error::invalid_input("At least one source dataset is required"))?;
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
    Ok(first)
}

pub(super) async fn validate_source(source: &Dataset, expected: &Dataset) -> Result<()> {
    ensure_can_read_manifest(&source.manifest)?;
    source.schema().check_compatible(
        expected.schema(),
        &SchemaCompareOptions {
            compare_metadata: true,
            compare_dictionary: true,
            compare_field_ids: true,
            ..Default::default()
        },
    )?;
    if source.version_id() != 1 || source.count_versions().await? != 1 {
        return Err(Error::invalid_input(format!(
            "Source {} must have exactly one version (initial version 1)",
            source.uri()
        )));
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
    Ok(())
}

pub(super) fn validate_fragment(fragment: &Fragment, fragment_count: usize) -> Result<()> {
    if fragment.deletion_file.is_some() {
        return Err(Error::invalid_input(
            "Sources with deletions are not supported",
        ));
    }
    if fragment.has_spilled_row_lineage() || !fragment.overlays.is_empty() {
        return Err(Error::invalid_input(
            "Spilled row lineage and overlays are not supported",
        ));
    }
    if fragment_count >= u32::MAX as usize {
        return Err(Error::invalid_input("Fragment ID space exhausted"));
    }
    Ok(())
}

pub(super) fn validate_file(
    file: &DataFile,
    expected_version: &mut Option<(u32, u32)>,
) -> Result<()> {
    if file.file_major_version < 2 {
        return Err(Error::invalid_input("Only V2 data files are supported"));
    }
    let version = (file.file_major_version, file.file_minor_version);
    if expected_version.is_some_and(|expected| expected != version) {
        return Err(Error::invalid_input(
            "Mixed data file versions are not supported",
        ));
    }
    *expected_version = Some(version);
    Ok(())
}

pub(super) fn validate_base(source: &Dataset, id: u32) -> Result<&BasePath> {
    source.manifest.base_paths.get(&id).ok_or_else(|| {
        Error::invalid_input(format!(
            "Source {} references missing base ID {id}",
            source.uri()
        ))
    })
}

pub(super) fn validate_base_id(base_count: usize) -> Result<u32> {
    u32::try_from(base_count).map_err(|_| Error::invalid_input("Base ID space exhausted"))
}
