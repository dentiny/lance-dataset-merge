use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_dataset_merge::Merger;
use tempfile::TempDir;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]))
}

async fn write(path: &Path, values: Vec<Option<i32>>) -> Dataset {
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(values))]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema());
    Dataset::write(
        reader,
        path.to_str().unwrap(),
        Some(WriteParams {
            max_rows_per_file: 2,
            max_rows_per_group: 2,
            enable_stable_row_ids: false,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
}

async fn values(dataset: &Dataset) -> Vec<Option<i32>> {
    let batch = dataset
        .scan()
        .scan_in_order(true)
        .try_into_batch()
        .await
        .unwrap();
    batch["value"]
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, current: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(current).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}

#[tokio::test]
async fn merges_multiple_fragments_without_writing_data_files() {
    let temp = TempDir::new().unwrap();
    let left_path = temp.path().join("left.lance");
    let right_path = temp.path().join("right.lance");
    let left = write(&left_path, vec![Some(1), None, Some(2)]).await;
    let right = write(&right_path, vec![Some(2), Some(3), Some(4)]).await;
    let expected_fragments = left.manifest.fragments.len() + right.manifest.fragments.len();
    assert!(left.manifest.fragments.len() > 1);
    // Both independently-created sources start allocating fragment IDs at zero.
    assert_eq!(
        left.manifest.fragments[0].id,
        right.manifest.fragments[0].id
    );
    let before = (files(&left_path), files(&right_path));
    let target = temp.path().join("merged.lance");
    let result = Merger::new(&[left, right])
        .merge(target.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        values(&result).await,
        vec![Some(1), None, Some(2), Some(2), Some(3), Some(4)]
    );
    assert_eq!(result.version_id(), 1);
    assert_eq!(result.manifest.fragments.len(), expected_fragments);
    assert_eq!(
        result
            .manifest
            .fragments
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>(),
        (0..expected_fragments as u64).collect::<Vec<_>>()
    );
    assert!(
        result
            .manifest
            .fragments
            .iter()
            .all(|f| f.files.iter().all(|file| file.base_id.is_some()))
    );
    assert!(!target.join("data").exists());
    assert!(!target.join("_indices").exists());
    assert_eq!(before, (files(&left_path), files(&right_path)));
    // Reopening proves base references are persisted, rather than held only in memory.
    let reopened = Dataset::open(target.to_str().unwrap()).await.unwrap();
    assert_eq!(values(&reopened).await, values(&result).await);
}

#[tokio::test]
async fn empty_sources_before_and_after_nonempty_source() {
    let temp = TempDir::new().unwrap();
    let empty = write(&temp.path().join("empty.lance"), vec![]).await;
    let data = write(&temp.path().join("data.lance"), vec![Some(7)]).await;
    assert!(empty.manifest.fragments.is_empty());
    for (name, sources) in [
        ("first", vec![empty.clone(), data.clone()]),
        ("last", vec![data.clone(), empty.clone()]),
    ] {
        let merged = Merger::new(&sources)
            .merge(temp.path().join(name).to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(values(&merged).await, vec![Some(7)]);
    }
}

#[tokio::test]
async fn all_empty_sources_produce_readable_empty_dataset() {
    let temp = TempDir::new().unwrap();
    let empty = write(&temp.path().join("empty.lance"), vec![]).await;
    let target = temp.path().join("merged.lance");
    Merger::new(&[empty.clone(), empty])
        .merge(target.to_str().unwrap())
        .await
        .unwrap();
    let reopened = Dataset::open(target.to_str().unwrap()).await.unwrap();
    assert!(reopened.manifest.fragments.is_empty());
    assert_eq!(reopened.count_rows(None).await.unwrap(), 0);
    assert_eq!(reopened.schema().fields.len(), 1);
    assert_eq!(reopened.schema().fields[0].name, "value");
    assert!(!target.join("data").exists());
}

#[tokio::test]
async fn no_sources_is_an_error_without_output() {
    let temp = TempDir::new().unwrap();
    let target = temp.path().join("merged.lance");
    let error = Merger::new(&[])
        .merge(target.to_str().unwrap())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
    assert!(error.to_string().contains("At least one source"));
    assert!(!target.exists());
}

#[tokio::test]
async fn single_and_repeated_source_keep_all_rows() {
    let temp = TempDir::new().unwrap();
    let source = write(&temp.path().join("source.lance"), vec![Some(5), None]).await;
    let single = Merger::new(std::slice::from_ref(&source))
        .merge(temp.path().join("single").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(values(&single).await, vec![Some(5), None]);
    let repeated = Merger::new(&[source.clone(), source])
        .merge(temp.path().join("repeated").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(values(&repeated).await, vec![Some(5), None, Some(5), None]);
    assert_eq!(repeated.manifest.base_paths.len(), 1);
}

#[tokio::test]
async fn existing_destination_is_unchanged() {
    let temp = TempDir::new().unwrap();
    let source = write(&temp.path().join("source"), vec![Some(1)]).await;
    let target = temp.path().join("target");
    write(&target, vec![Some(99)]).await;
    let before = files(&target);
    let error = Merger::new(std::slice::from_ref(&source))
        .merge(target.to_str().unwrap())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
    assert!(error.to_string().contains("Destination already exists"));
    assert_eq!(before, files(&target));
    // The source itself is also an existing destination, even through a path alias.
    let alias = temp.path().join("target/../source");
    assert!(
        Merger::new(&[source])
            .merge(alias.to_str().unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn schema_mismatch_including_an_empty_source_writes_nothing() {
    let temp = TempDir::new().unwrap();
    let source = write(&temp.path().join("source"), vec![Some(1)]).await;
    let other_schema = Arc::new(Schema::new(vec![Field::new(
        "different",
        DataType::Int32,
        true,
    )]));
    let empty_reader = RecordBatchIterator::new(
        Vec::<Result<RecordBatch, arrow_schema::ArrowError>>::new(),
        other_schema,
    );
    let other = Dataset::write(
        empty_reader,
        temp.path().join("other").to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    let target = temp.path().join("merged");
    let error = Merger::new(&[source, other])
        .merge(target.to_str().unwrap())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, lance_core::Error::SchemaMismatch { .. }));
    assert!(!target.exists());
}

#[tokio::test]
async fn deletions_are_rejected_before_writing() {
    let temp = TempDir::new().unwrap();
    let source = write(&temp.path().join("source"), vec![Some(1), Some(2)]).await;
    let mut deleted = write(&temp.path().join("deleted"), vec![Some(3), Some(4)]).await;
    deleted.delete("value = 3").await.unwrap();
    assert!(
        deleted
            .manifest
            .fragments
            .iter()
            .any(|fragment| fragment.deletion_file.is_some())
    );
    let target = temp.path().join("merged");
    let error = Merger::new(&[source, deleted])
        .merge(target.to_str().unwrap())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
    assert!(error.to_string().contains("deletions"));
    assert!(!target.exists());
}

#[tokio::test]
async fn stable_row_ids_are_rejected_even_for_empty_datasets() {
    let temp = TempDir::new().unwrap();
    let reader = RecordBatchIterator::new(
        Vec::<Result<RecordBatch, arrow_schema::ArrowError>>::new(),
        schema(),
    );
    let source = Dataset::write(
        reader,
        temp.path().join("source").to_str().unwrap(),
        Some(WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let target = temp.path().join("merged");
    let error = Merger::new(&[source])
        .merge(target.to_str().unwrap())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
    assert!(error.to_string().contains("Stable row IDs"));
    assert!(!target.exists());
}

#[tokio::test]
async fn uses_the_open_snapshot_after_source_append() {
    let temp = TempDir::new().unwrap();
    let source_path = temp.path().join("source");
    let snapshot = write(&source_path, vec![Some(1)]).await;
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(vec![2]))]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema());
    let latest = Dataset::write(
        reader,
        source_path.to_str().unwrap(),
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert!(latest.version_id() > snapshot.version_id());
    let before = files(&source_path);
    let result = Merger::new(&[snapshot])
        .merge(temp.path().join("merged").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(values(&result).await, vec![Some(1)]);
    assert_eq!(values(&latest).await, vec![Some(1), Some(2)]);
    assert_eq!(before, files(&source_path));
}

#[tokio::test]
async fn shallow_clones_remap_colliding_base_ids() {
    let temp = TempDir::new().unwrap();
    let mut left = write(&temp.path().join("left"), vec![Some(1), Some(2), Some(3)]).await;
    let mut right = write(&temp.path().join("right"), vec![Some(4)]).await;
    let clone_left = left
        .shallow_clone(
            temp.path().join("clone_left").to_str().unwrap(),
            left.version_id(),
            None,
        )
        .await
        .unwrap();
    let clone_right = right
        .shallow_clone(
            temp.path().join("clone_right").to_str().unwrap(),
            right.version_id(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        clone_left.manifest.fragments[0].files[0].base_id,
        clone_right.manifest.fragments[0].files[0].base_id
    );
    let target = temp.path().join("merged");
    Merger::new(&[clone_left, clone_right])
        .merge(target.to_str().unwrap())
        .await
        .unwrap();
    let reopened = Dataset::open(target.to_str().unwrap()).await.unwrap();
    assert_eq!(
        values(&reopened).await,
        vec![Some(1), Some(2), Some(3), Some(4)]
    );
    assert_eq!(reopened.manifest.base_paths.len(), 2);
    assert!(!target.join("data").exists());
}

#[tokio::test]
async fn concurrent_creators_never_overwrite_each_other() {
    let temp = TempDir::new().unwrap();
    let sources = [write(&temp.path().join("source"), vec![Some(1), Some(2)]).await];
    let target = temp.path().join("merged");
    let merger = Merger::new(&sources);
    let (a, b) = tokio::join!(
        merger.merge(target.to_str().unwrap()),
        merger.merge(target.to_str().unwrap())
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let reopened = Dataset::open(target.to_str().unwrap()).await.unwrap();
    assert_eq!(reopened.version_id(), 1);
    assert_eq!(values(&reopened).await, vec![Some(1), Some(2)]);
}

#[tokio::test]
async fn merges_object_store_uris_without_filesystem_resolution() {
    let session = Arc::new(lance::session::Session::default());
    let mut sources = Vec::new();
    for (uri, data) in [
        ("memory://merge/empty", vec![]),
        ("memory://merge/left", vec![Some(1), None]),
        ("memory://merge/right", vec![Some(2)]),
    ] {
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(data))]).unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema());
        let source = Dataset::write(
            reader,
            uri,
            Some(WriteParams {
                session: Some(session.clone()),
                enable_stable_row_ids: false,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        sources.push(source);
    }
    let merger = Merger::new(&sources);
    let target = "memory://merge/output";
    let merged = merger.merge(target).await.unwrap();
    assert_eq!(values(&merged).await, vec![Some(1), None, Some(2)]);
    assert!(
        merged
            .manifest
            .base_paths
            .values()
            .all(|base| sources.iter().any(|source| base.path == source.uri()))
    );
    let reopened = lance::dataset::builder::DatasetBuilder::from_uri(target)
        .with_session(session)
        .load()
        .await
        .unwrap();
    assert_eq!(values(&reopened).await, vec![Some(1), None, Some(2)]);
    let error = merger.merge(target).await.err().unwrap();
    assert!(error.to_string().contains("Destination already exists"));
    assert_eq!(reopened.version_id(), 1);
}
