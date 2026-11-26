use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::WriteParams;
use lance_dataset_merge::merge;
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
    let result = merge(&[left, right], &target).await.unwrap();
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
        let merged = merge(&sources, temp.path().join(name)).await.unwrap();
        assert_eq!(values(&merged).await, vec![Some(7)]);
    }
}

#[tokio::test]
async fn all_empty_sources_produce_readable_empty_dataset() {
    let temp = TempDir::new().unwrap();
    let empty = write(&temp.path().join("empty.lance"), vec![]).await;
    let target = temp.path().join("merged.lance");
    merge(&[empty.clone(), empty], &target).await.unwrap();
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
    let error = merge(&[], &target).await.err().unwrap();
    assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
    assert!(error.to_string().contains("At least one source"));
    assert!(!target.exists());
}
