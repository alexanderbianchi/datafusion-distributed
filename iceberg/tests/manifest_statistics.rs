#[cfg(test)]
mod tests {
    use std::error::Error;

    use datafusion::common::ColumnStatistics;
    use datafusion::common::stats::Precision;
    use datafusion::scalar::ScalarValue;
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg::test_utils::{
        FIXTURE_URI, IcebergTestHarness, taxi_metadata,
    };
    use iceberg::io::{MemoryStorage, Storage};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestListWriter,
        ManifestWriterBuilder, Struct,
    };

    #[tokio::test]
    async fn reports_column_metrics_from_manifests() -> Result<(), Box<dyn Error>> {
        let harness = harness_with_manifest_metrics().await?;
        let plan = harness
            .physical_plan("SELECT vendor_id, passenger_count FROM taxi")
            .await?;

        assert_eq!(
            plan.partition_statistics(None)?.column_statistics,
            expected_statistics()
        );
        Ok(())
    }

    #[tokio::test]
    async fn preserves_metrics_for_reordered_projection() -> Result<(), Box<dyn Error>> {
        let harness = harness_with_manifest_metrics().await?;
        let plan = harness
            .physical_plan("SELECT passenger_count, vendor_id FROM taxi")
            .await?;
        let mut expected = expected_statistics();
        expected.reverse();

        assert_eq!(plan.partition_statistics(None)?.column_statistics, expected);
        Ok(())
    }

    fn expected_statistics() -> Vec<ColumnStatistics> {
        vec![
            ColumnStatistics {
                null_count: Precision::Exact(5),
                min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(9))),
                byte_size: Precision::Inexact(400),
                ..ColumnStatistics::new_unknown()
            },
            ColumnStatistics {
                null_count: Precision::Exact(9),
                min_value: Precision::Inexact(ScalarValue::Int64(Some(10))),
                max_value: Precision::Inexact(ScalarValue::Int64(Some(40))),
                byte_size: Precision::Inexact(600),
                ..ColumnStatistics::new_unknown()
            },
        ]
    }

    // Planning-only fixture: the data-file paths need not exist because statistics come
    // from manifests. IDs 1 and 4 are vendor_id and passenger_count, not schema indexes.
    async fn harness_with_manifest_metrics() -> Result<IcebergTestHarness, Box<dyn Error>> {
        let metadata = taxi_metadata();
        let snapshot = metadata.current_snapshot().expect("taxi has a snapshot");
        let storage = MemoryStorage::new();
        let manifest_uri = format!("{FIXTURE_URI}/metadata/column-metrics.avro");
        let mut writer = ManifestWriterBuilder::new(
            storage.new_output(&manifest_uri)?,
            Some(snapshot.snapshot_id()),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();

        let mut file = DataFileBuilder::default();
        file.content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .record_count(87_500)
            .file_size_in_bytes(2_240_191)
            .partition(Struct::from_iter([Some(Literal::date_from_str(
                "2024-01-10",
            )?)]));
        writer.add_file(
            file.file_path(format!("{FIXTURE_URI}/data/metrics-first.parquet"))
                .null_value_counts([(1, 2), (4, 4)].into())
                .column_sizes([(1, 100), (4, 200)].into())
                .lower_bounds([(1, Datum::int(1)), (4, Datum::long(10))].into())
                .upper_bounds([(1, Datum::int(5)), (4, Datum::long(20))].into())
                .build()?,
            snapshot.sequence_number(),
        )?;
        writer.add_file(
            file.file_path(format!("{FIXTURE_URI}/data/metrics-second.parquet"))
                .null_value_counts([(1, 3), (4, 5)].into())
                .column_sizes([(1, 300), (4, 400)].into())
                .lower_bounds([(1, Datum::int(2)), (4, Datum::long(30))].into())
                .upper_bounds([(1, Datum::int(9)), (4, Datum::long(40))].into())
                .build()?,
            snapshot.sequence_number(),
        )?;
        let manifest = writer.write_manifest_file().await?;
        let mut list = ManifestListWriter::v2(
            storage
                .new_output(snapshot.manifest_list())?
                .writer()
                .await?,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        list.add_manifests([manifest].into_iter())?;
        list.close().await?;

        let mut harness = IcebergTestHarness::builder()
            .with_file(&manifest_uri, storage.read(&manifest_uri).await?.to_vec())
            .with_file(
                snapshot.manifest_list(),
                storage.read(snapshot.manifest_list()).await?.to_vec(),
            )
            .with_table_metadata(metadata)
            .build()
            .await?;
        harness.ctx.set_iceberg_column_stats_enabled(true);
        Ok(harness)
    }
}
