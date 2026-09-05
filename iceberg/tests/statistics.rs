#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::Statistics;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::error::Result;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion_distributed_iceberg::IcebergDataSource;
    use datafusion_distributed_iceberg::test_utils::{FIXTURE_URI, IcebergTestHarness};
    use iceberg::spec::{Operation, Snapshot, Summary};

    // Values from testdata/iceberg/taxi/metadata/v1.metadata.json snapshot summary.
    const TAXI_ROWS: usize = 175_000;
    const TAXI_BYTES: usize = 4_480_382;
    const TAXI_COLUMNS: usize = 13;
    const TAXI_MANIFEST_LIST: &str = "s3://iceberg-test/warehouse/taxi/metadata/snap-3167948105555765929-0-019fdb82-eb66-7582-99a7-9f864b92a53f.avro";

    #[tokio::test]
    async fn reports_exact_row_count_and_byte_size_for_full_scan() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let stats = source_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.num_rows, Precision::Exact(TAXI_ROWS));
        assert_eq!(stats.total_byte_size, Precision::Exact(TAXI_BYTES));
        Ok(())
    }

    #[tokio::test]
    async fn missing_snapshot_summary_statistics_are_absent() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .edit_current_snapshot_summary(|summary| {
                for key in ["total-records", "total-files-size"] {
                    assert!(summary.additional_properties.remove(key).is_some());
                }
            })
            .build()
            .await?;
        let stats = source_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.num_rows, Precision::Absent);
        assert_eq!(stats.total_byte_size, Precision::Absent);
        Ok(())
    }

    #[tokio::test]
    async fn reports_statistics_for_the_selected_snapshot() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .add_snapshot(snapshot_with_statistics(42, 42, 4_242))
            .build()
            .await?;
        harness
            .query(&format!(
                "CREATE EXTERNAL TABLE historical_taxi STORED AS ICEBERG \
                 LOCATION '{FIXTURE_URI}/metadata/v1.metadata.json' \
                 OPTIONS ('iceberg.snapshot_id' '42')"
            ))
            .await?;

        let stats = source_statistics(&harness, "SELECT * FROM historical_taxi").await?;

        // TODO(#687): Make statistics use the selected snapshot.
        assert_eq!(stats.num_rows, Precision::Exact(42));
        assert_eq!(stats.total_byte_size, Precision::Exact(4_242));
        Ok(())
    }

    #[tokio::test]
    async fn column_statistics_match_full_schema() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let stats = source_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.column_statistics.len(), TAXI_COLUMNS);
        Ok(())
    }

    #[tokio::test]
    async fn column_statistics_match_projected_schema() -> Result<()> {
        // Regression: a column_statistics vec shorter than the output schema
        // makes DataFusion panic while propagating statistics upstream.
        let harness = IcebergTestHarness::new().await?;
        let stats = source_statistics(&harness, "SELECT vendor_id, pickup_date FROM taxi").await?;

        assert_eq!(stats.column_statistics.len(), 2);
        assert_eq!(stats.num_rows, Precision::Exact(TAXI_ROWS));
        Ok(())
    }

    #[tokio::test]
    async fn statistics_propagate_through_filter() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness
            .physical_plan("SELECT vendor_id FROM taxi WHERE pickup_date = DATE '2024-01-10'")
            .await?;
        let stats = plan.partition_statistics(None)?;

        // The filter cannot keep the count exact, but it must not lose it.
        assert!(matches!(stats.num_rows, Precision::Inexact(_)));
        assert_eq!(stats.column_statistics.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn statistics_propagate_through_projection_and_sort() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness
            .physical_plan("SELECT vendor_id, trip_distance FROM taxi ORDER BY pickup_at")
            .await?;
        let stats = plan.partition_statistics(None)?;

        assert_eq!(stats.num_rows, Precision::Exact(TAXI_ROWS));
        assert_eq!(stats.column_statistics.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn explain_shows_statistics_on_the_iceberg_source() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness.physical_plan("SELECT vendor_id FROM taxi").await?;
        let display = displayable(plan.as_ref())
            .set_show_statistics(true)
            .indent(true)
            .to_string();

        insta::assert_snapshot!(display, @"
        CooperativeExec, statistics=[Rows=Exact(175000), Bytes=Exact(4480382), [(Col[0]:)]]
          DataSourceExec: format=iceberg, projection=[vendor_id], statistics=[Rows=Exact(175000), Bytes=Exact(4480382), [(Col[0]:)]]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn exact_row_count_lets_count_star_skip_the_scan() -> Result<()> {
        // With Precision::Exact(num_rows) the AggregateStatistics optimizer
        // rule answers COUNT(*) from metadata without reading any data file.
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness.query("SELECT count(*) FROM taxi").await?;

        insta::assert_snapshot!(plan, @"
        ProjectionExec: expr=[175000 as count(*)]
          PlaceholderRowExec
        ");
        insta::assert_snapshot!(batches, @"
        +----------+
        | count(*) |
        +----------+
        | 175000   |
        +----------+
        ");
        Ok(())
    }

    /// Finds the single Iceberg `DataSourceExec` in the plan and returns the
    /// statistics reported by the `IcebergDataSource` itself.
    async fn source_statistics(harness: &IcebergTestHarness, sql: &str) -> Result<Statistics> {
        let plan = harness.physical_plan(sql).await?;
        let exec = find_iceberg_exec(&plan).expect("plan contains an Iceberg DataSourceExec");
        Ok(Arc::unwrap_or_clone(exec.partition_statistics(None)?))
    }

    fn find_iceberg_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<DataSourceExec>> {
        if let Some(exec) = plan.downcast_ref::<DataSourceExec>() {
            if exec
                .data_source()
                .downcast_ref::<IcebergDataSource>()
                .is_some()
            {
                return Some(Arc::new(exec.clone()));
            }
        }
        plan.children().into_iter().find_map(find_iceberg_exec)
    }

    fn snapshot_with_statistics(id: i64, rows: usize, bytes: usize) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_sequence_number(0)
            .with_timestamp_ms(1_786_094_218_148)
            .with_manifest_list(TAXI_MANIFEST_LIST)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: [
                    ("total-records".to_string(), rows.to_string()),
                    ("total-files-size".to_string(), bytes.to_string()),
                ]
                .into_iter()
                .collect(),
            })
            .with_schema_id(0)
            .build()
    }
}
