#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::error::Result;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{DistributedExt, WorkerQueryContext, display_plan_ascii};
    use datafusion_distributed_iceberg::test_utils::{FIXTURE_URI, FixtureStorageFactory};
    use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};
    use futures::TryStreamExt;

    #[tokio::test]
    async fn reads_iceberg_work_units_on_remote_workers() -> Result<()> {
        let (mut ctx, _guard, _) = start_localhost_context(2, build_worker_state).await;
        ctx.set_iceberg_integration(integration_options());
        ctx.set_distributed_file_scan_config_bytes_per_partition(100_000)?;
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
             LOCATION '{FIXTURE_URI}/metadata/v1.metadata.json'"
        ))
        .await?
        .collect()
        .await?;

        let dataframe = ctx
            .sql("SELECT pickup_date, COUNT(*) AS trips FROM taxi GROUP BY pickup_date")
            .await?;
        let plan = dataframe.create_physical_plan().await?;
        assert!(display_plan_ascii(plan.as_ref(), false).contains("Stage"));
        let batches = execute_stream(plan, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let results = pretty_format_batches(&batches)?.to_string();

        assert_eq!(results.matches("25000").count(), 7);
        Ok(())
    }

    async fn build_worker_state(ctx: WorkerQueryContext) -> Result<SessionState> {
        Ok(ctx
            .builder
            .with_iceberg_integration(integration_options())
            .build())
    }

    fn integration_options() -> IcebergIntegrationOptions {
        IcebergIntegrationOptions {
            storage_factory: Arc::new(FixtureStorageFactory::default()),
            iceberg_runtime: datafusion_distributed_iceberg::iceberg::Runtime::current(),
        }
    }
}
