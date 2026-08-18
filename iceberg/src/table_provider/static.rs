use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, Statistics, plan_datafusion_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;

use crate::IcebergDataSource;
use crate::common::df_err;
use crate::data_source::{IcebergDataSourceOptions, snapshot_statistics};

/// Static, read-only provider for a table or a specific snapshot.
#[derive(Debug, Clone)]
pub struct IcebergStaticTableProvider {
    table: iceberg::table::Table,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
    statistics: Statistics,
    iceberg_runtime: iceberg::Runtime,
}

impl IcebergStaticTableProvider {
    /// Creates a provider that reads the provided table snapshot, or the current snapshot
    /// if none provided.
    pub fn try_new(
        table: iceberg::table::Table,
        snapshot_id: Option<i64>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Result<Self> {
        let table_schema = if let Some(snapshot_id) = snapshot_id {
            let snapshot = table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "snapshot id {snapshot_id} not found in table {}",
                        table.identifier().name()
                    )
                })?;
            snapshot.schema(table.metadata()).map_err(df_err)?
        } else {
            Arc::clone(table.metadata().current_schema())
        };

        let schema = Arc::new(schema_to_arrow_schema(&table_schema).map_err(df_err)?);
        let statistics = snapshot_statistics(&table, snapshot_id, &schema);

        Ok(Self {
            table,
            snapshot_id,
            schema,
            statistics,
            iceberg_runtime,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergStaticTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(self.statistics.clone())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(DataSourceExec::from_data_source(IcebergDataSource::new(
            self.table.clone(),
            self.schema.clone(),
            Partitioning::UnknownPartitioning(state.config().target_partitions()),
            IcebergDataSourceOptions {
                snapshot_id: self.snapshot_id,
                projection,
                filters,
                fetch: limit,
                iceberg_runtime: Some(self.iceberg_runtime.clone()),
            },
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::stats::Precision;

    use super::*;
    use crate::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn exposes_snapshot_statistics_through_table_provider() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let provider = harness.context().table_provider("taxi").await?;
        let statistics = provider
            .statistics()
            .expect("Iceberg tables expose snapshot statistics");

        assert_eq!(statistics.num_rows, Precision::Inexact(175_000));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(4_480_382));
        assert_eq!(
            statistics.column_statistics.len(),
            provider.schema().fields().len()
        );

        Ok(())
    }
}
