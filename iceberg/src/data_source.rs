use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::stats::Precision;
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSource;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_expr::{Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayFormatType, SortOrderPushdownResult};
use datafusion::prelude::Expr;
use datafusion_distributed::WorkUnitFeed;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::{FileIO, StorageConfig};
use tokio_stream::wrappers::ReceiverStream;

use crate::common::{convert_filters_to_predicate, df_err, iceberg_err};
use crate::{IcebergConfig, IcebergWorkUnitFeed};

/// Consumes a stream of [iceberg::scan::FileScanTask]s per partition and reads the underlying
/// files into an Arrow stream.
///
/// [iceberg::scan::FileScanTask] are discovered progressively during execution by the
/// [IcebergWorkUnitFeed], and this [DataSource] executes those tasks as they come, also in
/// a streaming fashion. This works seamlessly in both single-node and distributed execution:
///
/// ## Single Node
///
/// [iceberg::scan::FileScanTask] are streamed in-memory, with as many parallel streams as
/// partitions this [IcebergDataSource] exposes:
///
/// ```text
/// ┌────────────────────────────────────────────┐
/// │             IcebergDataSource              │
/// │                                            │
/// │┌──────────────────────────────────────────┐│
/// ││           IcebergWorkUnitFeed            ││
/// ││┌────────────┐┌────────────┐┌────────────┐││
/// │││   Feed 0   ││   Feed 1   ││   Feed 2   │││
/// ││└──────┬─────┘└──────┬─────┘└──────┬─────┘││
/// │└───────┼─────────────┼─────────────┼──────┘│
/// │  .─────▼─────. .─────▼─────. .─────▼─────. │
/// │ (FileScanTask (FileScanTask (FileScanTask )│
/// │  .───────────. `─────┬─────' .───────────. │
/// │ (FileScanTask )      │      (FileScanTask )│
/// │  `─────┬─────'       │       .───────────. │
/// │        │             │      (FileScanTask )│
/// │        │             │       `─────┬─────' │
/// │        │             │             │       │
/// │ ┌──────▼─────┐┌──────▼─────┐┌──────▼─────┐ │
/// │ │Partition 0 ││Partition 1 ││Partition 2 │ │
/// │ │ArrowReader ││ArrowReader ││ArrowReader │ │
/// │ └──────┬─────┘└──────┬─────┘└──────┬─────┘ │
/// │        │             │             │       │
/// │  .─────▼─────.       │       .─────▼─────. │
/// │ ( RecordBatch ).─────▼─────.( RecordBatch )│
/// │  `─────┬─────'( RecordBatch ).───────────. │
/// │        │       `─────┬─────'( RecordBatch )│
/// │        │             │       `───────────' │
/// └────────┼─────────────┼─────────────┼───────┘
///          ▼             ▼             ▼
/// ```
///
/// ## Distributed
///
/// [iceberg::scan::FileScanTask] are streamed over the network, with as many parallel streams as
/// partitions * distributed tasks:
///
/// ```text
///  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///                                      Coordinating Context                                   │
///  │
///   ┌────────────────────────────────────────────────────────────────────────────────────────┐│
///  ││                                  IcebergWorkUnitFeed                                   │
///   │┌─────────────┐┌─────────────┐┌────────────┐┌────────────┐┌─────────────┐┌─────────────┐││
///  │││   Feed 0    ││   Feed 1    ││   Feed 2   ││   Feed 3   ││   Feed 4    ││   Feed 5    ││
///   │└──────┬──────┘└─────┬───────┘└────┬───────┘└───────┬────┘└───────┬─────┘└──────┬──────┘││
///  └└───────┼─────────────┼─────────────┼────────────────┼─────────────┼─────────────┼───────┴
///     .─────▼─────. .─────▼─────. .─────▼─────.    .─────▼─────. .─────▼─────. .─────▼─────.
///    (FileScanTask (FileScanTask (FileScanTask )  (FileScanTask (FileScanTask (FileScanTask )
///     .───────────. `─────┬─────' .───────────.    `─────┬─────' .───────────. `─────┬─────'
///    (FileScanTask )      │      (FileScanTask )         │      (FileScanTask )      │
///     `─────┬─────'       │       .───────────.          │       `───────────'       │
///           │             │      (FileScanTask )         │             │             │
///  Worker 0 │             │       `─────┬─────'          │             │             │ Worker 1
/// ┌ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ┐┌ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ┐
///   ┌───────┼─────────────┼─────────────┼───────┐┌───────┼─────────────┼─────────────┼───────┐
/// │ │       │     IcebergD│taSource     │       ││       │     IcebergD│taSource     │       │ │
///   │       │             │             │       ││       │             │             │       │
/// │ │┌──────▼─────┐┌──────▼─────┐┌──────▼─────┐ ││┌──────▼─────┐┌──────▼─────┐┌──────▼─────┐ │ │
///   ││Partition 0 ││Partition 1 ││Partition 2 │ │││Partition 0 ││Partition 1 ││Partition 2 │ │
/// │ ││ArrowReader ││ArrowReader ││ArrowReader │ │││ArrowReader ││ArrowReader ││ArrowReader │ │ │
///   │└──────┬─────┘└──────┬─────┘└──────┬─────┘ ││└──────┬─────┘└──────┬─────┘└──────┬─────┘ │
/// │ │       │             │             │       ││       │             │             │       │ │
///   │ .─────▼─────.       │       .─────▼─────. ││       │             ▼             ▼       │
/// │ │( RecordBatch ).─────▼─────.( RecordBatch )││ .─────▼─────. .───────────. .───────────. │ │
///   │ `─────┬─────'( RecordBatch ).───────────. ││( RecordBatch ( RecordBatch ) RecordBatch )│
/// │ │       │       `─────┬─────'( RecordBatch )││ `─────┬─────' `───────────' `─────┬─────' │ │
///   │       │             │       `───────────' ││       │      ( RecordBatch )      │       │
/// │ │       │             │             │       ││       │       `─────┬─────'       │       │ │
///   └───────┼─────────────┼─────────────┼───────┘└───────┼─────────────┼─────────────┼───────┘
/// │         ▼             ▼             ▼       ││       ▼             ▼             ▼         │
///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// ```
///
/// This distributed mechanism is transparent to this [DataSource].
#[derive(Debug, Clone)]
pub struct IcebergDataSource {
    schema: SchemaRef,
    partitioning: Partitioning,
    fetch: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    iceberg_file_io: FileIO,
    iceberg_runtime: iceberg::Runtime,
    feed: WorkUnitFeed<IcebergWorkUnitFeed>,
    statistics: Arc<Statistics>,
}

/// Optional fields for building an [IcebergDataSource].
#[derive(Default, Clone)]
pub(crate) struct IcebergDataSourceOptions<'a> {
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) fetch: Option<usize>,
    pub(crate) filters: &'a [Expr],
    pub(crate) iceberg_runtime: Option<iceberg::Runtime>,
}

impl IcebergDataSource {
    /// Creates a new [`IcebergDataSource`] object.
    pub(crate) fn new(
        table: iceberg::table::Table,
        schema: SchemaRef,
        partitioning: Partitioning,
        opts: IcebergDataSourceOptions,
    ) -> Self {
        let output_schema = match opts.projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let projection = opts.projection.map(|v| {
            v.iter()
                .map(|p| schema.field(*p).name().clone())
                .collect::<Vec<String>>()
        });

        let predicates = convert_filters_to_predicate(opts.filters);

        let statistics = Arc::new(snapshot_statistics(
            &table,
            opts.snapshot_id,
            &output_schema,
        ));
        let iceberg_runtime = opts
            .iceberg_runtime
            .unwrap_or_else(iceberg::Runtime::current);

        Self {
            schema: output_schema,
            iceberg_file_io: table.file_io().clone(),
            partitioning: partitioning.clone(),
            fetch: opts.fetch,
            metrics: ExecutionPlanMetricsSet::new(),
            iceberg_runtime: iceberg_runtime.clone(),
            feed: WorkUnitFeed::new(IcebergWorkUnitFeed {
                iceberg_table: table,
                snapshot_id: opts.snapshot_id,
                projection,
                predicates,
                partitioning,
                iceberg_runtime,
                sync_manager: Default::default(),
            }),
            statistics,
        }
    }

    pub(crate) fn from_remote(
        schema: SchemaRef,
        partitioning: Partitioning,
        fetch: Option<usize>,
        iceberg_file_io: FileIO,
        iceberg_runtime: iceberg::Runtime,
        feed: WorkUnitFeed<IcebergWorkUnitFeed>,
        statistics: Statistics,
    ) -> Self {
        Self {
            schema,
            partitioning,
            fetch,
            metrics: ExecutionPlanMetricsSet::new(),
            iceberg_file_io,
            iceberg_runtime,
            feed,
            statistics: Arc::new(statistics),
        }
    }

    pub(crate) fn schema_ref(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn storage_config(&self) -> &StorageConfig {
        self.iceberg_file_io.config()
    }

    pub(crate) fn statistics(&self) -> &Statistics {
        &self.statistics
    }
}

impl IcebergDataSource {
    /// Returns the [WorkUnitFeed] implementation that feeds this
    /// DataSource with [iceberg::scan::FileScanTask] messages.
    pub fn feed(&self) -> &WorkUnitFeed<IcebergWorkUnitFeed> {
        &self.feed
    }
}

pub(crate) fn snapshot_statistics(
    table: &iceberg::table::Table,
    snapshot_id: Option<i64>,
    schema: &SchemaRef,
) -> Statistics {
    let snapshot = match snapshot_id {
        Some(snapshot_id) => table.metadata().snapshot_by_id(snapshot_id),
        None => table.metadata().current_snapshot(),
    };
    let Some(snapshot) = snapshot else {
        return Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: Statistics::unknown_column(schema),
        };
    };

    let summary = &snapshot.summary().additional_properties;
    Statistics {
        num_rows: summary_precision(summary.get("total-records")),
        total_byte_size: summary_precision(summary.get("total-files-size")),
        column_statistics: Statistics::unknown_column(schema),
    }
}

fn summary_precision(value: Option<&String>) -> Precision<usize> {
    value
        .and_then(|value| value.parse().ok())
        .map(Precision::Inexact)
        .unwrap_or(Precision::Absent)
}

fn divide_precision(value: Precision<usize>, divisor: usize) -> Precision<usize> {
    match value {
        Precision::Exact(value) => Precision::Exact(value.div_ceil(divisor)),
        Precision::Inexact(value) => Precision::Inexact(value.div_ceil(divisor)),
        Precision::Absent => Precision::Absent,
    }
}

impl DataSource for IcebergDataSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let config = IcebergConfig::from_task_context(&context);

        let reader =
            ArrowReaderBuilder::new(self.iceberg_file_io.clone(), self.iceberg_runtime.clone())
                .with_batch_size(context.session_config().batch_size())
                .with_data_file_concurrency_limit(config.data_file_concurrency_limit)
                .with_row_group_filtering_enabled(config.row_group_filtering_enabled)
                .with_row_selection_enabled(config.row_selection_enabled)
                .build();

        let feed = self
            .feed
            .feed(partition, context)?
            .map(|msg_or_err| match msg_or_err {
                Ok(msg) => msg.into_task().map_err(iceberg_err),
                Err(err) => Err(iceberg_err(err)),
            })
            .boxed();

        let mut stream = reader
            .read(feed)
            .map(|result| result.stream())
            .map_err(df_err)?
            .map_err(df_err);

        // Poll FileIO and Parquet futures on Iceberg's IO runtime rather than DataFusion's query
        // CPU runtime, which may intentionally have Tokio IO disabled. A bounded channel preserves
        // backpressure while moving completed batches back to DataFusion.
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let error_tx = tx.clone();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let io_task = self.iceberg_runtime.io().spawn(async move {
            let produce = async move {
                while let Some(batch) = stream.next().await {
                    if tx.send(batch).await.is_err() {
                        break;
                    }
                }
            };

            tokio::select! {
                () = produce => {}
                _ = cancel_rx => {}
            }
        });
        let task = Arc::new(SpawnedTask::spawn(async move {
            let _cancel_io_on_drop = cancel_tx;
            if let Err(err) = io_task.await {
                let _ = error_tx.send(Err(df_err(err))).await;
            }
        }));
        let stream = ReceiverStream::new(rx)
            .inspect(move |_| {
                let _ = &task;
            })
            .boxed();

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )) as SendableRecordBatchStream;

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(LimitStream::new(stream, 0, self.fetch, metrics)))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "format=iceberg")?;
        let Some(feed) = self.feed.inner() else {
            return Ok(());
        };
        if let Some(projection) = &feed.projection {
            write!(f, ", projection=[{}]", projection.join(", "))?;
        }
        if let Some(predicate) = &feed.predicates {
            write!(f, ", predicate={predicate}")?;
        }
        if let Some(fetch) = self.fetch {
            write!(f, ", fetch={fetch}")?;
        }
        Ok(())
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<datafusion::physical_expr::LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        let partitioning = match &self.partitioning {
            Partitioning::UnknownPartitioning(_) => {
                Partitioning::UnknownPartitioning(target_partitions)
            }
            Partitioning::RoundRobinBatch(_) => Partitioning::RoundRobinBatch(target_partitions),
            Partitioning::Hash(_, _) => return Ok(None),
        };
        let mut source = self.clone();
        let Some(feed) = source.feed.inner_mut() else {
            return Ok(None);
        };
        feed.partitioning = partitioning.clone();
        feed.sync_manager = Default::default();
        source.partitioning = partitioning;
        Ok(Some(Arc::new(source)))
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(Arc::clone(&self.schema))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        if partition.is_none() {
            return Ok(Arc::clone(&self.statistics));
        }

        let partition_count = self.partitioning.partition_count();
        let mut statistics = self.statistics.as_ref().clone();
        statistics.num_rows = divide_precision(statistics.num_rows, partition_count);
        statistics.total_byte_size = divide_precision(statistics.total_byte_size, partition_count);
        Ok(Arc::new(statistics))
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let mut self_clone = self.clone();
        self_clone.fetch = fetch;
        Some(Arc::new(self_clone))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.metrics.clone()
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        // TODO: Allow this DataSource to be pushed down filters. Some filters might be more
        //  straight forward to accept, like simple predicates, but some others might require
        //  a bit more work, like dynamic filters.
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            vec![PushedDown::No; filters.len()],
        ))
    }

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn DataSource>>> {
        // TODO: Allow this DataSource to be pushed down sort expressions.
        Ok(SortOrderPushdownResult::Unsupported)
    }
}
