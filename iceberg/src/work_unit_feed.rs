use std::sync::{Arc, Mutex, OnceLock};

use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{
    Result, exec_datafusion_err, exec_err, internal_err, not_impl_datafusion_err,
};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion_distributed::{DistributedWorkUnitFeedContext, WorkUnitFeedProvider};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Literal, NameMapping, PartitionSpec, PrimitiveLiteral, Struct};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::common::df_err;

/// Work unit feed implementation that yields [FileScanTask] messages at execution time.
///
/// It lazily spawns a task that scans an Iceberg table, and places each resolved [FileScanTask]
/// in P * T output channels, where:
///  - P is the number of partition in each distributed task.
///  - T is the number of distributed tasks
///
/// ```text
///                    ┌───────────────────────────┐
///                    │    Lazily spawned task    │
///                    │ ┌───────────────────────┐ │
///                    │ │  Iceberg Table Scan   │ │
///                    │ └───.─────────────.─────┘ │
///                    │    ( FileScanTask  )      │
///                    │     .─────────────.       │
///                    │    ( FileScanTask  )      │
///                    │     `─────────────'       │
///                    │           ...             │
///                    │     .─────────────.       │
///                    │    ( FileScanTask  )      │
///                    │     `─────┬┬┬┬────'       │
///         ┌──────────┼───────────┘││└────────────┼───────────┐
///         │          └────────────┼┼─────────────┘           │
///         │                ┌──────┘└────────┐                │
///         │                │                │                │
///         ▼                ▼                ▼                ▼
/// ┌───────────────┐┌───────────────┐┌───────────────┐┌───────────────┐
/// │ Output mpsc 0 ││ Output mpsc 1 ││ Output mpsc 2 ││ Output mpsc 3 │
/// │.─────────────.││.─────────────.││.─────────────.││.─────────────.│
/// ( FileScanTask  )( FileScanTask  )( FileScanTask  )( FileScanTask  )
/// │`─────────────'││.─────────────.││`─────────────'││.─────────────.│
/// │               │( FileScanTask  )│               │( FileScanTask  )
/// │               ││`─────────────'││               ││.─────────────.│
/// │               ││               ││               │( FileScanTask  )
/// │               ││               ││               ││`─────────────'│
/// │               ││               ││               ││               │
/// └───────────────┘└───────────────┘└───────────────┘└───────────────┘
/// ```
///
/// Each individual output channel ends up being a [datafusion_distributed::WorkUnit] stream that
/// goes to one partition of one distributed task:
///
/// ```text
/// ┌───────────────┐┌───────────────┐┌───────────────┐┌───────────────┐
/// │ Output mpsc 0 ││ Output mpsc 1 ││ Output mpsc 2 ││ Output mpsc 3 │
/// └───────────────┘└───────────────┘└───────────────┘└───────────────┘
///         │                │                │                │
///         │                │                │                │
/// ┌───────┼────────────────┼───────┐┌───────┼────────────────┼───────┐
/// │       ▼     Task 0     ▼       ││       ▼     Task 1     ▼       │
/// │┌──────────────┐┌──────────────┐││┌──────────────┐┌──────────────┐│
/// ││ Partition 0  ││ Partition 1  ││││ Partition 2  ││ Partition 3  ││
/// │└──────────────┘└──────────────┘││└──────────────┘└──────────────┘│
/// └────────────────────────────────┘└────────────────────────────────┘
/// ```
///
/// This works seamlessly in single-node and distributed mode using
/// [datafusion_distributed::WorkUnitFeed] machinery:
/// - If the query was not distributed, the [FileScanTask]s will be streamed in-memory.
/// - If the query was distributed, the [FileScanTask]s will be streamed over the network from
///   coordinator to workers.
#[derive(Debug)]
pub struct IcebergWorkUnitFeed {
    /// A table in the catalog.
    pub(crate) iceberg_table: iceberg::table::Table,
    /// Snapshot of the table to scan.
    pub(crate) snapshot_id: Option<i64>,
    /// Projection column names, None means all columns.
    pub(crate) projection: Option<Vec<String>>,
    /// Filters to apply to the table scan.
    pub(crate) predicates: Option<Predicate>,
    /// Partitioning scheme to which the feeds should adhere.
    /// Unknown and round-robin partitioning can be satisfied at file granularity. Hash
    /// partitioning requires routing rows, not whole files, and is rejected.
    pub(crate) partitioning: Partitioning,
    /// Runtime whose IO handle polls manifest planning and task production.
    pub(crate) iceberg_runtime: iceberg::Runtime,
    /// Container for the lazily initialized task that scans the Iceberg table.
    /// It will start as soon as the first [IcebergWorkUnitFeed::feed] is called.
    pub(crate) sync_manager: OnceLock<Result<SyncManager, Arc<DataFusionError>>>,
}

impl Clone for IcebergWorkUnitFeed {
    fn clone(&self) -> Self {
        Self {
            iceberg_table: self.iceberg_table.clone(),
            snapshot_id: self.snapshot_id,
            projection: self.projection.clone(),
            predicates: self.predicates.clone(),
            partitioning: self.partitioning.clone(),
            iceberg_runtime: self.iceberg_runtime.clone(),
            sync_manager: Default::default(),
        }
    }
}

type TakeableVec<T> = Vec<Mutex<Option<T>>>;

#[derive(Debug)]
pub(crate) struct SyncManager {
    task: Arc<SpawnedTask<()>>,
    feeds: TakeableVec<UnboundedReceiver<Result<FileScanTaskMessage>>>,
}

/// Wire representation of one Iceberg file scan task.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileScanTaskMessage {
    #[prost(bytes = "vec", tag = "1")]
    payload: Vec<u8>,
}

impl FileScanTaskMessage {
    fn try_new(task: FileScanTask) -> Result<Self> {
        let wire = FileScanTaskWire::try_from(task)?;
        let payload = rmp_serde::to_vec_named(&wire).map_err(|error| {
            exec_datafusion_err!("failed to serialize Iceberg file scan task: {error}")
        })?;
        Ok(Self { payload })
    }

    pub(crate) fn into_task(self) -> Result<FileScanTask> {
        let wire = rmp_serde::from_slice::<FileScanTaskWire>(&self.payload).map_err(|error| {
            exec_datafusion_err!("failed to deserialize Iceberg file scan task: {error}")
        })?;
        Ok(wire.into())
    }
}

#[derive(Serialize, Deserialize)]
struct FileScanTaskWire {
    task: FileScanTask,
    partition: Option<Vec<Option<PrimitiveLiteralWire>>>,
    partition_spec: Option<PartitionSpec>,
    name_mapping: Option<NameMapping>,
}

impl TryFrom<FileScanTask> for FileScanTaskWire {
    type Error = DataFusionError;

    fn try_from(mut task: FileScanTask) -> Result<Self> {
        let partition = task
            .partition
            .take()
            .map(|partition| {
                partition
                    .into_iter()
                    .map(|literal| literal.map(PrimitiveLiteralWire::try_from).transpose())
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;
        let partition_spec = task.partition_spec.take().map(Arc::unwrap_or_clone);
        let name_mapping = task.name_mapping.take().map(Arc::unwrap_or_clone);
        Ok(Self {
            task,
            partition,
            partition_spec,
            name_mapping,
        })
    }
}

impl From<FileScanTaskWire> for FileScanTask {
    fn from(wire: FileScanTaskWire) -> Self {
        let mut task = wire.task;
        task.partition = wire.partition.map(|partition| {
            partition
                .into_iter()
                .map(|literal| literal.map(Into::into))
                .collect::<Struct>()
        });
        task.partition_spec = wire.partition_spec.map(Arc::new);
        task.name_mapping = wire.name_mapping.map(Arc::new);
        task
    }
}

#[derive(Serialize, Deserialize)]
enum PrimitiveLiteralWire {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Binary(Vec<u8>),
    Int128(i128),
    UInt128(u128),
    AboveMax,
    BelowMin,
}

impl TryFrom<Literal> for PrimitiveLiteralWire {
    type Error = DataFusionError;

    fn try_from(literal: Literal) -> Result<Self> {
        let Literal::Primitive(literal) = literal else {
            return exec_err!("Iceberg partition values must be primitive literals");
        };
        Ok(match literal {
            PrimitiveLiteral::Boolean(value) => Self::Boolean(value),
            PrimitiveLiteral::Int(value) => Self::Int(value),
            PrimitiveLiteral::Long(value) => Self::Long(value),
            PrimitiveLiteral::Float(value) => Self::Float(value.into_inner()),
            PrimitiveLiteral::Double(value) => Self::Double(value.into_inner()),
            PrimitiveLiteral::String(value) => Self::String(value),
            PrimitiveLiteral::Binary(value) => Self::Binary(value),
            PrimitiveLiteral::Int128(value) => Self::Int128(value),
            PrimitiveLiteral::UInt128(value) => Self::UInt128(value),
            PrimitiveLiteral::AboveMax => Self::AboveMax,
            PrimitiveLiteral::BelowMin => Self::BelowMin,
        })
    }
}

impl From<PrimitiveLiteralWire> for Literal {
    fn from(literal: PrimitiveLiteralWire) -> Self {
        Literal::Primitive(match literal {
            PrimitiveLiteralWire::Boolean(value) => PrimitiveLiteral::Boolean(value),
            PrimitiveLiteralWire::Int(value) => PrimitiveLiteral::Int(value),
            PrimitiveLiteralWire::Long(value) => PrimitiveLiteral::Long(value),
            PrimitiveLiteralWire::Float(value) => PrimitiveLiteral::Float(OrderedFloat(value)),
            PrimitiveLiteralWire::Double(value) => PrimitiveLiteral::Double(OrderedFloat(value)),
            PrimitiveLiteralWire::String(value) => PrimitiveLiteral::String(value),
            PrimitiveLiteralWire::Binary(value) => PrimitiveLiteral::Binary(value),
            PrimitiveLiteralWire::Int128(value) => PrimitiveLiteral::Int128(value),
            PrimitiveLiteralWire::UInt128(value) => PrimitiveLiteral::UInt128(value),
            PrimitiveLiteralWire::AboveMax => PrimitiveLiteral::AboveMax,
            PrimitiveLiteralWire::BelowMin => PrimitiveLiteral::BelowMin,
        })
    }
}

impl WorkUnitFeedProvider for IcebergWorkUnitFeed {
    type WorkUnit = FileScanTaskMessage;

    fn feed(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Self::WorkUnit>>> {
        let wuf_ctx = DistributedWorkUnitFeedContext::from_ctx(&ctx);

        // This lazily spawns the tokio task that scans the Iceberg table.
        // Only the first IcebergWorkUnitFeed::feed call will get to execute it, and the
        // rest will just observe the already initialized result.
        let sync_manager_or_err = self.sync_manager.get_or_init(|| {
            // Start the table scan only once for all the .feed() calls.
            let scan_builder = match self.snapshot_id {
                Some(snapshot_id) => self.iceberg_table.scan().snapshot_id(snapshot_id),
                None => self.iceberg_table.scan(),
            };

            let mut scan_builder = match &self.projection {
                Some(column_names) => scan_builder.select(column_names),
                None => scan_builder.select_all(),
            };
            if let Some(pred) = &self.predicates {
                scan_builder = scan_builder.with_filter(pred.clone());
            }
            let table_scan = scan_builder.build().map_err(df_err)?;

            match &self.partitioning {
                Partitioning::UnknownPartitioning(_) | Partitioning::RoundRobinBatch(_) => {}
                Partitioning::Hash(_, _) => {
                    return Err(Arc::new(not_impl_datafusion_err!(
                        "Iceberg work-unit feeds cannot satisfy hash partitioning at file granularity"
                    )));
                }
            }

            // Fanout the FileScanTask stream across P * T output channels where:
            // - P is the number of output partitions per distributed task (`partition_count`)
            // - T is the number of distributed tasks (`fan_out_tasks`)
            let out_partitions = wuf_ctx.fan_out_tasks * self.partitioning.partition_count();
            let mut rxs = Vec::with_capacity(out_partitions);
            let mut txs = Vec::with_capacity(out_partitions);
            for _ in 0..out_partitions {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                rxs.push(Mutex::new(Some(rx)));
                txs.push(tx);
            }

            // Poll the complete planning stream on Iceberg's IO runtime. df-executor's query CPU
            // runtimes intentionally have Tokio IO disabled, and plan_files performs direct IO
            // before Iceberg can dispatch its internal work.
            let iceberg_runtime = self.iceberg_runtime.clone();
            let error_tx = txs[0].clone();
            let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
            // A reference to this spawned task needs to be held, otherwise it will automatically
            // be canceled. The cancellation sender also stops the nested IO task when that happens.
            let task = SpawnedTask::spawn(async move {
                let io_task = iceberg_runtime.io().spawn(async move {
                    let produce = async move {
                        let mut stream = match table_scan.plan_files().await {
                            Ok(stream) => stream
                                .map_err(df_err)
                                .and_then(|task| async move {
                                    FileScanTaskMessage::try_new(task)
                                })
                                .boxed(),
                            Err(err) => {
                                let _ = txs[0].send(Err(df_err(err)));
                                return;
                            }
                        };

                        // Unknown and round-robin partitioning both permit round-robin file
                        // assignment.
                        let mut i = 0;
                        while let Some(scan_task_or_err) = stream.next().await {
                            let _ = txs[i % txs.len()].send(scan_task_or_err);
                            i += 1;
                        }
                    };

                    tokio::select! {
                        () = produce => {}
                        _ = cancel_rx => {}
                    }
                });
                let _cancel_io_on_drop = cancel_tx;
                if let Err(err) = io_task.await {
                    let _ = error_tx.send(Err(df_err(err)));
                }
            });

            Ok(SyncManager {
                task: Arc::new(task),
                feeds: rxs,
            })
        });

        let sync_manager = match sync_manager_or_err {
            Ok(sync_manager) => sync_manager,
            Err(err) => return Err(DataFusionError::Shared(Arc::clone(err))),
        };

        let Some(feed) = sync_manager.feeds.get(partition) else {
            return internal_err!("Invalid feed index {partition}");
        };

        let Some(feed) = feed.lock().unwrap().take() else {
            return exec_err!("Feed with index {partition} already taken");
        };

        let task_ref = Arc::clone(&sync_manager.task);

        Ok(UnboundedReceiverStream::new(feed)
            .inspect(move |_| {
                let _ = &task_ref; // Keep the task alive as long as one feed is alive.
            })
            .boxed())
    }
}
