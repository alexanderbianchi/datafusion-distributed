use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::{Result, internal_err};
use datafusion::datasource::source::DataSource;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::{self, proto_error};
use iceberg::io::{FileIOBuilder, StorageConfig, StorageFactory};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use prost::Message;

use crate::work_unit_feed::FileScanTaskMessage;
use crate::{IcebergDataSource, IcebergWorkUnitFeed};
use datafusion_distributed::{WorkUnitFeed, WorkUnitFeedProto};

/// Physical plan codec for [`IcebergDataSource`].
#[derive(Debug, Clone)]
pub struct IcebergCodec {
    storage_factory: Arc<dyn StorageFactory>,
    iceberg_runtime: iceberg::Runtime,
}

impl IcebergCodec {
    /// Creates a codec using the storage and runtime configured on this process.
    pub fn new(
        storage_factory: Arc<dyn StorageFactory>,
        iceberg_runtime: iceberg::Runtime,
    ) -> Self {
        Self {
            storage_factory,
            iceberg_runtime,
        }
    }
}

impl Default for IcebergCodec {
    fn default() -> Self {
        Self::new(
            Arc::new(OpenDalResolvingStorageFactory::new()),
            iceberg::Runtime::current(),
        )
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct IcebergDataSourceProto {
    #[prost(message, optional, tag = "1")]
    schema: Option<protobuf::Schema>,
    #[prost(message, optional, tag = "2")]
    feed: Option<WorkUnitFeedProto>,
    #[prost(uint64, tag = "3")]
    partitions: u64,
    #[prost(enumeration = "PartitioningKind", tag = "4")]
    partitioning: i32,
    #[prost(uint64, optional, tag = "5")]
    fetch: Option<u64>,
    #[prost(bytes = "vec", tag = "6")]
    storage_config: Vec<u8>,
    #[prost(uint64, optional, tag = "7")]
    num_rows: Option<u64>,
    #[prost(uint64, optional, tag = "8")]
    total_byte_size: Option<u64>,
    #[prost(bool, tag = "9")]
    num_rows_exact: bool,
    #[prost(bool, tag = "10")]
    total_byte_size_exact: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
enum PartitioningKind {
    Unknown = 0,
    RoundRobin = 1,
}

impl PhysicalExtensionCodec for IcebergCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "IcebergDataSource should have no children, got {}",
                inputs.len()
            );
        }

        let proto = IcebergDataSourceProto::decode(buf)
            .map_err(|error| proto_error(format!("failed to decode IcebergDataSource: {error}")))?;
        let schema = proto
            .schema
            .ok_or_else(|| proto_error("IcebergDataSource is missing its schema"))?;
        let schema = SchemaRef::new((&schema).try_into()?);
        let feed = proto
            .feed
            .ok_or_else(|| proto_error("IcebergDataSource is missing its work-unit feed"))?;
        let feed = WorkUnitFeed::<IcebergWorkUnitFeed>::from_proto(feed)?;
        let storage_config = rmp_serde::from_slice::<StorageConfig>(&proto.storage_config)
            .map_err(|error| {
                proto_error(format!(
                    "failed to decode Iceberg storage configuration: {error}"
                ))
            })?;
        let file_io = FileIOBuilder::new(Arc::clone(&self.storage_factory))
            .with_props(storage_config.props())
            .build();
        let partitions = usize::try_from(proto.partitions)
            .map_err(|_| proto_error("Iceberg partition count does not fit in usize"))?;
        let partitioning = match PartitioningKind::try_from(proto.partitioning).map_err(|_| {
            proto_error(format!(
                "unknown Iceberg partitioning kind {}",
                proto.partitioning
            ))
        })? {
            PartitioningKind::Unknown => Partitioning::UnknownPartitioning(partitions),
            PartitioningKind::RoundRobin => Partitioning::RoundRobinBatch(partitions),
        };
        let fetch = proto
            .fetch
            .map(usize::try_from)
            .transpose()
            .map_err(|_| proto_error("Iceberg fetch limit does not fit in usize"))?;
        let statistics = datafusion::common::Statistics {
            num_rows: decode_precision(proto.num_rows, proto.num_rows_exact),
            total_byte_size: decode_precision(proto.total_byte_size, proto.total_byte_size_exact),
            column_statistics: datafusion::common::Statistics::unknown_column(&schema),
        };

        Ok(DataSourceExec::from_data_source(
            IcebergDataSource::from_remote(
                schema,
                partitioning,
                fetch,
                file_io,
                self.iceberg_runtime.clone(),
                feed,
                statistics,
            ),
        ))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(exec) = node.downcast_ref::<DataSourceExec>() else {
            return internal_err!(
                "expected DataSourceExec wrapping IcebergDataSource, got {}",
                node.name()
            );
        };
        let Some(source) = exec.data_source().downcast_ref::<IcebergDataSource>() else {
            return internal_err!("expected DataSourceExec wrapping IcebergDataSource");
        };

        let (partitioning, partitions) = match source.output_partitioning() {
            Partitioning::UnknownPartitioning(partitions) => {
                (PartitioningKind::Unknown, partitions)
            }
            Partitioning::RoundRobinBatch(partitions) => (PartitioningKind::RoundRobin, partitions),
            Partitioning::Hash(_, _) => {
                return internal_err!(
                    "IcebergDataSource hash partitioning cannot be serialized safely"
                );
            }
        };
        let storage_config = rmp_serde::to_vec_named(source.storage_config()).map_err(|error| {
            proto_error(format!(
                "failed to encode Iceberg storage configuration: {error}"
            ))
        })?;
        let statistics = source.statistics();
        let (num_rows, num_rows_exact) = encode_precision(&statistics.num_rows);
        let (total_byte_size, total_byte_size_exact) =
            encode_precision(&statistics.total_byte_size);
        let proto = IcebergDataSourceProto {
            schema: Some(protobuf::Schema::try_from(source.schema_ref().as_ref())?),
            feed: Some(source.feed().to_proto()),
            partitions: partitions as u64,
            partitioning: partitioning as i32,
            fetch: source.fetch().map(|value| value as u64),
            storage_config,
            num_rows,
            total_byte_size,
            num_rows_exact,
            total_byte_size_exact,
        };
        proto
            .encode(buf)
            .map_err(|error| proto_error(format!("failed to encode IcebergDataSource: {error}")))
    }
}

fn encode_precision(value: &datafusion::common::stats::Precision<usize>) -> (Option<u64>, bool) {
    match value {
        datafusion::common::stats::Precision::Exact(value) => (Some(*value as u64), true),
        datafusion::common::stats::Precision::Inexact(value) => (Some(*value as u64), false),
        datafusion::common::stats::Precision::Absent => (None, false),
    }
}

fn decode_precision(
    value: Option<u64>,
    exact: bool,
) -> datafusion::common::stats::Precision<usize> {
    let Some(value) = value.and_then(|value| usize::try_from(value).ok()) else {
        return datafusion::common::stats::Precision::Absent;
    };
    if exact {
        datafusion::common::stats::Precision::Exact(value)
    } else {
        datafusion::common::stats::Precision::Inexact(value)
    }
}

// Compile-time assertion that Iceberg work units satisfy the distributed feed's wire contract.
const _: fn() = || {
    fn assert_message<T: Message + Default>() {}
    assert_message::<FileScanTaskMessage>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{FixtureStorageFactory, IcebergTestHarness};
    use futures::StreamExt;

    #[tokio::test]
    async fn roundtrips_data_source_plan_and_file_scan_task() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let dataframe = harness.context().sql("SELECT * FROM taxi").await?;
        let plan = dataframe.create_physical_plan().await?;
        let source_plan = iceberg_plan(&plan)?;
        let source = iceberg_source(&source_plan)?;
        assert_eq!(
            source.statistics().num_rows,
            datafusion::common::stats::Precision::Inexact(175_000)
        );
        assert_eq!(
            source.statistics().total_byte_size,
            datafusion::common::stats::Precision::Inexact(4_480_382)
        );

        let message = source
            .feed()
            .feed(0, harness.context().task_ctx())?
            .next()
            .await
            .ok_or_else(|| proto_error("Iceberg fixture produced no file scan task"))??;
        let task = message.clone().into_task()?;
        let task_roundtrip = FileScanTaskMessage::decode(message.encode_to_vec().as_slice())
            .map_err(|error| proto_error(format!("failed to decode test work unit: {error}")))?
            .into_task()?;
        assert_eq!(task, task_roundtrip);

        let codec = IcebergCodec::new(
            Arc::new(FixtureStorageFactory::default()),
            iceberg::Runtime::current(),
        );
        let mut bytes = Vec::new();
        codec.try_encode(Arc::clone(&source_plan), &mut bytes)?;
        let decoded = codec.try_decode(&bytes, &[], &harness.context().task_ctx())?;

        let decoded = iceberg_source(&decoded)?;
        assert_eq!(source.schema_ref(), decoded.schema_ref());
        assert_eq!(
            source.output_partitioning().to_string(),
            decoded.output_partitioning().to_string()
        );
        assert_eq!(source.fetch(), decoded.fetch());
        assert_eq!(source.statistics(), decoded.statistics());
        assert_eq!(source.feed().to_proto(), decoded.feed().to_proto());
        Ok(())
    }

    fn iceberg_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
            && exec
                .data_source()
                .downcast_ref::<IcebergDataSource>()
                .is_some()
        {
            return Ok(Arc::clone(plan));
        }
        for child in plan.children() {
            if let Ok(plan) = iceberg_plan(child) {
                return Ok(plan);
            }
        }
        internal_err!("fixture query contains no IcebergDataSource")
    }

    fn iceberg_source(plan: &Arc<dyn ExecutionPlan>) -> Result<&IcebergDataSource> {
        let Some(exec) = plan.downcast_ref::<DataSourceExec>() else {
            return internal_err!("expected a DataSourceExec");
        };
        exec.data_source()
            .downcast_ref::<IcebergDataSource>()
            .ok_or_else(|| proto_error("expected an IcebergDataSource"))
    }
}
