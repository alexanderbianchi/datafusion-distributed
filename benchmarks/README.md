# Distributed DataFusion Benchmarks

### Generating Benchmarking data

Generate datasets into `benchmarks/data/`.

```shell
# TPC-H (default: SCALE_FACTOR=1, PARTITIONS=16 - override by setting these environment variables)
./gen-tpch.sh

# TPC-DS (only SCALE_FACTOR=1 is supported)
./gen-tpcds.sh
```

### Running Benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with:

```shell
WORKERS=0 ./benchmarks/run.sh --threads 2 --dataset tpch_sf1
```

- `--threads`: This is the physical threads that the Tokio runtime will use for executing the
  binary. It's recommended to set `--threads` to something small, like `2`, for throttling each
  individual process running queries, and simulate how adding throttled workers can speed up the
  queries.
- `--dataset`: Dataset directory name under `benchmarks/data/` (e.g. `tpch_sf1`, `tpcds_sf1`).

### Running Benchmarks benchmarks in distributed mode

The same script is used for running distributed benchmarks:

```shell
WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset tpch_sf1 --file-scan-config-bytes-per-partition 16777216
```

- `WORKERS`: Env variable that sets the amount of localhost workers used in the query.
- `--threads`: Sets the Tokio runtime threads for each individual worker and for the benchmarking
  binary.
- `--dataset`: Dataset directory name under `benchmarks/data/`.
- `--file-scan-config-bytes-per-partition`: How many bytes each partition is expected to scan. Lower values
  produce more partitions/tasks. Defaults to the engine default when unset.

### Iceberg fixture benchmark

The committed taxi fixture requires no data generation. Run its scan, filter, and aggregate
queries with the `iceberg_taxi` dataset:

```shell
WORKERS=2 ./benchmarks/run.sh --threads 2 --dataset iceberg_taxi --file-scan-config-bytes-per-partition 100000
```

Use `--query scan`, `--query filter`, or `--query aggregate` to run one workload.

### Remote Iceberg benchmark

Use `iceberg_remote` with an Iceberg metadata JSON in object storage. The table must use the
taxi fixture schema because it runs the same workloads as `iceberg_taxi`. Credentials are resolved
by the Iceberg OpenDAL storage backend (for AWS, environment credentials or the EC2 instance role):

```shell
ICEBERG_METADATA_LOCATION=s3://bucket/warehouse/taxi/metadata/v1.metadata.json \
WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset iceberg_remote \
  --file-scan-config-bytes-per-partition 16777216
```

All workers need network access and read permission for both metadata and data-file locations.
This invocation also works in the EC2 benchmark environment described in `cdk/README.md`.
