# Distributed Iceberg write support

> Draft issue / roadmap. Capability status was checked on 2026-09-11 and is
> expected to change as the linked upstream work lands.

## Summary

DataFusion Distributed (DFD) currently provides distributed reads for Iceberg
tables. We want to add a distributed write path that approaches the DML surface
provided by [Spark Iceberg] and the [Trino Iceberg connector], while continuing
to treat DFD as a library rather than prescribing a catalog, object store, or
deployment stack.

The intended architecture is:

1. DFD plans data and delete-file production as distributed stages.
2. Workers write complete immutable files and report their Iceberg file
   descriptors to the coordinator over a correctness-critical, AQE-like
   worker-to-coordinator channel.
3. DFD accepts results only from successful tasks/attempts and aggregates them
   in coordinator state keyed by write operation and task attempt.
4. The coordinator uses one `iceberg-rust` transaction action to produce
   manifests, a manifest list, a snapshot, and one atomic catalog update.

No worker mutates the catalog. A failed job must never publish a subset of the
successful workers' output.

[Spark Iceberg]: https://iceberg.apache.org/docs/latest/spark-writes/
[Trino Iceberg connector]: https://trino.io/docs/current/connector/iceberg.html

## Goals

- Support distributed writes to catalog-backed Iceberg tables.
- Match the important SQL and table semantics users expect from Spark and
  Trino, including append, overwrite, row-level DML, and maintenance rewrites.
- Use `iceberg-rust` for Iceberg writer, file metadata, manifest, snapshot,
  validation, retry, and catalog semantics rather than reimplementing the
  Iceberg specification in DFD.
- Keep data-file production distributed and the catalog commit coordinated and
  atomic.
- Make task retries, commit retries, cancellation, and cleanup explicit parts
  of correctness.
- Reject unsupported modes and properties during planning rather than silently
  producing output that differs from other Iceberg implementations.

## Non-goals

- Reimplementing Iceberg transaction actions in DFD.
- Requiring a particular catalog or object-store implementation.
- Making work unit feeds into a general worker-to-coordinator result channel.
- Committing one snapshot per worker or per output partition.
- Matching the exact physical file layout of Spark or Trino when the layouts
  are semantically equivalent. Intentional differences must nevertheless be
  documented and tested across implementations.

## Target feature surface

Spark and Trino are references for the desired user-facing behavior, not a
requirement to copy every engine-specific configuration name.

### SQL and operation support

| Feature | Spark Iceberg | Trino Iceberg | DFD target |
| --- | --- | --- | --- |
| `INSERT INTO ... VALUES` | Yes | Yes | Distributed append |
| `INSERT INTO ... SELECT` | Yes | Yes | Distributed append |
| `CREATE TABLE AS SELECT` | Yes | Yes | Create metadata, distributed write, atomic publish |
| `INSERT OVERWRITE` / overwrite by filter | Static and dynamic modes | Engine overwrite support | Full-table, filter, and dynamic-partition overwrite |
| `TRUNCATE` | Available through table operations | Yes | Metadata-only removal where valid |
| `DELETE` | Metadata-only partition delete or affected-file rewrite | Metadata-only identity-partition delete or v2 position deletes | Metadata-only, CoW, and MoR modes |
| `UPDATE` | Yes | Yes | CoW and MoR modes |
| `MERGE INTO` | Matched update/delete, not-matched insert, and not-matched-by-source clauses | Yes | Full DataFusion-supported clause surface, CoW and MoR |
| Rewrite/compact data files | Iceberg maintenance actions | `ALTER TABLE EXECUTE optimize` | Distributed candidate rewrite plus one `RewriteFiles` commit |
| Rewrite manifests | Iceberg maintenance actions | `optimize_manifests` | Coordinator metadata maintenance |
| Expire snapshots | Yes | `expire_snapshots` | Existing upstream action plus distributed-safe cleanup policy |
| Remove orphan files | Yes | `remove_orphan_files` | Deferred maintenance operation with retention safeguards |
| Branch/WAP writes | Branch identifiers and WAP branch configuration | Catalog-dependent | Eventual named-reference writes without weakening validation |

For `MERGE INTO`, the initial contract should include duplicate-match
validation: one source row may update a target row, otherwise the operation
fails. More recent Spark clauses such as `WHEN NOT MATCHED BY SOURCE` should be
tracked separately if DataFusion's logical DML API does not expose them yet.

### Write behavior and options

| Area | Reference behavior | DFD target |
| --- | --- | --- |
| File format | Spark/Trino expose Parquet, ORC, and Avro | Parquet first; fail closed for other formats until upstream writers exist |
| Compression and Parquet tuning | Codec, row-group, page-size, dictionary, and metrics properties | Honor supported Iceberg table properties through `iceberg-rust`; identify unsupported properties at planning time |
| Target file size | `write.target-file-size-bytes` / Trino `target_max_file_size` | Rolling writer using the table property, with metrics and tests for overshoot behavior |
| Partition transforms | Identity, bucket, truncate, year/month/day/hour and partition evolution | Evaluate the active write spec's transforms and retain the file's `partition_spec_id` |
| Write distribution | Spark `write.distribution-mode`: `none`, `hash`, `range` | Fanout correctness mode; hash-clustered default; range mode when DFD supports it |
| Fanout | Spark fanout avoids sorting but holds writers open | Explicit bounded-memory mode with limits/metrics; never an accidental fallback |
| Sort order | Spark range mode and Trino `sorted_by` | Honor Iceberg sort order within output files; reject unsupported transforms/types |
| Writer parallelism | Spark AQE / Trino writer scaling | DFD task-count policy independent of scan partition count |
| Schema evolution | Spark `mergeSchema` plus `write.spark.accept-any-schema` | Explicit opt-in and one atomic schema-plus-data transaction |
| Overwrite behavior | Spark static and dynamic overwrite | Explicit mode; never infer destructive full-table overwrite ambiguously |
| Row-level mode | Iceberg `write.delete.mode`, `write.update.mode`, `write.merge.mode` | Honor CoW/MoR when implemented; fail planning for unsupported modes |
| Object-store layout | Iceberg object-storage location provider | Honor upstream location generation and partition-path escaping |
| Commit retries | Table/catalog retry configuration | Delegate snapshot rebase and catalog retry to `iceberg-rust`; do not rerun worker writes for a catalog conflict |
| Task retries | Spark/Trino accept only successful task attempts | Stable operation/task identity, unique attempt paths, deduplication, and cleanup |
| Snapshot summary | Standard Iceberg fields plus useful operation counts | Correct added/removed file and row totals; include engine metrics where stable |
| Fault-tolerant execution | Trino supports reads and writes with retry policies | Define correctness before claiming retry support; add failure-injection tests |

The applicable upstream Iceberg properties are documented in the
[Iceberg configuration reference]. Engine-only properties need a DFD
configuration or planning equivalent rather than being copied by name.

[Iceberg configuration reference]: https://iceberg.apache.org/docs/latest/configuration/

## Current state

### DFD

- The `datafusion-distributed-iceberg` crate is read-only.
- Catalog-backed and static providers do not implement distributed writes.
- The codec serializes an `IcebergDataSource` scan, not write or commit nodes.
- Iceberg scans already use work unit feeds to discover `FileScanTask`s on the
  coordinator and stream them to worker leaf nodes.
- DFD can create network hash-shuffle and coalesce stage boundaries. General
  round-robin/range write distribution still needs explicit validation or
  planner work.
- `TaskKey` contains query, stage, and task identity, but not a write-operation
  or task-attempt identity suitable for output file ownership.

### `iceberg-rust` on `main`

DFD currently pins `iceberg-rust` revision
`4d83bc77dc10cff851a3ef427c451ff977bc3643`. The table below describes upstream
`main` at the status date, not necessarily that pinned revision. The dependency
and every capability must be rechecked when implementation starts.

| Layer | Capability | Status on `main` | Relevant work |
| --- | --- | --- | --- |
| Data writer | Parquet data files and Iceberg metrics | Available | `ParquetWriterBuilder`, `DataFileWriterBuilder` |
| File rolling | Target-size rollover | Available | `RollingFileWriterBuilder` |
| Partitioned writer | Unpartitioned, clustered, and fanout writers | Available | `UnpartitionedWriter`, `ClusteredWriter`, `FanoutWriter` |
| Partition metadata | Partition keys, paths, spec IDs, object-store layout | Available | Writer/location APIs |
| Delete writer | Equality-delete files | Available | `EqualityDeleteFileWriterBuilder` |
| Delete writer | Position-delete files / v3 deletion vectors | Not available as a complete writer on `main` | [#2218], closed [#2219], closed [#2678] |
| File descriptor transport | JSON encode/decode for `DataFile` | Available | `serialize_data_file_to_json`, `deserialize_data_file_from_json` |
| Commit | Fast append of data files | Available | `Transaction::fast_append()` |
| Commit | Overwrite files | Missing | [#2185] |
| Commit | Replace partitions | Missing | [#2269] |
| Commit | Delete files | Missing | [#2269] |
| Commit | Row delta | Missing | [#1104], [#2203]; closed alternatives [#2785] and [#2678] |
| Commit | Rewrite files | In progress, not on `main` | [#1607], [#3046] |
| CoW execution | Plan affected files, rewrite complete files, return add/remove sets | In progress, not on `main` | [#2752] |
| DataFusion integration | Append-only `insert_into` | Available locally upstream, but execution nodes are private | Upstream `IcebergWriteExec` and `IcebergCommitExec` |
| DataFusion integration | Overwrite/DELETE/UPDATE/MERGE | Missing or proof-of-concept work | [#2201], [#2205] |

[#1104]: https://github.com/apache/iceberg-rust/issues/1104
[#1607]: https://github.com/apache/iceberg-rust/issues/1607
[#2185]: https://github.com/apache/iceberg-rust/pull/2185
[#2201]: https://github.com/apache/iceberg-rust/issues/2201
[#2203]: https://github.com/apache/iceberg-rust/pull/2203
[#2205]: https://github.com/apache/iceberg-rust/issues/2205
[#2218]: https://github.com/apache/iceberg-rust/issues/2218
[#2219]: https://github.com/apache/iceberg-rust/pull/2219
[#2269]: https://github.com/apache/iceberg-rust/issues/2269
[#2620]: https://github.com/apache/iceberg-rust/pull/2620
[#2678]: https://github.com/apache/iceberg-rust/pull/2678
[#2752]: https://github.com/apache/iceberg-rust/pull/2752
[#2785]: https://github.com/apache/iceberg-rust/pull/2785
[#3046]: https://github.com/apache/iceberg-rust/pull/3046

### Upstream transaction roadmap

The [`iceberg-rust` missing-write-actions epic][#2269] tracks the gap between
`FastAppendAction` and the Java transaction API:

| Action | Iceberg semantics | DFD features unblocked |
| --- | --- | --- |
| `FastAppend` | Add data files without removing existing files | `INSERT INTO`, append API, append CTAS |
| `OverwriteFiles` | Add and remove files selected explicitly or by row filter | Static/filter overwrite, metadata delete, CoW DELETE/UPDATE/MERGE |
| `ReplacePartitions` | Replace partitions present in incoming output | Dynamic partition overwrite |
| `DeleteFiles` | Remove referenced files | Metadata-only deletes and some maintenance operations |
| `RowDelta` | Atomically add data and delete files with conflict validation | MoR DELETE/UPDATE/MERGE using equality/position deletes or deletion vectors |
| `RewriteFiles` | Atomically replace data/delete files | Compaction and rewrite maintenance |

Draft RFC [#2620] proposes retry-persistent action state and a shared merging
snapshot path. Its important invariants include:

- stable snapshot and commit identity across catalog retries;
- validation through the refreshed parent on every attempt;
- reuse only when all semantic dependencies are unchanged;
- rebuilding parent/sequence/row-ID state, the complete manifest set, manifest
  list, and `TableCommit` after a rebase;
- write-once generated metadata paths;
- tracking transaction-owned metadata artifacts;
- reachability-based cleanup after confirmed success, full cleanup after
  confirmed failure, and no deletion after an unknown commit outcome.

PR [#3046] is a current implementation direction for
`MergingSnapshotProducer` and `RewriteFilesAction`; it is not yet the full RFC.
It intentionally leaves retry caching, overwrite, row delta, and complete
conflict detection to follow-ups.

## Distributed commit precedent in other engines

Iceberg does not define a language-neutral distributed commit protocol. The
[table specification] defines immutable files, manifests, snapshots,
optimistic concurrency, and the atomic replacement of table metadata. It does
not define worker/coordinator messages, task-attempt selection, result
serialization, checkpoint coordination, or distributed cleanup. Those are
engine responsibilities.

The common implementation pattern is nevertheless consistent:

```text
distributed writers
    │
    │ completed task write results
    ▼
engine coordinator or committer
    │
    │ one Iceberg transaction action
    ▼
manifest(s) + manifest list + snapshot
    │
    │ atomic catalog update
    ▼
committed table
```

[table specification]: https://iceberg.apache.org/spec/#commits

### Iceberg Java `WriteResult`

Iceberg Java provides a useful de facto, though not language-neutral, task
result abstraction in [`WriteResult.java`]:

```text
WriteResult
  dataFiles
  deleteFiles
  referencedDataFiles
  rewrittenDeleteFiles
```

Its builder can aggregate multiple results. DFD should model its semantic
payload on this abstraction and add DFD-specific operation, task, attempt,
chunking, and cleanup identity around it.

[`WriteResult.java`]: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/io/WriteResult.java

### Spark

Spark executors return a `WriterCommitMessage` from `DataWriter.commit()`.
Iceberg's `SparkWrite.TaskCommit` contains the task's closed `DataFile`s. The
driver receives messages from successful tasks, adds their files to one
`AppendFiles`, `ReplacePartitions`, `OverwriteFiles`, or row-delta operation,
and commits once. Task abort deletes that task's generated files.

This is the closest conceptual match to an AQE-like DFD result store:

```text
Spark TaskCommit[]              DFD TaskWriteResult[]
        │                                │
        ▼                                ▼
driver BatchWrite.commit()      coordinator commit exec
```

See [`SparkWrite.java`].

[`SparkWrite.java`]: https://github.com/apache/iceberg/blob/main/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java

### Trino

Trino workers close their `IcebergPageSink` writers and return serialized
`CommitTaskData` fragments. Each fragment contains the path, file format and
size, metrics, partition spec ID and partition value, split offsets, content
type, and sort-order ID. Coordinator-side `IcebergMetadata.finishInsert()` or
`finishMerge()` decodes the fragments and constructs one Iceberg transaction.
For inserts, fragments are decoded one at a time to bound coordinator memory.

This is a direct precedent for carrying versioned write metadata through DFD's
worker-to-coordinator channel rather than exposing it as query rows.

See [`IcebergPageSink.java`] and [`IcebergMetadata.java`].

[`IcebergPageSink.java`]: https://github.com/trinodb/trino/blob/master/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergPageSink.java
[`IcebergMetadata.java`]: https://github.com/trinodb/trino/blob/master/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java

### Flink

Flink adds checkpoint durability to the same pattern. Parallel writers produce
`WriteResult`s, an `IcebergWriteAggregator` aggregates them into complete
`DeltaManifests`, and an `IcebergFilesCommitter` publishes the pending results
when a checkpoint completes. Its serializers are explicitly versioned, and a
committed checkpoint ID is recorded in snapshot metadata to prevent duplicate
commits after recovery.

Flink demonstrates the scaled alternative to returning every `DataFile`: a
worker or aggregator may write a **complete immutable manifest** and return its
descriptor. The final committer still chooses only successful attempts and
publishes all selected manifests through one snapshot. No partial manifest is
ever committed.

See [`WriteResultSerializer.java`], [`IcebergWriteAggregator.java`], and
[`IcebergFilesCommitter.java`].

[`WriteResultSerializer.java`]: https://github.com/apache/iceberg/blob/main/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/WriteResultSerializer.java
[`IcebergWriteAggregator.java`]: https://github.com/apache/iceberg/blob/main/flink/v2.2/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergWriteAggregator.java
[`IcebergFilesCommitter.java`]: https://github.com/apache/iceberg/blob/main/flink/v2.2/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java

### Implication for DFD

An AQE-like return path is appropriate, but write results differ from sampling,
display metrics, and optimization hints: they are durable correctness state.
DFD must couple a complete result to task success, select one successful
attempt per logical task, retain results through commit/cleanup, and treat a
missing or partial result as task failure. The Iceberg transaction remains
coordinator-only.

## Proposed distributed architecture

### Append

```text
coordinator
  IcebergAppendCommitExec
    drains writer stage and waits for successful TaskWriteResults
      distributed writer stage
        IcebergDataFileWriteExec
          SortExec?                         clustered mode
            NetworkShuffleExec?             hash/range write distribution
              ProjectExec?                  Iceberg partition/sort transforms
                distributed input

worker coordinator channel
  TaskWriteResult(operation, task, attempt, complete DataFile descriptors)
    ────────────────────────────────────────────────────────────────►
  coordinator write-result store
```

Workers use public `iceberg-rust` writer primitives and close every output file
before reporting its descriptor. The coordinator gathers only successful task
outputs, validates/deduplicates them, and calls:

```text
transaction = Transaction::new(refreshed_table)
action = transaction.fast_append().add_data_files(successful_files)
transaction = action.apply(transaction)
transaction.commit(catalog)
```

The transaction writes complete manifests and a complete manifest list before
performing one atomic catalog update. A snapshot may reference multiple
manifest files; atomicity does not require combining everything into one
manifest. It requires that only a complete snapshot graph is published.

### Overwrite, CoW, and MoR

The worker-result protocol should follow Iceberg Java's de facto
`WriteResult` contract where possible:

```text
IcebergWriteResult
  data_files: [DataFile]
  delete_files: [DeleteFile]
  referenced_data_files: [path]
  rewritten_delete_files: [DeleteFile]
  task_summary: rows, bytes, operation counts
```

Explicit data-file removals selected during CoW planning can remain
coordinator-owned operation intent rather than being rediscovered by workers.
The coordinator maps the complete successful result set to exactly one
upstream action (`OverwriteFiles`, `ReplacePartitions`, `RowDelta`, or
`RewriteFiles`). It must not approximate one action using another; their
validation and conflict semantics differ.

For CoW operations, existing Iceberg scan work unit feeds can distribute the
candidate files to rewrite. Each selected file must be read completely after
row-level filtering identifies it, because unaffected rows in the file must be
copied into replacements. PR [#2752] is intended to provide this lower-level
primitive.

For MoR operations, scans must preserve file path and row position metadata so
workers can produce correctly scoped position deletes/deletion vectors. The
final `RowDelta` atomically publishes new data and delete files.

### Worker result transport

DFD work unit feeds flow **from the coordinator to worker leaf nodes**. They are
appropriate for assigning progressively discovered scan/rewrite work, but not
for sending writer results back to the coordinator.

DFD already has the opposite-direction transport needed here. Its coordinator
channel carries `LoadInfo`, task metrics, and completed dynamic filters from a
worker to coordinator-owned stores. Iceberg write results can follow that AQE
pattern with a new correctness-critical message, for example:

```text
TaskWriteResult {
  write_operation_id,
  task_key,
  attempt_id,
  sequence_number,
  write_result: IcebergWriteResult,
  generated_paths,
  final_chunk,
  checksum,
}
```

The commit node drains the distributed writer stage, waits for one complete
successful result for every expected logical task, rejects duplicate or mixed
attempts, and then builds the transaction. This avoids exposing Iceberg commit
metadata as a synthetic SQL result schema and avoids an extra metadata
`NetworkCoalesceExec`.

Unlike AQE sampling or display metrics, write results are durable correctness
state. The protocol must therefore couple result completeness to task success:

- a task is not commit-eligible until all result chunks and its final marker
  have arrived and passed validation;
- a disconnected or failed task contributes nothing, even if some chunks
  arrived;
- only one attempt for each logical task is selected;
- task completion and result publication must not race in a way that can make a
  partial result appear successful;
- results remain available until commit or cleanup reaches a terminal outcome;
- large results need chunking, back-pressure, limits, and checksums.

The normal `ExecutionPlan` output path remains a viable simpler implementation:
a writer can emit Arrow batches and a network coalesce can feed them to the
commit node. The AQE-like side channel is preferable if the completion and
recovery semantics above are added to DFD's protocol rather than treating the
messages like best-effort metrics.

### Result serialization

`DataFile` is not a plain stable Rust/Protobuf wire type. `iceberg-rust`
currently exposes schema-aware JSON helpers, and its upstream DataFusion writer
uses one JSON value per Arrow string row. That is sufficient for a first
implementation but should be versioned explicitly.

Options for DFD are:

1. an Arrow action schema with Iceberg fields represented as typed columns;
2. a binary/protobuf envelope carrying a versioned Iceberg descriptor;
3. the existing upstream JSON representation plus schema/spec/version context.

The protocol must preserve at least content type, path, format, partition
value, partition spec ID, record/file metrics, bounds, equality IDs, sort
order, referenced data file, and v3 offsets. It must be able to evolve without
requiring all workers and coordinators to run byte-identical crate internals.

### Partitioning and sorting

Iceberg table partitioning, DataFusion execution partitioning, and DFD worker
placement are separate concepts.

For a clustered write, DFD should:

1. evaluate the active Iceberg write spec's transformed partition tuple;
2. hash- or range-distribute on that tuple;
3. sort locally by the required partition and table sort keys;
4. run a `ClusteredWriter` per output partition.

A fanout writer can accept arbitrarily partitioned input and is useful as a
correctness-first or explicit mode, but its open-writer memory must be bounded.
Hashing raw source columns is not equivalent to hashing transformed values for
`bucket`, `truncate`, or temporal transforms and may produce unnecessary files.

Read partitioning is not a prerequisite for partitioned writes. A write can
always insert a write-side shuffle. Read-side partition information may later
allow that shuffle to be elided when DFD can prove compatibility.

## DFD-specific feedback for upstream work

The transaction RFC and implementation are the right boundary: DFD should send
completed file descriptors to one coordinator-side transaction rather than
serialize a live transaction across workers. The following requirements are
particularly important to DFD and should be verified or raised upstream.

### Required for correctness

- [ ] **Explicit operation semantics and validation.** Every action must expose
  Java-compatible conflict predicates and isolation behavior, not only shared
  manifest mechanics. Validation must rerun against the refreshed catalog base.
- [ ] **Source-spec-aware manifest rewriting.** A filtered old manifest must be
  rewritten using that manifest's partition spec, not the current default
  spec. Removed-file summary metrics must also use each file's spec.
- [ ] **Sequence and row lineage preservation.** Rewritten entries must retain
  their original data/file sequence numbers; attempt-local sequence and row ID
  allocation must be rebuilt after rebase.
- [ ] **Delete applicability.** Position deletes must be matched by referenced
  data-file path as well as sequence/partition rules. Equality-delete and v3
  deletion-vector invariants need equivalent cross-engine tests.
- [ ] **Missing-file failure.** Rewrite/remove operations must fail if an
  intended removal disappeared because of a concurrent operation; publishing
  only the add half would duplicate rows.
- [ ] **Stable but write-once commit artifacts.** A stable `commit_uuid` is
  useful across retries, but a retry must resolve an unknown previous outcome
  before writing to a path that might already be referenced by a committed
  snapshot.
- [ ] **Outcome-aware cleanup.** Upstream owns generated manifests and manifest
  lists; DFD owns caller-supplied worker data/delete files. The API or errors
  must let DFD distinguish confirmed failure from unknown catalog outcome so it
  does not delete live files.
- [ ] **Chained snapshot totals.** Tests must verify `total-data-files`,
  `total-delete-files`, and row totals after multiple snapshots and rewrites,
  not just per-operation added/deleted counters.
- [ ] **Partition evolution tests.** Cover worker-produced files under the
  current spec while filtering manifests/files written under older specs.
- [ ] **Multi-action atomicity.** Schema evolution plus data changes and data
  plus delete files must remain one `TableCommit`, with action replay ordering
  preserved.

### API and scale requests

- [ ] **Public task-writer factory/configuration.** Upstream's DataFusion
  `IcebergWriteExec`, `IcebergCommitExec`, and `TaskWriter` are private. Expose
  a reusable task-writer API or factory so DFD needs only a thin serializable
  execution node rather than copying writer composition.
- [ ] **Stable descriptor codec.** Keep a public, documented round-trip format
  for `DataFile`/delete-file descriptors, including partition schema/spec and
  format version. A non-JSON representation would reduce distributed metadata
  overhead but is not required initially.
- [ ] **Caller-controlled file identity.** Writers must accept operation, stage,
  task, and attempt identity in generated file names while preserving Iceberg
  location-provider behavior and path escaping.
- [ ] **Bounded metadata ingestion.** `add_data_files(Vec<DataFile>)` requires the
  coordinator to hold every descriptor. For large writes, consider a streaming
  builder or an API to attach already-complete manifests from successful tasks.
  Such an API must validate inheritance, spec IDs, ownership, and retry safety.
- [ ] **No partial-manifest API.** If workers eventually write manifests, the
  transaction must accept only closed immutable manifests. The coordinator
  chooses manifests from successful attempts and publishes all of them in one
  snapshot.
- [ ] **Bounded caches and metrics.** RFC source/derived caches and fanout
  writers need explicit memory bounds and observable hit, miss, spill, and
  cleanup metrics.
- [ ] **Cancellation-safe close/abort.** Writer APIs should return all known
  generated paths or provide an abort result even when closing one of several
  partition writers fails.
- [ ] **Thread/runtime neutrality.** Public core writers and transaction actions
  should not assume they run in the coordinator's process or a particular Tokio
  runtime; DFD supplies storage and runtime context explicitly.

These requests should stay narrowly separated: distributed scheduling and the
worker-result protocol belong in DFD, while Iceberg file and transaction
semantics belong upstream.

## Failure and retry model

A production write path needs the same concerns collected in Comet's
[production-quality native Iceberg writes epic].

[production-quality native Iceberg writes epic]: https://github.com/apache/datafusion-comet/issues/5649
[#5649]: https://github.com/apache/datafusion-comet/issues/5649

### Identities

Every output should be attributable to:

```text
write_operation_id
query_id
stage_id
logical_task_id
attempt_id
file_ordinal
```

A new attempt must not overwrite another attempt's file. Only one successful
attempt per logical task contributes descriptors to the commit.

### Ownership

- Workers/DFD own newly written data and delete files until commit success is
  confirmed.
- The upstream transaction owns manifests and manifest lists it generates.
- Ownership transfers to the table only after a confirmed successful commit.
- Unknown commit outcome transfers nothing to cleanup: resolve by refreshing
  and searching for the stable snapshot/operation identity before deleting or
  retrying.

### Required tests

- Worker failure before and after closing a file.
- One partition fails after other partitions finish.
- Cancellation while writers are open.
- Duplicate task attempt completion.
- Coordinator failure before commit, during catalog update, and after a
  successful update response is lost.
- Concurrent append, overwrite, rewrite, and row-delta conflicts.
- Partition-spec and schema evolution between planning and commit.
- Storage inspection after every injected failure, not only table row checks.
- Independent Spark or Trino reads of committed tables and independent writes
  followed by DFD reads.

## Roadmap

### Phase 0: protocol and upstream boundary

- [ ] Define a versioned `IcebergWriteSpec` containing table identity,
  metadata/schema/spec/sort IDs, storage configuration, write properties,
  output mode, and operation/task identity.
- [ ] Define a versioned worker action/result schema.
- [ ] Decide whether to request an upstream public task-writer factory or build
  DFD's thin node directly from public core writer builders.
- [ ] Define ownership, cleanup, retry, and unknown-outcome contracts before
  enabling task retries.

### Phase 1: distributed Parquet append — possible today

- [ ] Implement append-only `insert_into` for catalog-backed providers; keep
  static/snapshot providers read-only.
- [ ] Add a serializable worker `IcebergDataFileWriteExec`.
- [ ] Support unpartitioned, hash-clustered partitioned, and explicit fanout
  writes.
- [ ] Return complete `DataFile` descriptors through a versioned AQE-like
  worker-to-coordinator result protocol, or initially through Arrow/network
  streams if the side-channel completion contract is not ready.
- [ ] Add coordinator-only `IcebergAppendCommitExec` using `fast_append`.
- [ ] Honor target file size, Parquet compression/tuning, partition paths,
  object-store layout, and snapshot properties supported upstream.
- [ ] Add attempt-safe file names, deduplication, cancellation, and best-effort
  orphan cleanup.
- [ ] Test local and multi-worker `VALUES` and `SELECT` appends, partitioned and
  unpartitioned tables, concurrent appends, and catalog retry.

This phase does not depend on [#2620]. It should still align its identity and
cleanup model with the RFC to avoid incompatible retry semantics later.

### Phase 2: write distribution and production hardening — possible today

- [ ] Add explicit writer task-count policy and distributed round-robin support
  for unpartitioned writes.
- [ ] Add range distribution and sort-order enforcement where DataFusion and
  DFD can express them.
- [ ] Bound fanout memory and expose write/file/partition metrics.
- [ ] Add failure injection and cross-engine compatibility suites.
- [ ] Add supported-property allowlisting and fail-closed diagnostics, following
  the lesson from Comet [#5649].

### Phase 3: CoW and compaction — upstream work in progress

- [ ] Consume a landed equivalent of [#2752] for affected-file rewriting.
- [ ] Consume `OverwriteFiles` for full/filter overwrite and CoW DML.
- [ ] Consume `RewriteFiles` from the [#2620]/[#3046] lineage for compaction.
- [ ] Implement metadata-only partition/file deletes when predicates can be
  proven to cover complete files.
- [ ] Add static overwrite, CoW `DELETE`, and data-file compaction.

### Phase 4: dynamic overwrite — blocked on `ReplacePartitions`

- [ ] Track the complete set of Iceberg partition tuples produced by successful
  writer tasks.
- [ ] Atomically replace exactly those partitions.
- [ ] Test hidden transforms and partition-spec evolution.

### Phase 5: MoR row-level DML — blocked on writers plus `RowDelta`

- [ ] Add scan metadata columns for data-file path and row position.
- [ ] Add position-delete and/or deletion-vector writers; use equality deletes
  where the operation semantics permit them.
- [ ] Commit data and delete files in one `RowDelta` snapshot.
- [ ] Implement MoR `DELETE`, `UPDATE`, and `MERGE INTO`.
- [ ] Verify all delete application paths on reads before enabling writes that
  produce those delete forms.

### Phase 6: complete DML and maintenance surface

- [ ] Dynamic and filter overwrite parity.
- [ ] CoW and MoR `UPDATE`/`MERGE` planner selection.
- [ ] Rewrite delete files and manifests.
- [ ] CTAS/replace-table atomic publication.
- [ ] Branch/WAP writes.
- [ ] Snapshot expiration and orphan cleanup with conservative retention.
- [ ] ORC/Avro writes when supported by upstream writers.

## Acceptance criteria for the first release

- Catalog-backed `INSERT INTO ... VALUES` and `INSERT INTO ... SELECT` work on
  one or multiple workers for partitioned and unpartitioned Parquet tables.
- Exactly one catalog commit occurs and it references files only from successful
  logical task attempts.
- A worker or coordinator failure never exposes a partial snapshot.
- Unsupported formats, modes, transforms, and relevant write properties fail
  during planning with actionable errors.
- Partition paths, metrics, sequence numbers, and snapshot summaries can be
  read correctly by at least one independent Iceberg implementation.
- Failure tests inspect both table visibility and uncommitted objects.
- Documentation clearly distinguishes supported behavior, known semantic
  differences, and cleanup limitations.

## References

- [Spark Iceberg writes](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Iceberg write properties](https://iceberg.apache.org/docs/latest/configuration/#write-properties)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [`iceberg-rust` missing write actions epic](https://github.com/apache/iceberg-rust/issues/2269)
- [`iceberg-rust` stateful transaction RFC](https://github.com/apache/iceberg-rust/pull/2620)
- [`iceberg-rust` merging snapshot/rewrite implementation](https://github.com/apache/iceberg-rust/pull/3046)
- [Comet production-quality native Iceberg writes](https://github.com/apache/datafusion-comet/issues/5649)
- [DFD Iceberg umbrella](https://github.com/datafusion-contrib/datafusion-distributed/issues/599)
