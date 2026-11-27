# ClickHouse

Start with `internal/builtindialects/clickhouse.go`,
`internal/dbschema/clickhouse`, `internal/planner/dialects/clickhouse`, and
`internal/migratorext/clickhouse`. Extensions live under `dialect/clickhouse`.

- Record the server version, database engine, table engine, and cluster scope.
  Preserve engine arguments, sorting and partition expressions, primary key,
  settings, codecs, and time-to-live expressions as distinct schema facts.
- A MergeTree primary key does not enforce uniqueness. Keep `ORDER BY` and
  `PRIMARY KEY` separate when explicitly declared. Do not replace the user's
  keys with a workload recommendation from a tuning guide.
- Trace materialized-view dependencies and target or inner-table ownership
  before replacement and cleanup. An inspected storage table is not always
  an independently declared object.
- Distinguish local execution from distributed data definition language
  (DDL). `ON CLUSTER` work is queued for individual hosts. A client timeout
  does not establish that every host failed or that replay is safe.
- For mutations, replication, or distributed DDL changes, establish what the
  command's completion means and inspect the relevant status and readback.
  Do not infer transaction rollback or cluster-wide completion from a local
  success response.

Use the affected owner's tests and `integration/dbschema/clickhouse`, including
storage planning and view round trips. A single-server test establishes only
single-server behavior. Name any required distributed or replication evidence
that the available environment cannot supply.

Sources: ClickHouse [MergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree)
and [distributed DDL](https://clickhouse.com/docs/reference/statements/distributed-ddl).
Verify version and deployment-specific conditions before changing capabilities.
