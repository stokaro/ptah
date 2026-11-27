# Select database guidance

Use this reference when a task depends on a database's SQL, catalog, identity,
or execution behavior. Load only the affected engine's page. A shared parser
or contract change may need several targets; choose them from the behavior
that differs, not from the length of this table.

## Establish the target

Read the selected descriptor in `internal/builtindialects` and its runtime
registration in `engine/builtin`. Those declarations own the built-in list,
capabilities, and version selection. Record the actual server version,
edition, storage engine, connection settings, and privileges where relevant.
A PostgreSQL wire protocol or MySQL family label does not prove equivalent
schema behavior.

| Selected target | Conditional reference |
| --- | --- |
| PostgreSQL | [PostgreSQL](databases/postgres.md) |
| MySQL | [MySQL](databases/mysql.md) |
| MariaDB | [MariaDB](databases/mariadb.md) |
| SQLite | [SQLite](databases/sqlite.md) |
| ClickHouse | [ClickHouse](databases/clickhouse.md) |
| SQL Server | [SQL Server](databases/sqlserver.md) |
| CockroachDB | [CockroachDB](databases/cockroachdb.md) |
| YugabyteDB YSQL | [YugabyteDB](databases/yugabytedb.md) |
| Oracle Database | [Oracle](databases/oracle.md) |

## Preserve the declared schema

Application tuning advice is not a schema conversion rule. Do not substitute
a preferred collation, remove an enum or nullable type, change a sorting key,
or add an index without the requested semantic change. Preserve unsupported
facts as unsupported coverage or a refusal instead of silently dropping them.

The linked vendor manuals identify hazards, not Ptah support promises. Check
the documentation for the actual target version before changing a versioned
capability. Confirm the result through Ptah's real caller and engine readback.
When the catalog or permissions hide information, keep incomplete inspection
distinct from explicit absence.

## Choose evidence at the owner

Each engine page names implementation and test entry points. Start with the
regression and its owning package. Expand to affected family members when
shared code changes. For changed database effects, keep a live regression
that checks persisted state, failure state, and cleanup as applicable.

Use [integration evidence](../../TESTING.md#integration-evidence) and the
current integration workflow for setup and complete checks. A filtered tagged
test is a local diagnostic; the existing `testcontour` runner owns the complete
suite. Do not invent another engine matrix or run every engine for a local
parser change. Report missing engine evidence as pending.

## Route external providers

Concrete engine implementation and live tests belong in the provider
repository. Start with its instructions and tests:

| Engine | Owner repository |
| --- | --- |
| DuckDB | [ptah-duckdb-plugin](https://github.com/stokaro/ptah-duckdb-plugin) |
| Spanner | [ptah-spanner-plugin](https://github.com/stokaro/ptah-spanner-plugin) |
| YDB | [ptah-ydb-plugin](https://github.com/stokaro/ptah-ydb-plugin) |

Read [provider contracts](../skills/ptah-provider-contracts/SKILL.md) for
host, SDK, feature negotiation, or delivery changes. A host fixture proves
the protocol contract. It does not prove the provider's engine behavior.
Do not copy provider-specific catalog or execution rules into a neutral host
layer to avoid making a companion change.
