// Package clickhouse provides ClickHouse-specific schema reading and writing.
//
// ClickHouse is wired into the rest of Ptah via the standard
// dbschema.SchemaReader / dbschema.SchemaWriter interfaces. Two ClickHouse
// idiosyncrasies show up at the implementation level:
//
//   - Transactions are not used. ClickHouse only supports experimental
//     transactions (and only against MergeTree-family engines with explicit
//     opt-in); for the migration paths Ptah currently exercises every
//     statement is executed standalone. BeginTransaction / Commit / Rollback
//     are therefore no-ops that record the dry-run state.
//   - DropAllTables iterates `system.tables` and emits `DROP TABLE … SYNC`
//     for each. SYNC forces a synchronous drop so the table is actually
//     gone by the time the call returns — without it, subsequent CREATE
//     TABLE statements can fail with "Table exists" until the async drop
//     completes.
package clickhouse

import (
	_ "github.com/ClickHouse/clickhouse-go/v2" // driver registers as "clickhouse"
)
