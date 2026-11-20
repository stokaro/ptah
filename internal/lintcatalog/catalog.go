// Package lintcatalog enumerates every lint rule Ptah's binaries can report.
//
// The enumeration is derived from the registries the linters actually consult:
// [lint.Rules] for migration lint, over the rules a runtime's engine owners
// publish, and [sqllint.CatalogIDs] for SQL lint. The default source is the
// bundled runtime beside the rule catalogs official executable providers
// published for the engines it does not serve, as internal/providerpublished
// recorded them; [MigrationEntriesFrom] takes another, such as one holding an
// executable provider, whose published rule catalog then joins the
// enumeration. What a registry cannot carry -- a rule's one-line meaning, its
// Ptah name, and the Atlas check it reports -- is declared here and joined to
// the registry by code. [Entries] fails when the two sides disagree in either
// direction, so a rule cannot exist in the code and be missing from the
// documentation, and a documented rule cannot outlive the code that emitted it.
//
// The documentation page is generated from this package rather than written by
// hand for exactly that reason: a hand-written list of rule identifiers is a
// second declaration, and a second declaration drifts.
package lintcatalog

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlascodes"
	"ptah.run/internal/providerpublished"
	"ptah.run/internal/sqllint"
	"ptah.run/migration/lint"
)

// Kind separates the two linters, which have separate registries and separate
// commands.
type Kind string

const (
	// KindMigration is migration lint, registered in [lint.Rules].
	KindMigration Kind = "migration"
	// KindSQL is standalone SQL lint, registered in [sqllint.CatalogIDs].
	KindSQL Kind = "sql"
)

// Entry is one rule, joined from a registry and this package's declaration.
//
// A rule is identified by its name. A name can stand for one hazard on several
// engines, each owner checking it for its own: the catalog holds one entry per
// name and linter, with the dialects of every owner and the first owner's
// title.
type Entry struct {
	// Name is the rule's Ptah name, `category/name`: two lowercase segments of
	// letters and digits joined by single hyphens, the first a declared
	// [lintnames.Category]. It is the rule's identity in findings, selectors
	// and reports.
	//
	// Rules share a name only as one hazard: rules of one linter under one
	// name are checked on dialects that do not overlap, and every rule under a
	// name has the same default severity. A name may also appear once in each
	// linter, for a hazard migration lint and SQL lint both read.
	Name string
	// Owner says who publishes the rule.
	Owner Owner
	// Kind says which linter registers it.
	Kind Kind
	// Title is the registry's short label for the hazard.
	Title string
	// Summary is the one-line meaning a reader looks up.
	Summary string
	// Severity is the rule's default severity.
	Severity string
	// Dialects lists the dialects the rule applies to; empty means every one.
	Dialects []string
	// Native reports whether native ptah can emit the rule.
	Native bool
	// Compat reports whether ptah-compat can emit the rule.
	Compat bool
	// YDB is what a migration lint rule with no dialect restriction does on a
	// YDB run, and empty for every other rule; see [YDBVerdict].
	YDB YDBVerdict
	// YDBNote says what the verdict rests on: the YQL statement the rule
	// reads, the one YQL does not have, or the rule that replaces it.
	YDBNote string
}

// YDBVerdict classifies what a migration lint rule that runs on every dialect
// does on YDB.
//
// A rule with no dialect restriction runs on a YDB run whether or not anyone
// asked what it does there, so each one carries a verdict, declared here and
// checked against the analyzer: [Validate] refuses such a rule without one,
// and a test drives every rule marked [YDBApplies] over YQL it must report.
// The YD family is YDB's own and needs none.
type YDBVerdict string

const (
	// YDBApplies marks a rule that reads the YQL spelling of its statement
	// and reports a hazard YDB has.
	YDBApplies YDBVerdict = "applies"
	// YDBNoSuchStatement marks a rule whose statement YQL does not have: YDB
	// refuses it as a syntax error or has no such object, so the rule cannot
	// report on a migration YDB runs.
	YDBNoSuchStatement YDBVerdict = "no such statement in YQL"
	// YDBReplaced marks a rule whose every finding on YDB a YD rule reports
	// instead, with the reason the statement fails there.
	YDBReplaced YDBVerdict = "replaced"
	// YDBNeedsDevDatabase marks a rule that reads the state a dev database
	// replay supplies, on YDB a replay in a dev realm. A run without a dev
	// database names the rule as unmet rather than reporting less in silence.
	YDBNeedsDevDatabase YDBVerdict = "needs a dev database"
	// YDBProviderReads marks a rule about something the provider does on YDB
	// rather than the host, so the rule does not run there.
	YDBProviderReads YDBVerdict = "read by the provider"
)

// ruleMeta is everything about a rule that no registry can carry.
type ruleMeta struct {
	// Summary is the one-line meaning.
	Summary string
	// NativeOnly marks a rule the compatibility surface never reports. It is
	// pinned by a test that runs the analyzer under both profiles, so it cannot
	// become a claim about behavior that has changed.
	NativeOnly bool
	// YDB and YDBNote are the rule's [YDBVerdict] and what it rests on, for a
	// rule that runs on every dialect.
	YDB     YDBVerdict
	YDBNote string
}

// migrationRuleMeta declares the documentation-only facts about each migration
// lint rule. Adding a rule to migration/lint without adding it here fails
// [Entries]; removing one without removing the entry here fails it too.
//
// With [sqlRuleMeta] and [atlasChecks] it is the correspondence between the
// code a rule reports today, which keys it, the rule's Ptah name, and the
// Atlas check it reports: each rule names its Atlas check or none, and each
// Atlas check names its rules or a stated gap. [Validate] holds both
// directions.
var migrationRuleMeta = map[string]ruleMeta{
	"rollout/drop-table": {
		Summary: "dropping a table retires a name deployed clients still query, which is a rollout break a backup does not mitigate",
		YDB:     YDBApplies,
		YDBNote: "`DROP TABLE`",
	},
	"rollout/drop-column": {
		Summary: "dropping a column retires a name deployed clients still select and insert, whether or not the column held rows",
		YDB:     YDBApplies,
		YDBNote: "`ALTER TABLE ... DROP COLUMN`",
	},
	"locking/partitioned-index": {
		Summary: "an index on a partitioned table locks the parent and every partition at once, and CONCURRENTLY is refused there",
	},
	"locking/add-exclusion-constraint": {
		Summary: "adding an EXCLUDE constraint holds an ACCESS EXCLUSIVE lock while it builds the index and validates every row",
	},
	"locking/replace-primary-key": {
		Summary: "replacing a primary key builds the new unique index under an ACCESS EXCLUSIVE lock",
	},
	"rollout/weaken-replica-identity": {
		Summary: "REPLICA IDENTITY FULL or NOTHING changes what logical replication can carry for the table",
	},
	"design/disable-autovacuum": {
		Summary: "disabling autovacuum leaves dead rows for nothing to reclaim; the statement is cheap and the cost is paid later",
	},
	"rewrite/replace-primary-key": {
		Summary: "replacing a primary key rebuilds the table and every secondary index that stores it as a row pointer",
	},
	"rewrite/change-engine": {
		Summary: "changing the storage engine copies the table and blocks writes for the duration",
	},
	"rewrite/change-partitioning": {
		Summary: "a partitioning change rewrites every row and accepts no ALGORITHM or LOCK clause to soften it",
	},
	"rewrite/add-stored-generated-column": {
		Summary: "adding a STORED generated column computes and stores a value for every row; on MySQL and MariaDB it copies the table and blocks writes",
	},
	"locking/add-auto-increment-column": {
		Summary: "adding an AUTO_INCREMENT column rebuilds the table in place and still blocks writes",
	},
	"rewrite/modify-generated-column": {
		Summary: "changing a STORED generated column recomputes it for every row, copying the table and blocking writes",
	},
	"locking/add-check": {
		Summary: "adding a CHECK constraint validates every existing row and can hold locks, and a row that fails the predicate fails the migration",
	},
	"locking/enforce-check": {
		Summary: "enforcing a CHECK constraint revalidates every row; unenforcing one stops the server refusing the values it was there to refuse",
	},
	"destructive/drop-system-versioning": {
		Summary: "DROP SYSTEM VERSIONING deletes every historical row version permanently, and no rollback restores it",
	},
	"data-dependent/set-not-null": {
		Summary: "a nullable column made NOT NULL meets the rows the table already holds: MySQL, MariaDB, SQL Server and ClickHouse fail on a row holding NULL or rewrite it to the type's default, and Spanner validates every existing row in a schema change, which a NULL fails and leaves the column nullable",
	},
	"correctness/immutable-calls-volatile": {
		Summary: "a routine declared immutable calls something whose result changes between two calls with the same arguments",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YQL stores no routine",
	},
	"destructive/drop-table": {
		Summary: "DROP TABLE, and on MariaDB CREATE OR REPLACE TABLE, destroys the table and its rows. " +
			"On the compatibility surface a rename reports here, retiring the old name without moving the rows. " +
			"The native surface skips the drop that ends a table rebuild, whose copy keeps every row",
		YDB:     YDBApplies,
		YDBNote: "`DROP TABLE`",
	},
	"dependency/drop-column-in-use": {
		Summary: "a column a view or routine still reads is dropped, and the finding names what breaks; " +
			"a view dropped or rebuilt with a plain projection before the column drop is left out",
		YDB:     YDBNeedsDevDatabase,
		YDBNote: "what reads a column comes from a dev database replay",
	},
	"destructive/drop-column": {
		Summary: "DROP COLUMN destroys the column and every value stored in it; an IF EXISTS drop of a column the dev database shows absent is not reported",
		YDB:     YDBApplies,
		YDBNote: "`ALTER TABLE ... DROP COLUMN`",
	},
	"data-dependent/alter-column-type": {
		Summary: "a column type change can truncate or reject existing values and may rewrite the table under a lock; a clause that restates the column's current type, as the dev database records it, is not reported",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YQL changes no column type; `ALTER COLUMN ... TYPE` is a syntax error",
	},
	"integrity/drop-not-null": {
		Summary: "DROP NOT NULL removes a column-level data protection",
		YDB:     YDBApplies,
		YDBNote: "`ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL`",
	},
	"integrity/drop-untyped-constraint": {
		Summary: "an untyped DROP CONSTRAINT removes a data protection the SQL does not name",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB has no named constraint to drop",
	},
	"destructive/remove-enum-value": {
		Summary: "removing an enum value can invalidate rows that still hold it",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB has no enum type",
	},
	"destructive/drop-object": {
		// Broader than Atlas DS101, which is the schema drop alone: this rule
		// also fires on DROP TYPE, EXTENSION, FUNCTION, PROCEDURE, TRIGGER,
		// ROLE, and POLICY, and on YDB on DROP USER, DROP GROUP, DROP TOPIC,
		// DROP TRANSFER, DROP ASYNC REPLICATION ... CASCADE and DROP
		// COORDINATION NODE, so the rule is ours even though it covers the
		// Atlas one.
		Summary: "dropping a schema, type, extension, function, procedure, trigger, role or policy, or a YDB object the YDB table " +
			"below names, removes behavior or data; an IF EXISTS drop of a type or a trigger the dev database shows absent is not reported",
		YDB: YDBApplies,
		YDBNote: "Drops YDB users, groups, topics, transfers, replication replica tables with CASCADE, " +
			"coordination nodes and streaming queries. Topic positions, streaming checkpoints and " +
			"coordination resources are lost with their objects",
	},
	"coverage/routine-body": {
		Summary: "the migration defines a routine whose body is not analyzed, so a clean result says nothing about what the body does",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YQL stores no routine",
	},
	"coverage/unparsed-statement": {
		Summary: "the SQL parser could not read a statement and no server ran it, so a clean result says nothing about it",
		YDB:     YDBProviderReads,
		YDBNote: "the rule is about Ptah's SQL parser, and YQL is read by its provider",
	},
	"online/unproven": {
		Summary: "the statement is outside the set measured to take no lock that conflicts with reads and writes: on PostgreSQL it is unmeasured, and on MySQL and MariaDB it does not ask the server to apply it without blocking writes, so the server was never given the chance to refuse",
	},
	"online/lock-held-across-scan": {
		Summary: "a statement validates a constraint behind a lock an earlier statement in the same transaction took, so the scan runs with readers and writers waiting",
	},
	"destructive/truncate-table": {
		Summary: "TRUNCATE deletes every row in the table",
		YDB:     YDBApplies,
		YDBNote: "`TRUNCATE TABLE`, on lines with `truncate_table`",
	},
	"security/disable-row-level-security": {
		Summary: "DISABLE ROW LEVEL SECURITY removes an access-control protection",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB has no row-level security",
	},
	"security/no-force-row-level-security": {
		Summary: "NO FORCE ROW LEVEL SECURITY exempts the table owner from every policy on the table",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB has no row-level security",
	},
	"integrity/drop-foreign-key": {
		Summary: "dropping a foreign key removes referential-integrity enforcement",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB has no foreign key",
	},
	"integrity/drop-check": {
		Summary: "dropping a check constraint removes a value-validation guarantee",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB has no `CHECK` constraint",
	},
	"integrity/drop-primary-key": {
		Summary: "dropping a primary key removes row identity and can break replication",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB cannot drop a primary key",
	},
	"data-dependent/add-not-null-column": {
		Summary: "adding a NOT NULL column without a default fails or blocks on a populated table",
		YDB:     YDBReplaced,
		YDBNote: "`ydb/add-column-form`, since YDB refuses a NOT NULL column without a default even on an empty table",
	},
	"file-form/missing-down": {
		Summary: "no matching .down.sql exists, so a failed deploy cannot be rolled back mechanically",
		YDB:     YDBApplies,
		YDBNote: "the migration directory's files",
	},
	"file-form/empty-migration": {
		Summary: "the migration carries no executable statements",
		YDB:     YDBApplies,
		YDBNote: "the migration directory's files",
	},
	"data-dependent/add-unique-index": {
		Summary: "a unique index built over existing rows fails on the first duplicate",
		YDB:     YDBApplies,
		YDBNote: "`ADD INDEX ... UNIQUE` where the target adds one to an existing table; `capability/unique-index-on-existing-table` replaces it where it does not",
	},
	"data-dependent/rebuild-index-as-unique": {
		Summary: "an index dropped and rebuilt as unique fails on the first duplicate and leaves the table without it",
		YDB:     YDBApplies,
		YDBNote: "`DROP INDEX` then `ADD INDEX ... UNIQUE`, where the target adds one; `capability/unique-index-on-existing-table` replaces it where it does not",
	},
	"file-form/file-name": {
		Summary: "the file name does not follow the migration file-name convention",
		YDB:     YDBApplies,
		YDBNote: "the migration directory's files",
	},
	"file-form/refused-file": {
		Summary:    "a run refuses the migration directory for this file: the directory's format does not read its name, another file carries its version and direction, or the revision table cannot keep its version",
		NativeOnly: true,
		YDB:        YDBApplies,
		YDBNote:    "the migration directory's files",
	},
	"rollout/rename": {
		Summary:    "a rename retires a name deployed code still refers to",
		NativeOnly: true,
		YDB:        YDBApplies,
		YDBNote:    "`ALTER TABLE ... RENAME TO`",
	},
	"locking/create-index": {
		Summary: "CREATE INDEX without CONCURRENTLY blocks writes for the whole build",
	},
	"transaction/add-enum-value": {
		Summary: "ALTER TYPE ... ADD VALUE cannot run inside a transaction before PostgreSQL 12, and the value stays unusable in the same transaction after it",
	},
	"transaction/concurrent-index": {
		Summary: "CONCURRENTLY cannot run inside the migration's transaction",
	},
	"transaction/per-chunk-index": {
		Summary: "a TimescaleDB per-chunk index build cannot run inside the migration's transaction",
	},
	"locking/add-primary-key": {
		Summary: "adding a primary key takes an ACCESS EXCLUSIVE lock and can scan existing rows",
	},
	"locking/add-unique-constraint": {
		Summary: "adding a unique constraint takes an ACCESS EXCLUSIVE lock and validates rows",
	},
	"locking/drop-index": {
		Summary: "DROP INDEX without CONCURRENTLY blocks writes while the index is removed",
	},
	"design/column-alignment": {
		Summary: "the declared column order can waste tuple padding",
	},
	"rewrite/volatile-default": {
		Summary: "a volatile DEFAULT on an added column rewrites or evaluates every existing row",
	},
	"locking/set-not-null": {
		Summary: "SET NOT NULL scans the table to validate existing rows",
	},
	"locking/add-primary-key-on-nullable": {
		Summary: "a primary key over nullable columns sets them NOT NULL and scans every row on top of the index build",
	},
	"rewrite/set-persistence": {
		Summary: "changing LOGGED or UNLOGGED rewrites the table under heavyweight locks",
	},
	"locking/create-trigger": {
		Summary: "CREATE TRIGGER takes a SHARE ROW EXCLUSIVE lock and can block writes",
	},
	"rewrite/add-identity-column": {
		Summary: "adding an identity column can rewrite existing rows",
	},
	"security/definer-without-search-path": {
		Summary: "a SECURITY DEFINER routine that does not pin search_path resolves unqualified names through the caller's",
	},
	"naming/identifier-too-long": {
		Summary:    "a name longer than the 63 bytes PostgreSQL keeps, which the server cuts with a notice and stores under its first bytes",
		NativeOnly: true,
	},
	"naming/identifier-collision": {
		Summary:    "a name PostgreSQL cuts to a name already in use where it refuses the second declaration, so the migration fails when it runs",
		NativeOnly: true,
	},
	"rewrite/set-access-method": {
		Summary: "changing a table's access method rewrites the table",
	},
	"security/dynamic-sql-from-value": {
		Summary: "a routine builds and runs a statement from a value it does not quote",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YQL stores no routine",
	},
	"naming/schema-name": {
		Summary: "a schema this migration creates or renames to violates the configured naming convention",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB has no schema to create",
	},
	"naming/table-name": {
		Summary: "a table this migration creates or renames to violates the configured naming convention",
		YDB:     YDBApplies,
		YDBNote: "`CREATE TABLE` and `RENAME TO`, judged on the unquoted path",
	},
	"naming/column-name": {
		Summary: "a column this migration declares, adds, or renames to violates the configured naming convention",
		YDB:     YDBApplies,
		YDBNote: "the columns of `CREATE TABLE` and `ADD COLUMN`",
	},
	"naming/index-name": {
		Summary: "an index or unique key this migration names violates the configured naming convention",
		YDB:     YDBApplies,
		YDBNote: "`INDEX` in `CREATE TABLE`, `ADD INDEX` and `RENAME INDEX`",
	},
	"naming/foreign-key-name": {
		Summary: "a foreign key this migration names violates the configured naming convention",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB has no foreign key",
	},
	"naming/check-name": {
		Summary: "a check constraint this migration names violates the configured naming convention",
		YDB:     YDBNoSuchStatement,
		YDBNote: "YDB has no `CHECK` constraint",
	},
	"locking/blocking-alter-table": {
		Summary: "this ALTER TABLE form usually rebuilds the table and blocks writes for the duration",
	},
	"side-effect/inline-reference": {
		Summary: "an inline REFERENCES clause on a column is ignored by MySQL and enforced by MariaDB",
	},
	"rewrite/remove-enum-value": {
		Summary: "removing an ENUM member copies the table, and a row still holding it fails the copy in strict mode",
	},
	"rewrite/reorder-enum-values": {
		Summary: "reordering ENUM members renumbers every row and copies the table",
	},
	"rewrite/insert-enum-value": {
		Summary: "inserting an ENUM member before the end renumbers the members after it and copies the table",
	},
	"rewrite/widen-enum": {
		Summary: "growing an ENUM past 255 members widens each value to two bytes and copies the table",
	},
	"rewrite/remove-set-member": {
		Summary: "removing a SET member copies the table, and a row still holding it fails the copy in strict mode",
	},
	"rewrite/reorder-set-members": {
		Summary: "reordering SET members renumbers every row and copies the table",
	},
	"rewrite/insert-set-member": {
		Summary: "inserting a SET member before the end renumbers the members after it and copies the table",
	},
	"rewrite/widen-set": {
		Summary: "growing a SET across a multiple of eight members adds a byte to each value and copies the table",
	},
	"rewrite/alter-column-type": {
		Summary: "a column type change the engine cannot apply in place rewrites the table: PostgreSQL rewrites it and its indexes when it cannot prove the change safe, and InnoDB copies it and blocks writes",
	},
	"rewrite/in-place-column-change": {
		Summary: "a column change the dev database shows the engine applies without rewriting the table, info only: on PostgreSQL an ALTER COLUMN TYPE applied as a catalog edit with no rebuild or scan, and on MySQL and MariaDB a MODIFY, CHANGE or CONVERT InnoDB applies in place, with the algorithm",
	},
	"locking/add-foreign-key": {
		Summary: "adding a foreign key validates existing rows and can copy or lock the table and block writes, on both tables on PostgreSQL",
	},
	"rewrite/add-primary-key": {
		Summary: "adding a primary key rebuilds the table in place around the new clustered index",
	},
	"rewrite/drop-primary-key": {
		Summary: "dropping a primary key without adding one in the same statement copies the table",
	},
	"rewrite/convert-charset": {
		Summary: "converting a table's character set re-encodes its columns and copies the table",
	},
	"locking/add-fulltext-index": {
		Summary: "adding a FULLTEXT index can rebuild the table and block writes",
	},
	"locking/add-spatial-index": {
		Summary: "adding a SPATIAL index can rebuild the table and block writes",
	},
	"rewrite/set-not-null": {
		Summary: "declaring a column NOT NULL rebuilds the table: SQLite cannot enforce it on existing nullable data without a rebuild, and MySQL and MariaDB rebuild in place; whether an existing NULL fails the statement is the question data-dependent/set-not-null answers",
	},
	"transaction/mixed-statements": {
		Summary: "the migration mixes statements that cannot share one transaction",
	},
	"transaction/explicit-transaction": {
		Summary: "a statement that controls transaction state, which the migrator refuses in a wrapped file and in one that runs without a transaction; a golang-migrate file on PostgreSQL, up or down, is read as the migrator runs it",
	},
	"side-effect/drop-row-deletion-policy": {
		Summary: "`ALTER TABLE ... DROP TTL` drops a table's row deletion policy, so rows the policy would delete stay",
	},
	"capability/unique-index-on-existing-table": {
		Summary: "a unique index added to a table that exists, which a YDB line without `unique_index_on_existing_table` refuses",
	},
	"transaction/scheme-and-data-in-one-query": {
		Summary: "a block or an action call that runs a scheme statement and a statement reading or writing a table in one query, which YDB refuses whole",
	},
	"ydb/add-column-form": {
		Summary: "an `ADD COLUMN` YDB refuses: NOT NULL without a default on every line, or a default where `add_column_with_default` is false",
	},
	"dependency/drop-indexed-column": {
		Summary: "a `DROP COLUMN` of a column an index keys or covers, or the TTL reads, which YDB refuses until the index or the TTL is gone",
	},
	"side-effect/reset-min-partition-count": {
		Summary: "turning auto partitioning by size or by load on resets the minimum partition count to 1 unless the same statement sets it; silent where the directory's own migrations left the minimum at 1",
	},
	"dependency/drop-table-read-by-view": {
		Summary: "a `DROP TABLE` or `ALTER TABLE ... RENAME TO` of a table a view reads: YDB keeps the view, which reads its table by path, and every read of the view fails",
	},
	"side-effect/widen-serial-sequence": {
		Summary: "an `ALTER SEQUENCE` on a 16-bit or 32-bit Serial's sequence, which raises its maximum to the Int64 maximum, so the column wraps to negative values without an error",
	},
	"side-effect/replay-sequence-restart": {
		Summary: "an `ALTER SEQUENCE` without a `RESTART` on a sequence an earlier statement restarted, which YDB replays, so the next insert takes a key a row holds",
	},
	"dependency/rename-table-with-changefeed": {
		Summary: "a `RENAME TO` of a table that carries a changefeed, which YDB refuses until every changefeed is dropped",
	},
	"side-effect/revoke-grant-option": {
		Summary: "`REVOKE GRANT OPTION FOR` on YDB takes the permission it names as well, since YDB keeps the grant option as the permission `ydb.access.grant`",
	},
	"side-effect/dropped-principal-keeps-permissions": {
		Summary: "`DROP USER` or `DROP GROUP` on YDB leaves the principal's permissions behind, and a principal created later under the name holds them",
	},
	"side-effect/reset-topic-setting": {
		Summary: "an `ALTER TOPIC` that resets a topic setting, or a consumer setting other than its availability period, which YDB 26.2 accepts and keeps as it was, and 25.1 refuses",
	},
	"side-effect/ignored-topic-setting": {
		Summary: "a topic or consumer setting YDB accepts and keeps nothing of, such as a storage limit or a codec it does not know",
	},
	"destructive/drop-default-resource-pool": {
		Summary: "`DROP RESOURCE POOL default` on YDB, after which every query of the database fails with `Resource pool default not found`",
	},
	"destructive/drop-backup-collection": {
		Summary: "`DROP BACKUP COLLECTION` deletes every backup the collection holds, and stops a YDB 25.1 server",
	},
	"capability/analyze": {
		Summary: "`ANALYZE` in a migration, which YDB refuses unless `EnableColumnStatistics` is on, and on 25.1 refuses on a row table",
	},
	"side-effect/drop-replication-without-failover": {
		Summary: "a `DROP ASYNC REPLICATION` without `CASCADE` of a replication not failed over, whose replica tables YDB keeps read-only for good",
	},
	"security/secret-in-clear": {
		Summary: "a password or a token written in clear in an async replication or a transfer, which YDB keeps without reading it back while the migration file holds it",
	},
	"side-effect/reset-partition-size": {
		Summary: "turning auto partitioning by size on resets the partition size to 2048 MB unless the same statement sets it",
	},
	"side-effect/implicit-column-family": {
		Summary: "an `ALTER TABLE` that names a column family the table does not have, which YDB creates with its own settings rather than refusing, and YQL cannot drop",
	},
	"capability/vector-index": {
		Summary: "a vector index a YDB line does not build: any where `vector_indexes` is false, and one over bit vectors where `vector_bit_type` is false",
	},
	"side-effect/vector-index-misses-writes": {
		Summary: "rows written into a table holding a vector index on a YDB line where `vector_index_maintained_on_write` is false, which the index does not find",
	},
	"security/secret-value-in-migration": {
		Summary: "a secret's value written into the migration: a `CREATE SECRET` or `ALTER SECRET` whose value is not a `$PTAH_SECRET_...` reference, or a statement on the deprecated `OBJECT ... (TYPE SECRET)`",
	},
	"security/deprecated-secret-object": {
		Summary: "an external data source that names its credential by a deprecated secret object (`..._SECRET_NAME`), whose value the database administrator reads in clear",
	},
	"destructive/discard-streaming-query-state": {
		Summary: "replacing a streaming query or changing its body discards checkpoint state that rollback cannot restore",
	},
	"side-effect/orphaned-comment": {
		Summary: "a dropped or renamed column or index whose comment stays on the YDB table as an attribute under its old name",
	},
}

// sqlRuleMeta declares the same facts for the standalone SQL linter, which
// only native ptah reaches.
var sqlRuleMeta = map[string]ruleMeta{
	"correctness/index-on-undeclared-column": {
		Summary: "an index names a column the schema does not declare",
	},
	"naming/identifier-too-long": {
		Summary: "a name longer than the identifier limit of a server that cuts long names, which stores the object under its first bytes",
	},
	"naming/identifier-collision": {
		Summary: "a name the server cuts to a name already in use where it refuses the second declaration",
	},
	"correctness/syntax-error": {
		Summary: "the statement is malformed or opens with words no engine Ptah reads has, so no rule could inspect it",
	},
	"coverage/unsupported-statement": {
		Summary: "nothing read the statement, which a server may still accept: the parser holds no grammar for it, or steps over it as it does a SELECT",
	},
	"design/no-primary-key": {
		Summary: "the created table declares no primary key",
	},
	"capability/missing": {
		Summary: "the statement needs a capability the target server version does not have",
	},
	"coverage/unanalyzed-statements": {
		Summary: "the file carried statement kinds no rule examined, so a clean result would not mean it was checked",
	},
	"coverage/dynamic-sql": {
		Summary: "a routine body builds SQL at run time, so static analysis of that routine stops there",
	},
}

// everyDialect is the label for a rule with no dialect restriction.
const everyDialect = "all"

// severityError is the severity spelling both linters use for an error-grade
// finding.
const severityError = "error"

// DialectsLabel renders the dialect restriction for a heading or a table cell.
func (e Entry) DialectsLabel() string {
	if len(e.Dialects) == 0 {
		return everyDialect
	}
	return strings.Join(e.Dialects, ", ")
}

// SurfaceLabel renders which binaries report the rule.
func (e Entry) SurfaceLabel() string {
	switch {
	case e.Native && e.Compat:
		return "both"
	case e.Native:
		return "native only"
	case e.Compat:
		return "compat only"
	}
	return "none"
}

// Entries returns every rule both linters register, migration lint first,
// reading the rules engine owners publish from the documented runtime: the
// bundled one, and the recorded catalog of each official provider engine it
// does not serve.
func Entries() ([]Entry, error) {
	runtime, err := documented()
	if err != nil {
		return nil, err
	}
	return EntriesFrom(runtime)
}

// documented is the runtime the documentation enumerates: the bundled
// runtime, with the rules official providers published for the engines it does
// not serve. An engine that moves from the bundle to its provider keeps its
// rules on the page through the provider's record.
func documented() (lintext.Runtime, error) {
	runtime, err := builtin.New()
	if err != nil {
		return nil, err
	}
	return providerpublished.LintRuntime(runtime, runtime)
}

// EntriesFrom is [Entries] with runtime as the source of the rules engine
// owners publish; see [MigrationEntriesFrom].
func EntriesFrom(runtime lintext.Runtime) ([]Entry, error) {
	migration, err := MigrationEntriesFrom(runtime)
	if err != nil {
		return nil, err
	}
	sql, err := SQLEntries()
	if err != nil {
		return nil, err
	}
	return append(migration, sql...), nil
}

// MigrationEntries returns every rule migration lint applies with the
// documented runtime (see [Entries]), in the order lint.Rules returns them:
// the rules migration lint checks itself, then the rules the engine owners
// publish.
func MigrationEntries() ([]Entry, error) {
	runtime, err := documented()
	if err != nil {
		return nil, err
	}
	return MigrationEntriesFrom(runtime)
}

// MigrationEntriesFrom returns every rule migration lint applies with
// runtime, in the order lint.Rules returns them: the rules migration lint
// checks itself, then every rule the runtime's engine owners publish.
//
// The runtime is the source of the owners' rules and nothing else; only its
// rule listing is read. A runtime that holds an executable provider lists the
// rule catalog the provider published when it started, so an engine an
// official provider serves is documented from that catalog the way a bundled
// owner is documented from its registration. The meanings stay here, keyed by
// code, and the join refuses a published rule this package does not describe
// as it refuses a bundled one.
func MigrationEntriesFrom(runtime lintext.Runtime) ([]Entry, error) {
	rules, err := lint.Rules(runtime)
	if err != nil {
		return nil, err
	}
	return migrationEntriesFrom(rules)
}

// migrationEntriesFrom is the join, separated from the registry it normally
// reads so a test can hand it a registry the catalog does not describe. Driving
// the disagreement through the real registry would mean registering a rule
// process-wide and leaving it there for every test that runs after.
//
// Rules that share a name are one entry: the dialects of every rule, in the
// order the rules appear, and the title of the first.
func migrationEntriesFrom(rules []lint.Rule) ([]Entry, error) {
	providerOwned, err := providerOwnedNames()
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(rules))
	declared := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		meta, ok := migrationRuleMeta[rule.Name]
		if !ok {
			return nil, fmt.Errorf("migration lint registers rule %s with no catalog entry: add it to migrationRuleMeta so the documented list cannot fall behind the code", rule.Name)
		}
		if _, shared := declared[rule.Name]; shared {
			at := slices.IndexFunc(entries, func(entry Entry) bool { return entry.Name == rule.Name })
			entries[at] = joinShared(entries[at], rule, ownerOf(rule.Name, rule.Dialects, providerOwned))
			continue
		}
		declared[rule.Name] = struct{}{}
		entries = append(entries, Entry{
			Name:     rule.Name,
			Owner:    ownerOf(rule.Name, rule.Dialects, providerOwned),
			Kind:     KindMigration,
			Title:    rule.Title,
			Summary:  meta.Summary,
			Severity: string(rule.Severity),
			Dialects: slices.Clone(rule.Dialects),
			Native:   true,
			Compat:   !meta.NativeOnly,
			YDB:      meta.YDB,
			YDBNote:  meta.YDBNote,
		})
	}
	if err := checkOrphans(migrationRuleMeta, declared, "migration lint"); err != nil {
		return nil, err
	}
	return entries, nil
}

// joinShared adds a rule that shares the entry's name to the entry: its
// dialects join the entry's, and an owner mix stays host-owned, because the
// rows document what Ptah itself carries.
func joinShared(entry Entry, rule lint.Rule, owner Owner) Entry {
	if owner == OwnerHost {
		entry.Owner = OwnerHost
	}
	if len(entry.Dialects) == 0 || len(rule.Dialects) == 0 {
		entry.Dialects = nil
	} else {
		for _, dialect := range rule.Dialects {
			if !slices.Contains(entry.Dialects, dialect) {
				entry.Dialects = append(entry.Dialects, dialect)
			}
		}
	}
	return entry
}

// SQLEntries returns every rule the standalone SQL linter can report.
func SQLEntries() ([]Entry, error) {
	return sqlEntriesFrom(sqllint.CatalogIDs())
}

// sqlEntriesFrom is the SQL-linter half of the same join, split for the same
// reason as [migrationEntriesFrom].
func sqlEntriesFrom(names []string) ([]Entry, error) {
	entries := make([]Entry, 0, len(names))
	declared := make(map[string]struct{}, len(names))
	for _, name := range names {
		meta, ok := sqlRuleMeta[name]
		if !ok {
			return nil, fmt.Errorf("SQL lint can report rule %s with no catalog entry: add it to sqlRuleMeta so the documented list cannot fall behind the code", name)
		}
		declared[name] = struct{}{}
		entries = append(entries, Entry{
			Name:     name,
			Owner:    OwnerHost,
			Kind:     KindSQL,
			Title:    sqllint.CatalogTitle(name),
			Summary:  meta.Summary,
			Severity: string(sqllint.CatalogSeverity(name)),
			Native:   true,
		})
	}
	if err := checkOrphans(sqlRuleMeta, declared, "SQL lint"); err != nil {
		return nil, err
	}
	return entries, nil
}

// checkOrphans fails when this package documents a rule no registry produces,
// which is the other half of the drift the catalog exists to prevent.
func checkOrphans(meta map[string]ruleMeta, declared map[string]struct{}, surface string) error {
	orphans := make([]string, 0, len(meta))
	for name := range meta {
		if _, ok := declared[name]; !ok {
			orphans = append(orphans, name)
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	slices.Sort(orphans)
	return fmt.Errorf("the catalog documents %s rule(s) %s that no longer exist: remove the catalog entry or restore the rule", surface, strings.Join(orphans, ", "))
}

// Validate fails on every way the catalog and the code can disagree: a rule
// with no one-line meaning, a rule whose name breaks the naming rule (see
// [lintnames.Category] and [Entry.Name]), a declared category no rule uses, a
// YDB verdict that is missing or misplaced, and every disagreement between the
// Atlas correspondence table, the Atlas check list, the reviewed reference and
// the migration rules registered (see [atlascodes.Validate]).
func Validate(entries []Entry) error {
	var registered []string
	for _, entry := range entries {
		if entry.Kind == KindMigration {
			registered = append(registered, entry.Name)
		}
	}
	problems := atlascodes.Validate(registered)
	for _, entry := range entries {
		if strings.TrimSpace(entry.Summary) == "" {
			problems = append(problems, fmt.Sprintf("rule %s has no one-line meaning", entry.Name))
		}
		problems = append(problems, ydbVerdictProblems(entry)...)
	}
	problems = append(problems, nameProblems(entries)...)
	problems = append(problems, unusedCategoryProblems(entries)...)
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("lint catalog is inconsistent with the code:\n  %s", strings.Join(problems, "\n  "))
}

// ydbVerdictProblems checks that a migration lint rule says what it does on
// YDB exactly when it runs there without being YDB's own: a rule with no
// dialect restriction carries a known verdict and a note, and every other rule
// carries neither.
func ydbVerdictProblems(entry Entry) []string {
	everyDialect := entry.Kind == KindMigration && len(entry.Dialects) == 0
	switch {
	case everyDialect && !slices.Contains(ydbVerdicts, entry.YDB):
		return []string{fmt.Sprintf("rule %s runs on every dialect and says nothing known about YDB (%q): "+
			"declare its YDBVerdict in migrationRuleMeta", entry.Name, entry.YDB)}
	case everyDialect && strings.TrimSpace(entry.YDBNote) == "":
		return []string{fmt.Sprintf("rule %s has a YDB verdict and no note saying what it rests on", entry.Name)}
	case !everyDialect && (entry.YDB != "" || entry.YDBNote != ""):
		return []string{fmt.Sprintf("rule %s declares a YDB verdict but does not run on every dialect", entry.Name)}
	}
	return nil
}

// ydbVerdicts are the verdicts a rule may carry.
var ydbVerdicts = []YDBVerdict{YDBApplies, YDBNoSuchStatement, YDBReplaced, YDBNeedsDevDatabase, YDBProviderReads}
