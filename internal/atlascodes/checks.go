package atlascodes

// Status is what Ptah does about one Atlas analyzer check.
type Status string

const (
	// StatusCovered means a Ptah rule reports the same hazard. The identifier
	// it reports under is in the rule tables above; it is not always the Atlas
	// one.
	StatusCovered Status = "covered"
	// StatusPartial means a Ptah rule reports part of the hazard, and the note
	// says which part is missing.
	StatusPartial Status = "partial"
	// StatusAbsent means no Ptah rule reports it. The row exists so that a
	// reader looking the code up learns that, rather than finding nothing and
	// having to guess whether the code exists at all.
	StatusAbsent Status = "not implemented"
	// StatusWaived means Ptah will not implement it, and the note says why.
	StatusWaived Status = "waived"
)

// Check is one analyzer check code Atlas documents, and what Ptah does
// about it.
//
// The list is Atlas's, taken from the analyzer documentation at
// https://atlasgo.io/lint/analyzers. Nothing here is measured from any Atlas
// binary; it is a reading of a published page, and the Pro column repeats what
// that page marks.
type Check struct {
	// Code is the Atlas check code.
	Code string
	// Meaning is what the Atlas documentation says it detects.
	Meaning string
	// Pro records that the Atlas documentation marks the check as an Atlas Pro
	// feature. Ptah reports what it implements through both surfaces
	// regardless, which is the point of listing them.
	Pro bool
	// PtahRules names the Ptah rule identifiers that report the hazard. Every
	// code here must be registered by a linter or [Validate] fails, so a rule
	// rename cannot leave a dangling claim.
	PtahRules []string
	// Status is what Ptah does about it.
	Status Status
	// Note qualifies a partial or waived status.
	Note string
}

// checks lists every check code the Atlas analyzer documentation carries,
// in the documentation's own family order.
//
// The Pro-marked rows and their statuses were audited against that page on
// July 28, 2026 and are carried here unchanged; the non-Pro rows were mapped
// afterwards against the same registries this package reads.
var checks = []Check{
	{Code: "DS101", Meaning: "schema was dropped", PtahRules: []string{"destructive/drop-object"}, Status: StatusCovered},
	{Code: "DS102", Meaning: "table was dropped", PtahRules: []string{"destructive/drop-table"}, Status: StatusCovered},
	{Code: "DS103", Meaning: "non-virtual column was dropped", PtahRules: []string{"destructive/drop-column"}, Status: StatusCovered},

	// Both rows below report the structural risk: the dev database a run
	// replays the directory on holds the schema and no rows, so a proven
	// violation is not claimed, and the message carries the GROUP BY that
	// proves or clears it. A table this migration creates, and a column it
	// adds without a DEFAULT, meet no duplicates and are not reported
	// (migration/lint/unique.go, stokaro/ptah#2942). The schema state the
	// version starts from refines both: a unique index or primary key that
	// already covers the key's columns proves the rows unique and silences
	// them, and a dropped index is known by its columns, so a unique rebuild
	// under a new name is MF102's replacement too (stokaro/ptah#2957). The
	// identifiers were Ptah's own missing-down and empty-file rules before
	// the convention; those are MF101P and MF102P now, so the Atlas checks
	// keep their codes.
	{
		Code: "MF101", Meaning: "adding a unique index to an existing column",
		PtahRules: []string{"data-dependent/add-unique-index"}, Status: StatusCovered,
		Note: "structural: the build fails on the first duplicate; the message names the query that settles it and what a failed CONCURRENTLY build leaves behind, and a unique index the dev database already holds over the columns silences it",
	},
	{
		Code: "MF102", Meaning: "modifying a non-unique index to unique",
		PtahRules: []string{"data-dependent/rebuild-index-as-unique"}, Status: StatusCovered,
		Note: "an index dropped earlier in the file and rebuilt as unique under the same name, or under a new name over the columns the dev database records for it; the message adds that the failure leaves the table without the index it had",
	},
	{Code: "MF103", Meaning: "adding a non-nullable column to an existing table", PtahRules: []string{"data-dependent/add-not-null-column"}, Status: StatusCovered},
	// DD103 reads the column's current nullability from the dev database,
	// because MODIFY, SQL Server's ALTER COLUMN and ClickHouse's MODIFY COLUMN
	// restate the whole column and the text cannot tell a change from a
	// restatement (migration/lint/notnull.go, stokaro/ptah#2942). PG303 names
	// the two built-in engines that speak PostgreSQL's wire protocol beside it,
	// each measured to fail SET NOT NULL on a NULL row, and DD103 names
	// ClickHouse, measured to fail the mutation and leave the table unreadable
	// (stokaro/ptah#2958). SP101 is the official Spanner provider's rule for the
	// same statement, recorded in internal/providerpublished.
	{
		Code: "MF104", Meaning: "modifying a nullable column to non-nullable might fail",
		PtahRules: []string{"locking/set-not-null", "rewrite/set-not-null", "data-dependent/set-not-null", "data-dependent/set-not-null"}, Status: StatusCovered,
		Note: "PG303 on PostgreSQL, CockroachDB and YugabyteDB, LT101 on SQLite, DD103 on MySQL, MariaDB, SQL Server and ClickHouse, SP101 on Spanner; every engine measured, each with its own failure named",
	},

	{Code: "BC101", Meaning: "renaming a table", PtahRules: []string{"rollout/rename"}, Status: StatusCovered},
	{Code: "BC102", Meaning: "renaming a column", PtahRules: []string{"rollout/rename"}, Status: StatusCovered, Note: "one rule reports both object kinds"},
	{
		Code: "BC103", Meaning: "dropping a table", PtahRules: []string{"rollout/drop-table"}, Status: StatusCovered,
		Note: "the rollout break, not the row loss DS101 reports on the same statement, and separately " +
			"suppressible: accepting the data loss is not accepting deployed clients failing. Native " +
			"`ptah migrations lint` only; no apply gate",
	},
	{
		Code: "BC104", Meaning: "dropping a column", PtahRules: []string{"rollout/drop-column"}, Status: StatusCovered,
		Note: "as BC103, beside DS102, and reported on the same surface; a column the migration created or added earlier is exempt from both, since no deployed client read it",
	},

	{Code: "MY101", Meaning: "adding a non-nullable column without a DEFAULT to an existing table", PtahRules: []string{"data-dependent/add-not-null-column"}, Status: StatusCovered, Note: "DD101 applies to every dialect"},
	{Code: "MY102", Meaning: "an inline REFERENCES clause in ADD COLUMN has no effect", PtahRules: []string{"side-effect/inline-reference"}, Status: StatusCovered},

	// MySQL removes an enum value by restating the whole member list in a
	// MODIFY COLUMN, which is why DS106 does not answer for this check: its
	// scan matches the PostgreSQL spellings, DROP VALUE and DELETE FROM
	// pg_enum. The list a column has is not in the statement either, so the
	// eight rules below read it from the schema state the version starts from.
	//
	// Measured on MySQL 8.4 and MariaDB 11.8.9 by asking for ALGORITHM=INSTANT
	// and INPLACE: removing, reordering and inserting a member are refused by
	// both, appending at the end is applied in place, and an append that
	// crosses 255 (ENUM) or a multiple of eight (SET) members is refused
	// again. Each rule compares the list the dev database reports with the
	// list the MODIFY or CHANGE assigns, and says which of those the
	// migration did; without that state the statement is still reported by
	// DS103 and MY101, and the run names these rules as unmet
	// (stokaro/ptah#2942).
	{Code: "MY110", Meaning: "removing enum values from a column requires a table copy", PtahRules: []string{"rewrite/remove-enum-value"}, Status: StatusCovered},
	{Code: "MY111", Meaning: "reordering enum values requires a table copy", PtahRules: []string{"rewrite/reorder-enum-values"}, Status: StatusCovered},
	{Code: "MY112", Meaning: "inserting enum values other than at the end requires a table copy", PtahRules: []string{"rewrite/insert-enum-value"}, Status: StatusCovered},
	{Code: "MY113", Meaning: "exceeding 256 enum values changes storage size and requires a table copy", PtahRules: []string{"rewrite/widen-enum"}, Status: StatusCovered},

	{Code: "MY120", Meaning: "removing set values from a column requires a table copy", PtahRules: []string{"rewrite/remove-set-member"}, Status: StatusCovered},
	{Code: "MY121", Meaning: "reordering set values requires a table copy", PtahRules: []string{"rewrite/reorder-set-members"}, Status: StatusCovered},
	{Code: "MY122", Meaning: "inserting set values other than at the end requires a table copy", PtahRules: []string{"rewrite/insert-set-member"}, Status: StatusCovered},
	{Code: "MY123", Meaning: "exceeding a set-size boundary changes storage size and requires a table copy", PtahRules: []string{"rewrite/widen-set"}, Status: StatusCovered},

	// The three copies below were measured on MySQL 8.4 and MariaDB 11.8.9
	// with ALGORITHM=INSTANT, INPLACE and LOCK=NONE (dialect/mysql/mysqllint/mysqlcost.go).
	// MY130 and MY136 compare the column's current type, character set and
	// collation, read from the dev database with the keys on the column, with
	// what the clause assigns, so a VARCHAR widened within one length-prefix
	// class or a utf8mb3 column converted to utf8mb4 is not reported as the
	// copy it is not, while a collation change or that same conversion on a
	// keyed column is reported as the copy MySQL makes of it; without that
	// state the statement is still reported by DS103 and MY101, and the run
	// names these rules as unmet (stokaro/ptah#2942, stokaro/ptah#2957).
	{
		Code: "MY130", Meaning: "changing a column type requires a table copy", Pro: true,
		PtahRules: []string{"rewrite/alter-column-type"}, Status: StatusCovered,
		Note: "fires only for a change InnoDB refuses to apply in place, with the old and new type and the boundary, character set, collation or key that decides it; MY130P is the info finding for the change applied in place",
	},
	{Code: "MY131", Meaning: "adding a foreign key blocks DML", Pro: true, PtahRules: []string{"locking/add-foreign-key"}, Status: StatusCovered},
	{Code: "MY132", Meaning: "adding a primary key requires a table rebuild", Pro: true, PtahRules: []string{"rewrite/add-primary-key"}, Status: StatusCovered},
	{
		Code: "MY133", Meaning: "dropping a primary key without adding one requires a table copy", Pro: true,
		PtahRules: []string{"rewrite/drop-primary-key", "integrity/drop-primary-key"}, Status: StatusCovered,
		Note: "MY133 names the copy and CD103 the lost uniqueness guarantee; the message names the MariaDB case where another NOT NULL UNIQUE key keeps the change in place",
	},
	{Code: "MY134", Meaning: "adding a FULLTEXT index blocks DML", Pro: true, PtahRules: []string{"locking/add-fulltext-index"}, Status: StatusCovered},
	{Code: "MY135", Meaning: "adding a SPATIAL index blocks DML", Pro: true, PtahRules: []string{"locking/add-spatial-index"}, Status: StatusCovered},
	{
		Code: "MY136", Meaning: "changing the table character set requires a table rebuild", Pro: true,
		PtahRules: []string{"rewrite/convert-charset"}, Status: StatusCovered,
		Note: "names the columns whose re-encoding forces the copy; a conversion that touches no column, or only utf8mb3 to utf8mb4 on short VARCHAR and CHAR columns no key covers, is not reported",
	},
	{
		Code: "MY137", Meaning: "modifying the primary key rebuilds the table and its secondary indexes", Pro: true,
		PtahRules: []string{"rewrite/replace-primary-key"}, Status: StatusCovered,
		Note: "one statement that drops and adds; measured in place with writes allowed, so it subsumes MY132 rather than repeating it, and leaves CD103 to report the identity loss",
	},
	{
		Code: "MY138", Meaning: "changing the storage engine requires a table copy and blocks DML", Pro: true,
		PtahRules: []string{"rewrite/change-engine"}, Status: StatusCovered,
		Note: "any ENGINE= clause, including one naming the engine the table already uses, which costs the same because it is the documented way to force a rebuild",
	},
	{
		Code: "MY139", Meaning: "partitioning or removing partitioning requires a table copy and blocks DML", Pro: true,
		PtahRules: []string{"rewrite/change-partitioning"}, Status: StatusCovered,
		Note: "measured, this statement form accepts no ALGORITHM or LOCK clause at all, so the finding says there is no online-DDL negotiation rather than naming a copy",
	},
	{
		Code: "MY140", Meaning: "adding a STORED generated column requires a table copy and blocks DML", Pro: true,
		PtahRules: []string{"rewrite/add-stored-generated-column"}, Status: StatusCovered,
	},
	{
		Code: "MY141", Meaning: "adding an AUTO_INCREMENT column rebuilds the table and blocks DML", Pro: true,
		PtahRules: []string{"locking/add-auto-increment-column"}, Status: StatusCovered,
		Note: "the rebuild is in place and writes still stop: LOCK=NONE is refused, which is why the finding reports cost and locking separately",
	},
	{
		Code: "MY142", Meaning: "adding a column before existing columns prevents an instant operation on older versions", Pro: true,
		Status: StatusAbsent,
		Note: "measured absent on every MySQL release line this repository declares: ADD COLUMN ... FIRST accepts ALGORITHM=INSTANT on 8.4.11 and on 8.0.46, and" +
			"the lowest declared line is 8.4, so a rule would be a false positive wherever Ptah is tested",
	},
	{
		Code: "MY143", Meaning: "modifying a generated column requires a table copy and blocks DML", Pro: true,
		PtahRules: []string{"rewrite/modify-generated-column"}, Status: StatusCovered,
		Note: "the MODIFY and CHANGE side; adding one is MY140, and DS103 still reports the type change where the declared type moves",
	},
	{
		Code: "MY144", Meaning: "adding a CHECK constraint scans all existing rows", Pro: true,
		PtahRules: []string{"locking/add-check"}, Status: StatusCovered,
	},
	{
		Code: "MY145", Meaning: "modifying or enforcing a CHECK constraint re-validates all existing rows", Pro: true,
		PtahRules: []string{"locking/enforce-check"}, Status: StatusCovered,
		Note: "MySQL only: MariaDB has no ENFORCED or NOT ENFORCED syntax, so a mariadb finding would describe a statement that server refuses to parse",
	},
	{
		Code: "MY146", Meaning: "dropping system versioning permanently deletes all history rows", Pro: true,
		PtahRules: []string{"destructive/drop-system-versioning"}, Status: StatusCovered,
		Note: "MariaDB only: MySQL 8.4 has no system versioning at all; reported as an error because the history is deleted rather than made expensive",
	},
	{
		Code: "MY147", Meaning: "changing column nullability requires a table rebuild", Pro: true,
		PtahRules: []string{"rewrite/set-not-null"}, Status: StatusCovered,
		Note: "the cost; measured in place with writes allowed. Whether an existing NULL fails the statement is the separate question DD103 answers from the baseline",
	},
	{
		Code: "MY148", Meaning: "changing a column character set or collation requires a table copy and blocks DML", Pro: true,
		PtahRules: []string{"rewrite/alter-column-type"}, Status: StatusPartial,
		Note: "MY130 names this consequence where it can prove the copy from the dev database; without that baseline, or on a spelling it cannot resolve to a before-and-after, the statement is not reported",
	},

	{Code: "LT101", Meaning: "modifying a nullable column to non-nullable without a DEFAULT", PtahRules: []string{"rewrite/set-not-null"}, Status: StatusCovered},

	{
		Code: "PG101", Meaning: "index created without CONCURRENTLY", Pro: true,
		PtahRules: []string{"locking/create-index"}, Status: StatusCovered,
		Note: "on a TimescaleDB hypertable, which refuses CONCURRENTLY, the message names the per-chunk build instead when the dev database shows the table is one; that build is not reported",
	},
	{Code: "PG102", Meaning: "index dropped without CONCURRENTLY", Pro: true, PtahRules: []string{"locking/drop-index"}, Status: StatusCovered},
	{
		Code: "PG103", Meaning: "concurrent operation without the atlas:txmode none header", Pro: true,
		PtahRules: []string{"transaction/concurrent-index"}, Status: StatusCovered,
		Note: "the atlas:txmode none header and Ptah's own directive both silence it, and so does a golang-migrate file that holds nothing else",
	},
	{Code: "PG104", Meaning: "PRIMARY KEY creation acquires an ACCESS EXCLUSIVE lock", Pro: true, PtahRules: []string{"locking/add-primary-key"}, Status: StatusCovered},
	{Code: "PG105", Meaning: "UNIQUE constraint creation acquires an ACCESS EXCLUSIVE lock", Pro: true, PtahRules: []string{"locking/add-unique-constraint"}, Status: StatusCovered},
	{Code: "PG110", Meaning: "creating a table with non-optimal data alignment", PtahRules: []string{"design/column-alignment"}, Status: StatusCovered},

	// Both rows below were measured on PostgreSQL 18.6 by relfilenode and the
	// heap-scan counter (dialect/postgres/pglint/pgcost.go). PG301 compares the
	// column's current type and collation, read from the dev database with
	// the indexes on the column, with the clause's, so a widening PostgreSQL
	// applies as a catalog edit is not reported as a rewrite and a collation
	// change is reported as the index rebuild it is; PG304 reads the key
	// columns' nullability the same way, and for a USING INDEX form the key
	// columns of the index it names. Without that state DS103 and PG104 keep
	// the statement and the run names these rules as unmet (stokaro/ptah#2942,
	// stokaro/ptah#2957).
	{
		Code: "PG301", Meaning: "a column type change requires a table and index rewrite", Pro: true,
		PtahRules: []string{"rewrite/alter-column-type"}, Status: StatusCovered,
		Note: "fires for a change PostgreSQL rewrites for, naming the abort a value can cause, and for a collation change on an indexed column, naming the indexes it rebuilds; PG301P is the info finding for a catalog edit",
	},
	{Code: "PG302", Meaning: "a volatile DEFAULT on an added column rewrites the table", Pro: true, PtahRules: []string{"rewrite/volatile-default"}, Status: StatusCovered},
	{Code: "PG303", Meaning: "SET NOT NULL scans existing rows", Pro: true, PtahRules: []string{"locking/set-not-null"}, Status: StatusCovered},
	{
		Code: "PG304", Meaning: "PRIMARY KEY on nullable columns requires a full scan", Pro: true,
		PtahRules: []string{"locking/add-primary-key-on-nullable", "locking/add-primary-key"}, Status: StatusCovered,
		Note: "PG304 names the columns the key sets NOT NULL and the extra scan that costs, for a column list and for USING INDEX alike; PG104 names the lock every ADD PRIMARY KEY takes",
	},
	{Code: "PG305", Meaning: "a CHECK constraint requires a full table scan", Pro: true, PtahRules: []string{"locking/add-check"}, Status: StatusCovered},
	{Code: "PG306", Meaning: "a FOREIGN KEY requires a full scan and blocks writes", Pro: true, PtahRules: []string{"locking/add-foreign-key"}, Status: StatusCovered},
	{Code: "PG307", Meaning: "a logging-mode change rewrites the table", Pro: true, PtahRules: []string{"rewrite/set-persistence"}, Status: StatusCovered},
	{Code: "PG308", Meaning: "trigger creation acquires a SHARE ROW EXCLUSIVE lock", Pro: true, PtahRules: []string{"locking/create-trigger"}, Status: StatusCovered},
	{Code: "PG309", Meaning: "a STORED generated column rewrites the table", Pro: true, PtahRules: []string{"rewrite/add-stored-generated-column"}, Status: StatusCovered},
	{Code: "PG310", Meaning: "an identity column rewrites the table", Pro: true, PtahRules: []string{"rewrite/add-identity-column"}, Status: StatusCovered},
	{Code: "PG311", Meaning: "an access-method change rewrites the table", Pro: true, PtahRules: []string{"rewrite/set-access-method"}, Status: StatusCovered},
	{
		Code: "PG108", Meaning: "an index on a partitioned table blocks writes on all its partitions", Pro: true,
		PtahRules: []string{"locking/partitioned-index"}, Status: StatusPartial,
		Note: "reported where the migration declares the parent PARTITION BY, or the dev database shows an earlier parent partitioned; " +
			"without a dev database PG101 reports it instead",
	},
	{
		Code: "PG109", Meaning: "an EXCLUDE constraint takes an ACCESS EXCLUSIVE lock and scans the table", Pro: true,
		PtahRules: []string{"locking/add-exclusion-constraint"}, Status: StatusCovered,
	},
	{
		Code: "PG312", Meaning: "redefining a primary key builds its unique index under an ACCESS EXCLUSIVE lock", Pro: true,
		PtahRules: []string{"locking/replace-primary-key"}, Status: StatusCovered,
		Note: "distinct from Ptah's own PG312P, which is about a SECURITY DEFINER routine and keeps its trailing P; the USING INDEX form builds nothing under the lock and is not reported",
	},
	{
		Code: "PG314", Meaning: "changing REPLICA IDENTITY to FULL or NOTHING risks the logical replication setup", Pro: true,
		PtahRules: []string{"rollout/weaken-replica-identity"}, Status: StatusCovered,
		Note: "FULL and NOTHING carry different consequences and are reported with different messages; DEFAULT and USING INDEX keep a usable row identity and are not reported",
	},
	{
		Code: "PG320", Meaning: "disabling autovacuum lets dead rows accumulate", Pro: true,
		PtahRules: []string{"design/disable-autovacuum"}, Status: StatusCovered,
		Note: "the one rule here whose hazard is not a lock or a rewrite: the statement takes only a SHARE UPDATE EXCLUSIVE lock and the cost is paid later",
	},

	{Code: "CD101", Meaning: "a foreign-key constraint was dropped", Pro: true, PtahRules: []string{"integrity/drop-foreign-key"}, Status: StatusCovered},
	{Code: "CD102", Meaning: "a check constraint was dropped", Pro: true, PtahRules: []string{"integrity/drop-check"}, Status: StatusCovered},
	{Code: "CD103", Meaning: "a primary-key constraint was dropped", Pro: true, PtahRules: []string{"integrity/drop-primary-key"}, Status: StatusCovered},

	{Code: "TX101", Meaning: "statements cannot run in a single transaction", Pro: true, PtahRules: []string{"transaction/mixed-statements"}, Status: StatusCovered},
	{Code: "TX201", Meaning: "a nested transaction was detected", Pro: true, PtahRules: []string{"transaction/explicit-transaction"}, Status: StatusCovered,
		Note: "reported on every engine the migrator applies, because the migrator refuses the file; a golang-migrate file on PostgreSQL, up or down, is read as the migrator runs its own transactions"},

	// The six naming rules read the convention a project configures, as the
	// `naming` section of .ptah-lint.yaml or the `lint { naming { } }` block
	// of a project file, and stay silent without one: the convention is the
	// project's to state (migration/lint/naming.go, stokaro/ptah#2942).
	{Code: "NM101", Meaning: "a schema name violates the naming convention", PtahRules: []string{"naming/schema-name"}, Status: StatusCovered, Note: "needs a configured naming convention"},
	{Code: "NM102", Meaning: "a table name violates the naming convention", PtahRules: []string{"naming/table-name"}, Status: StatusCovered, Note: "needs a configured naming convention"},
	{Code: "NM103", Meaning: "a column name violates the naming convention", PtahRules: []string{"naming/column-name"}, Status: StatusCovered, Note: "needs a configured naming convention"},
	{
		Code: "NM104", Meaning: "an index name violates the naming convention", PtahRules: []string{"naming/index-name"}, Status: StatusCovered,
		Note: "needs a configured naming convention; a unique or primary key constraint counts as an index, as it does for Atlas",
	},
	{Code: "NM105", Meaning: "a foreign-key constraint name violates the naming convention", PtahRules: []string{"naming/foreign-key-name"}, Status: StatusCovered, Note: "needs a configured naming convention"},
	{Code: "NM106", Meaning: "a check constraint name violates the naming convention", PtahRules: []string{"naming/check-name"}, Status: StatusCovered, Note: "needs a configured naming convention"},

	// What Atlas analyzes is dynamic SQL built by concatenation or
	// interpolation in EXEC and EXECUTE statements; what Ptah observes is the
	// tokenized body of a routine the migration defines, once a dialect names
	// its language: PL/pgSQL EXECUTE, MySQL PREPARE FROM, T-SQL EXEC and
	// sp_executesql (migration/lint/injection.go, stokaro/ptah#2942). A value
	// that reaches the routine from outside is not visible and not claimed.
	{
		Code: "SA101", Meaning: "a possible SQL injection vulnerability was detected",
		PtahRules: []string{"security/dynamic-sql-from-value"}, Status: StatusCovered,
		Note: "reports a routine body that builds its statement from an unquoted value; a literal text, quote_ident/quote_literal, format's %I and %L, QUOTENAME, and parameters are the safe forms it leaves alone",
	},

	{
		Code: "OW101", Meaning: "a user is not authorized to modify a resource", Pro: true, Status: StatusWaived,
		Note: "binds to a schema-ownership annotation set and an account model Ptah does not have",
	},
	{
		Code: "OW102", Meaning: "a user is explicitly denied access to a resource", Pro: true, Status: StatusWaived,
		Note: "same reason as OW101",
	},
}

// Checks returns every documented Atlas check code and Ptah's status on it.
func Checks() []Check {
	out := make([]Check, len(checks))
	copy(out, checks)
	return out
}

// checkFor finds one Atlas check by code.
func checkFor(code string) (Check, bool) {
	for _, check := range checks {
		if check.Code == code {
			return check, true
		}
	}
	return Check{}, false
}

// Counts totals the statuses, so the page's summary sentence is counted
// rather than remembered.
func Counts() map[Status]int {
	counts := make(map[Status]int)
	for _, check := range checks {
		counts[check.Status]++
	}
	return counts
}
