package lintcatalog

// AtlasStatus is what Ptah does about one Atlas analyzer check.
type AtlasStatus string

const (
	// StatusCovered means a Ptah rule reports the same hazard. The identifier
	// it reports under is in the rule tables above; it is not always the Atlas
	// one.
	StatusCovered AtlasStatus = "covered"
	// StatusPartial means a Ptah rule reports part of the hazard, and the note
	// says which part is missing.
	StatusPartial AtlasStatus = "partial"
	// StatusAbsent means no Ptah rule reports it. The row exists so that a
	// reader looking the code up learns that, rather than finding nothing and
	// having to guess whether the code exists at all.
	StatusAbsent AtlasStatus = "not implemented"
	// StatusWaived means Ptah will not implement it, and the note says why.
	StatusWaived AtlasStatus = "waived"
)

// AtlasCheck is one analyzer check code Atlas documents, and what Ptah does
// about it.
//
// The list is Atlas's, taken from the analyzer documentation at
// https://atlasgo.io/lint/analyzers. Nothing here is measured from any Atlas
// binary; it is a reading of a published page, and the Pro column repeats what
// that page marks.
type AtlasCheck struct {
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
	Status AtlasStatus
	// Note qualifies a partial or waived status.
	Note string
}

// atlasChecks lists every check code the Atlas analyzer documentation carries,
// in the documentation's own family order.
//
// The Pro-marked rows and their statuses were audited against that page on
// July 28, 2026 and are carried here unchanged; the non-Pro rows were mapped
// afterwards against the same registries this package reads.
var atlasChecks = []AtlasCheck{
	{Code: "DS101", Meaning: "schema was dropped", PtahRules: []string{"DS107"}, Status: StatusCovered},
	{Code: "DS102", Meaning: "table was dropped", PtahRules: []string{"DS101"}, Status: StatusCovered},
	{Code: "DS103", Meaning: "non-virtual column was dropped", PtahRules: []string{"DS102"}, Status: StatusCovered},

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
		PtahRules: []string{"MF101"}, Status: StatusCovered,
		Note: "structural: the build fails on the first duplicate; the message names the query that settles it and what a failed CONCURRENTLY build leaves behind, and a unique index the dev database already holds over the columns silences it",
	},
	{
		Code: "MF102", Meaning: "modifying a non-unique index to unique",
		PtahRules: []string{"MF102"}, Status: StatusCovered,
		Note: "an index dropped earlier in the file and rebuilt as unique under the same name, or under a new name over the columns the dev database records for it; the message adds that the failure leaves the table without the index it had",
	},
	{Code: "MF103", Meaning: "adding a non-nullable column to an existing table", PtahRules: []string{"DD101"}, Status: StatusCovered},
	// DD103 reads the column's current nullability from the dev database,
	// because MODIFY and SQL Server's ALTER COLUMN restate the whole column
	// and the text cannot tell a change from a restatement
	// (migration/lint/notnull.go, stokaro/ptah#2942). The row stays partial
	// for the reason its note gives, not for the engines it covers.
	{
		Code: "MF104", Meaning: "modifying a nullable column to non-nullable might fail",
		PtahRules: []string{"PG303", "LT101", "DD103"}, Status: StatusPartial,
		Note: "PostgreSQL (PG303), SQLite (LT101), MySQL, MariaDB and SQL Server (DD103) are measured and covered; " +
			"CockroachDB and YugabyteDB run no PostgreSQL rule under their own dialect names, and ClickHouse and Spanner are not measured, so those four are not",
	},

	{Code: "BC101", Meaning: "renaming a table", PtahRules: []string{"BC101"}, Status: StatusCovered},
	{Code: "BC102", Meaning: "renaming a column", PtahRules: []string{"BC101"}, Status: StatusCovered, Note: "one rule reports both object kinds"},

	{Code: "MY101", Meaning: "adding a non-nullable column without a DEFAULT to an existing table", PtahRules: []string{"DD101"}, Status: StatusCovered, Note: "DD101 applies to every dialect"},
	{Code: "MY102", Meaning: "an inline REFERENCES clause in ADD COLUMN has no effect", PtahRules: []string{"MY102"}, Status: StatusCovered},

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
	{Code: "MY110", Meaning: "removing enum values from a column requires a table copy", PtahRules: []string{"MY110"}, Status: StatusCovered},
	{Code: "MY111", Meaning: "reordering enum values requires a table copy", PtahRules: []string{"MY111"}, Status: StatusCovered},
	{Code: "MY112", Meaning: "inserting enum values other than at the end requires a table copy", PtahRules: []string{"MY112"}, Status: StatusCovered},
	{Code: "MY113", Meaning: "exceeding 256 enum values changes storage size and requires a table copy", PtahRules: []string{"MY113"}, Status: StatusCovered},

	{Code: "MY120", Meaning: "removing set values from a column requires a table copy", PtahRules: []string{"MY120"}, Status: StatusCovered},
	{Code: "MY121", Meaning: "reordering set values requires a table copy", PtahRules: []string{"MY121"}, Status: StatusCovered},
	{Code: "MY122", Meaning: "inserting set values other than at the end requires a table copy", PtahRules: []string{"MY122"}, Status: StatusCovered},
	{Code: "MY123", Meaning: "exceeding a set-size boundary changes storage size and requires a table copy", PtahRules: []string{"MY123"}, Status: StatusCovered},

	// The three copies below were measured on MySQL 8.4 and MariaDB 11.8.9
	// with ALGORITHM=INSTANT, INPLACE and LOCK=NONE (migration/lint/mysqlcost.go).
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
		PtahRules: []string{"MY130"}, Status: StatusCovered,
		Note: "fires only for a change InnoDB refuses to apply in place, with the old and new type and the boundary, character set, collation or key that decides it",
	},
	{Code: "MY131", Meaning: "adding a foreign key blocks DML", Pro: true, PtahRules: []string{"MY131"}, Status: StatusCovered},
	{Code: "MY132", Meaning: "adding a primary key requires a table rebuild", Pro: true, PtahRules: []string{"MY132"}, Status: StatusCovered},
	{
		Code: "MY133", Meaning: "dropping a primary key without adding one requires a table copy", Pro: true,
		PtahRules: []string{"MY133", "CD103"}, Status: StatusCovered,
		Note: "MY133 names the copy and CD103 the lost uniqueness guarantee; the message names the MariaDB case where another NOT NULL UNIQUE key keeps the change in place",
	},
	{Code: "MY134", Meaning: "adding a FULLTEXT index blocks DML", Pro: true, PtahRules: []string{"MY134"}, Status: StatusCovered},
	{Code: "MY135", Meaning: "adding a SPATIAL index blocks DML", Pro: true, PtahRules: []string{"MY135"}, Status: StatusCovered},
	{
		Code: "MY136", Meaning: "changing the table character set requires a table rebuild", Pro: true,
		PtahRules: []string{"MY136"}, Status: StatusCovered,
		Note: "names the columns whose re-encoding forces the copy; a conversion that touches no column, or only utf8mb3 to utf8mb4 on short VARCHAR and CHAR columns no key covers, is not reported",
	},

	{Code: "LT101", Meaning: "modifying a nullable column to non-nullable without a DEFAULT", PtahRules: []string{"LT101"}, Status: StatusCovered},

	{Code: "PG101", Meaning: "index created without CONCURRENTLY", Pro: true, PtahRules: []string{"PG101"}, Status: StatusCovered},
	{Code: "PG102", Meaning: "index dropped without CONCURRENTLY", Pro: true, PtahRules: []string{"PG106"}, Status: StatusCovered},
	{
		Code: "PG103", Meaning: "concurrent operation without the atlas:txmode none header", Pro: true,
		PtahRules: []string{"PG103"}, Status: StatusCovered,
		Note: "the atlas:txmode none header and Ptah's own directive both silence it",
	},
	{Code: "PG104", Meaning: "PRIMARY KEY creation acquires an ACCESS EXCLUSIVE lock", Pro: true, PtahRules: []string{"PG104"}, Status: StatusCovered},
	{Code: "PG105", Meaning: "UNIQUE constraint creation acquires an ACCESS EXCLUSIVE lock", Pro: true, PtahRules: []string{"PG105"}, Status: StatusCovered},
	{Code: "PG110", Meaning: "creating a table with non-optimal data alignment", PtahRules: []string{"PG110"}, Status: StatusCovered},

	// Both rows below were measured on PostgreSQL 18.6 by relfilenode and the
	// heap-scan counter (migration/lint/pgcost.go). PG301 compares the
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
		PtahRules: []string{"PG301"}, Status: StatusCovered,
		Note: "fires for a change PostgreSQL rewrites for, naming the abort a value can cause, and for a collation change on an indexed column, naming the indexes it rebuilds; the timestamp to timestamptz pair says when the TimeZone decides",
	},
	{Code: "PG302", Meaning: "a volatile DEFAULT on an added column rewrites the table", Pro: true, PtahRules: []string{"PG302"}, Status: StatusCovered},
	{Code: "PG303", Meaning: "SET NOT NULL scans existing rows", Pro: true, PtahRules: []string{"PG303"}, Status: StatusCovered},
	{
		Code: "PG304", Meaning: "PRIMARY KEY on nullable columns requires a full scan", Pro: true,
		PtahRules: []string{"PG304", "PG104"}, Status: StatusCovered,
		Note: "PG304 names the columns the key sets NOT NULL and the extra scan that costs, for a column list and for USING INDEX alike; PG104 names the lock every ADD PRIMARY KEY takes",
	},
	{Code: "PG305", Meaning: "a CHECK constraint requires a full table scan", Pro: true, PtahRules: []string{"PG305"}, Status: StatusCovered},
	{Code: "PG306", Meaning: "a FOREIGN KEY requires a full scan and blocks writes", Pro: true, PtahRules: []string{"PG306"}, Status: StatusCovered},
	{Code: "PG307", Meaning: "a logging-mode change rewrites the table", Pro: true, PtahRules: []string{"PG307"}, Status: StatusCovered},
	{Code: "PG308", Meaning: "trigger creation acquires a SHARE ROW EXCLUSIVE lock", Pro: true, PtahRules: []string{"PG308"}, Status: StatusCovered},
	{Code: "PG309", Meaning: "a STORED generated column rewrites the table", Pro: true, PtahRules: []string{"PG309"}, Status: StatusCovered},
	{Code: "PG310", Meaning: "an identity column rewrites the table", Pro: true, PtahRules: []string{"PG310"}, Status: StatusCovered},
	{Code: "PG311", Meaning: "an access-method change rewrites the table", Pro: true, PtahRules: []string{"PG311"}, Status: StatusCovered},

	{Code: "CD101", Meaning: "a foreign-key constraint was dropped", Pro: true, PtahRules: []string{"CD101"}, Status: StatusCovered},
	{Code: "CD102", Meaning: "a check constraint was dropped", Pro: true, PtahRules: []string{"CD102"}, Status: StatusCovered},
	{Code: "CD103", Meaning: "a primary-key constraint was dropped", Pro: true, PtahRules: []string{"CD103"}, Status: StatusCovered},

	{Code: "TX101", Meaning: "statements cannot run in a single transaction", Pro: true, PtahRules: []string{"TX101"}, Status: StatusCovered},
	{Code: "TX201", Meaning: "a nested transaction was detected", Pro: true, PtahRules: []string{"TX201"}, Status: StatusCovered},

	// The six naming rules read the convention a project configures, as the
	// `naming` section of .ptah-lint.yaml or the `lint { naming { } }` block
	// of a project file, and stay silent without one: the convention is the
	// project's to state (migration/lint/naming.go, stokaro/ptah#2942).
	{Code: "NM101", Meaning: "a schema name violates the naming convention", PtahRules: []string{"NM101"}, Status: StatusCovered, Note: "needs a configured naming convention"},
	{Code: "NM102", Meaning: "a table name violates the naming convention", PtahRules: []string{"NM102"}, Status: StatusCovered, Note: "needs a configured naming convention"},
	{Code: "NM103", Meaning: "a column name violates the naming convention", PtahRules: []string{"NM103"}, Status: StatusCovered, Note: "needs a configured naming convention"},
	{
		Code: "NM104", Meaning: "an index name violates the naming convention", PtahRules: []string{"NM104"}, Status: StatusCovered,
		Note: "needs a configured naming convention; a unique or primary key constraint counts as an index, as it does for Atlas",
	},
	{Code: "NM105", Meaning: "a foreign-key constraint name violates the naming convention", PtahRules: []string{"NM105"}, Status: StatusCovered, Note: "needs a configured naming convention"},
	{Code: "NM106", Meaning: "a check constraint name violates the naming convention", PtahRules: []string{"NM106"}, Status: StatusCovered, Note: "needs a configured naming convention"},

	// What Atlas analyzes is dynamic SQL built by concatenation or
	// interpolation in EXEC and EXECUTE statements; what Ptah observes is the
	// tokenized body of a routine the migration defines, once a dialect names
	// its language: PL/pgSQL EXECUTE, MySQL PREPARE FROM, T-SQL EXEC and
	// sp_executesql (migration/lint/injection.go, stokaro/ptah#2942). A value
	// that reaches the routine from outside is not visible and not claimed.
	{
		Code: "SA101", Meaning: "a possible SQL injection vulnerability was detected",
		PtahRules: []string{"SA101"}, Status: StatusCovered,
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

// AtlasChecks returns every documented Atlas check code and Ptah's status on it.
func AtlasChecks() []AtlasCheck {
	out := make([]AtlasCheck, len(atlasChecks))
	copy(out, atlasChecks)
	return out
}

// atlasCheckFor finds one Atlas check by code.
func atlasCheckFor(code string) (AtlasCheck, bool) {
	for _, check := range atlasChecks {
		if check.Code == code {
			return check, true
		}
	}
	return AtlasCheck{}, false
}

// AtlasCounts totals the statuses, so the page's summary sentence is counted
// rather than remembered.
func AtlasCounts() map[AtlasStatus]int {
	counts := make(map[AtlasStatus]int)
	for _, check := range atlasChecks {
		counts[check.Status]++
	}
	return counts
}
