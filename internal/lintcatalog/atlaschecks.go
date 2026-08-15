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

	{Code: "MF101", Meaning: "adding a unique index to an existing column", Status: StatusAbsent},
	{Code: "MF102", Meaning: "modifying a non-unique index to unique", Status: StatusAbsent},
	{Code: "MF103", Meaning: "adding a non-nullable column to an existing table", PtahRules: []string{"DD101"}, Status: StatusCovered},
	{
		Code: "MF104", Meaning: "modifying a nullable column to non-nullable might fail",
		PtahRules: []string{"PG303", "LT101"}, Status: StatusPartial,
		Note: "reported on PostgreSQL and SQLite; the other dialects have no equivalent rule",
	},

	{Code: "BC101", Meaning: "renaming a table", PtahRules: []string{"BC101"}, Status: StatusCovered},
	{Code: "BC102", Meaning: "renaming a column", PtahRules: []string{"BC101"}, Status: StatusCovered, Note: "one rule reports both object kinds"},

	{Code: "MY101", Meaning: "adding a non-nullable column without a DEFAULT to an existing table", PtahRules: []string{"DD101"}, Status: StatusCovered, Note: "DD101 applies to every dialect"},
	{Code: "MY102", Meaning: "an inline REFERENCES clause in ADD COLUMN has no effect", PtahRules: []string{"MY102"}, Status: StatusCovered},

	// MySQL removes an enum value by restating the whole member list in a
	// MODIFY COLUMN, which is why DS106 does not answer for this check: its
	// scan matches the PostgreSQL spellings, DROP VALUE and DELETE FROM
	// pg_enum. Measured on `ALTER TABLE orders MODIFY COLUMN status
	// ENUM('new','paid') NOT NULL;` with --dialect mysql, the rules that fire
	// are DS103 and MY101.
	{
		Code: "MY110", Meaning: "removing enum values from a column requires a table copy",
		PtahRules: []string{"DS103", "MY101"}, Status: StatusPartial,
		Note: "the MODIFY COLUMN is reported as a column type change and a lock-heavy rebuild; the old and new member lists are not compared, so the removal itself has no code",
	},
	{Code: "MY111", Meaning: "reordering enum values requires a table copy", Status: StatusAbsent},
	{Code: "MY112", Meaning: "inserting enum values other than at the end requires a table copy", Status: StatusAbsent},
	{Code: "MY113", Meaning: "exceeding 256 enum values changes storage size and requires a table copy", Status: StatusAbsent},

	{Code: "MY120", Meaning: "removing set values from a column requires a table copy", Status: StatusAbsent},
	{Code: "MY121", Meaning: "reordering set values requires a table copy", Status: StatusAbsent},
	{Code: "MY122", Meaning: "inserting set values other than at the end requires a table copy", Status: StatusAbsent},
	{Code: "MY123", Meaning: "exceeding a set-size boundary changes storage size and requires a table copy", Status: StatusAbsent},

	{
		Code: "MY130", Meaning: "changing a column type requires a table copy", Pro: true,
		PtahRules: []string{"MY101", "DS103"}, Status: StatusPartial,
		Note: "MODIFY and CHANGE are reported as lock-heavy DDL; the table-copy cost has no code",
	},
	{Code: "MY131", Meaning: "adding a foreign key blocks DML", Pro: true, PtahRules: []string{"MY131"}, Status: StatusCovered},
	{Code: "MY132", Meaning: "adding a primary key requires a table rebuild", Pro: true, PtahRules: []string{"MY132"}, Status: StatusCovered},
	{
		Code: "MY133", Meaning: "dropping a primary key without adding one requires a table copy", Pro: true,
		PtahRules: []string{"CD103"}, Status: StatusPartial,
		Note: "the drop is reported; the table-copy cost has no code",
	},
	{Code: "MY134", Meaning: "adding a FULLTEXT index blocks DML", Pro: true, PtahRules: []string{"MY134"}, Status: StatusCovered},
	{Code: "MY135", Meaning: "adding a SPATIAL index blocks DML", Pro: true, PtahRules: []string{"MY135"}, Status: StatusCovered},
	{
		Code: "MY136", Meaning: "changing the table character set requires a table rebuild", Pro: true,
		PtahRules: []string{"MY101"}, Status: StatusPartial,
		Note: "only the CONVERT TO CHARACTER SET and CONVERT TO CHARSET spellings are scanned",
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

	{
		Code: "PG301", Meaning: "a column type change requires a table and index rewrite", Pro: true,
		PtahRules: []string{"DS103"}, Status: StatusPartial,
		Note: "reported as a data-safety risk, without rewrite and lock analysis",
	},
	{Code: "PG302", Meaning: "a volatile DEFAULT on an added column rewrites the table", Pro: true, PtahRules: []string{"PG302"}, Status: StatusCovered},
	{Code: "PG303", Meaning: "SET NOT NULL scans existing rows", Pro: true, PtahRules: []string{"PG303"}, Status: StatusCovered},
	{
		Code: "PG304", Meaning: "PRIMARY KEY on nullable columns requires a full scan", Pro: true,
		PtahRules: []string{"PG104"}, Status: StatusPartial,
		Note: "every ADD PRIMARY KEY is reported; the nullable-column refinement needs schema state",
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

	{Code: "NM101", Meaning: "a schema name violates the naming convention", Status: StatusAbsent},
	{Code: "NM102", Meaning: "a table name violates the naming convention", Status: StatusAbsent},
	{Code: "NM103", Meaning: "a column name violates the naming convention", Status: StatusAbsent},
	{Code: "NM104", Meaning: "an index name violates the naming convention", Status: StatusAbsent},
	{Code: "NM105", Meaning: "a foreign-key constraint name violates the naming convention", Status: StatusAbsent},
	{Code: "NM106", Meaning: "a check constraint name violates the naming convention", Status: StatusAbsent},

	{Code: "SA101", Meaning: "a possible SQL injection vulnerability was detected", Status: StatusAbsent},

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
	counts := map[AtlasStatus]int{}
	for _, check := range atlasChecks {
		counts[check.Status]++
	}
	return counts
}
