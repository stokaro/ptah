package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/safety"
)

func TestPlanFileWithStatementsFromSQLReclassifiesStatements(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "original",
		Dialect:         "sqlite",
		FromFingerprint: "sha256:" + fingerprintHex('a'),
		ToFingerprint:   "sha256:" + fingerprintHex('b'),
		Destructive:     false,
		Statements: []atlasschema.PlanStatement{
			{SQL: `CREATE TABLE additive (id INTEGER)`, Severity: safety.Safe},
		},
	}

	edited := plan.WithStatementsFromSQL("DROP TABLE victim;\nCREATE TABLE fresh (id INTEGER);\n")

	// An edit that introduces a DROP must not be saved carrying the pre-edit
	// destructive=false verdict; the marker and the per-statement severity are
	// re-derived from the SQL the plan actually holds.
	c.Assert(edited.Statements, qt.HasLen, 2)
	c.Assert(edited.Statements[0].SQL, qt.Contains, "DROP TABLE victim")
	c.Assert(edited.Statements[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(edited.Statements[0].Reason, qt.Not(qt.Equals), "")
	c.Assert(edited.Statements[1].Severity, qt.Equals, safety.Safe)
	c.Assert(edited.Destructive, qt.IsTrue)
}

func TestPlanFileWithStatementsFromSQLKeepsIdentityAndFingerprints(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "original",
		Dialect:         "sqlite",
		FromFingerprint: "sha256:" + fingerprintHex('a'),
		ToFingerprint:   "sha256:" + fingerprintHex('b'),
		Exclude:         []string{"legacy_*"},
		Statements:      []atlasschema.PlanStatement{{SQL: `CREATE TABLE additive (id INTEGER)`}},
	}

	edited := plan.WithStatementsFromSQL("CREATE TABLE replaced (id INTEGER);")

	// Only the statements change. `from` binds the plan to the live source
	// database, and re-deriving it here would silently defeat apply-time
	// staleness detection.
	c.Assert(edited.FromFingerprint, qt.Equals, plan.FromFingerprint)
	c.Assert(edited.ToFingerprint, qt.Equals, plan.ToFingerprint)
	c.Assert(edited.Name, qt.Equals, "original")
	c.Assert(edited.Dialect, qt.Equals, "sqlite")
	c.Assert(edited.FormatVersion, qt.Equals, atlasschema.PlanFormatVersion)
	c.Assert(edited.Exclude, qt.DeepEquals, []string{"legacy_*"})
}

func TestPlanFileWithStatementsFromSQLDoesNotMutateTheReceiver(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{
		Dialect:     "sqlite",
		Destructive: false,
		Statements:  []atlasschema.PlanStatement{{SQL: `CREATE TABLE additive (id INTEGER)`, Severity: safety.Safe}},
	}

	_ = plan.WithStatementsFromSQL("DROP TABLE victim;")

	// The method returns a copy. A caller that discards the result on a later
	// validation failure must still hold the plan it started with.
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, "additive")
	c.Assert(plan.Destructive, qt.IsFalse)
}

func TestPlanFileWithStatementsFromSQLSplitsWithThePlansOwnDialect(t *testing.T) {
	c := qt.New(t)
	// A lone backslash before the closing quote escapes it in the MySQL family
	// and does not in SQLite, so the same text is one statement there and two
	// here. This is the input that separates "the plan's dialect reaches the
	// splitter" from "some fixed dialect does" — a plan whose dialect is
	// dropped would split every edit the SQLite way.
	const edited = `INSERT INTO t VALUES ('a\'); SELECT 1;`

	tests := []struct {
		name    string
		dialect string
		want    int
	}{
		{name: "sqlite", dialect: "sqlite", want: 2},
		{name: "postgres", dialect: "postgres", want: 2},
		{name: "mysql", dialect: "mysql", want: 1},
		{name: "mariadb", dialect: "mariadb", want: 1},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			plan := atlasschema.PlanFile{Dialect: tt.dialect}

			got := plan.WithStatementsFromSQL(edited)

			c.Assert(got.Statements, qt.HasLen, tt.want)
		})
	}
}

func TestPlanFileWithStatementsFromSQLRoundTripsItsOwnSQLExactly(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{
		Dialect: "sqlite",
		Statements: []atlasschema.PlanStatement{
			{
				SQL:      "-- WARNING: This will delete all data in table \"victim\"!\nDROP TABLE IF EXISTS \"victim\"",
				Severity: safety.Destructive,
				Reason:   "drops a table",
			},
			{SQL: "CREATE TABLE \"fresh\" (\n  \"id\" INTEGER PRIMARY KEY\n)", Severity: safety.Safe},
		},
	}

	got := plan.WithStatementsFromSQL(plan.SQL())

	// Feeding a plan its own SQL back must be the identity. This is what makes
	// `--edit` safe when the operator quits the editor without typing: the
	// Atlas .plan.hcl shape has no severity field, so the generated
	// "-- WARNING" comment is the only in-artifact signal that the plan
	// destroys data, and a split that stripped comments would erase it.
	c.Assert(got.Statements, qt.HasLen, 2)
	c.Assert(got.Statements[0].SQL, qt.Equals, plan.Statements[0].SQL)
	c.Assert(got.Statements[1].SQL, qt.Equals, plan.Statements[1].SQL)
	c.Assert(got.SQL(), qt.Equals, plan.SQL())
	c.Assert(got.Destructive, qt.IsTrue)
}

func TestPlanFileWithStatementsFromSQLClassifiesPastLeadingComments(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{Dialect: "sqlite"}

	got := plan.WithStatementsFromSQL("-- this comment mentions CREATE TABLE\nDROP TABLE victim;")

	// The comment is kept in the statement text but must not steer the
	// classifier: severity comes from the executable body alone.
	c.Assert(got.Statements, qt.HasLen, 1)
	c.Assert(got.Statements[0].SQL, qt.Contains, "-- this comment mentions CREATE TABLE")
	c.Assert(got.Statements[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(got.Destructive, qt.IsTrue)
}

func TestPlanFileWithStatementsFromSQLClassifiesExecutableComments(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{name: "MySQL versioned comment", dialect: "mysql", sql: "/*!50003 DROP TABLE victim */;"},
		{name: "MariaDB native comment", dialect: "mariadb", sql: "/*M!100100 DROP TABLE victim */;"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			plan := atlasschema.PlanFile{Dialect: test.dialect}.WithStatementsFromSQL(test.sql)

			c.Assert(plan.Statements, qt.HasLen, 1)
			c.Assert(plan.Statements[0].Severity, qt.Equals, safety.Destructive)
			c.Assert(plan.Statements[0].Reason, qt.Equals, "DROP TABLE removes the table and all rows")
			c.Assert(plan.Destructive, qt.IsTrue)
		})
	}
}

func TestPlanFileWithStatementsFromSQLDropsCommentOnlyEdits(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{
		Dialect:    "sqlite",
		Statements: []atlasschema.PlanStatement{{SQL: `CREATE TABLE additive (id INTEGER)`}},
	}

	got := plan.WithStatementsFromSQL("-- the operator removed everything\n\n")

	// Comment-only text carries no statement, so HasChanges goes false and the
	// caller can refuse to save rather than write a plan that applies cleanly
	// and changes nothing.
	c.Assert(got.Statements, qt.HasLen, 0)
	c.Assert(got.HasChanges(), qt.IsFalse)
}

// fingerprintHex builds a 64-character hex digest body from a repeated digit,
// so a test fingerprint parses as sha256 without hard-coding a real hash.
func fingerprintHex(fill byte) string {
	out := make([]byte, 64)
	for i := range out {
		out[i] = fill
	}
	return string(out)
}
