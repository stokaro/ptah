package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// analyzeTypeChange lints one migration against the two-table fixture the
// cost cases use, with whatever state the row supplies. The two in-place
// info rules are disabled so that each row measures DS103's own judgment:
// where a change is applied in place they subsume DS103, which
// inplace_test.go pins on its own.
func analyzeTypeChange(c *qt.C, dialect, alter string, columns ...lint.BaselineColumn) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(costFS(alter)), lint.Options{
		Dialect:   dialect,
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:  columns,
		Disabled:  []string{"MY130P", "PG301P"},
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

func ds103Codes(codes []string) []string {
	var kept []string
	for _, code := range codes {
		if code == "DS103" {
			kept = append(kept, code)
		}
	}
	return kept
}

// TestColumnTypeChangedRule_StaysQuietWhereTheStateShowsNoTypeChange pins
// the refinement: a clause that restates the column's current type, in any
// spelling the server folds to the same storage, changed no type.
func TestColumnTypeChangedRule_StaysQuietWhereTheStateShowsNoTypeChange(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
		columns []lint.BaselineColumn
	}{
		{
			name:    "MODIFY that only sets NOT NULL",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL;",
			columns: []lint.BaselineColumn{costColumn("total", "int", "", "latin1")},
		},
		{
			name:    "the display width and a synonym fold away",
			dialect: "mariadb",
			alter:   "ALTER TABLE orders MODIFY total INTEGER(11) DEFAULT 0;",
			columns: []lint.BaselineColumn{costColumn("total", "int(11)", "", "latin1")},
		},
		{
			name:    "the same character set spelled out",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) CHARACTER SET latin1 COMMENT 'x';",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "latin1", "latin1")},
		},
		{
			name:    "a collation change alone is not a type change",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "latin1", "latin1")},
		},
		{
			name:    "CHANGE that only renames",
			dialect: "mysql",
			alter:   "ALTER TABLE orders CHANGE total amount INT;",
			columns: []lint.BaselineColumn{costColumn("total", "int", "", "latin1")},
		},
		{
			name:    "the same member list restated",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY kind ENUM('a','b') NOT NULL;",
			columns: []lint.BaselineColumn{costColumn("kind", "enum('a','b')", "latin1", "latin1")},
		},
		{
			name:    "two clauses that both restate",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL, MODIFY note VARCHAR(10) NULL;",
			columns: []lint.BaselineColumn{costColumn("total", "int", "", "latin1"), costColumn("note", "varchar(10)", "latin1", "latin1")},
		},
		{
			name:    "PostgreSQL restates the type",
			dialect: "postgres",
			alter:   "ALTER TABLE orders ALTER COLUMN total TYPE integer;",
			columns: []lint.BaselineColumn{pgColumn("total", "integer", false)},
		},
		{
			name:    "PostgreSQL restates the type through a synonym and SET DATA TYPE",
			dialect: "postgres",
			alter:   "ALTER TABLE orders ALTER COLUMN total SET DATA TYPE int4;",
			columns: []lint.BaselineColumn{pgColumn("total", "integer", false)},
		},
		{
			name:    "a PostgreSQL collation change alone is not a type change",
			dialect: "postgres",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "C";`,
			columns: []lint.BaselineColumn{pgColumn("body", "text", false)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTypeChange(c, test.dialect, test.alter, test.columns...)
			c.Assert(ds103Codes(rulesOf(analysis.Findings())), qt.HasLen, 0)
			c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
		})
	}
}

// TestColumnTypeChangedRule_KeepsReportingWhereTheStateSettlesNothing pins
// the fail-closed side: without the state, with a column or spelling the
// state cannot resolve, with a clause the site readers do not recognize, on
// a dialect with no type comparison, and for a change that is real but not
// one a measured rule subsumes.
func TestColumnTypeChangedRule_KeepsReportingWhereTheStateSettlesNothing(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
		columns []lint.BaselineColumn
	}{
		{
			name:    "no state at all",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL;",
		},
		{
			name:    "a column the state does not know",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY other INT NOT NULL;",
			columns: []lint.BaselineColumn{costColumn("total", "int", "", "latin1")},
		},
		{
			name:    "a spelling the state cannot parse",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL;",
			columns: []lint.BaselineColumn{costColumn("total", "", "", "latin1")},
		},
		{
			name:    "one restating clause beside one that changes",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL, MODIFY note VARCHAR(20);",
			columns: []lint.BaselineColumn{costColumn("total", "int", "", "latin1"), costColumn("note", "varchar(10)", "latin1", "latin1")},
		},
		{
			name:    "a clause the site readers do not recognize",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL, MODIFY note;",
			columns: []lint.BaselineColumn{costColumn("total", "int", "", "latin1"), costColumn("note", "varchar(10)", "latin1", "latin1")},
		},
		{
			name:    "a character set conversion the server applies in place is still a type change",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) CHARACTER SET utf8mb4;",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3")},
		},
		{
			name:    "an in-place widening is still a type change",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20);",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "latin1", "latin1")},
		},
		{
			name:    "a PostgreSQL widening is still a type change",
			dialect: "postgres",
			alter:   "ALTER TABLE orders ALTER COLUMN note TYPE varchar(20);",
			columns: []lint.BaselineColumn{pgColumn("note", "character varying(10)", false)},
		},
		{
			name:    "a dialect with no type comparison",
			dialect: "sqlite",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL;",
			columns: []lint.BaselineColumn{costColumn("total", "int", "", "")},
		},
		{
			name:    "no dialect to compare with",
			dialect: "",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL;",
			columns: []lint.BaselineColumn{costColumn("total", "int", "", "latin1")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTypeChange(c, test.dialect, test.alter, test.columns...)
			c.Assert(ds103Codes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"DS103"})
		})
	}
}

// TestColumnTypeChangedRule_NamesItsInputWhenTheRunSuppliesNone: the rule
// still reports from the text, and the run says the refinement was missing.
func TestColumnTypeChangedRule_NamesItsInputWhenTheRunSuppliesNone(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeTypeChange(c, "mysql", "ALTER TABLE orders MODIFY total INT NOT NULL;")
	c.Assert(ds103Codes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"DS103"})
	var unmet []string
	for _, entry := range analysis.UnmetInputs() {
		unmet = append(unmet, entry.Rule+":"+entry.Input.String())
	}
	c.Assert(unmet, qt.Contains, "DS103:baseline schema that refines the statement text")
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{2})
}

// TestColumnTypeChangedRule_StillAnswersTheStatementDirective: the finding
// keeps its statement context, so a ptah:nolint above the statement
// silences it as before.
func TestColumnTypeChangedRule_StillAnswersTheStatementDirective(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeTypeChange(c, "mysql", "-- ptah:nolint DS103\nALTER TABLE orders MODIFY total BIGINT;")
	c.Assert(ds103Codes(rulesOf(analysis.Findings())), qt.HasLen, 0)
}
