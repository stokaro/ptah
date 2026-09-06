package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// notNullFS is the fixture: a table whose column may or may not be nullable
// today, which only the dev database knows, then the migration under test.
func notNullFS(migration string) map[string]string {
	return map[string]string{
		"1_base.sql":   "CREATE TABLE orders (id int NOT NULL, total int);",
		"2_change.sql": migration,
	}
}

func notNullColumn(name, columnType string, notNull bool) lint.BaselineColumn {
	return lint.BaselineColumn{Version: 2, Table: "orders", Name: name, ColumnType: columnType, NotNull: notNull}
}

func analyzeNotNull(c *qt.C, dialect, migration string, baseline ...lint.BaselineColumn) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(notNullFS(migration)), lint.Options{
		Dialect:   dialect,
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:  baseline,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// TestNullableMadeNotNullRule_ReportsTheColumnsThatWereNullable pins DD103
// to the engines whose spelling restates the column, with the consequence
// each was measured to have.
func TestNullableMadeNotNullRule_ReportsTheColumnsThatWereNullable(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
		want      []string
	}{
		{
			name:      "MySQL MODIFY",
			dialect:   "mysql",
			migration: "ALTER TABLE orders MODIFY total INT NOT NULL;",
			want: []string{
				"MODIFY COLUMN orders.total makes a nullable column NOT NULL: a row holding NULL fails the rebuild under the strict SQL mode MySQL defaults to (Invalid use of NULL value)",
				"silently rewritten to the type's implicit default (0, '', or the zero date) when strict mode is off",
				"UPDATE orders SET total = ... WHERE total IS NULL",
			},
		},
		{
			name:      "MySQL MODIFY with a DEFAULT, which helps no existing row",
			dialect:   "mysql",
			migration: "ALTER TABLE orders MODIFY COLUMN total INT NOT NULL DEFAULT 0;",
			want:      []string{"a DEFAULT in the same clause applies to new rows only"},
		},
		{
			name:      "MySQL CHANGE",
			dialect:   "mysql",
			migration: "ALTER TABLE orders CHANGE total amount INT NOT NULL;",
			want:      []string{"CHANGE COLUMN orders.total makes a nullable column NOT NULL"},
		},
		{
			name:      "MariaDB names its own error",
			dialect:   "mariadb",
			migration: "ALTER TABLE orders MODIFY total INT NOT NULL;",
			want:      []string{"strict SQL mode MariaDB defaults to (Data truncated for column)"},
		},
		{
			name:      "SQL Server ALTER COLUMN",
			dialect:   "sqlserver",
			migration: "ALTER TABLE orders ALTER COLUMN total int NOT NULL;",
			want: []string{
				"ALTER COLUMN orders.total makes a nullable column NOT NULL: SQL Server scans the table once to check every row",
				"Msg 515, Cannot insert the value NULL into column",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeNotNull(c, test.dialect, test.migration, notNullColumn("total", "int", false))
			c.Assert(rulesOf(analysis.Findings()), qt.Contains, "DD103")
			message := messageOf(analysis.Findings(), "DD103")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

func TestNullableMadeNotNullRule_StaysQuietWhereNoRowCanChange(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
		column    lint.BaselineColumn
	}{
		{
			name:      "the column is NOT NULL already",
			dialect:   "mysql",
			migration: "ALTER TABLE orders MODIFY total INT NOT NULL;",
			column:    notNullColumn("total", "int", true),
		},
		{
			name:      "the clause does not say NOT NULL",
			dialect:   "mysql",
			migration: "ALTER TABLE orders MODIFY total BIGINT;",
			column:    notNullColumn("total", "int", false),
		},
		{
			name:      "the clause says NULL",
			dialect:   "mysql",
			migration: "ALTER TABLE orders MODIFY total INT NULL;",
			column:    notNullColumn("total", "int", false),
		},
		{
			name:      "a column the baseline does not know",
			dialect:   "mysql",
			migration: "ALTER TABLE orders MODIFY other INT NOT NULL;",
			column:    notNullColumn("total", "int", false),
		},
		{
			name:      "SQL Server, the column is NOT NULL already",
			dialect:   "sqlserver",
			migration: "ALTER TABLE orders ALTER COLUMN total int NOT NULL;",
			column:    notNullColumn("total", "int", true),
		},
		{
			name:      "the PostgreSQL spelling belongs to PG303",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ALTER COLUMN total SET NOT NULL;",
			column:    notNullColumn("total", "integer", false),
		},
		{
			// SQL Server has no SET NOT NULL; the statement fails to parse
			// there, which is not what this rule is about.
			name:      "the PostgreSQL spelling under a SQL Server run restates no column",
			dialect:   "sqlserver",
			migration: "ALTER TABLE orders ALTER COLUMN total SET NOT NULL;",
			column:    notNullColumn("total", "int", false),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeNotNull(c, test.dialect, test.migration, test.column)
			c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "DD103")
		})
	}
}

// TestNullableMadeNotNullRule_NamesItsInputWhenTheRunSuppliesNone: the text
// cannot tell a column made NOT NULL from one that was, so without the
// schema state the rule says nothing and the run names it as unmet.
func TestNullableMadeNotNullRule_NamesItsInputWhenTheRunSuppliesNone(t *testing.T) {
	c := qt.New(t)

	analysis := analyzeNotNull(c, "mysql", "ALTER TABLE orders MODIFY total INT NOT NULL;")

	c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "DD103")
	var unmet []string
	for _, input := range analysis.UnmetInputs() {
		unmet = append(unmet, input.Rule)
	}
	c.Assert(unmet, qt.Contains, "DD103")
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{2})
}

// TestNullableMadeNotNullRule_AnswersTheAtlasCode: MF104 in a config reaches
// every rule that reports the hazard, this one included.
func TestNullableMadeNotNullRule_AnswersTheAtlasCode(t *testing.T) {
	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(fixture(notNullFS("ALTER TABLE orders MODIFY total INT NOT NULL;")), lint.Options{
		Dialect:   "mysql",
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:  []lint.BaselineColumn{notNullColumn("total", "int", false)},
		Disabled:  []string{"MF104"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "DD103")
}
