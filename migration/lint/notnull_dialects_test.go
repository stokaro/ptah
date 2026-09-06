package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

func pg303Codes(codes []string) []string {
	var kept []string
	for _, code := range codes {
		if code == "PG303" {
			kept = append(kept, code)
		}
	}
	return kept
}

// TestSetNotNullRule_NamesEachEngineItWasMeasuredOn pins PG303 to the four
// engines that take the SET NOT NULL spelling, with the consequence each was
// measured to have. Every other PG rule still names postgres alone.
func TestSetNotNullRule_NamesEachEngineItWasMeasuredOn(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: "postgres", want: "scans the whole table under an ACCESS EXCLUSIVE lock"},
		{dialect: "cockroachdb", want: "validates every existing row in a schema-change job, and a row holding NULL fails it (validation of column NOT NULL failed on row, SQLSTATE 23502)"},
		{dialect: "yugabytedb", want: "checks every existing row, and a row holding NULL aborts it (column contains null values)"},
		{dialect: "spanner", want: "runs as a schema change that validates every existing row, and a row holding NULL fails it (FAILED_PRECONDITION: Cannot specify a null value for column) and leaves the column nullable"},
		{dialect: "", want: "scans the whole table under an ACCESS EXCLUSIVE lock"},
	}

	for _, test := range tests {
		t.Run("dialect "+test.dialect, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeNotNull(c, test.dialect, "ALTER TABLE orders ALTER COLUMN total SET NOT NULL;")
			c.Assert(pg303Codes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"PG303"})
			c.Assert(messageOf(analysis.Findings(), "PG303"), qt.Contains, test.want)
		})
	}
}

// TestSetNotNullRule_StaysOffTheOtherEngines: the spelling belongs to LT101
// on SQLite and means nothing on the MySQL family and SQL Server.
func TestSetNotNullRule_StaysOffTheOtherEngines(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql", "mariadb", "sqlserver", "clickhouse"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeNotNull(c, dialect, "ALTER TABLE orders ALTER COLUMN total SET NOT NULL;")
			c.Assert(pg303Codes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestSetNotNullRule_StillAnswersTheStatementDirective: the finding keeps
// its statement context, so a directive above the statement silences it.
func TestSetNotNullRule_StillAnswersTheStatementDirective(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeNotNull(c, "cockroachdb", "-- ptah:nolint PG303\nALTER TABLE orders ALTER COLUMN total SET NOT NULL;")
	c.Assert(pg303Codes(rulesOf(analysis.Findings())), qt.HasLen, 0)
}

// TestNullableMadeNotNullRule_ReadsClickHouseNullabilityFromTheType pins
// DD103 on ClickHouse, where a MODIFY COLUMN to a type that is not Nullable
// is the NOT NULL, and the failure is the measured broken mutation.
func TestNullableMadeNotNullRule_ReadsClickHouseNullabilityFromTheType(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeNotNull(c, "clickhouse", "ALTER TABLE orders MODIFY COLUMN total Int32;", notNullColumn("total", "Nullable(Int32)", false))
	c.Assert(rulesOf(analysis.Findings()), qt.Contains, "DD103")
	message := messageOf(analysis.Findings(), "DD103")
	c.Assert(message, qt.Contains, "MODIFY COLUMN orders.total makes a nullable column NOT NULL")
	c.Assert(message, qt.Contains, "mutation that fails on the first NULL (CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN)")
	c.Assert(message, qt.Contains, "every read of the table fails with the same error until the mutation is killed")
	c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
}

// TestNullableMadeNotNullRule_StaysQuietOnClickHouseWhereNothingBecomesNotNull
// pins the quiet ClickHouse shapes: a column that is not Nullable already,
// a MODIFY to a Nullable type, a type the parser cannot read, and the same
// text on an engine whose NOT NULL is a keyword.
func TestNullableMadeNotNullRule_StaysQuietOnClickHouseWhereNothingBecomesNotNull(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
		column    lint.BaselineColumn
	}{
		{
			name:      "already non-Nullable",
			dialect:   "clickhouse",
			migration: "ALTER TABLE orders MODIFY COLUMN total Int64;",
			column:    notNullColumn("total", "Int32", true),
		},
		{
			name:      "modified to a Nullable type",
			dialect:   "clickhouse",
			migration: "ALTER TABLE orders MODIFY COLUMN total Nullable(Int64);",
			column:    notNullColumn("total", "Nullable(Int32)", false),
		},
		{
			name:      "a wrapped type the parser does not read",
			dialect:   "clickhouse",
			migration: "ALTER TABLE orders MODIFY COLUMN total LowCardinality(String);",
			column:    notNullColumn("total", "Nullable(String)", false),
		},
		{
			name:      "the ClickHouse spelling on MySQL says nothing about NOT NULL",
			dialect:   "mysql",
			migration: "ALTER TABLE orders MODIFY COLUMN total INT;",
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

// TestNullableMadeNotNullRule_NamesItsInputOnClickHouse: without the state
// the rule cannot tell Nullable from not, and says so.
func TestNullableMadeNotNullRule_NamesItsInputOnClickHouse(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeNotNull(c, "clickhouse", "ALTER TABLE orders MODIFY COLUMN total Int32;")
	c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "DD103")
	var unmet []string
	for _, entry := range analysis.UnmetInputs() {
		unmet = append(unmet, entry.Rule)
	}
	c.Assert(unmet, qt.Contains, "DD103")
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{2})
}
