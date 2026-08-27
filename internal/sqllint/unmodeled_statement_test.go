package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/sqllint"
)

// TestLintSource_AFileOfUnanalyzedStatementsIsNotClean pins the gap #1270
// names: "No supported code object is silently skipped and reported as clean
// when analysis was incomplete."
//
// Measured before this: on postgres, CREATE VIEW, CREATE MATERIALIZED VIEW,
// CREATE POLICY, CREATE SEQUENCE, CREATE TYPE, ALTER TABLE and DROP TABLE each
// produced no findings and exit 0. No rule had looked at any of them, and the
// result was indistinguishable from a file that passed.
//
// The finding is SQL004 and not SQL002. SQL002 says the PARSER does not model a
// statement, at error severity; every kind below is modeled exactly as intended
// -- CREATE POLICY has its own test asserting it lints clean -- and simply has
// no rule looking at it.
func TestLintSource_AFileOfUnanalyzedStatementsIsNotClean(t *testing.T) {
	rows := []struct {
		name string
		sql  string
		want string
	}{
		{name: "view", sql: "CREATE VIEW v AS SELECT 1;", want: "CREATE VIEW"},
		{name: "materialized view", sql: "CREATE MATERIALIZED VIEW mv AS SELECT 1;", want: "CREATE MATERIALIZED VIEW"},
		{name: "policy", sql: "CREATE POLICY p ON users USING (true);", want: "CREATE POLICY"},
		{name: "sequence", sql: "CREATE SEQUENCE s;", want: "CREATE SEQUENCE"},
		{name: "alter table", sql: "ALTER TABLE users ADD COLUMN x int;", want: "ALTER TABLE"},
		{name: "drop table", sql: "DROP TABLE users;", want: "DROP TABLE"},
		{
			// The override: this parses to an EnumNode, and naming the node
			// would report a keyword no author wrote.
			name: "enum type", sql: "CREATE TYPE mood AS ENUM ('a');", want: "CREATE TYPE",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(
				sqllint.Source{Name: "t.sql", SQL: row.sql},
				sqllint.Options{Dialect: "postgres"},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Rule, qt.Equals, sqllint.RuleStatementsNotAnalyzed)
			c.Assert(findings[0].Severity, qt.Equals, sqllint.SeverityInfo)
			c.Assert(findings[0].Message, qt.Contains, row.want)
		})
	}
}

// TestLintSource_TheUnanalyzedFindingIsOnePerFile pins the shape, which is a
// decision rather than an accident.
//
// A migration is mostly statements this linter has no rule for. One finding per
// statement would outnumber the real ones several times over and bury them --
// the failure mode of a rule meant to make incompleteness visible is being
// ignored.
func TestLintSource_TheUnanalyzedFindingIsOnePerFile(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(sqllint.Source{
		Name: "many.sql",
		SQL: "CREATE VIEW a AS SELECT 1;\n" +
			"CREATE VIEW b AS SELECT 2;\n" +
			"CREATE SEQUENCE s;\n" +
			"DROP TABLE t;\n",
	}, sqllint.Options{Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, sqllint.RuleStatementsNotAnalyzed)
	// Each kind once, in the order the file reads.
	c.Assert(findings[0].Message, qt.Contains, "CREATE VIEW, CREATE SEQUENCE, DROP TABLE")
}

// TestLintSource_AnAnalyzedStatementStaysSilent is the control the test above
// needs.
//
// Without it, answering every statement with SQL002 would pass: a rule that
// reports everything is as useless as one that reports nothing, and it would
// bury the findings that mean something.
func TestLintSource_AnAnalyzedStatementStaysSilent(t *testing.T) {
	rows := []struct {
		name string
		sql  string
	}{
		{name: "a table with a primary key", sql: "CREATE TABLE t (id int PRIMARY KEY);"},
		{name: "an ordinary index", sql: "CREATE INDEX idx ON t (a);"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(
				sqllint.Source{Name: "t.sql", SQL: row.sql},
				sqllint.Options{Dialect: "postgres"},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 0, qt.Commentf("findings: %#v", findings))
		})
	}
}
