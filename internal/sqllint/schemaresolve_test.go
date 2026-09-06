package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqllint"
)

// usersSchema declares one table with two columns, as a run supplies it.
func usersSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "User", Type: "INT"},
			{Name: "email", StructName: "User", Type: "TEXT"},
		},
	}
}

// codesOf lists the rules a lint run reported.
func codesOf(findings []sqllint.Finding) []string {
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	return codes
}

// TestLintSource_AnIndexIsCheckedAgainstTheSuppliedSchema is #1270's criterion
// 7: analysis against a concrete schema state.
//
// Every rule before this read the statement in front of it. A CREATE INDEX
// names a table and columns declared somewhere else, so without a schema the
// linter could only say the statement parses.
func TestLintSource_AnIndexIsCheckedAgainstTheSuppliedSchema(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(
		sqllint.Source{Name: "index.sql", SQL: "CREATE INDEX idx ON users (nickname);"},
		sqllint.Options{Dialect: platform.Postgres, Schema: usersSchema()},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(codesOf(findings), qt.DeepEquals, []string{"DDL002"})
	c.Assert(findings[0].Message, qt.Contains, "nickname")
	c.Assert(findings[0].Message, qt.Contains, "users")
}

// TestLintSource_AnIndexOverDeclaredColumnsIsSilent is the control.
//
// A rule that reported every index would satisfy the test above and mean
// nothing. This is the case that has to stay quiet.
func TestLintSource_AnIndexOverDeclaredColumnsIsSilent(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(
		sqllint.Source{Name: "index.sql", SQL: "CREATE INDEX idx ON users (email);"},
		sqllint.Options{Dialect: platform.Postgres, Schema: usersSchema()},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(codesOf(findings), qt.HasLen, 0)
}

// TestLintSource_AnIndexResolvesAgainstItsOwnFile covers the common shape.
//
// A schema file declares its tables and its indexes together, so the check is
// useful with no schema supplied at all -- and a run that supplies one is
// asking about SQL that will meet a database, which is why the schema wins
// where both know the table.
func TestLintSource_AnIndexResolvesAgainstItsOwnFile(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(
		sqllint.Source{Name: "schema.sql", SQL: "CREATE TABLE users (id INT PRIMARY KEY);\n" +
			"CREATE INDEX idx ON users (email);"},
		sqllint.Options{Dialect: platform.Postgres},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(codesOf(findings), qt.DeepEquals, []string{"DDL002"})
}

// TestLintSource_AnIndexOnAnUnknownTableIsSilent keeps the rule from reporting
// what it cannot know.
//
// Neither the file nor a schema declares the table, so the columns are not
// established as missing -- they are unestablished. Reporting them would be the
// confident wrong answer.
func TestLintSource_AnIndexOnAnUnknownTableIsSilent(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(
		sqllint.Source{Name: "index.sql", SQL: "CREATE INDEX idx ON elsewhere (email);"},
		sqllint.Options{Dialect: platform.Postgres, Schema: usersSchema()},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(codesOf(findings), qt.HasLen, 0)
}

// TestLintSource_AnExpressionIndexNamesNoColumn is the boundary.
//
// `lower(email)` names no single column, and Columns carries the whole
// expression text for legacy callers -- so a rule reading Columns rather than
// Parts would report a missing column called `lower(email)`.
func TestLintSource_AnExpressionIndexNamesNoColumn(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(
		sqllint.Source{Name: "index.sql", SQL: "CREATE INDEX idx ON users (lower(email));"},
		sqllint.Options{Dialect: platform.Postgres, Schema: usersSchema()},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(codesOf(findings), qt.HasLen, 0)
}
