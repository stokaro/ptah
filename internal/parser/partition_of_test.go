package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlext"
	"ptah.run/internal/parser"
)

// TestParse_PartitionOfWithoutItsOwner_FailurePath is the refusal every target
// without the owner's pattern reaches: it names the statement, where a column list
// expected at that position would report an unexpected token.
func TestParse_PartitionOfWithoutItsOwner_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, dialect, want string
	}{
		{name: "a target that reads no partition", dialect: "mysql", want: `line 1, column 17: CREATE TABLE e1 PARTITION OF declares a PostgreSQL partition, which Ptah does not read on mysql`},
		{name: "cockroachdb", dialect: "cockroachdb", want: `line 1, column 17: CREATE TABLE e1 PARTITION OF declares a PostgreSQL partition, which Ptah does not read on cockroachdb`},
		{name: "a read with no target", want: `line 1, column 17: CREATE TABLE e1 PARTITION OF declares a PostgreSQL partition, which Ptah does not read without a target`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser("CREATE TABLE e1 PARTITION OF events DEFAULT;", parser.WithDialect(test.dialect), parser.WithSQL(sqlext.None())).Parse()

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestParse_PartitionByIsNotPartitionOf keeps the two clauses apart: a table option
// written where a column list stands stays the error it was.
func TestParse_PartitionByIsNotPartitionOf(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("CREATE TABLE e1 PARTITION BY RANGE (a);", parser.WithDialect("postgres"), parser.WithSQL(sqlext.None())).Parse()

	c.Assert(err, qt.ErrorMatches, `line \d+, column \d+: expected '\(' after table name: .*`)
	c.Assert(statements, qt.IsNil)
}
