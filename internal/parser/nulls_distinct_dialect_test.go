package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/parser"
)

// NULLS [NOT] DISTINCT is a PostgreSQL unique-constraint clause. Measured
// 2026-09-03 against MySQL 8.4.11 and MariaDB 11.8.9, every spelling below is
// answered with error 1064 (SQLSTATE 42000) in a table constraint and in
// CREATE UNIQUE INDEX alike, so the parser refuses it rather than accepting a
// clause the renderer would then have to drop. See stokaro/ptah#2788.

func TestParseNullsDistinct_MySQLFamilyFailurePath(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantClause string
	}{
		{
			name:       "named table constraint, not distinct",
			sql:        "CREATE TABLE t (a INT, CONSTRAINT uq UNIQUE NULLS NOT DISTINCT (a));",
			wantClause: "NULLS NOT DISTINCT",
		},
		{
			name:       "named table constraint, distinct",
			sql:        "CREATE TABLE t (a INT, CONSTRAINT uq UNIQUE NULLS DISTINCT (a));",
			wantClause: "NULLS DISTINCT",
		},
		{
			name:       "bare table constraint, not distinct",
			sql:        "CREATE TABLE t (a INT, UNIQUE NULLS NOT DISTINCT (a));",
			wantClause: "NULLS NOT DISTINCT",
		},
		{
			name:       "bare table constraint, distinct",
			sql:        "CREATE TABLE t (a INT, UNIQUE NULLS DISTINCT (a));",
			wantClause: "NULLS DISTINCT",
		},
		{
			name:       "standalone unique index, not distinct",
			sql:        "CREATE UNIQUE INDEX uq ON t (a) NULLS NOT DISTINCT;",
			wantClause: "NULLS NOT DISTINCT",
		},
		{
			name:       "standalone unique index, distinct",
			sql:        "CREATE UNIQUE INDEX uq ON t (a) NULLS DISTINCT;",
			wantClause: "NULLS DISTINCT",
		},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				result, err := parser.NewParser(test.sql, parser.WithDialect(dialect)).Parse()

				c.Assert(result, qt.IsNil)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err.Error(), qt.Contains, dialect+" does not support the "+test.wantClause+" clause")
				var capabilityErr *ptaherr.CapabilityError
				c.Assert(err, qt.ErrorAs, &capabilityErr)
				c.Assert(capabilityErr.Dialect, qt.Equals, dialect)
				c.Assert(capabilityErr.Feature, qt.Equals, "NULLS DISTINCT unique-constraint semantics")
			})
		}
	}
}

// PostgreSQL is the control that keeps the refusal dialect-scoped: the same
// six inputs still parse, and the clause still reaches the model.
func TestParseNullsDistinct_PostgresHappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{name: "named table constraint, not distinct", sql: "CREATE TABLE t (a INT, CONSTRAINT uq UNIQUE NULLS NOT DISTINCT (a));", want: false},
		{name: "named table constraint, distinct", sql: "CREATE TABLE t (a INT, CONSTRAINT uq UNIQUE NULLS DISTINCT (a));", want: true},
		{name: "bare table constraint, not distinct", sql: "CREATE TABLE t (a INT, UNIQUE NULLS NOT DISTINCT (a));", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(result.Statements, qt.HasLen, 1)
			table, ok := result.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Constraints, qt.HasLen, 1)
			c.Assert(table.Constraints[0].NullsDistinct, qt.IsNotNil)
			c.Assert(*table.Constraints[0].NullsDistinct, qt.Equals, test.want)
		})
	}
}

// The control that keeps the refusal off a legitimate declaration. Measured
// 2026-09-03, MySQL 8.4.11 and MariaDB 11.8.9 both CREATE this table and
// report INDEX_NAME "NULLS": there, NULLS is an index name rather than the
// head of a clause. Whatever the name grammar makes of it -- today it is
// stokaro/ptah#2776, a named UNIQUE without KEY or INDEX -- the answer must
// not be the dialect refusal, because a refusal here would reject DDL both
// servers accept.
func TestParseNullsAsIndexName_IsNotTheDialectRefusal(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(
				"CREATE TABLE t (a INT, CONSTRAINT uq UNIQUE NULLS (a));",
				parser.WithDialect(dialect),
			).Parse()

			c.Assert(err, qt.IsNotNil)
			c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrUnsupportedFeature)
		})
	}
}
