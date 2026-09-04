package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/sqlschema"
)

// renderErrorFromSQL is the error rendering one CREATE TABLE for a dialect.
//
// From SQL rather than from a hand-built model, for the reason
// renderedFromSQL states: the two spellings this file separates differ only in
// what the parser records, so a model written by hand would be the test author
// choosing which one is under test.
func renderErrorFromSQL(c *qt.C, dialect, sql string) error {
	c.Helper()
	database, _, err := sqlschema.Read([]byte(sql), dialect)
	c.Assert(err, qt.IsNil)
	_, err = renderer.GetOrderedCreateStatements(&database, dialect)
	return err
}

// TestSQLServerRefusesPrimaryKeyBesideUniqueOnOneColumn is stokaro/ptah#2812.
//
// SQL Server does not fold `a INT PRIMARY KEY UNIQUE` the way the other
// engines do -- it refuses the statement. Measured on SQL Server 2022:
//
//	Msg 8151, Level 16, State 1
//	Both a PRIMARY KEY and UNIQUE constraint have been defined for column 'a',
//	table 't_inline'. Only one is allowed.
//
// Ptah rendered `[a] INT PRIMARY KEY` at exit 0. There is no rendering that
// reproduces the source, because the source is not a statement this engine
// accepts, so emitting the key alone hands back a table nobody described and
// silently discards what the author wrote.
func TestSQLServerRefusesPrimaryKeyBesideUniqueOnOneColumn(t *testing.T) {
	c := qt.New(t)

	err := renderErrorFromSQL(c, platform.SQLServer, `CREATE TABLE t (a INT PRIMARY KEY UNIQUE);`)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, "PRIMARY KEY and UNIQUE")
	c.Assert(err.Error(), qt.Contains, `"a"`)
}

// TestSQLServerKeepsTheTableLevelSpellingThatItAccepts is the control that
// makes the refusal narrow rather than a ban on the two words appearing
// together.
//
// `a INT UNIQUE, PRIMARY KEY (a)` is a different statement and SQL Server
// accepts it. Measured on SQL Server 2022, `sys.indexes` for the created table
// holds two rows, one `is_primary_key` and one `is_unique_constraint`, and the
// same query over Ptah's rendering of it holds the same pair.
//
// Without this row the refusal could have been achieved by rejecting any
// column carrying UNIQUE beside any primary key, which would take a working
// declaration away from every SQL Server author.
func TestSQLServerKeepsTheTableLevelSpellingThatItAccepts(t *testing.T) {
	c := qt.New(t)

	rendered := renderedFromSQL(c, platform.SQLServer, `CREATE TABLE t (a INT UNIQUE, PRIMARY KEY (a));`)

	c.Assert(rendered, qt.Contains, "UNIQUE")
	c.Assert(rendered, qt.Contains, "PRIMARY KEY ([a])")
}

// TestOtherDialectsStillFoldPrimaryKeyBesideUnique keeps the refusal on the one
// engine that refuses.
//
// The same source is valid on MySQL and PostgreSQL, and each folds it its own
// way -- measured on MySQL 8.4.11, a key plus a secondary unique index; on
// PostgreSQL 18, `pg_constraint` shows only the key. A refusal written into
// shared lowering rather than into this renderer would take both away, and the
// PostgreSQL renderer emitting the key alone is correct rather than lossy.
func TestOtherDialectsStillFoldPrimaryKeyBesideUnique(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "mysql keeps both", dialect: platform.MySQL, want: "`a` INT PRIMARY KEY UNIQUE"},
		{name: "postgres keeps the key", dialect: platform.Postgres, want: `"a" INT PRIMARY KEY NOT NULL`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered := renderedFromSQL(c, test.dialect, `CREATE TABLE t (a INT PRIMARY KEY UNIQUE);`)

			c.Assert(rendered, qt.Contains, test.want)
		})
	}
}
