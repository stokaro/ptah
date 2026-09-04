package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/sqlschema"
)

// renderedFromSQL parses one CREATE TABLE and renders it back for a dialect.
//
// From SQL rather than from a hand-built model, because the two spellings this
// file is about differ only in what the PARSER records: a table-level primary
// key fills `Table.PrimaryKey`, an inline one does not, and a model written by
// hand would be the test author choosing the answer.
func renderedFromSQL(c *qt.C, dialect, sql string) string {
	c.Helper()
	database, _, err := sqlschema.Read([]byte(sql), dialect)
	c.Assert(err, qt.IsNil)
	rendered, err := renderer.GetOrderedCreateStatements(&database, dialect)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Not(qt.HasLen), 0)
	return rendered[0]
}

// TestMySQLFamilyUniqueBesidePrimary_TableLevelForm is stokaro/ptah#2787, in
// the spelling that is wrong on both engines.
//
// `a INT UNIQUE, PRIMARY KEY (a)` builds a primary key AND a secondary unique
// index named `a` on MySQL 8.4 and on MariaDB 11.8 alike -- measured. Ptah
// rendered `a INT PRIMARY KEY`, a table with different key semantics than the
// one it read, at exit 0.
//
// The key stays a table constraint here rather than moving onto the column,
// and that is the half a reader is most likely to think cosmetic. It is not:
// MariaDB folds `a INT PRIMARY KEY UNIQUE` into the primary key alone, so the
// column spelling cannot carry this source's second index at all.
func TestMySQLFamilyUniqueBesidePrimary_TableLevelForm(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered := renderedFromSQL(c, test.dialect,
				`CREATE TABLE t (a INT UNIQUE, PRIMARY KEY (a));`)
			c.Assert(rendered, qt.Contains, "`a` INT UNIQUE")
			c.Assert(rendered, qt.Contains, "PRIMARY KEY (`a`)")
			c.Assert(rendered, qt.Not(qt.Contains), "`a` INT PRIMARY KEY")
		})
	}
}

// TestMySQLFamilyUniqueBesidePrimary_InlineForm is the other spelling, where
// the two engines deliberately disagree.
//
// `a INT PRIMARY KEY UNIQUE` builds two indexes on MySQL 8.4 and one on
// MariaDB 11.8. Writing back what the source wrote is what reproduces each
// engine's own answer, so both dialects render the same text here -- and that
// is the point rather than an oversight.
func TestMySQLFamilyUniqueBesidePrimary_InlineForm(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered := renderedFromSQL(c, test.dialect,
				`CREATE TABLE t (a INT PRIMARY KEY UNIQUE);`)
			c.Assert(rendered, qt.Contains, "`a` INT PRIMARY KEY UNIQUE")
			c.Assert(rendered, qt.Not(qt.Contains), "PRIMARY KEY (`a`)")
		})
	}
}

// TestMySQLFamilyUniqueBesidePrimary_ControlsForm is what the repair is not
// allowed to change.
//
// Two ways to satisfy the tests above and destroy the feature: emit UNIQUE for
// every primary column, or move every single-column primary key into a table
// constraint. Each row here is one of them, and each names the source that
// carries no second key at all.
func TestMySQLFamilyUniqueBesidePrimary_ControlsForm(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		sql        string
		want       string
		wantAbsent string
	}{
		{
			name:    "an inline primary key alone stays inline and gains no UNIQUE",
			dialect: platform.MySQL, sql: `CREATE TABLE t (a INT PRIMARY KEY);`,
			want: "`a` INT PRIMARY KEY", wantAbsent: "UNIQUE",
		},
		{
			name:    "a table-level primary key over a plain column still moves onto it",
			dialect: platform.MySQL, sql: `CREATE TABLE t (a INT, PRIMARY KEY (a));`,
			want: "`a` INT PRIMARY KEY", wantAbsent: "UNIQUE",
		},
		{
			name:    "a unique column with no key of its own is untouched",
			dialect: platform.MariaDB, sql: `CREATE TABLE t (a INT UNIQUE);`,
			want: "`a` INT UNIQUE", wantAbsent: "PRIMARY KEY",
		},
		{
			name:    "a composite key was already a table constraint",
			dialect: platform.MariaDB, sql: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b));`,
			want: "PRIMARY KEY (`a`, `b`)", wantAbsent: "UNIQUE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered := renderedFromSQL(c, test.dialect, test.sql)
			c.Assert(rendered, qt.Contains, test.want)
			c.Assert(rendered, qt.Not(qt.Contains), test.wantAbsent)
		})
	}
}

// TestUniqueBesidePrimary_TableConstraintIsDialectAgnostic is the half of the
// repair that is not about the MySQL family.
//
// Keeping a table-level key over a unique column as a table constraint is
// decided in `internal/modelast`, above every renderer, so it reaches dialects
// this issue was not filed about. That is deliberate rather than incidental:
// the column spelling loses the second key on more engines than MariaDB.
//
// Measured, one column declared both ways:
//
//	engine             a INT UNIQUE, PRIMARY KEY (a)   a INT PRIMARY KEY UNIQUE
//	MySQL 8.4.11       key + unique index              key + unique index
//	MariaDB 11.8.9     key + unique index              key alone
//	PostgreSQL 18      key alone                       key alone
//	SQL Server 2022    key + unique constraint         refused, Msg 8151
//
// So SQL Server needs the table constraint as much as MariaDB does -- more, as
// it will not accept the inline pair at all -- and PostgreSQL folds either way,
// which is why its rendering is unchanged in meaning by this.
func TestUniqueBesidePrimary_TableConstraintIsDialectAgnostic(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "sqlserver", dialect: platform.SQLServer, want: "[a] INT UNIQUE"},
		{name: "postgres", dialect: platform.Postgres, want: `"a" INT UNIQUE`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered := renderedFromSQL(c, test.dialect,
				`CREATE TABLE t (a INT UNIQUE, PRIMARY KEY (a));`)
			c.Assert(rendered, qt.Contains, test.want)
			c.Assert(rendered, qt.Contains, "PRIMARY KEY (")
		})
	}
}
