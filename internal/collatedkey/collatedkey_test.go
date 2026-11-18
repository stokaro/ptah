package collatedkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/collatedkey"
)

// TestRead_HappyPath reads the spellings SQLite accepts for one collated key.
// Each is the same key to sqlite3 3.51.0, which reports column code and
// collation NOCASE in pragma_index_xinfo for all of them, and the community
// binary v1.3.0 reports a database holding any of them synced with a document
// that writes `code COLLATE NOCASE`. libsql is SQLite's descriptor under
// another name.
func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		element string
		want    collatedkey.Key
	}{
		{name: "bare", dialect: "sqlite", element: "code COLLATE NOCASE", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "double-quoted", dialect: "sqlite", element: `"code" COLLATE NOCASE`, want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "bracketed", dialect: "sqlite", element: "[code] COLLATE NOCASE", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "backquoted", dialect: "sqlite", element: "`code` COLLATE NOCASE", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "a comment and spacing", dialect: "sqlite", element: "code  COLLATE /*x*/ NOCASE\n", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "ascending", dialect: "sqlite", element: "code COLLATE NOCASE ASC", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "descending", dialect: "sqlite", element: `"code" COLLATE NOCASE DESC`, want: collatedkey.Key{Text: "code COLLATE NOCASE", Desc: true}},
		{name: "a name with a space", dialect: "sqlite", element: "[my code] COLLATE NOCASE", want: collatedkey.Key{Text: `"my code" COLLATE NOCASE`}},
		{name: "a quoted name with a quote", dialect: "sqlite", element: `"a""b" COLLATE NOCASE`, want: collatedkey.Key{Text: `"a""b" COLLATE NOCASE`}},
		{name: "a backquoted name with a doubled backquote", dialect: "sqlite", element: "`a``b` COLLATE NOCASE", want: collatedkey.Key{Text: "\"a`b\" COLLATE NOCASE"}},
		{name: "a keyword", dialect: "sqlite", element: `"order" COLLATE NOCASE`, want: collatedkey.Key{Text: `"order" COLLATE NOCASE`}},
		{name: "a double-quoted collation", dialect: "sqlite", element: `code COLLATE "NOCASE"`, want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "a single-quoted collation", dialect: "sqlite", element: "code COLLATE 'NOCASE'", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "a bracketed collation", dialect: "sqlite", element: "code COLLATE [NOCASE]", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "a backquoted collation", dialect: "sqlite", element: "code COLLATE `NOCASE`", want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
		{name: "a collation that needs its quotes", dialect: "sqlite", element: `code COLLATE "my coll"`, want: collatedkey.Key{Text: `code COLLATE "my coll"`}},
		{name: "libsql", dialect: "libsql", element: `"code" COLLATE NOCASE ASC`, want: collatedkey.Key{Text: "code COLLATE NOCASE"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := collatedkey.Read(tt.dialect, tt.element)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

// TestRead_FailurePath leaves alone every element that is not one column under
// one COLLATE, and every element of a dialect whose descriptor does not state
// that a collated key names its column in every quoting. MySQL reads
// `"code"` as a string unless ANSI_QUOTES is set, PostgreSQL reads `Code` as
// code and `"Code"` as Code, and SQL Server and ClickHouse key no column under
// a collation of its own.
func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		element string
	}{
		{name: "a column", dialect: "sqlite", element: "code"},
		{name: "a column that descends", dialect: "sqlite", element: "code DESC"},
		{name: "an expression", dialect: "sqlite", element: "lower(code) COLLATE NOCASE"},
		{name: "no collation", dialect: "sqlite", element: "code COLLATE"},
		{name: "a word after the direction", dialect: "sqlite", element: "code COLLATE NOCASE DESC x"},
		{name: "a word for a direction", dialect: "sqlite", element: "code COLLATE NOCASE x"},
		{name: "a string literal", dialect: "sqlite", element: "'code' COLLATE NOCASE"},
		{name: "a number", dialect: "sqlite", element: "1 COLLATE NOCASE"},
		{name: "a bracket inside brackets, which sqlite3 refuses", dialect: "sqlite", element: "[a]]b] COLLATE NOCASE"},
		{name: "a bracket inside a bracketed collation", dialect: "sqlite", element: "code COLLATE [NO]]CASE]"},
		{name: "PostgreSQL", dialect: "postgres", element: `"code" COLLATE "C"`},
		{name: "MySQL", dialect: "mysql", element: `"code" COLLATE NOCASE DESC`},
		{name: "MariaDB", dialect: "mariadb", element: "`code` COLLATE utf8mb4_bin"},
		{name: "SQL Server", dialect: "sqlserver", element: "[code] COLLATE Latin1_General_CS_AS ASC"},
		{name: "ClickHouse", dialect: "clickhouse", element: `"code" COLLATE NOCASE DESC`},
		{name: "an unknown dialect", dialect: "nosuch", element: "code COLLATE NOCASE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := collatedkey.Read(tt.dialect, tt.element)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, collatedkey.Key{})
		})
	}
}

// TestColumnCollation_HappyPath reads the collation a column definition
// declares. Each want is the coll sqlite3 3.51.0 reports in
// pragma_index_xinfo for an index on that column, which is the only place the
// server says what collation a column has.
func TestColumnCollation_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		definition string
		want       string
	}{
		{name: "bare", dialect: "sqlite", definition: "code TEXT COLLATE NOCASE", want: "NOCASE"},
		{name: "as written", dialect: "sqlite", definition: "code TEXT COLLATE NoCase", want: "NoCase"},
		{name: "after constraints", dialect: "sqlite", definition: "code VARCHAR(10) NOT NULL DEFAULT ('y') COLLATE [NOCASE] REFERENCES p(x)", want: "NOCASE"},
		{name: "after a string default", dialect: "sqlite", definition: "code TEXT DEFAULT 'x' COLLATE NOCASE", want: "NOCASE"},
		{name: "named", dialect: "sqlite", definition: `code TEXT CONSTRAINT cf COLLATE "RTRIM"`, want: "RTRIM"},
		{name: "backquoted", dialect: "sqlite", definition: "code TEXT COLLATE `NOCASE`", want: "NOCASE"},
		{name: "a string for a name", dialect: "sqlite", definition: "code TEXT COLLATE 'NOCASE'", want: "NOCASE"},
		{name: "the last of two", dialect: "sqlite", definition: "code TEXT COLLATE nocase COLLATE RTRIM", want: "RTRIM"},
		{name: "after a generated column's AS", dialect: "sqlite", definition: "code TEXT AS (a) VIRTUAL COLLATE NOCASE", want: "NOCASE"},
		{name: "BINARY written out", dialect: "sqlite", definition: "code TEXT COLLATE BINARY", want: "BINARY"},
		{name: "on an integer column", dialect: "sqlite", definition: "n INTEGER COLLATE NOCASE", want: "NOCASE"},
		{name: "a column named COLLATE", dialect: "sqlite", definition: `"COLLATE" TEXT COLLATE NOCASE`, want: "NOCASE"},
		{name: "libsql", dialect: "libsql", definition: "code TEXT COLLATE NOCASE", want: "NOCASE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := collatedkey.ColumnCollation(tt.dialect, tt.definition)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

// TestColumnCollation_FailurePath covers definitions whose column declares no
// collation, sqlite3 3.51.0 reporting BINARY for each, and the dialects whose
// descriptor does not state the fact.
func TestColumnCollation_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		definition string
	}{
		{name: "no COLLATE", dialect: "sqlite", definition: "code TEXT NOT NULL"},
		{name: "a COLLATE in a CHECK", dialect: "sqlite", definition: "code TEXT CHECK (code COLLATE NOCASE <> 'a')"},
		{name: "a COLLATE in a generated expression", dialect: "sqlite", definition: "code TEXT AS (a COLLATE NOCASE) VIRTUAL"},
		{name: "a COLLATE in a comment", dialect: "sqlite", definition: "code /* COLLATE NOCASE */ TEXT -- COLLATE RTRIM"},
		{name: "a COLLATE in a default", dialect: "sqlite", definition: "code TEXT DEFAULT ('a' COLLATE NOCASE)"},
		{name: "a quoted column named COLLATE", dialect: "sqlite", definition: `"COLLATE" TEXT`},
		{name: "a table constraint", dialect: "sqlite", definition: "UNIQUE (code COLLATE NOCASE)"},
		{name: "PostgreSQL", dialect: "postgres", definition: `code text COLLATE "C"`},
		{name: "MySQL", dialect: "mysql", definition: "code varchar(10) COLLATE utf8mb4_bin"},
		{name: "an unknown dialect", dialect: "nosuch", definition: "code TEXT COLLATE NOCASE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := collatedkey.ColumnCollation(tt.dialect, tt.definition)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
