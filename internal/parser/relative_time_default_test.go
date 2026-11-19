package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A string default the engine reads as a moment relative to when it reads
// it is refused on a date or time column: PostgreSQL reads a default when it
// creates the table, and keeps that moment for every row.
func TestParse_RelativeTimeDefault_FailurePath(t *testing.T) {
	rows := []struct {
		name, dialect, column, wantErr string
	}{
		{
			dialect: "postgres", name: "now on a timestamptz", column: "a timestamptz DEFAULT 'now'",
			wantErr: `(?s).*column a: DEFAULT 'now' on a timestamptz column is read once, when the table is created.*such as now\(\) or CURRENT_DATE`,
		},
		{dialect: "postgres", name: "today in mixed case on a date", column: "b date DEFAULT 'Today'", wantErr: `(?s).*column b: DEFAULT 'Today' on a date column is read once.*`},
		{dialect: "postgres", name: "a cast", column: "c timestamp DEFAULT 'now'::timestamp", wantErr: `(?s).*column c: DEFAULT 'now'::timestamp on a timestamp column.*`},
		{dialect: "postgres", name: "tomorrow on a time", column: "d time DEFAULT ' tomorrow '", wantErr: `(?s).*column d: DEFAULT ' tomorrow ' on a time column.*`},
		{dialect: "postgres", name: "yesterday", column: "e timestamp with time zone DEFAULT 'yesterday'", wantErr: `(?s).*column e: DEFAULT 'yesterday'.*`},
		{dialect: "postgres", name: "in parentheses", column: "f timestamptz DEFAULT ('now')", wantErr: `(?s).*column f: DEFAULT \('now'\) on a timestamptz column is read once.*`},
		{dialect: "postgres", name: "cast in parentheses", column: "g date DEFAULT ('today'::date)", wantErr: `(?s).*column g: DEFAULT \('today'::date\) on a date column.*`},
		{dialect: "yugabytedb", name: "YugabyteDB", column: "h timestamptz DEFAULT 'now'", wantErr: `(?s).*column h: DEFAULT 'now' on a timestamptz column is read once.*`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parseDialect(c, row.dialect, "CREATE TABLE t (id int PRIMARY KEY, "+row.column+");")

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A fixed date or time string, a function, the same word on a text column, and
// the same word on an engine that declares no such literal stay as written.
func TestParse_RelativeTimeDefault_HappyPath(t *testing.T) {
	rows := []struct {
		name, dialect, column, want string
	}{
		{name: "epoch", dialect: "postgres", column: "a timestamptz DEFAULT 'epoch'", want: "'epoch'"},
		{name: "a text column", dialect: "postgres", column: "a text DEFAULT 'now'", want: "'now'"},
		{name: "MySQL", dialect: "mysql", column: "a varchar(8) DEFAULT 'now'", want: "'now'"},
		{name: "SQLite", dialect: "sqlite", column: "a datetime DEFAULT 'now'", want: "'now'"},
		{name: "CockroachDB, which keeps the string", dialect: "cockroachdb", column: "a timestamptz DEFAULT 'now'", want: "'now'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parseDialect(c, row.dialect, "CREATE TABLE t (id int PRIMARY KEY, "+row.column+");")

			c.Assert(err, qt.IsNil)
			c.Assert(defaultOf(c, statements).Value, qt.Equals, row.want)
		})
	}
}
