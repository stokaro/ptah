package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/sqlutil"
)

// noBackslashEscapes is a MySQL session whose sql_mode names
// NO_BACKSLASH_ESCAPES.
var noBackslashEscapes = capability.ForDialect(platform.MySQL).With(capability.NoBackslashEscapes, true)

// A session that reads a backslash as the character cuts its text where its
// strings end: a backslash before a closing quote ends the string, so a
// semicolon after it ends the statement and one inside it does not. The
// default sql_mode reads the same backslash as an escape.
func TestSplitSQLStatementsForSession(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		caps capability.Capabilities
		want []string
	}{
		{
			name: "a quote after a backslash under NO_BACKSLASH_ESCAPES",
			sql:  "CREATE TABLE t (q varchar(9) DEFAULT 'it\\''s;x', b varchar(9) DEFAULT 'x\\');\nDROP TABLE u;",
			caps: noBackslashEscapes,
			want: []string{"CREATE TABLE t (q varchar(9) DEFAULT 'it\\''s;x', b varchar(9) DEFAULT 'x\\')", "DROP TABLE u"},
		},
		{
			name: "a statement hidden behind a backslash under NO_BACKSLASH_ESCAPES",
			sql:  "ALTER TABLE t ALTER COLUMN u SET DEFAULT 'a\\'; DROP TABLE t; -- '",
			caps: noBackslashEscapes,
			want: []string{"ALTER TABLE t ALTER COLUMN u SET DEFAULT 'a\\'", "DROP TABLE t", "-- '"},
		},
		{
			name: "the same text under the default sql_mode",
			sql:  "ALTER TABLE t ALTER COLUMN u SET DEFAULT 'a\\'; DROP TABLE t; -- '",
			caps: capability.ForDialect(platform.MySQL),
			want: []string{"ALTER TABLE t ALTER COLUMN u SET DEFAULT 'a\\'; DROP TABLE t; -- '"},
		},
		{
			name: "no capabilities",
			sql:  "SELECT 'a\\'; SELECT 1';",
			want: []string{"SELECT 'a\\'; SELECT 1'"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.SplitSQLStatementsForSession(test.sql, platform.MySQL, test.caps), qt.DeepEquals, test.want)
		})
	}
}

// A comment after a string that ends in a backslash is a comment under
// NO_BACKSLASH_ESCAPES and part of the string under the default sql_mode.
func TestStripCommentsForSession(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want string
	}{
		{name: "NO_BACKSLASH_ESCAPES", caps: noBackslashEscapes, want: "SELECT 'x\\' "},
		{name: "the default sql_mode", caps: capability.ForDialect(platform.MySQL), want: "SELECT 'x\\' -- y'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.StripCommentsForSession("SELECT 'x\\' -- y'", platform.MySQL, test.caps), qt.Equals, test.want)
		})
	}
}
