package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
)

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "single statement",
			sql:  "CREATE TABLE users (id SERIAL PRIMARY KEY);",
			expected: []string{
				"CREATE TABLE users (id SERIAL PRIMARY KEY)",
			},
		},
		{
			name: "multiple statements",
			sql:  "CREATE TABLE users (id SERIAL PRIMARY KEY); CREATE INDEX idx_users_id ON users(id);",
			expected: []string{
				"CREATE TABLE users (id SERIAL PRIMARY KEY)",
				"CREATE INDEX idx_users_id ON users(id)",
			},
		},
		{
			name: "statements with comments",
			sql:  "-- Create users table\nCREATE TABLE users (id SERIAL PRIMARY KEY);\n-- Create index\nCREATE INDEX idx_users_id ON users(id);",
			expected: []string{
				"CREATE TABLE users (id SERIAL PRIMARY KEY)",
				"CREATE INDEX idx_users_id ON users(id)",
			},
		},
		{
			name:     "empty SQL",
			sql:      "",
			expected: make([]string, 0),
		},
		{
			name:     "only comments",
			sql:      "-- This is a comment\n/* Another comment */",
			expected: make([]string, 0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := sqlutil.SplitStatements(tt.sql)
			c.Assert(result, qt.DeepEquals, tt.expected)
		})
	}
}

// TestSplitStatements_AtlasDelimiterBeforeCommentStripping pins the order the
// composition works in: the `-- atlas:delimiter` directive lives in a comment,
// so normalizing client delimiters must happen before comments are stripped or
// the directive is destroyed with the comment that carries it.
func TestSplitStatements_AtlasDelimiterBeforeCommentStripping(t *testing.T) {
	c := qt.New(t)

	sql := `-- atlas:delimiter -- end
CREATE PROCEDURE dorepeat(p1 INT)
BEGIN
    SET @x = 0;
    REPEAT SET @x = @x + 1; UNTIL @x > p1 END REPEAT;
END;
-- end
CALL dorepeat(1000);`

	result := sqlutil.SplitStatements(sql)

	c.Assert(result, qt.DeepEquals, []string{
		`CREATE PROCEDURE dorepeat(p1 INT)
BEGIN
    SET @x = 0;
    REPEAT SET @x = @x + 1; UNTIL @x > p1 END REPEAT;
END`,
		"CALL dorepeat(1000)",
	})
}

func TestSplitStatementsForDialect_BlankDialectFallsBack(t *testing.T) {
	c := qt.New(t)

	// A blank dialect falls back to the dialect-blind split, matching
	// SplitStatements exactly.
	sql := "CREATE TABLE users (id SERIAL PRIMARY KEY); CREATE INDEX idx ON users(id);"
	c.Assert(sqlutil.SplitStatementsForDialect("", sql), qt.DeepEquals, sqlutil.SplitStatements(sql))
	c.Assert(sqlutil.SplitStatementsForDialect("  ", sql), qt.DeepEquals, sqlutil.SplitStatements(sql))
}

// TestSplitStatementsForDialect_StripsCommentsPerStatement pins the dialect
// path's shape: statements are split first, then each is stripped and dropped
// when only comments remain.
func TestSplitStatementsForDialect_StripsCommentsPerStatement(t *testing.T) {
	c := qt.New(t)

	sql := "-- create users\nCREATE TABLE users (id INT);\n-- trailing comment only\n"

	c.Assert(sqlutil.SplitStatementsForDialect(platform.MySQL, sql), qt.DeepEquals, []string{
		"CREATE TABLE users (id INT)",
	})
}

// TestSplitStatementsForDialect_MySQLBackslashEscape pins the reason the
// dialect-aware entry point exists: a semicolon inside a backslash-escaped
// string literal must not leak out into a separately-executed statement on the
// engines that process C-style escapes.
func TestSplitStatementsForDialect_MySQLBackslashEscape(t *testing.T) {
	c := qt.New(t)

	sql := `INSERT INTO t (v) VALUES ('a\';b'); INSERT INTO t (v) VALUES ('c');`

	c.Assert(sqlutil.SplitStatementsForDialect(platform.MySQL, sql), qt.DeepEquals, []string{
		`INSERT INTO t (v) VALUES ('a\';b')`,
		`INSERT INTO t (v) VALUES ('c')`,
	})
}
