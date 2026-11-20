package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

// A MySQL-family script keeps a banner line that starts with # and holds an
// apostrophe out of the SQL: the server reads every # as a comment, so the
// apostrophe opens no literal and the routine below it splits at its own
// delimiter.
func TestSplitSQLStatementsForDialect_HashBannerAboveDelimiter_HappyPath(t *testing.T) {
	const script = "#=========== setup: don't touch ===========\n" +
		"DELIMITER $$\n" +
		"CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END$$\n" +
		"DELIMITER ;\n" +
		"SELECT 3;\n"
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitSQLStatementsForDialect(script, dialect)

			c.Assert(got, qt.DeepEquals, []string{
				"#=========== setup: don't touch ===========\nCREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END",
				"SELECT 3",
			})
		})
	}
}

// Where the dialect lists # as a line comment, the comment runs to the end of
// the line whatever follows the #, so an apostrophe in it opens no literal.
func TestSplitSQLStatementsForDialect_ListedHashComment_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		script  string
		want    []string
	}{
		{"mysql operator spelling", "mysql", "SELECT 1 #> don't\n;SELECT 3;", []string{"SELECT 1 #> don't", "SELECT 3"}},
		{"mariadb operator spelling", "mariadb", "SELECT 1 #> don't\n;SELECT 3;", []string{"SELECT 1 #> don't", "SELECT 3"}},
		{"clickhouse hash", "clickhouse", "SELECT 1 #= don't\n;SELECT 3;", []string{"SELECT 1 #= don't", "SELECT 3"}},
		{"clickhouse slashes", "clickhouse", "SELECT 1 // don't\n;SELECT 3;", []string{"SELECT 1 // don't", "SELECT 3"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitSQLStatementsForDialect(test.script, test.dialect)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A ClickHouse script that declares `//` as its delimiter ends its statements
// at it: an active delimiter wins over a comment marker.
func TestSplitSQLStatementsForDialect_ClickHouseSlashDelimiter_HappyPath(t *testing.T) {
	c := qt.New(t)

	got := sqlutil.SplitSQLStatementsForDialect("DELIMITER //\nSELECT 1//\nSELECT 2//\nDELIMITER ;\nSELECT 3;\n", "clickhouse")

	c.Assert(got, qt.DeepEquals, []string{"SELECT 1", "SELECT 2", "SELECT 3"})
}

// Where the dialect lists no #, it is an operator character: PostgreSQL's
// XOR and JSON path operators, and SQL Server's temporary table names, leave
// the rest of the line as SQL, so a literal that follows closes as written.
func TestSplitSQLStatementsForDialect_UnlistedHash_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		script  string
		want    []string
	}{
		{"postgres xor", "postgres", "SELECT 5 # 3 AS x, 'it''s';\nSELECT 1;\n", []string{"SELECT 5 # 3 AS x, 'it''s'", "SELECT 1"}},
		{"postgres json path", "postgres", "SELECT '{\"a\":1}'::jsonb #> '{a}';\nSELECT 1;\n", []string{"SELECT '{\"a\":1}'::jsonb #> '{a}'", "SELECT 1"}},
		{"postgres hash before apostrophe", "postgres", "SELECT 5 # 3, 'x;y';\nSELECT 1;\n", []string{"SELECT 5 # 3, 'x;y'", "SELECT 1"}},
		{"cockroachdb xor", "cockroachdb", "SELECT 5 # 3, 'it''s';\nSELECT 1;\n", []string{"SELECT 5 # 3, 'it''s'", "SELECT 1"}},
		{"yugabytedb xor", "yugabytedb", "SELECT 5 # 3, 'it''s';\nSELECT 1;\n", []string{"SELECT 5 # 3, 'it''s'", "SELECT 1"}},
		{"sqlite hash", "sqlite", "SELECT 5 # 3, 'it''s';\nSELECT 1;\n", []string{"SELECT 5 # 3, 'it''s'", "SELECT 1"}},
		{"sqlserver temp table", "sqlserver", "SELECT * INTO #t FROM u WHERE a = 'x;y';\nSELECT 1;\n", []string{"SELECT * INTO #t FROM u WHERE a = 'x;y'", "SELECT 1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitSQLStatementsForDialect(test.script, test.dialect)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A script with no dialect keeps the lenient reading: a # opens a comment
// unless an operator character follows it.
func TestSplitSQLStatements_NoDialectHash_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"hash then space is a comment", "SELECT 1 # don't\n;SELECT 3;", []string{"SELECT 1 # don't", "SELECT 3"}},
		{"hash then operator character is not", "SELECT 5 #> 3, 'a;b';SELECT 3;", []string{"SELECT 5 #> 3, 'a;b'", "SELECT 3"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitSQLStatements(test.script)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
