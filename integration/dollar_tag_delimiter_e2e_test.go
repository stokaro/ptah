//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// dollarTagDelimiterScript is written between `-- atlas:delimiter |` lines,
// and its function body is quoted with a tag outside ASCII and holds the
// delimiter, as the bitwise OR `3 | 4`.
const dollarTagDelimiterScript = "-- atlas:delimiter |\n" +
	"CREATE FUNCTION dollar_tag_or() RETURNS int LANGUAGE sql AS $é$ SELECT 3 | 4 $é$|\n"

// TestMigrationsUpKeepsADelimiterInsideATagOutsideASCIIE2E applies
// dollarTagDelimiterScript on each PostgreSQL-family engine and calls the
// function it creates. Each server reads `$é$` as the start of a string,
// so the body is one constant and the function returns 7. Read with a tag rule
// that allows only ASCII, the client delimiter rewrite takes the `|` inside
// the body for the delimiter and writes a semicolon there, and the server
// refuses the function.
func TestMigrationsUpKeepsADelimiterInsideATagOutsideASCIIE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := probeEscapeTargets[test.engine](c)
			migrations := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_or.up.sql"),
				[]byte(dollarTagDelimiterScript), 0o600), qt.IsNil)

			applied, err := runPtahNativeWithError("migrations", "up", "--db-url", target.url, "--migrations-dir", migrations)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			db, err := sql.Open(target.driver, target.driverURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			var got int
			c.Assert(db.QueryRowContext(c.Context(), "SELECT dollar_tag_or()").Scan(&got), qt.IsNil)
			c.Assert(got, qt.Equals, 7)
		})
	}
}

// applyHeredocMigration applies script, a migration that creates the table
// heredoc_default with a String column s, in a scratch database of the
// ClickHouse server at url, and returns the default the server stores for s.
func applyHeredocMigration(c *qt.C, url, script string) string {
	c.Helper()
	database := createdDevDialectDatabase(c, url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	migrations := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_heredoc.up.sql"), []byte(script), 0o600), qt.IsNil)

	applied, err := runPtahNativeWithError("migrations", "up", "--db-url", database, "--migrations-dir", migrations)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	var stored string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT default_expression FROM system.columns WHERE database = currentDatabase() "+
			"AND table = 'heredoc_default' AND name = 's'").Scan(&stored), qt.IsNil)
	return stored
}

// TestMigrationsUpSendsADigitFirstHeredocAsOneStatementE2E applies a
// ClickHouse migration whose column default is a heredoc with a tag that
// begins with a digit and holds the statement's terminator, in a plain file
// and under a delimiter directive, on each release line CI starts. ClickHouse
// reads `$1abc$` as a heredoc, so the statement is one and the default is the
// heredoc's text. Read by the PostgreSQL family's tag rule, where `$1` is a
// parameter, the semicolon inside the heredoc ends the statement, and
// ClickHouse refuses both halves.
func TestMigrationsUpSendsADigitFirstHeredocAsOneStatementE2E(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		for _, test := range []struct {
			name, script, want string
		}{
			{
				name:   "plain file",
				script: "CREATE TABLE heredoc_default (id UInt8, s String DEFAULT $1abc$x; y$1abc$) ENGINE = MergeTree ORDER BY id;\n",
				want:   "'x; y'",
			},
			{
				name: "delimiter directive",
				script: "-- atlas:delimiter |\n" +
					"CREATE TABLE heredoc_default (id UInt8, s String DEFAULT $1abc$x| y$1abc$) ENGINE = MergeTree ORDER BY id|\n",
				want: "'x| y'",
			},
		} {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				got := applyHeredocMigration(c, server.url, test.script)

				c.Assert(got, qt.Equals, test.want)
			})
		}
	}
}

// TestMigrationsUpSendsAHeredocWithATagOutsideASCIIAsOneStatementE2E applies
// a migration whose column default is a heredoc with a tag outside ASCII
// holding the terminator or the declared delimiter, on ClickHouse 24.10,
// which reads such a tag. 26.9 and 26.10 refuse the tag with code 62 however
// the text is split, so they are not here. Read with a tag rule that allows
// only ASCII, the plain file is split at the semicolon and refused, and under
// the directive the rewrite takes the `|` for the delimiter and the server
// stores `x; y`.
func TestMigrationsUpSendsAHeredocWithATagOutsideASCIIAsOneStatementE2E(t *testing.T) {
	for _, test := range []struct {
		name, script, want string
	}{
		{
			name:   "plain file",
			script: "CREATE TABLE heredoc_default (id UInt8, s String DEFAULT $\u00e9$x; y$\u00e9$) ENGINE = MergeTree ORDER BY id;\n",
			want:   "'x; y'",
		},
		{
			name: "delimiter directive",
			script: "-- atlas:delimiter |\n" +
				"CREATE TABLE heredoc_default (id UInt8, s String DEFAULT $\u00e9$x| y$\u00e9$) ENGINE = MergeTree ORDER BY id|\n",
			want: "'x| y'",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := applyHeredocMigration(c, legacyClickHouseURL(c), test.script)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// unclosedTagScript names a column `$ab$`, a tag no closing tag follows, and
// creates a second table after it.
const unclosedTagScript = "CREATE TABLE unclosed (id UInt8, $ab$ UInt8) ENGINE = MergeTree ORDER BY id;\n" +
	"CREATE TABLE after_unclosed (id UInt8) ENGINE = MergeTree ORDER BY id;\n"

// TestMigrationsUpEndsAStatementAfterAnUnclosedClickHouseTagE2E applies
// unclosedTagScript on each ClickHouse release line CI starts and reads both
// tables back. ClickHouse reads a tag that is never closed as part of a name,
// so the first statement ends at its semicolon and the second runs. Read as
// a heredoc that runs to the end of the text, the two statements are sent as
// one, and ClickHouse refuses them.
func TestMigrationsUpEndsAStatementAfterAnUnclosedClickHouseTagE2E(t *testing.T) {
	c := qt.New(t)
	for _, server := range clickHouseServers(c) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			database := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			migrations := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_unclosed.up.sql"),
				[]byte(unclosedTagScript), 0o600), qt.IsNil)

			applied, err := runPtahNativeWithError("migrations", "up", "--db-url", database, "--migrations-dir", migrations)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			conn, err := dbschema.ConnectToDatabase(c.Context(), database)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			var columns string
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT arrayStringConcat(groupArray(concat(table, '.', name)), ',') FROM "+
					"(SELECT table, name FROM system.columns WHERE database = currentDatabase() "+
					"AND table IN ('unclosed', 'after_unclosed') ORDER BY table, position)").Scan(&columns), qt.IsNil)
			c.Assert(columns, qt.Equals, "after_unclosed.id,unclosed.id,unclosed.$ab$")
		})
	}
}
