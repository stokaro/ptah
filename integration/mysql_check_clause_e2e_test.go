//go:build integration

package integration_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// MySQL's information_schema prints a CHECK clause and a functional index
// expression escaped once more, as the body of a string: each backslash and
// each quote carries a backslash. MariaDB prints them as SQL. The reader
// takes the escaping off on MySQL, so both engines yield a clause a server
// accepts, and the comparison reads each string by the value it stands for.

// checkClauseServers are the MySQL-family servers the CHECK rows run on.
var checkClauseServers = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin},
}

// checkClauseConverging are tables whose CHECK strings each server prints in
// a spelling other than the declared one: MySQL writes 'it\'s' for 'it”s',
// a raw tab for '\t' and 'abc' for "abc", and MariaDB 'x\\%y' for 'x\%y'.
var checkClauseConverging = []struct {
	name   string
	schema string
}{
	{name: "a doubled quote", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'it''s'));`},
	{name: "an escaped backslash", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'p\\'));`},
	{name: "an escaped tab", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'x\ty'));`},
	{name: "a backslash before a percent sign", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'x\%y'));`},
	{name: "a double-quoted string", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> "abc"));`},
	{name: "a double-quoted string holding a quote", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> "d'q"));`},
}

// TestMySQLFamilyCheckStringsConvergeE2E applies each table to an empty
// database and holds the next run to no change.
func TestMySQLFamilyCheckStringsConvergeE2E(t *testing.T) {
	for _, row := range checkClauseConverging {
		for _, server := range checkClauseServers {
			t.Run(row.name+"/"+server.name, func(t *testing.T) {
				c := qt.New(t)
				_, url := newMySQLFamilyScratch(c, server.engine).database(c, "ck_apply")
				file := quotedTextSchemaFile(c, row.schema)

				applied := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--auto-approve")
				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				again := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
				c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
	}
}

// checkClauseChanged are live CHECK strings and declared ones that stand for
// another value, each one a spelling the comparison folds away from the
// other: a quote dropped, a backslash dropped, and a double-quoted string read
// as the same text in single quotes.
var checkClauseChanged = []quotedTextRow{
	{
		name:    "a quote against none",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'it''s'));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'its'));`,
		planned: `CHECK (c <> 'its');`,
	},
	{
		name:    "one backslash against two",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'p\\'));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'p\\\\'));`,
		planned: `CHECK (c <> 'p\\\\');`,
	},
	{
		name:    "a double-quoted string against another value",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> "abc"));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'abd'));`,
		planned: `CHECK (c <> 'abd');`,
	},
}

// TestMySQLFamilyCheckStringsThatDifferPlanE2E holds each changed CHECK to a
// planned replacement.
func TestMySQLFamilyCheckStringsThatDifferPlanE2E(t *testing.T) {
	for _, row := range checkClauseChanged {
		for _, server := range checkClauseServers {
			t.Run(row.name+"/"+server.name, func(t *testing.T) {
				c := qt.New(t)
				_, url := newMySQLFamilyScratch(c, server.engine).builtFrom(c, row.live)

				planned := quotedTextDryRun(c, url, row.desired)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Contains, row.planned)
			})
		}
	}
}

// TestMySQLFunctionalIndexWithAStringConvergesE2E applies an index on an
// expression holding a quote and holds the next run to no change. MySQL
// prints the expression escaped as it prints a CHECK clause. MariaDB has no
// index on an expression.
func TestMySQLFunctionalIndexWithAStringConvergesE2E(t *testing.T) {
	c := qt.New(t)
	_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "ck_index")
	file := quotedTextSchemaFile(c, "CREATE TABLE t (id int PRIMARY KEY, c varchar(20), KEY ix ((concat(c, 'it''s'))));")

	applied := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--auto-approve")
	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	again := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--dry-run")

	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// checkClauseReadTable holds a CHECK with no string and one for each string
// spelling the servers rewrite.
// The rows are created over the Go driver, whose connection character set is
// utf8mb4, so MySQL introduces each string with _utf8mb4.
const checkClauseReadTable = `CREATE TABLE t (id int PRIMARY KEY, c varchar(20),
	CONSTRAINT k_plain CHECK (id >= 0),
	CONSTRAINT k_quote CHECK (c <> 'it''s'),
	CONSTRAINT k_backslash CHECK (c <> 'p\\'),
	CONSTRAINT k_tab CHECK (c <> 'x\ty'),
	CONSTRAINT k_double CHECK (c <> "d'q"));`

// TestMySQLFamilyReaderYieldsACheckClauseAsSQLE2E reads each CHECK back
// through the reader every command uses. On both engines the clause is SQL
// the server accepts: MySQL's with each string introduced and the whole
// clause parenthesized, MariaDB's as the server prints it.
func TestMySQLFamilyReaderYieldsACheckClauseAsSQLE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		want   map[string]string
	}{
		{
			name:   "mysql",
			engine: dbtarget.MySQLAdmin,
			want: map[string]string{
				"k_plain":     "(`id` >= 0)",
				"k_quote":     "(`c` <> _utf8mb4'it\\'s')",
				"k_backslash": "(`c` <> _utf8mb4'p\\\\')",
				"k_tab":       "(`c` <> _utf8mb4'x\ty')",
				"k_double":    "(`c` <> _utf8mb4'd\\'q')",
			},
		},
		{
			name:   "mariadb",
			engine: dbtarget.MariaDBAdmin,
			want: map[string]string{
				"k_plain":     "`id` >= 0",
				"k_quote":     "`c` <> 'it\\'s'",
				"k_backslash": "`c` <> 'p\\\\'",
				"k_tab":       "`c` <> 'x\\ty'",
				"k_double":    "`c` <> 'd\\'q'",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, url := newMySQLFamilyScratch(c, test.engine).builtFrom(c, checkClauseReadTable)
			conn, err := dbschema.ConnectToDatabase(c.Context(), url)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

			read, err := conn.Reader().ReadSchemaContext(c.Context())

			c.Assert(err, qt.IsNil)
			got := make(map[string]string)
			for _, check := range slices.DeleteFunc(slices.Clone(read.Constraints), withoutCheckClause) {
				got[check.Name] = *check.CheckClause
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// withoutCheckClause reports a constraint that carries no CHECK clause.
func withoutCheckClause(constraint catalog.Constraint) bool {
	return constraint.CheckClause == nil
}

// checkClausesRead answers every CHECK condition of a database, as the server
// reports it, in name order.
const checkClausesRead = `SELECT CONSTRAINT_NAME AS n, CHECK_CLAUSE AS e
	FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ?`

// checkClausesAndIndexExpressionsRead answers what [checkClausesRead] does and
// every functional key part's expression too. MariaDB has neither the column
// nor the key part.
const checkClausesAndIndexExpressionsRead = checkClausesRead + `
	UNION ALL
	SELECT INDEX_NAME, EXPRESSION
	FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND COLUMN_NAME IS NULL`

// TestMySQLFamilyCheckRollbackRestoresTheCheckE2E generates a migration that
// drops a CHECK holding a quote, applies it and rolls it back. The rollback
// adds the CHECK from the text the reader yields, so the text has to be SQL the
// server accepts, and the server has to hold the same condition afterwards.
// On MySQL the table also carries a functional index on a string.
func TestMySQLFamilyCheckRollbackRestoresTheCheckE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		live   string
		read   string
	}{
		{
			name:   "mysql",
			engine: dbtarget.MySQLAdmin,
			live:   `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), KEY ix ((concat(c, 'it''s'))), CONSTRAINT k CHECK (c <> 'it''s'));`,
			read:   checkClausesAndIndexExpressionsRead,
		},
		{
			name:   "mariadb",
			engine: dbtarget.MariaDBAdmin,
			live:   `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'it''s'));`,
			read:   checkClausesRead,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, test.engine)
			database, url := scratch.builtFrom(c, test.live)
			built := scratch.expressionsOf(c, test.read, database)
			dir := c.TempDir()
			file := quotedTextSchemaFile(c, `CREATE TABLE t (id int PRIMARY KEY, c varchar(20));`)
			generated := runPtahIn(c, "migrations", "generate", "--db-url", url, "--schema-file", file, "--migrations-dir", dir, "--name", "drop_check")
			c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
			up := runPtahIn(c, "migrations", "up", "--db-url", url, "--migrations-dir", dir, "--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
			c.Assert(scratch.expressionsOf(c, test.read, database), qt.Equals, "")

			down := runPtahIn(c, "migrations", "down", "--db-url", url, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 0, historyComment(down))
			c.Assert(scratch.expressionsOf(c, test.read, database), qt.Equals, built)
		})
	}
}

// expressionsOf answers the rows read returns for database, its name bound to
// every placeholder, joined in name order.
func (s mysqlScratch) expressionsOf(c *qt.C, read, database string) string {
	c.Helper()
	args := make([]any, strings.Count(read, "?"))
	for i := range args {
		args[i] = database
	}
	var expressions string
	err := s.admin.QueryRowContext(c.Context(),
		"SELECT COALESCE(GROUP_CONCAT(x.e ORDER BY x.n SEPARATOR ' | '), '') FROM ("+read+") x",
		args...,
	).Scan(&expressions)
	c.Assert(err, qt.IsNil)
	return expressions
}
