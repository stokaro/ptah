//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// keySelectorTarget is a database built from a row's live schema, and a way to
// read one text value back from it.
type keySelectorTarget struct {
	url  string
	read func(c *qt.C, query string) string
}

func postgresKeySelectorTarget(c *qt.C, live string) keySelectorTarget {
	c.Helper()
	url := postgresScratchDevURL(c, "ptah_key_selector")
	execPostgresFamilySQL(c, c.Context(), url, live)
	return keySelectorTarget{url: url, read: func(c *qt.C, query string) string {
		c.Helper()
		var value string
		c.Assert(openReadScopeDatabase(c, url).QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil)
		return value
	}}
}

// mySQLFamilyKeySelectorTarget builds the database on engine's server, and
// names it with the URL scheme that states engine's dialect.
func mySQLFamilyKeySelectorTarget(engine foreignKeySelectorEngine) func(c *qt.C, live string) keySelectorTarget {
	return func(c *qt.C, live string) keySelectorTarget {
		c.Helper()
		scratch := newMySQLFamilyScratch(c, engine.admin)
		name, url := scratch.builtFrom(c, live)
		return keySelectorTarget{url: engine.scheme + strings.TrimPrefix(url, "mysql://"), read: readMySQLFamily(scratch, name)}
	}
}

// keySelectorServers are the builders of a database on each server the rows
// name.
func keySelectorServers() map[string]func(c *qt.C, live string) keySelectorTarget {
	servers := map[string]func(c *qt.C, live string) keySelectorTarget{"postgresql": postgresKeySelectorTarget}
	for _, engine := range foreignKeySelectorEngines {
		servers[engine.name] = mySQLFamilyKeySelectorTarget(engine)
	}
	return servers
}

func readMySQLFamily(scratch mysqlScratch, name string) func(c *qt.C, query string) string {
	return func(c *qt.C, query string) string {
		c.Helper()
		db, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name))
		c.Assert(err, qt.IsNil)
		defer func() { c.Check(db.Close(), qt.IsNil) }()
		var value string
		c.Assert(db.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil)
		return value
	}
}

const (
	postgresKindsIndexes = "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes " +
		"WHERE schemaname = 'public' AND tablename = 'kinds'"
	mySQLKindsIndexes = "SELECT GROUP_CONCAT(DISTINCT INDEX_NAME ORDER BY INDEX_NAME) FROM information_schema.STATISTICS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kinds'"
	mySQLKindsID = "SELECT CONCAT_WS(' ', COLUMN_KEY, EXTRA, IS_NULLABLE) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kinds' AND COLUMN_NAME = 'id'"
)

// TestExcludeNamesAKeyByTheTargetsNameE2E applies a desired schema with a
// selector, reads the table back, and plans and compares again. A selector
// that names a key by the name the target's server gives it keeps the key out
// of the plan, and the runs that follow find nothing to do.
//
// The UNIQUE rows place an unrelated index under the name the other engine
// would give the column's UNIQUE, and name that index. The UNIQUE the server
// holds is not named, so it stays, under its own name; a desired side that
// read the other engine's name as the UNIQUE's would drop it.
func TestExcludeNamesAKeyByTheTargetsNameE2E(t *testing.T) {
	type keySelectorRow struct {
		name, server                 string
		live, desired, exclude, read string
		want                         string
	}
	rows := []keySelectorRow{{
		name:   "postgresql/an index named as MySQL names a UNIQUE",
		server: "postgresql",
		live: "CREATE TABLE public.kinds (id int PRIMARY KEY, code text NOT NULL, CONSTRAINT kinds_code_key UNIQUE (code));\n" +
			"CREATE INDEX code ON public.kinds (id, code);\n",
		desired: "CREATE TABLE public.kinds (id int PRIMARY KEY, code text NOT NULL UNIQUE);\n",
		exclude: "public.kinds.code[type=index]",
		read:    postgresKindsIndexes,
		want:    "code,kinds_code_key,kinds_pkey",
	}}
	for _, engine := range foreignKeySelectorEngines {
		rows = append(rows,
			keySelectorRow{
				name:   engine.name + "/an index named as PostgreSQL names a UNIQUE",
				server: engine.name,
				live: "CREATE TABLE kinds (id int NOT NULL, code varchar(20) NOT NULL, PRIMARY KEY (id), " +
					"UNIQUE KEY code (code), KEY kinds_code_key (id, code));",
				desired: "CREATE TABLE kinds (id int NOT NULL, code varchar(20) NOT NULL UNIQUE, PRIMARY KEY (id));\n",
				exclude: "kinds.kinds_code_key[type=index]",
				read:    mySQLKindsIndexes,
				want:    "code,kinds_code_key,PRIMARY",
			},
			keySelectorRow{
				name:    engine.name + "/the primary key as an index, declared on the column",
				server:  engine.name,
				live:    "CREATE TABLE kinds (id int NOT NULL AUTO_INCREMENT, code varchar(20), PRIMARY KEY (id));",
				desired: "CREATE TABLE kinds (id int AUTO_INCREMENT PRIMARY KEY, code varchar(20));\n",
				exclude: "kinds.PRIMARY[type=index]",
				read:    mySQLKindsID,
				want:    "PRI auto_increment NO",
			},
			keySelectorRow{
				name:    engine.name + "/the primary key as a constraint, declared nowhere",
				server:  engine.name,
				live:    "CREATE TABLE kinds (id int NOT NULL AUTO_INCREMENT, code varchar(20), PRIMARY KEY (id));",
				desired: "CREATE TABLE kinds (id int NOT NULL AUTO_INCREMENT, code varchar(20));\n",
				exclude: "kinds.PRIMARY[type=constraint]",
				read:    mySQLKindsID,
				want:    "PRI auto_increment NO",
			},
		)
	}
	servers := keySelectorServers()
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := servers[row.server](c, row.live)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(row.desired), 0o600), qt.IsNil)
			apply := []string{"schema", "apply", "--db-url", target.url, "--schema-file", schema, "--exclude", row.exclude}

			applied := runPtahIn(c, append(apply, "--auto-approve")...)
			read := target.read(c, row.read)
			planned := runPtahIn(c, append(apply, "--dry-run")...)
			compared := runPtahIn(c, "schema", "compare", "--db-url", target.url, "--schema-file", schema, "--exit-code", "--exclude", row.exclude)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(read, qt.Equals, row.want)
			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(compared.ExitCode, qt.Equals, 0, historyComment(compared))
			c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, includeNoDiffs)
		})
	}
}

// sqliteKeySelectorLive holds a CHECK, a column's UNIQUE and an index.
const sqliteKeySelectorLive = "CREATE TABLE kinds (id integer PRIMARY KEY, code text NOT NULL UNIQUE, " +
	"CONSTRAINT code_len CHECK (length(code) > 0));\nCREATE INDEX kinds_code ON kinds (code);\n"

// TestExcludeRefusesAConstraintSQLiteRewritesE2E excludes a CHECK on SQLite,
// where a desired schema without the UNIQUE plans a new table written from the
// declaration. The CHECK the selector keeps out of scope would not be in it, so
// both binaries refuse the selector, and the table keeps its CHECK and its
// UNIQUE. Excluding the whole table keeps both.
func TestExcludeRefusesAConstraintSQLiteRewritesE2E(t *testing.T) {
	const refusal = `exclude selector "kinds.code_len[type=check]" names constraint "code_len" of table "kinds", ` +
		`and sqlite changes a constraint only by writing a new table from the declaration, ` +
		`which would leave the constraint out; exclude the whole table instead`
	c := qt.New(t)
	dir := c.TempDir()
	path := filepath.Join(dir, "live.db")
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(c.Context(), sqliteKeySelectorLive)
	c.Assert(err, qt.IsNil)
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE kinds (id integer PRIMARY KEY, code text NOT NULL);\n"), 0o600), qt.IsNil)
	url := "sqlite://" + filepath.ToSlash(path)

	applied := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", schema, "--exclude", "kinds.code_len[type=check]", "--auto-approve")
	diffed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "diff",
		"--from", url, "--to", "file://"+filepath.ToSlash(schema), "--dev-url", "sqlite://dev?mode=memory",
		"--exclude", "kinds.code_len[type=check]")
	whole := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", schema, "--exclude", "kinds", "--auto-approve")
	var table string
	c.Assert(db.QueryRowContext(c.Context(), "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'kinds'").Scan(&table), qt.IsNil)

	c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
	c.Assert(applied.Stderr, qt.Contains, refusal)
	c.Assert(diffed.ExitCode, qt.Equals, 1, historyComment(diffed))
	c.Assert(diffed.Stderr, qt.Contains, refusal)
	c.Assert(whole.ExitCode, qt.Equals, 0, historyComment(whole))
	c.Assert(table, qt.Equals, "CREATE TABLE kinds (id integer PRIMARY KEY, code text NOT NULL UNIQUE, "+
		"CONSTRAINT code_len CHECK (length(code) > 0))")
}
