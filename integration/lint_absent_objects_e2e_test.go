//go:build integration

package integration_test

import (
	"maps"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
)

// absentObjectRow is a directory whose second version drops an object that
// may not be there, or changes an enum type, and what each binary answers for
// it under `--dev-url <dev> --latest 1`. communityExit and communityTotal are
// the exit code and `.TotalChanges` of the pinned community binary v1.3.0,
// measured on that binary. strictExit and strictTotal are what ptah-compat
// answers in strict CE mode, fullExit and fullTotal what it answers on the
// default surface, and codes the rules it reports on both. nativeExit and
// nativeCodes are what `ptah migrations lint` answers. Where a row's exit code
// differs from the community binary's, the difference is DS107, Ptah's own
// rule, reporting the drop of an object the starting state holds.
type absentObjectRow struct {
	name           string
	files          map[string]string
	communityExit  int
	communityTotal string
	strictExit     int
	strictTotal    string
	fullExit       int
	fullTotal      string
	codes          []string
	nativeExit     int
	nativeCodes    []string
}

// absentObjectFiles is a directory of two versions, base and head.
func absentObjectFiles(base, head string) map[string]string {
	return map[string]string{"1_base.sql": base, "2_head.sql": head}
}

// sqliteTrigger is a trigger on the users table of [countBase].
const sqliteTrigger = "CREATE TRIGGER users_ai AFTER INSERT ON users BEGIN UPDATE users SET name = name WHERE id = NEW.id; END;\n"

// sqliteAbsentObjectRows are the SQLite directories.
var sqliteAbsentObjectRows = []absentObjectRow{
	{
		name:          "IF EXISTS drop of an absent trigger",
		files:         absentObjectFiles(countBase, "DROP TRIGGER IF EXISTS missing;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		name:          "control: IF EXISTS drop of a present trigger",
		files:         absentObjectFiles(countBase+sqliteTrigger, "DROP TRIGGER IF EXISTS users_ai;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 1, strictTotal: "0", fullExit: 1, fullTotal: "1",
		codes: []string{"DS107"}, nativeExit: 1, nativeCodes: []string{"DS107"},
	},
	{
		name:          "IF EXISTS drop of an absent view",
		files:         absentObjectFiles(countBase, "DROP VIEW IF EXISTS v;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		name:          "control: IF EXISTS drop of a present view",
		files:         absentObjectFiles(countBase+"CREATE VIEW v AS SELECT id FROM users;\n", "DROP VIEW IF EXISTS v;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "1",
		nativeExit: 0,
	},
	{
		name:          "IF EXISTS drop of an absent index qualified by main",
		files:         absentObjectFiles(countBase, "DROP INDEX IF EXISTS main.x;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		name:          "control: IF EXISTS drop of a present index qualified by main",
		files:         absentObjectFiles(countBase+"CREATE INDEX x ON users (name);\n", "DROP INDEX IF EXISTS main.x;\n"),
		communityExit: 0, communityTotal: "1", strictExit: 0, strictTotal: "1", fullExit: 0, fullTotal: "1",
		nativeExit: 0,
	},
	{
		name:          "IF EXISTS drop of an absent table qualified by main",
		files:         absentObjectFiles(countBase, "DROP TABLE IF EXISTS main.missing;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		name:          "a first file of twelve statements that creates main.a and drops a",
		files:         map[string]string{"1_init.sql": countTables(1, 10) + "CREATE TABLE main.a (id INTEGER);\nDROP TABLE a;\n"},
		communityExit: 0, communityTotal: "10", strictExit: 0, strictTotal: "10", fullExit: 0, fullTotal: "10",
		nativeExit: 0,
	},
	{
		name:          "a first file of twelve statements that creates a and drops main.a",
		files:         map[string]string{"1_init.sql": countTables(1, 10) + "CREATE TABLE a (id INTEGER);\nDROP TABLE main.a;\n"},
		communityExit: 0, communityTotal: "10", strictExit: 0, strictTotal: "10", fullExit: 0, fullTotal: "10",
		nativeExit: 0,
	},
}

// postgresTrigger is a trigger on the users table of [postgresCountBase], with
// the function it runs.
const postgresTrigger = "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;\n" +
	"CREATE TRIGGER users_tg BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION f();\n"

// postgresMood is an enum type beside the users table of [postgresCountBase].
const postgresMood = postgresCountBase + "CREATE TYPE mood AS ENUM ('a', 'b');\n"

// postgresAbsentObjectRows are the PostgreSQL directories, measured against
// PostgreSQL 17 under a dev URL whose search_path is public, on a fresh
// database for every run.
var postgresAbsentObjectRows = []absentObjectRow{
	{
		name:          "IF EXISTS drop of an absent trigger",
		files:         absentObjectFiles(postgresCountBase, "DROP TRIGGER IF EXISTS missing ON users;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		name:          "control: IF EXISTS drop of a present trigger",
		files:         absentObjectFiles(postgresCountBase+postgresTrigger, "DROP TRIGGER IF EXISTS users_tg ON users;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 1, strictTotal: "0", fullExit: 1, fullTotal: "1",
		codes: []string{"DS107"}, nativeExit: 1, nativeCodes: []string{"DS107"},
	},
	{
		name:          "IF EXISTS drop of an absent type",
		files:         absentObjectFiles(postgresCountBase, "DROP TYPE IF EXISTS missing;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		name:          "control: IF EXISTS drop of a present enum type",
		files:         absentObjectFiles(postgresMood, "DROP TYPE IF EXISTS mood;\n"),
		communityExit: 0, communityTotal: "1", strictExit: 1, strictTotal: "1", fullExit: 1, fullTotal: "1",
		codes: []string{"DS107"}, nativeExit: 1, nativeCodes: []string{"DS107"},
	},
	{
		name:          "drop of a composite type",
		files:         absentObjectFiles(postgresCountBase+"CREATE TYPE pair AS (a int, b int);\n", "DROP TYPE pair;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 1, strictTotal: "0", fullExit: 1, fullTotal: "1",
		codes: []string{"DS107"}, nativeExit: 1, nativeCodes: []string{"DS107"},
	},
	{
		name:          "IF EXISTS drop of an absent view",
		files:         absentObjectFiles(postgresCountBase, "DROP VIEW IF EXISTS v;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		name:          "IF EXISTS drop of an absent sequence",
		files:         absentObjectFiles(postgresCountBase, "DROP SEQUENCE IF EXISTS missing;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		// The sequence is described on the column that owns it. The community
		// binary counts the table, whose default the CASCADE removes; strict
		// mode counts no change to a sequence and does not see the default go.
		name: "control: IF EXISTS drop of a serial column's sequence",
		files: absentObjectFiles("CREATE TABLE users (id serial PRIMARY KEY, name text);\n",
			"DROP SEQUENCE IF EXISTS users_id_seq CASCADE;\n"),
		communityExit: 0, communityTotal: "1", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "1",
		nativeExit: 0,
	},
	{
		name:          "IF EXISTS drop of an absent column",
		files:         absentObjectFiles(postgresCountBase, "ALTER TABLE users DROP COLUMN IF EXISTS missing;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		name:          "control: IF EXISTS drop of a present column",
		files:         absentObjectFiles(postgresCountBase, "ALTER TABLE users DROP COLUMN IF EXISTS name;\n"),
		communityExit: 1, communityTotal: "1", strictExit: 1, strictTotal: "1", fullExit: 1, fullTotal: "1",
		codes: []string{"DS103"}, nativeExit: 1, nativeCodes: []string{"BC104", "DS102"},
	},
	{
		name:          "an enum label added",
		files:         absentObjectFiles(postgresMood, "ALTER TYPE mood ADD VALUE 'c';\n"),
		communityExit: 0, communityTotal: "1", strictExit: 0, strictTotal: "1", fullExit: 0, fullTotal: "1",
		codes: []string{"PG102"}, nativeExit: 0, nativeCodes: []string{"PG102"},
	},
	{
		name:          "an enum label added IF NOT EXISTS that the type has",
		files:         absentObjectFiles(postgresMood, "ALTER TYPE mood ADD VALUE IF NOT EXISTS 'a';\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		codes: []string{"PG102"}, nativeExit: 0, nativeCodes: []string{"PG102"},
	},
	{
		name:          "an enum label renamed",
		files:         absentObjectFiles(postgresMood, "ALTER TYPE mood RENAME VALUE 'a' TO 'z';\n"),
		communityExit: 0, communityTotal: "1", strictExit: 0, strictTotal: "1", fullExit: 0, fullTotal: "1",
		nativeExit: 0,
	},
	{
		name:          "a first file of twelve statements that creates public.a and drops a",
		files:         map[string]string{"1_init.sql": countTables(1, 10) + "CREATE TABLE public.a (id int);\nDROP TABLE a;\n"},
		communityExit: 0, communityTotal: "10", strictExit: 0, strictTotal: "10", fullExit: 0, fullTotal: "10",
		nativeExit: 0,
	},
}

// timescaleDaily is a version 1 that creates a hypertable and a continuous
// aggregate of it, daily.
const timescaleDaily = "CREATE TABLE metrics (time timestamptz NOT NULL, value double precision);\n" +
	"SELECT create_hypertable('metrics', 'time');\n" +
	"CREATE MATERIALIZED VIEW daily WITH (timescaledb.continuous) AS\n" +
	"SELECT time_bucket('1 day', time) AS bucket, avg(value) AS value FROM metrics GROUP BY bucket WITH NO DATA;\n"

// timescaleAbsentObjectRows are the TimescaleDB directories, measured against
// TimescaleDB 2.32.0 on PostgreSQL 17. The reader records a continuous
// aggregate as a TimescaleDB object and on neither view list, and the
// community binary does not describe it.
var timescaleAbsentObjectRows = []absentObjectRow{
	{
		name:          "control: IF EXISTS drop of a continuous aggregate",
		files:         absentObjectFiles(timescaleDaily, "DROP MATERIALIZED VIEW IF EXISTS daily;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "1",
		nativeExit: 0,
	},
}

// mysqlCountBase is the version 1 the MySQL rows below start from.
const mysqlCountBase = "CREATE TABLE users (id int, name text);\n"

// mysqlAbsentObjectRows are the MySQL directories, measured against MySQL 8.4
// on a fresh database for every run.
var mysqlAbsentObjectRows = []absentObjectRow{
	{
		name:          "IF EXISTS drop of an absent trigger",
		files:         absentObjectFiles(mysqlCountBase, "DROP TRIGGER IF EXISTS missing;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
	{
		name: "control: drop of a present trigger",
		files: absentObjectFiles(mysqlCountBase+"CREATE TRIGGER users_ai AFTER INSERT ON users FOR EACH ROW SET @x = 1;\n",
			"DROP TRIGGER users_ai;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 1, strictTotal: "0", fullExit: 1, fullTotal: "1",
		codes: []string{"DS107"}, nativeExit: 1, nativeCodes: []string{"DS107"},
	},
	{
		name:          "IF EXISTS drop of an absent view",
		files:         absentObjectFiles(mysqlCountBase, "DROP VIEW IF EXISTS v;\n"),
		communityExit: 0, communityTotal: "0", strictExit: 0, strictTotal: "0", fullExit: 0, fullTotal: "0",
		nativeExit: 0,
	},
}

// compatRuleCode matches a rule a `migrate lint` text report names, as in
// `-- L1 [DS107]: ...` or `-- L1: Dropping ... analyzers#DS103`.
var compatRuleCode = regexp.MustCompile(`(?:-- L\d+ \[|analyzers#)([A-Z]+\d+P?)\b`)

// nativeRuleCode matches a rule a `ptah migrations lint` report names, as in
// `migrations/2_head.sql:1 [error] DS107: ...`.
var nativeRuleCode = regexp.MustCompile(`(?m)^\S+:\d+ \[[a-z]+\] ([A-Z]+\d+P?):`)

// ruleCodes returns the rules a report names, each once, in code order, and
// nil for none.
func ruleCodes(code *regexp.Regexp, stdout string) []string {
	named := make(map[string]bool)
	for _, match := range code.FindAllStringSubmatch(stdout, -1) {
		named[match[1]] = true
	}
	return slices.Sorted(maps.Keys(named))
}

// absentObjectDev is where the runs of one engine replay. url returns a fresh
// dev database for every run, and env holds what a run adds to its
// environment.
type absentObjectDev struct {
	url func(*qt.C) string
	env []string
}

// disposableServer declares a server dev database disposable, which a version
// that creates a routine or a trigger needs: Ptah refuses to replay a stored
// body on a server it cannot confine the body's effects to.
var disposableServer = []string{devdocker.DisposableServerEnvVar + "=1"}

// assertCompatAbsentObject runs ptah-compat on the row's directory in strict
// CE mode and on the default surface, and holds every answer to the row.
func assertCompatAbsentObject(c *qt.C, row absentObjectRow, dev absentObjectDev) {
	c.Helper()
	dir := changeCountProject(c, row.files)
	full := clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), dev.env...)}
	strict := clirun.Options{Dir: dir, Env: append(strictEnvironment(), dev.env...)}

	strictText := clirun.Run(c, clirun.Compat, strict, changeCountArgs(dev.url(c), "1")...)
	strictTotal := clirun.Run(c, clirun.Compat, strict, changeCountArgs(dev.url(c), "1", totalTemplate...)...)
	fullText := clirun.Run(c, clirun.Compat, full, changeCountArgs(dev.url(c), "1")...)
	fullTotal := clirun.Run(c, clirun.Compat, full, changeCountArgs(dev.url(c), "1", totalTemplate...)...)

	c.Assert(strictText.ExitCode, qt.Equals, row.strictExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", strictText.Stdout, strictText.Stderr))
	c.Assert(ruleCodes(compatRuleCode, strictText.Stdout), qt.DeepEquals, row.codes, qt.Commentf("stdout:\n%s", strictText.Stdout))
	c.Assert(strictTotal.Stdout, qt.Equals, row.strictTotal, qt.Commentf("stderr:\n%s", strictTotal.Stderr))
	c.Assert(fullText.ExitCode, qt.Equals, row.fullExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", fullText.Stdout, fullText.Stderr))
	c.Assert(ruleCodes(compatRuleCode, fullText.Stdout), qt.DeepEquals, row.codes, qt.Commentf("stdout:\n%s", fullText.Stdout))
	c.Assert(fullTotal.Stdout, qt.Equals, row.fullTotal, qt.Commentf("stderr:\n%s", fullTotal.Stderr))
}

// assertNativeAbsentObject runs `ptah migrations lint` on the row's directory
// and holds its answer to the row.
func assertNativeAbsentObject(c *qt.C, row absentObjectRow, dev absentObjectDev) {
	c.Helper()
	dir := changeCountProject(c, row.files)
	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), dev.env...)},
		"migrations", "lint", "--dir", "migrations", "--dev-url", dev.url(c), "--latest", "1")

	c.Assert(got.ExitCode, qt.Equals, row.nativeExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(ruleCodes(nativeRuleCode, got.Stdout), qt.DeepEquals, row.nativeCodes, qt.Commentf("stdout:\n%s", got.Stdout))
}

// assertCommunityAbsentObject runs the pinned community binary on the row's
// directory, each run on the dev database devURL returns, so the row's
// community answers stay its measurement.
func assertCommunityAbsentObject(c *qt.C, reference string, row absentObjectRow, devURL func(*qt.C) string) {
	c.Helper()
	dir := changeCountProject(c, row.files)

	text, textStderr, textErr := runCLIProcess(c.Context(), dir, reference, changeCountArgs(devURL(c), "1")...)
	total, totalStderr, _ := runCLIProcess(c.Context(), dir, reference, changeCountArgs(devURL(c), "1", totalTemplate...)...)

	c.Assert(exitCodeOf(textErr), qt.Equals, row.communityExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", text, textStderr))
	c.Assert(total, qt.Equals, row.communityTotal, qt.Commentf("stderr:\n%s", totalStderr))
}

// sqliteDev is the in-memory dev database of every SQLite run.
var sqliteDev = absentObjectDev{url: func(*qt.C) string { return sqliteDevURL }}

// postgresDev is a fresh database of the PostgreSQL target for every run.
func postgresDev(t *testing.T) absentObjectDev {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	return absentObjectDev{
		url: func(c *qt.C) string {
			return postgresDevURL(c, postgresAdmin(c, adminURL), adminURL)
		},
		env: disposableServer,
	}
}

// timescaleDev is a fresh database of the TimescaleDB target for every run,
// with the extension created in it before the run starts.
func timescaleDev(t *testing.T) absentObjectDev {
	adminURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	return absentObjectDev{
		url: func(c *qt.C) string {
			url := postgresDevURL(c, postgresAdmin(c, adminURL), adminURL)
			dev := postgresAdmin(c, url)
			_, err := dev.ExecContext(c.Context(), "CREATE EXTENSION IF NOT EXISTS timescaledb")
			c.Assert(err, qt.IsNil)
			return url
		},
		env: disposableServer,
	}
}

// mysqlDevURL returns a dev URL on a fresh database of the MySQL target, in
// the form ptah connects with.
func mysqlDevURL(c *qt.C) string {
	c.Helper()
	_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "lint_absent")
	return url
}

// mysqlDev is a fresh database of the MySQL target for every run.
var mysqlDev = absentObjectDev{url: mysqlDevURL, env: disposableServer}

// mysqlTCPAddress is the `tcp(host:port)` address of Ptah's driver-style
// MySQL URL.
var mysqlTCPAddress = regexp.MustCompile(`@tcp\(([^)]*)\)/`)

// communityMySQLURL is a MySQL dev URL with its address written host:port,
// the one form the community binary reads.
func communityMySQLURL(url string) string {
	return mysqlTCPAddress.ReplaceAllString(url, "@$1/")
}

// communityMySQLDevURL is [mysqlDevURL] in the form [communityMySQLURL]
// writes.
func communityMySQLDevURL(c *qt.C) string {
	c.Helper()
	return communityMySQLURL(mysqlDevURL(c))
}

// TestCompatMigrateLintAbsentObjectsE2E holds ptah-compat's answer for each
// directory on SQLite, PostgreSQL, TimescaleDB and MySQL: an IF EXISTS drop
// of an object the starting state does not hold changes nothing and reports
// nothing.
func TestCompatMigrateLintAbsentObjectsE2E(t *testing.T) {
	for _, row := range sqliteAbsentObjectRows {
		t.Run("sqlite/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertCompatAbsentObject(c, row, sqliteDev)
		})
	}
	postgres := postgresDev(t)
	for _, row := range postgresAbsentObjectRows {
		t.Run("postgres/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertCompatAbsentObject(c, row, postgres)
		})
	}
	timescale := timescaleDev(t)
	for _, row := range timescaleAbsentObjectRows {
		t.Run("timescaledb/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertCompatAbsentObject(c, row, timescale)
		})
	}
	for _, row := range mysqlAbsentObjectRows {
		t.Run("mysql/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertCompatAbsentObject(c, row, mysqlDev)
		})
	}
}

// TestMigrationsLintAbsentObjectsE2E holds the native report for the same
// directories.
func TestMigrationsLintAbsentObjectsE2E(t *testing.T) {
	for _, row := range sqliteAbsentObjectRows {
		t.Run("sqlite/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertNativeAbsentObject(c, row, sqliteDev)
		})
	}
	postgres := postgresDev(t)
	for _, row := range postgresAbsentObjectRows {
		t.Run("postgres/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertNativeAbsentObject(c, row, postgres)
		})
	}
	timescale := timescaleDev(t)
	for _, row := range timescaleAbsentObjectRows {
		t.Run("timescaledb/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertNativeAbsentObject(c, row, timescale)
		})
	}
	for _, row := range mysqlAbsentObjectRows {
		t.Run("mysql/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertNativeAbsentObject(c, row, mysqlDev)
		})
	}
}

// TestCommunityMigrateLintAbsentObjectsE2E runs the pinned community binary
// on the same directories, so their community answers stay its measurement.
func TestCommunityMigrateLintAbsentObjectsE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, row := range sqliteAbsentObjectRows {
		t.Run("sqlite/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertCommunityAbsentObject(c, reference, row, sqliteDev.url)
		})
	}
	postgres := postgresDev(t)
	for _, row := range postgresAbsentObjectRows {
		t.Run("postgres/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertCommunityAbsentObject(c, reference, row, postgres.url)
		})
	}
	timescale := timescaleDev(t)
	for _, row := range timescaleAbsentObjectRows {
		t.Run("timescaledb/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertCommunityAbsentObject(c, reference, row, timescale.url)
		})
	}
	for _, row := range mysqlAbsentObjectRows {
		t.Run("mysql/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertCommunityAbsentObject(c, reference, row, communityMySQLDevURL)
		})
	}
}

// mysqlQualifiedDrop writes a project on a fresh MySQL database whose first
// version creates users, other and a view of users, and whose second drops
// other qualified by the database's name, and returns the project and the dev
// URL in the form ptah connects with.
func mysqlQualifiedDrop(c *qt.C) (dir, devURL string) {
	c.Helper()
	name, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "lint_qualified")
	dir = changeCountProject(c, absentObjectFiles(
		mysqlCountBase+"CREATE TABLE other (id int);\nCREATE VIEW v AS SELECT id FROM users;\n",
		"DROP TABLE IF EXISTS "+name+".other;\n",
	))
	return dir, url
}

// TestMigrateLintMySQLQualifiedDropBesideAViewE2E drops a table the starting
// state holds, qualified by the name of the dev database, from a state that
// also holds a view. The MySQL reader names the database on a view and on no
// table; a state read as written holds the view in a schema of its own and no
// table there, so the drop would read as the drop of a table that is not
// there. Every surface reports it, as the community binary does.
func TestMigrateLintMySQLQualifiedDropBesideAViewE2E(t *testing.T) {
	t.Run("ptah-compat", func(t *testing.T) {
		c := qt.New(t)
		dir, devURL := mysqlQualifiedDrop(c)
		got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
			changeCountArgs(devURL, "1")...)

		c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
		c.Assert(ruleCodes(compatRuleCode, got.Stdout), qt.DeepEquals, []string{"DS102"})
		c.Assert(summaryCount(got.Stdout), qt.Equals, "-- 1 schema change")
	})
	t.Run("ptah migrations lint", func(t *testing.T) {
		c := qt.New(t)
		dir, devURL := mysqlQualifiedDrop(c)
		got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
			"migrations", "lint", "--dir", "migrations", "--dev-url", devURL, "--latest", "1")

		c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
		c.Assert(ruleCodes(nativeRuleCode, got.Stdout), qt.DeepEquals, []string{"BC103", "DS101"})
	})
	t.Run("community binary", func(t *testing.T) {
		reference := atlasReferenceBinary(t)
		c := qt.New(t)
		dir, devURL := mysqlQualifiedDrop(c)
		text, stderr, err := runCLIProcess(c.Context(), dir, reference,
			changeCountArgs(communityMySQLURL(devURL), "1")...)

		c.Assert(exitCodeOf(err), qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", text, stderr))
		c.Assert(summaryCount(text), qt.Equals, "-- 1 schema change")
	})
}
