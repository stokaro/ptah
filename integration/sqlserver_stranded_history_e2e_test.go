//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A run that names no schema keeps the history in dbo. A history an account
// kept in its own default schema, through --migrations-schema or a URL that
// named the schema, is out of sight there, and reading the empty dbo would
// apply every migration again. So the run refuses and names both schemas. A
// table of the history's name that another tool keeps there holds no history,
// and the run starts its own in dbo.

// sqlServerStrandedHistory is the refusal for an account whose default schema
// is app, after the table name.
const sqlServerStrandedHistory = `in schema "app", the default schema of the connection's user, and none in ` +
	`schema "dbo", where this run keeps and reads it: pass --migrations-schema app (--revisions-schema app in ` +
	`ptah-compat), or add schema=app to the URL, to keep using the table where it is; move the migration ` +
	`tables into schema "dbo"; or pass --migrations-schema dbo (--revisions-schema dbo in ptah-compat) to start ` +
	`a new history in schema "dbo"`

// sqlServerAppMigrations creates a table in app once and inserts one row into
// it, so a run that applies them again leaves an extra row.
var sqlServerAppMigrations = map[string]string{
	"1_create.up.sql":   "IF OBJECT_ID(N'app.acct', N'U') IS NULL CREATE TABLE app.acct (id INT NOT NULL);\n",
	"1_create.down.sql": "DROP TABLE app.acct;\n",
	"2_seed.up.sql":     "INSERT INTO app.acct VALUES (1);\n",
	"2_seed.down.sql":   "DELETE FROM app.acct;\n",
}

// sqlServerAppRows counts the rows the migrations of sqlServerAppMigrations
// inserted.
const sqlServerAppRows = `SELECT CONVERT(nvarchar(20), COUNT(*)) FROM app.acct`

// TestMigrationsSQLServerRefuseAHistoryInTheAccountsSchemaE2E keeps the
// history of an account whose default schema is app in app, first through
// --migrations-schema and then through the URL's schema parameter, and runs
// up and status without either. Each is refused, and the migrations are not
// applied again.
func TestMigrationsSQLServerRefuseAHistoryInTheAccountsSchemaE2E(t *testing.T) {
	tests := []struct {
		name  string
		named func(account, dir string) []string
	}{
		{"kept through --migrations-schema", func(account, dir string) []string {
			return []string{"migrations", "up", "--db-url", account, "--migrations-dir", dir, "--migrations-schema", "app"}
		}},
		{"kept through the URL's schema parameter", func(account, dir string) []string {
			return []string{"migrations", "up", "--db-url", account + "&schema=app", "--migrations-dir", dir}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbURL := sqlServerRoleScratchDatabase(c)
			account := sqlServerAccount(c, dbURL, "app")
			dir := c.TempDir()
			writeFiles(c, dir, sqlServerAppMigrations)
			run := func(args ...string) clirun.Result {
				return clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute}, args...)
			}

			kept := run(test.named(account, dir)...)
			up := run("migrations", "up", "--db-url", account, "--migrations-dir", dir)
			status := run("migrations", "status", "--db-url", account, "--migrations-dir", dir)

			c.Assert(kept.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", kept.Stdout, kept.Stderr))
			c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(up.Stderr, qt.Contains, `found revision table "schema_migrations" `+sqlServerStrandedHistory)
			c.Assert(status.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", status.Stdout, status.Stderr))
			c.Assert(status.Stderr, qt.Contains, `found revision table "schema_migrations" `+sqlServerStrandedHistory)
			c.Assert(heldColumns(c, dbURL, sqlServerAppRows), qt.Equals, "1")
			c.Assert(heldColumns(c, dbURL, sqlServerOwnTables), qt.Equals, "app.schema_migrations,app.schema_migrations_log")
		})
	}
}

// TestMigrateApplySQLServerRefusesAHistoryInTheAccountsSchemaE2E is the same
// through ptah-compat migrate apply and migrate status, with the Atlas
// revision table kept in app through --revisions-schema.
func TestMigrateApplySQLServerRefusesAHistoryInTheAccountsSchemaE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")
	dir := c.TempDir()
	writeFiles(c, dir, map[string]string{
		"20260101000001_create.sql": sqlServerAppMigrations["1_create.up.sql"],
		"20260101000002_seed.sql":   sqlServerAppMigrations["2_seed.up.sql"],
	})
	dirURL := "file://" + filepath.ToSlash(dir)
	run := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Timeout: time.Minute}, args...)
	}

	hashed := run("migrate", "hash", "--dir", dirURL)
	kept := run("migrate", "apply", "--url", account, "--dir", dirURL, "--revisions-schema", "app")
	apply := run("migrate", "apply", "--url", account, "--dir", dirURL)
	status := run("migrate", "status", "--url", account, "--dir", dirURL)

	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))
	c.Assert(kept.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", kept.Stdout, kept.Stderr))
	c.Assert(apply.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", apply.Stdout, apply.Stderr))
	c.Assert(apply.Stderr, qt.Contains, `found revision table "atlas_schema_revisions" `+sqlServerStrandedHistory)
	c.Assert(status.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", status.Stdout, status.Stderr))
	c.Assert(status.Stderr, qt.Contains, `found revision table "atlas_schema_revisions" `+sqlServerStrandedHistory)
	c.Assert(heldColumns(c, dbURL, sqlServerAppRows), qt.Equals, "1")
	c.Assert(heldColumns(c, dbURL, sqlServerOwnTables), qt.Equals, "app.atlas_schema_revisions")
}

// TestMigrationsSQLServerReadAHistoryInTheSchemaTheRunNamesE2E is the
// control: the same account names app again, through the flag or the URL,
// and finds its history there. Status prints the schema it reads.
func TestMigrationsSQLServerReadAHistoryInTheSchemaTheRunNamesE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")
	dir := c.TempDir()
	writeFiles(c, dir, sqlServerAppMigrations)
	run := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute}, args...)
	}

	kept := run("migrations", "up", "--db-url", account, "--migrations-dir", dir, "--migrations-schema", "app")
	byFlag := run("migrations", "up", "--db-url", account, "--migrations-dir", dir, "--migrations-schema", "app")
	byURL := run("migrations", "status", "--db-url", account+"&schema=app", "--migrations-dir", dir)

	c.Assert(kept.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", kept.Stdout, kept.Stderr))
	c.Assert(byFlag.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", byFlag.Stdout, byFlag.Stderr))
	c.Assert(byFlag.Stdout, qt.Contains, "Database is already up to date!")
	c.Assert(byURL.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", byURL.Stdout, byURL.Stderr))
	c.Assert(byURL.Stdout, qt.Contains, "Schema: app\n\nCurrent Version: 2\n")
	c.Assert(heldColumns(c, dbURL, sqlServerAppRows), qt.Equals, "1")
}

// TestMigrationsStatusSQLServerPrintsTheSchemaItReadsE2E reads, as an account
// whose default schema is app, the history sa kept in dbo: status prints dbo,
// where it reads.
func TestMigrationsStatusSQLServerPrintsTheSchemaItReadsE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")
	dir := c.TempDir()
	writeFiles(c, dir, sqlServerAppMigrations)

	asSA := clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
		"migrations", "up", "--db-url", dbURL, "--migrations-dir", dir)
	status := clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
		"migrations", "status", "--db-url", account, "--migrations-dir", dir)

	c.Assert(asSA.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", asSA.Stdout, asSA.Stderr))
	c.Assert(status.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", status.Stdout, status.Stderr))
	c.Assert(status.Stdout, qt.Contains, "Schema: dbo\n\nCurrent Version: 2\n")
}

// TestMigrationsSQLServerStartAHistoryBesideAnotherToolsTableE2E runs, as an
// account whose default schema is app, beside a table in app that another tool
// keeps under the history's name: golang-migrate's schema_migrations for ptah,
// and a table of one column for ptah-compat. Neither holds a history, so each
// run creates its own in dbo and applies the migrations once, and the other
// tool's table stays as it was.
func TestMigrationsSQLServerStartAHistoryBesideAnotherToolsTableE2E(t *testing.T) {
	tests := []struct {
		name    string
		foreign string
		target  clirun.Target
		args    []string
		tables  string
	}{
		{
			name:    "ptah beside golang-migrate's table",
			foreign: "CREATE TABLE app.schema_migrations (version BIGINT PRIMARY KEY, dirty BIT NOT NULL)",
			target:  clirun.Ptah,
			args:    []string{"migrations", "up", "--migrations-dir", "{dir}", "--db-url", "{account}"},
			tables:  "app.schema_migrations,dbo.schema_migrations,dbo.schema_migrations_log",
		},
		{
			name:    "ptah-compat beside a table of one column",
			foreign: "CREATE TABLE app.atlas_schema_revisions (id INT NOT NULL)",
			target:  clirun.Compat,
			args:    []string{"migrate", "apply", "--dir", "{dirURL}", "--url", "{account}"},
			tables:  "app.atlas_schema_revisions,dbo.atlas_schema_revisions",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbURL := sqlServerRoleScratchDatabase(c)
			account := sqlServerAccount(c, dbURL, "app")
			_, err := connectDevDialect(c, dbURL).ExecContext(c.Context(), test.foreign)
			c.Assert(err, qt.IsNil)
			dir := c.TempDir()
			writeFiles(c, dir, sqlServerAppMigrations)
			atlasDir := c.TempDir()
			writeFiles(c, atlasDir, map[string]string{
				"20260101000001_create.sql": sqlServerAppMigrations["1_create.up.sql"],
				"20260101000002_seed.sql":   sqlServerAppMigrations["2_seed.up.sql"],
			})
			dirURL := "file://" + filepath.ToSlash(atlasDir)
			hashed := clirun.Run(c, clirun.Compat, clirun.Options{Timeout: time.Minute}, "migrate", "hash", "--dir", dirURL)
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))
			args := make([]string, 0, len(test.args))
			for _, arg := range test.args {
				args = append(args, strings.NewReplacer("{dir}", dir, "{dirURL}", dirURL, "{account}", account).Replace(arg))
			}

			got := clirun.Run(c, test.target, clirun.Options{Timeout: time.Minute}, args...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(heldColumns(c, dbURL, sqlServerAppRows), qt.Equals, "1")
			c.Assert(heldColumns(c, dbURL, sqlServerOwnTables), qt.Equals, test.tables)
		})
	}
}

// TestMigrationsSQLServerReadTheHistoryInDboBesideOneInAppE2E keeps a history
// in app, through --migrations-schema, and one in dbo, as sa: the second run
// applies the insert again, so app.acct holds two rows. The account then runs
// up without a schema. dbo holds a history, so the run reads it, finds every
// migration applied, and applies nothing.
func TestMigrationsSQLServerReadTheHistoryInDboBesideOneInAppE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")
	dir := c.TempDir()
	writeFiles(c, dir, sqlServerAppMigrations)
	run := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute}, args...)
	}

	inApp := run("migrations", "up", "--db-url", account, "--migrations-dir", dir, "--migrations-schema", "app")
	inDbo := run("migrations", "up", "--db-url", dbURL, "--migrations-dir", dir)
	unnamed := run("migrations", "up", "--db-url", account, "--migrations-dir", dir)

	c.Assert(inApp.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", inApp.Stdout, inApp.Stderr))
	c.Assert(inDbo.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", inDbo.Stdout, inDbo.Stderr))
	c.Assert(unnamed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", unnamed.Stdout, unnamed.Stderr))
	c.Assert(unnamed.Stdout, qt.Contains, "Database is already up to date!")
	c.Assert(heldColumns(c, dbURL, sqlServerAppRows), qt.Equals, "2")
}

// sqlServerLimitedAccount creates a user whose default schema is schema, who
// may create tables in that schema alone, and returns dbURL logged in as it.
func sqlServerLimitedAccount(c *qt.C, dbURL, schema string) string {
	c.Helper()
	admin := connectDevDialect(c, dbtarget.URL(c, dbtarget.SQLServer))
	login := fmt.Sprintf("ptah_limited_%d", time.Now().UnixNano()%1_000_000_000_000)
	password := fmt.Sprintf("L1mited-%d-Aa!", time.Now().UnixNano())
	_, err := admin.ExecContext(c.Context(), fmt.Sprintf("CREATE LOGIN [%s] WITH PASSWORD = N'%s', CHECK_POLICY = OFF", login, password))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), fmt.Sprintf("DROP LOGIN [%s]", login))
		c.Check(dropErr, qt.IsNil)
	})
	conn := connectDevDialect(c, dbURL)
	for _, batch := range []string{
		"CREATE SCHEMA " + schema,
		fmt.Sprintf("CREATE USER [%s] FOR LOGIN [%s] WITH DEFAULT_SCHEMA = [%s]", login, login, schema),
		fmt.Sprintf("GRANT CREATE TABLE TO [%s]", login),
		fmt.Sprintf("GRANT ALTER, SELECT, INSERT, UPDATE, DELETE, REFERENCES ON SCHEMA::[%s] TO [%s]", schema, login),
	} {
		_, err := conn.ExecContext(c.Context(), batch)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", batch))
	}
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(login, password)
	return parsed.String()
}

// TestMigrationsUpSQLServerNamesTheSchemaForAUserOutsideDboE2E starts a
// history as a user who may create tables in app alone. The run that names no
// schema cannot create its table in dbo, and the error names the flag and the
// parameter that keep it in app; the run that names app starts it there.
func TestMigrationsUpSQLServerNamesTheSchemaForAUserOutsideDboE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	limited := sqlServerLimitedAccount(c, dbURL, "app")
	dir := c.TempDir()
	writeFiles(c, dir, map[string]string{
		"1_create.up.sql":   "CREATE TABLE app.t1 (id INT NOT NULL);\n",
		"1_create.down.sql": "DROP TABLE app.t1;\n",
	})
	up := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
			append([]string{"migrations", "up", "--db-url", limited, "--migrations-dir", dir}, args...)...)
	}

	unnamed := up()
	named := up("--migrations-schema", "app")

	c.Assert(unnamed.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", unnamed.Stdout, unnamed.Stderr))
	c.Assert(unnamed.Stderr, qt.Contains, `failed to create migrations table in schema "dbo", where Ptah keeps its history `+
		`when no schema is named; for a user that cannot create tables there, pass --migrations-schema app `+
		`(--revisions-schema app in ptah-compat), or add schema=app to the URL: `)
	c.Assert(named.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", named.Stdout, named.Stderr))
	c.Assert(heldColumns(c, dbURL, sqlServerOwnTables), qt.Equals, "app.schema_migrations,app.schema_migrations_log")
}
