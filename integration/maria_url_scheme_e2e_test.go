//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/internal/dbtarget"
)

// mariaSchemeTable is the desired state every test here applies, and what
// mariaSchemeScratch.columns reads back from the catalog.
const mariaSchemeTable = "CREATE TABLE widgets (id int NOT NULL, name varchar(20) NOT NULL, PRIMARY KEY (id));"

// mariaSchemeURLs are the spellings of one MariaDB database the tests connect
// with. {userinfo} is the escaped credentials a URL carries, {credentials} the
// raw ones go-sql-driver's own form carries, {addr} the server and {db} the
// database.
//
// The pinned community binary v1.3.0 opens `maria://`, in either letter case,
// on every verb that takes a database URL, and reads it exactly as it reads
// `mariadb://` and `mysql://`: the three schemes gave byte-identical `schema
// inspect` output against MariaDB 11.8.9 and against MySQL 8.4.11
// (stokaro/ptah#3744). The tcp() row is Ptah's own address form, which the
// community binary refuses for every scheme; it is here because that form is
// where a scheme is recognized as text, before net/url can read it.
var mariaSchemeURLs = []struct {
	name     string
	template string
}{
	{name: "maria", template: "maria://{userinfo}@{addr}/{db}"},
	{name: "maria in upper case", template: "MARIA://{userinfo}@{addr}/{db}"},
	{name: "maria in the driver's tcp form", template: "maria://{credentials}@tcp({addr})/{db}"},
}

// mariaSchemeScratch is the administrative connection to the MariaDB server
// under test, and the parts of its address the URLs above are built from.
type mariaSchemeScratch struct {
	admin  *sql.DB
	config *mysqldriver.Config
}

func newMariaSchemeScratch(c *qt.C) mariaSchemeScratch {
	c.Helper()
	dsn := dbtarget.DriverDSN(c, dbtarget.MariaDBAdmin)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	admin, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })

	// The server says which engine it is before Ptah is asked. A MySQL
	// address left in the MariaDB variable would otherwise pass every test
	// below while measuring the wrong engine.
	var version string
	c.Assert(admin.QueryRowContext(c.Context(), "SELECT VERSION()").Scan(&version), qt.IsNil)
	c.Assert(version, qt.Contains, "MariaDB")
	return mariaSchemeScratch{admin: admin, config: config}
}

// database creates an empty database, dropped when the test ends.
func (s mariaSchemeScratch) database(c *qt.C, prefix string) string {
	c.Helper()
	name := fmt.Sprintf("ptah_%s_%d", prefix, time.Now().UnixNano())
	createMySQLDatabase(c, c.Context(), s.admin, name)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), s.admin, name) })
	return name
}

// url spells the database named by the template of one mariaSchemeURLs row.
func (s mariaSchemeScratch) url(template, database string) string {
	return strings.NewReplacer(
		"{userinfo}", url.UserPassword(s.config.User, s.config.Passwd).String(),
		"{credentials}", s.config.User+":"+s.config.Passwd,
		"{addr}", s.config.Addr,
		"{db}", database,
	).Replace(template)
}

// columns answers the columns of `widgets` in the database, in order, read
// from the catalog rather than through Ptah.
func (s mariaSchemeScratch) columns(c *qt.C, database string) string {
	c.Helper()
	var columns sql.NullString
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'widgets'`,
		database,
	).Scan(&columns)
	c.Assert(err, qt.IsNil)
	return columns.String
}

// writeMariaSchemeFile writes the desired state and returns its path.
func writeMariaSchemeFile(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(mariaSchemeTable+"\n"), 0o600), qt.IsNil)
	return path
}

// TestCompatSchemaApplyConnectsWithTheMariaSchemeE2E applies a schema file to
// a database named with each spelling, rehearsing on a dev database named the
// same way, and reads the table back from the catalog.
func TestCompatSchemaApplyConnectsWithTheMariaSchemeE2E(t *testing.T) {
	for _, test := range mariaSchemeURLs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMariaSchemeScratch(c)
			target := scratch.database(c, "maria_target")
			dev := scratch.database(c, "maria_dev")
			schema := writeMariaSchemeFile(c)

			out, err := runCompatVerb("schema", "apply",
				"--url", scratch.url(test.template, target),
				"--dev-url", scratch.url(test.template, dev),
				"--to", "file://"+schema,
				"--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(scratch.columns(c, target), qt.Equals, "id,name")
		})
	}
}

// TestCompatSchemaInspectReadsTheMariaSchemeAsMariaDBE2E inspects one database
// through each spelling and through `mariadb://`: the output is the same
// document, which is how the community binary answered the same comparison.
func TestCompatSchemaInspectReadsTheMariaSchemeAsMariaDBE2E(t *testing.T) {
	for _, test := range mariaSchemeURLs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMariaSchemeScratch(c)
			target := scratch.database(c, "maria_inspect")
			_, err := scratch.admin.ExecContext(c.Context(), "CREATE TABLE `"+target+"`.widgets (id int NOT NULL, PRIMARY KEY (id))")
			c.Assert(err, qt.IsNil)

			reference, err := runCompatVerb("schema", "inspect", "--url", scratch.url("mariadb://{userinfo}@{addr}/{db}", target))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", reference))
			got, err := runCompatVerb("schema", "inspect", "--url", scratch.url(test.template, target))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", got))
			c.Assert(got, qt.Contains, `table "widgets"`)
			c.Assert(got, qt.Equals, reference)
		})
	}
}

// TestCompatMigrateVerbsConnectWithTheMariaSchemeE2E plans a migration on a
// dev database, applies it and reports its status, each against a database
// named with the spelling, and reads the table back from the catalog.
func TestCompatMigrateVerbsConnectWithTheMariaSchemeE2E(t *testing.T) {
	for _, test := range mariaSchemeURLs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMariaSchemeScratch(c)
			target := scratch.database(c, "maria_migrate")
			dev := scratch.database(c, "maria_migrate_dev")
			schema := writeMariaSchemeFile(c)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)

			planned, err := runCompatVerb("migrate", "diff", "init",
				"--dir", "file://"+dir, "--to", "file://"+schema, "--dev-url", scratch.url(test.template, dev))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", planned))
			applied, err := runCompatVerb("migrate", "apply",
				"--dir", "file://"+dir, "--url", scratch.url(test.template, target))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			status, err := runCompatVerb("migrate", "status",
				"--dir", "file://"+dir, "--url", scratch.url(test.template, target))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", status))
			c.Assert(status, qt.Contains, "Migration Status: OK")
			c.Assert(scratch.columns(c, target), qt.Equals, "id,name")
		})
	}
}

// TestNativeSchemaApplyConnectsWithTheMariaSchemeE2E is the native half. The
// scheme is a spelling of a dialect, and a dialect spelling is accepted by
// every surface that reads one, so `ptah` connects with it as `ptah-compat`
// does.
func TestNativeSchemaApplyConnectsWithTheMariaSchemeE2E(t *testing.T) {
	for _, test := range mariaSchemeURLs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMariaSchemeScratch(c)
			target := scratch.database(c, "maria_native")
			dev := scratch.database(c, "maria_native_dev")
			schema := writeMariaSchemeFile(c)

			runPtahNative(c, "schema", "apply",
				"--db-url", scratch.url(test.template, target),
				"--dev-url", scratch.url(test.template, dev),
				"--schema-file", schema,
				"--auto-approve")

			c.Assert(scratch.columns(c, target), qt.Equals, "id,name")
		})
	}
}
