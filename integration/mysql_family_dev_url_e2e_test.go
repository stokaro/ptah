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

// The pinned community binary v1.3.0 opens `mysql://`, `mariadb://` and
// `maria://` with one driver and reads MySQL or MariaDB from the server. Against
// one server it accepts any pair of the three for a target and its dev
// database on `schema apply`, `schema diff` and `migrate diff`. Against a
// MariaDB target and a MySQL dev database, or the reverse, it refuses `schema
// apply` and `migrate diff` whatever the spelling (stokaro/ptah#3756). These
// tests hold ptah-compat to both halves.

// mysqlFamilyServer is one server reached over TCP with an administrative
// account, with the parts its URLs are built from.
type mysqlFamilyServer struct {
	admin  *sql.DB
	config *mysqldriver.Config
}

func newMySQLFamilyServer(c *qt.C, engine dbtarget.Engine) mysqlFamilyServer {
	c.Helper()
	dsn := dbtarget.DriverDSN(c, engine)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	admin, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	c.Assert(admin.PingContext(c.Context()), qt.IsNil)
	return mysqlFamilyServer{admin: admin, config: config}
}

// database creates an empty database, dropped when the test ends.
func (s mysqlFamilyServer) database(c *qt.C, prefix string) string {
	c.Helper()
	name := fmt.Sprintf("ptah_%s_%d", prefix, time.Now().UnixNano())
	createMySQLDatabase(c, c.Context(), s.admin, name)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), s.admin, name) })
	return name
}

// url spells the database with the scheme.
func (s mysqlFamilyServer) url(scheme, database string) string {
	return (&url.URL{
		Scheme: scheme,
		User:   url.UserPassword(s.config.User, s.config.Passwd),
		Host:   s.config.Addr,
		Path:   "/" + database,
	}).String()
}

// tables answers the tables of a database in name order, read from the catalog.
func (s mysqlFamilyServer) tables(c *qt.C, database string) string {
	c.Helper()
	var tables sql.NullString
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT GROUP_CONCAT(TABLE_NAME ORDER BY TABLE_NAME)
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?`,
		database,
	).Scan(&tables)
	c.Assert(err, qt.IsNil)
	return tables.String
}

// mysqlFamilyDesiredState is the schema file the tests plan toward.
func mysqlFamilyDesiredState(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE widgets (id int NOT NULL, PRIMARY KEY (id));\n"), 0o600), qt.IsNil)
	return path
}

// mysqlFamilySpellingPairs are a target and a dev database on one server,
// spelled differently.
var mysqlFamilySpellingPairs = []struct {
	name         string
	engine       dbtarget.Engine
	targetScheme string
	devScheme    string
}{
	{name: "MySQL, mysql target, mariadb dev", engine: dbtarget.MySQLAdmin, targetScheme: "mysql", devScheme: "mariadb"},
	{name: "MySQL, maria target, mysql dev", engine: dbtarget.MySQLAdmin, targetScheme: "maria", devScheme: "mysql"},
	{name: "MariaDB, mariadb target, mysql dev", engine: dbtarget.MariaDBAdmin, targetScheme: "mariadb", devScheme: "mysql"},
	{name: "MariaDB, mysql target, maria dev", engine: dbtarget.MariaDBAdmin, targetScheme: "mysql", devScheme: "maria"},
}

// TestCompatSchemaApplyAcceptsADevURLSpelledDifferentlyE2E applies a schema
// file to a target and rehearses it on a dev database of the same server whose
// URL spells the family differently, then reads the table back.
func TestCompatSchemaApplyAcceptsADevURLSpelledDifferentlyE2E(t *testing.T) {
	for _, pair := range mysqlFamilySpellingPairs {
		t.Run(pair.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, pair.engine)
			target := server.database(c, "family_target")
			dev := server.database(c, "family_dev")

			out, err := runCompatVerb("schema", "apply",
				"--url", server.url(pair.targetScheme, target),
				"--dev-url", server.url(pair.devScheme, dev),
				"--to", "file://"+mysqlFamilyDesiredState(c),
				"--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(server.tables(c, target), qt.Equals, "widgets")
		})
	}
}

// TestCompatSchemaApplyReplaysADirectoryOnADevURLSpelledDifferentlyE2E applies
// a migration directory, which is replayed on the dev database to become the
// desired state. The dev URL is checked against the target's dialect before it
// is connected, and that dialect is the target server's; a scheme of the other
// family member names the same server.
func TestCompatSchemaApplyReplaysADirectoryOnADevURLSpelledDifferentlyE2E(t *testing.T) {
	for _, pair := range mysqlFamilySpellingPairs {
		t.Run(pair.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, pair.engine)
			target := server.database(c, "family_dir_target")
			dev := server.database(c, "family_dir_dev")
			dir, _ := writeRewrittenMigrationProject(c,
				"CREATE TABLE widgets (id int NOT NULL, PRIMARY KEY (id));", "")

			out, err := runCompatVerb("schema", "apply",
				"--url", server.url(pair.targetScheme, target),
				"--dev-url", server.url(pair.devScheme, dev),
				"--to", "file://"+dir,
				"--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(server.tables(c, target), qt.Equals, "widgets")
		})
	}
}

// TestCompatSchemaDiffAcceptsADevURLSpelledDifferentlyE2E diffs a database
// against a schema file on a dev database whose URL spells the family
// differently.
func TestCompatSchemaDiffAcceptsADevURLSpelledDifferentlyE2E(t *testing.T) {
	for _, pair := range mysqlFamilySpellingPairs {
		t.Run(pair.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, pair.engine)
			from := server.database(c, "family_from")
			dev := server.database(c, "family_diff_dev")

			out, err := runCompatVerb("schema", "diff",
				"--from", server.url(pair.targetScheme, from),
				"--to", "file://"+mysqlFamilyDesiredState(c),
				"--dev-url", server.url(pair.devScheme, dev))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "CREATE TABLE `widgets`")
		})
	}
}

// TestCompatMigrateDiffAcceptsADesiredDatabaseSpelledDifferentlyE2E plans a
// migration toward a database whose URL spells the family differently from the
// dev URL, and reads the planned file.
func TestCompatMigrateDiffAcceptsADesiredDatabaseSpelledDifferentlyE2E(t *testing.T) {
	for _, pair := range mysqlFamilySpellingPairs {
		t.Run(pair.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, pair.engine)
			desired := server.database(c, "family_desired")
			dev := server.database(c, "family_mdiff_dev")
			_, err := server.admin.ExecContext(c.Context(), "CREATE TABLE `"+desired+"`.widgets (id int NOT NULL, PRIMARY KEY (id))")
			c.Assert(err, qt.IsNil)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)

			out, err := runCompatVerb("migrate", "diff", "init",
				"--dir", "file://"+dir,
				"--to", server.url(pair.targetScheme, desired),
				"--dev-url", server.url(pair.devScheme, dev))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(migrationFilesContent(c, dir), qt.Contains, "CREATE TABLE `widgets`")
		})
	}
}

// TestCompatRefusesADevURLNamingTheTargetInAnotherSpellingE2E names the target
// database as the dev database, spelled as the other member of the family. The
// dev database is reset before the plan is rehearsed, so the run is refused
// before anything is dropped, and the target keeps its table.
//
// Compared as two dialects, the URLs are distinct, the reset runs, and the
// target loses its table even though the rehearsal itself is refused
// (stokaro/ptah#3769). The community binary refuses the same argv because the
// dev database it is given is not clean.
func TestCompatRefusesADevURLNamingTheTargetInAnotherSpellingE2E(t *testing.T) {
	for _, pair := range mysqlFamilySpellingPairs {
		t.Run(pair.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, pair.engine)
			target := server.database(c, "family_guard")
			_, err := server.admin.ExecContext(c.Context(), "CREATE TABLE `"+target+"`.keep (id int NOT NULL, PRIMARY KEY (id))")
			c.Assert(err, qt.IsNil)

			out, err := runCompatVerb("schema", "apply",
				"--url", server.url(pair.targetScheme, target),
				"--dev-url", server.url(pair.devScheme, target),
				"--to", "file://"+mysqlFamilyDesiredState(c),
				"--auto-approve")

			c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the target database: .*`, qt.Commentf("%s", out))
			c.Assert(server.tables(c, target), qt.Equals, "keep")
		})
	}
}

// mysqlFamilyEnginePairs are a target and a dev database on servers of the two
// engines, each spelled as its own engine and as the other.
var mysqlFamilyEnginePairs = []struct {
	name         string
	target       dbtarget.Engine
	targetScheme string
	dev          dbtarget.Engine
	devScheme    string
	targetServer string
	devServer    string
}{
	{
		name:   "MariaDB target, MySQL dev, each spelled as itself",
		target: dbtarget.MariaDBAdmin, targetScheme: "mariadb", dev: dbtarget.MySQLAdmin, devScheme: "mysql",
		targetServer: "mariadb", devServer: "mysql",
	},
	{
		name:   "MariaDB target, MySQL dev, both spelled mysql",
		target: dbtarget.MariaDBAdmin, targetScheme: "mysql", dev: dbtarget.MySQLAdmin, devScheme: "mysql",
		targetServer: "mariadb", devServer: "mysql",
	},
	{
		name:   "MySQL target, MariaDB dev, both spelled mariadb",
		target: dbtarget.MySQLAdmin, targetScheme: "mariadb", dev: dbtarget.MariaDBAdmin, devScheme: "mariadb",
		targetServer: "mysql", devServer: "mariadb",
	},
}

// TestCompatSchemaApplyRefusesADevServerOfTheOtherEngineE2E rehearses a MariaDB
// target on a MySQL dev database and the reverse. The servers are compared, so
// no spelling makes the pair one engine, and the target is left as it was.
func TestCompatSchemaApplyRefusesADevServerOfTheOtherEngineE2E(t *testing.T) {
	for _, pair := range mysqlFamilyEnginePairs {
		t.Run(pair.name, func(t *testing.T) {
			c := qt.New(t)
			targetServer := newMySQLFamilyServer(c, pair.target)
			devServer := newMySQLFamilyServer(c, pair.dev)
			target := targetServer.database(c, "engine_target")
			dev := devServer.database(c, "engine_dev")

			out, err := runCompatVerb("schema", "apply",
				"--url", targetServer.url(pair.targetScheme, target),
				"--dev-url", devServer.url(pair.devScheme, dev),
				"--to", "file://"+mysqlFamilyDesiredState(c),
				"--auto-approve")

			c.Assert(err, qt.ErrorMatches,
				fmt.Sprintf(`--dev-url dialect %q does not match --url dialect %q`, pair.devServer, pair.targetServer),
				qt.Commentf("%s", out))
			c.Assert(targetServer.tables(c, target), qt.Equals, "")
		})
	}
}

// TestCompatMigrateDiffRefusesADesiredServerOfTheOtherEngineE2E plans toward a
// database on one engine with a dev database on the other.
func TestCompatMigrateDiffRefusesADesiredServerOfTheOtherEngineE2E(t *testing.T) {
	for _, pair := range mysqlFamilyEnginePairs {
		t.Run(pair.name, func(t *testing.T) {
			c := qt.New(t)
			desiredServer := newMySQLFamilyServer(c, pair.target)
			devServer := newMySQLFamilyServer(c, pair.dev)
			desired := desiredServer.database(c, "engine_desired")
			dev := devServer.database(c, "engine_mdiff_dev")
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)

			out, err := runCompatVerb("migrate", "diff", "init",
				"--dir", "file://"+dir,
				"--to", desiredServer.url(pair.targetScheme, desired),
				"--dev-url", devServer.url(pair.devScheme, dev))

			c.Assert(err, qt.ErrorMatches,
				fmt.Sprintf(`load --to schema: --to database dialect %q does not match --dev-url dialect %q`, pair.targetServer, pair.devServer),
				qt.Commentf("%s", out))
			c.Assert(migrationFilesContent(c, dir), qt.Equals, "")
		})
	}
}

// TestNativeReadsTheEngineFromTheServerE2E connects native ptah with each
// spelling to each server. The server says which engine it is, whatever the
// scheme: a MariaDB spelling of a MySQL server is a MySQL connection.
func TestNativeReadsTheEngineFromTheServerE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		scheme string
		want   string
	}{
		{name: "mariadb spelling of MySQL", engine: dbtarget.MySQLAdmin, scheme: "mariadb", want: "Connected to mysql database"},
		{name: "maria spelling of MySQL", engine: dbtarget.MySQLAdmin, scheme: "maria", want: "Connected to mysql database"},
		{name: "mysql spelling of MariaDB", engine: dbtarget.MariaDBAdmin, scheme: "mysql", want: "Connected to mariadb database"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, test.engine)
			database := server.database(c, "family_native")

			out := runPtahNative(c, "db", "read", "--db-url", server.url(test.scheme, database))

			c.Assert(out, qt.Contains, test.want)
		})
	}
}

// migrationFilesContent is every .sql file of a migration directory, in name
// order, joined.
func migrationFilesContent(c *qt.C, dir string) string {
	c.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	var content strings.Builder
	for _, path := range paths {
		data, err := os.ReadFile(path)
		c.Assert(err, qt.IsNil)
		content.Write(data)
	}
	return content.String()
}
