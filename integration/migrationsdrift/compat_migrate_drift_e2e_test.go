//go:build integration

package migrationsdrift_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// These tests run `ptah-compat migrate drift` over the targets of an atlas.hcl
// env with for_each, on databases `ptah-compat migrate apply` migrated. The
// report reads the revision table that verb wrote, where Atlas keeps it, and
// the server decides whether each database matches its history.

// compatDriftMigrations is a history in the Atlas directory layout.
var compatDriftMigrations = map[string]string{
	"20240101000000_users.sql":       "CREATE TABLE drift_users (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL);\n",
	"20240102000000_users_email.sql": "CREATE INDEX drift_users_email ON drift_users (email);\n",
}

// compatDriftProject is a project directory holding the hashed history in
// migrations, and the databases of one engine's server the tests create.
type compatDriftProject struct {
	engine driftEngine
	dir    string
	shadow string
}

func newCompatDriftProject(c *qt.C, engine driftEngine) compatDriftProject {
	c.Helper()
	project := compatDriftProject{engine: engine, dir: c.TempDir()}
	project.directory(c, "migrations", compatDriftMigrations)
	project.shadow, _ = project.database(c)
	return project
}

// directory writes files into a new migration directory of the project and
// hashes it.
func (p compatDriftProject) directory(c *qt.C, name string, files map[string]string) {
	c.Helper()
	dir := filepath.Join(p.dir, name)
	c.Assert(os.Mkdir(dir, 0o755), qt.IsNil)
	for file, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, file), []byte(content), 0o600), qt.IsNil)
	}
	p.run(c, 0, "migrate", "hash", "--dir", "file://"+name)
}

// database creates an empty database on the server, dropped when the test
// ends, and returns its address and name.
func (p compatDriftProject) database(c *qt.C) (url, name string) {
	c.Helper()
	address := dbtarget.URL(c, p.engine.admin)
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	name = fmt.Sprintf("ptah_cdrift_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), fmt.Sprintf(p.engine.drop, name))
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	url, err = atlasurl.WithDatabaseName(address, name)
	c.Assert(err, qt.IsNil)
	return url, name
}

// migrated is a new database `migrate apply` migrated to the last version,
// with statements run on it afterwards.
func (p compatDriftProject) migrated(c *qt.C, statements ...string) string {
	c.Helper()
	return p.migratedFrom(c, "migrations", statements...)
}

// migratedFrom is [compatDriftProject.migrated] from the project's directory
// called dir.
func (p compatDriftProject) migratedFrom(c *qt.C, dir string, statements ...string) string {
	c.Helper()
	url, _ := p.database(c)
	p.run(c, 0, "migrate", "apply", "--url", url, "--dir", "file://"+dir)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return url
}

// run runs ptah-compat in the project directory with the drift verb enabled,
// and asserts the exit status.
func (p compatDriftProject) run(c *qt.C, exitCode int, args ...string) clirun.Result {
	c.Helper()
	result := clirun.Run(c, clirun.Compat, clirun.Options{
		Dir:     p.dir,
		Env:     append(os.Environ(), "PTAH_ATLAS_MIGRATE_DRIFT=1"),
		Timeout: 2 * time.Minute,
	}, args...)
	c.Assert(result.ExitCode, qt.Equals, exitCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result
}

// drift writes an env "tenants" with one target per url and reports it.
func (p compatDriftProject) drift(c *qt.C, exitCode int, urls ...string) clirun.Result {
	c.Helper()
	quoted := make([]string, len(urls))
	for i, url := range urls {
		quoted[i] = strconv.Quote(url)
	}
	c.Assert(os.WriteFile(filepath.Join(p.dir, "atlas.hcl"), []byte("env \"tenants\" {\n"+
		"  for_each = ["+strings.Join(quoted, ", ")+"]\n"+
		"  url      = each.value\n"+
		"  dev      = "+strconv.Quote(p.shadow)+"\n"+
		"  migration {\n"+
		"    dir = \"file://migrations\"\n"+
		"  }\n"+
		"}\n"), 0o600), qt.IsNil)
	return p.run(c, exitCode, "migrate", "drift", "--env", "tenants")
}

func (p compatDriftProject) scalar(c *qt.C, url, query string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var value int
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil, qt.Commentf("%s", query))
	return value
}

// header is what the report prints for a target before its outcome: the
// revision table, and the highest version that table records.
func (p compatDriftProject) header(c *qt.C, url string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var version string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT MAX(version) FROM "+p.engine.atlasTable).Scan(&version), qt.IsNil)
	return "Revision table: " + p.engine.atlasTable + "\nCurrent version: " + version + "\nPending migrations: 0\n"
}

func TestCompatMigrateDriftE2E_HappyPathReportsEveryTargetOfAForEachEnv(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			project := newCompatDriftProject(c, engine)
			first, second := project.migrated(c), project.migrated(c)

			result := project.drift(c, 0, first, second)

			c.Assert(result.Stderr, qt.Equals, "")
			c.Assert(project.header(c, first), qt.Equals, project.header(c, second))
			c.Assert(strings.Count(result.Stdout, project.header(c, first)), qt.Equals, 2)
			c.Assert(result.Stdout, qt.Matches, "(?s).*"+regexp.QuoteMeta("Summary: 2 targets: 2 without drift, 0 with drift, 0 not compared.\n"))
			c.Assert(project.scalar(c, project.shadow, engine.tables), qt.Equals, 0)
		})
	}
}

// Each target replays the directory its own instance names on the dev
// database: the second directory holds only the first migration, and the
// second database ran only that one. Replayed from the first directory, the
// second target would report a pending migration.
func TestCompatMigrateDriftE2E_HappyPathReplaysTheDirectoryEachTargetNames(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			project := newCompatDriftProject(c, engine)
			project.directory(c, "migrations-second", map[string]string{
				"20240101000000_users.sql": compatDriftMigrations["20240101000000_users.sql"],
			})
			first, second := project.migrated(c), project.migratedFrom(c, "migrations-second")
			c.Assert(os.WriteFile(filepath.Join(project.dir, "atlas.hcl"), []byte("locals {\n"+
				"  urls = { first = "+strconv.Quote(first)+", second = "+strconv.Quote(second)+" }\n"+
				"  dirs = { first = \"file://migrations\", second = \"file://migrations-second\" }\n"+
				"}\n\n"+
				"env \"split\" {\n"+
				"  for_each = toset([\"first\", \"second\"])\n"+
				"  url      = local.urls[each.value]\n"+
				"  dev      = "+strconv.Quote(project.shadow)+"\n"+
				"  migration {\n"+
				"    dir = local.dirs[each.value]\n"+
				"  }\n"+
				"}\n"), 0o600), qt.IsNil)

			result := project.run(c, 0, "migrate", "drift", "--env", "split")

			c.Assert(result.Stderr, qt.Equals, "")
			c.Assert(project.header(c, first), qt.Not(qt.Equals), project.header(c, second))
			c.Assert(strings.Count(result.Stdout, project.header(c, first)), qt.Equals, 1)
			c.Assert(strings.Count(result.Stdout, project.header(c, second)), qt.Equals, 1)
			c.Assert(result.Stdout, qt.Matches, "(?s).*"+regexp.QuoteMeta("Summary: 2 targets: 2 without drift, 0 with drift, 0 not compared.\n"))
			c.Assert(project.scalar(c, project.shadow, engine.tables), qt.Equals, 0)
		})
	}
}

// A drifted target and a database the server does not hold exit 1 together,
// the status the community binary gives any failure, and the run goes past
// both to the last target.
func TestCompatMigrateDriftE2E_FailurePathReportsEveryTargetAndExitsOne(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			project := newCompatDriftProject(c, engine)
			drifted := project.migrated(c, "ALTER TABLE drift_users ADD COLUMN nickname VARCHAR(40)")
			clean := project.migrated(c)
			missing, missingName := missingDatabase(c, engine)

			result := project.drift(c, 1, drifted, missing, clean)

			c.Assert(result.Stderr, qt.Equals, "")
			c.Assert(result.Stdout, qt.Contains, "  - extra column drift_users.nickname\n")
			c.Assert(result.Stdout, qt.Contains, "Instance 3 of env \"tenants\"")
			c.Assert(result.Stdout, qt.Matches, "(?s).*"+regexp.QuoteMeta("Summary: 3 targets: 1 without drift, 1 with drift, 1 not compared.\n"))
			c.Assert(project.scalar(c, drifted, engine.nickname), qt.Equals, 1)
			c.Assert(project.scalar(c, clean, engine.nickname), qt.Equals, 0)
			c.Assert(project.scalar(c, dbtarget.URL(c, engine.admin), fmt.Sprintf(engine.databases, missingName)), qt.Equals, 0)
		})
	}
}
