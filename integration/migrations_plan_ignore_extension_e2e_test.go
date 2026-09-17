//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// An extension the database carries and the declaration deliberately does not
// describe, driven through the shipped binary against a live PostgreSQL server.
//
// The whole subject is what the live catalog answers: an extension is read
// database-wide, and the plan that follows either proposes dropping it or does
// not. Nothing offline decides that -- a fixture can say the desired schema
// declares no extension, but only a server can say the database has one
// (stokaro/ptah#3373).
//
// The drop is classified destructive, so the flag is also the difference
// between `--check-destructive` failing on every run and passing, which is the
// half a caller running this in continuous integration actually reads.

// ignoreExtensionEntities declares one table and no extension. The extension
// under test is created by the setup below, the way a bootstrap step would.
const ignoreExtensionEntities = `package entities

//ptah:schema:table name="notes"
type Note struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//ptah:schema:field name="body" type="TEXT" not_null="true"
	Body string
}
`

// declaredExtensionEntity declares an extension the database does not carry.
// btree_gin rather than pg_trgm so the declared-and-created case and the
// carried-and-left-alone case are two extensions and cannot be confused.
const declaredExtensionEntity = `
//ptah:schema:extension name="btree_gin" if_not_exists="false"
type _ struct{}
`

// TestMigrationsPlanIgnoreExtensionE2E measures the plan three ways against one
// database: without the flag, with it, and with the project-config key that is
// meant to say the same thing.
//
// The run without the flag is the control. Without it a plan that never
// proposed the drop -- because the extension read broke, say -- would satisfy
// the other two rows just as well.
func TestMigrationsPlanIgnoreExtensionE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { adminDB.Close() })

	databaseName := fmt.Sprintf("ptah_ignore_ext_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, databaseName)
	t.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, databaseName) })
	scopedURL := replaceDatabaseName(c, dbURL, databaseName)

	targetDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { targetDB.Close() })
	execExitCodeSQL(c, ctx, targetDB, `CREATE EXTENSION IF NOT EXISTS pg_trgm`)

	workDir := c.TempDir()
	root := filepath.Join(workDir, "entities")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(ignoreExtensionEntities), 0o600), qt.IsNil)

	// The project config lives outside the working directory on purpose.
	// ./ptah.yaml is auto-discovered, so a config written beside the run would
	// reach the control too and the run without the flag would quietly stop
	// being a control.
	configDir := filepath.Join(workDir, "project")
	c.Assert(os.MkdirAll(configDir, 0o750), qt.IsNil)
	configPath := filepath.Join(configDir, "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("ignore_extensions:\n  - pg_trgm\n"), 0o600), qt.IsNil)

	plan := func(c *qt.C, extra ...string) clirun.Result {
		c.Helper()
		args := append([]string{
			"migrations", "plan",
			"--db-url", scopedURL,
			"--root-dir", root,
		}, extra...)
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir}, args...)
	}

	t.Run("the extension the declaration does not describe is dropped", func(t *testing.T) {
		c := qt.New(t)

		got := plan(c)

		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
		c.Assert(got.Stdout, qt.Contains, `DROP EXTENSION IF EXISTS "pg_trgm";`)
		c.Assert(got.Stdout, qt.Contains, `CREATE TABLE "notes"`)
	})

	t.Run("the flag leaves it alone", func(t *testing.T) {
		c := qt.New(t)

		got := plan(c, "--ignore-extension", "pg_trgm")

		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
		c.Assert(got.Stdout, qt.Not(qt.Contains), "DROP EXTENSION")
		c.Assert(got.Stdout, qt.Contains, `CREATE TABLE "notes"`)
	})

	t.Run("the project config says the same thing", func(t *testing.T) {
		c := qt.New(t)

		got := plan(c, "--config", configPath)

		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
		c.Assert(got.Stdout, qt.Not(qt.Contains), "DROP EXTENSION")
		c.Assert(got.Stdout, qt.Contains, `CREATE TABLE "notes"`)
	})

	// The same file, discovered rather than named, which is how a project that
	// keeps one in its root actually runs.
	t.Run("the project config is found beside the run", func(t *testing.T) {
		c := qt.New(t)

		got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: configDir},
			"migrations", "plan", "--db-url", scopedURL, "--root-dir", root)

		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
		c.Assert(got.Stdout, qt.Not(qt.Contains), "DROP EXTENSION")
		c.Assert(got.Stdout, qt.Contains, `CREATE TABLE "notes"`)
	})

	// The half the two spellings of this request used to disagree on. An
	// extension the declaration DOES carry is created whether or not the flag
	// names it: the flag says what not to remove, and a declaration is not a
	// removal. Through the binary because the filter that got this wrong lived
	// below the CLI and nothing above it could see the difference.
	t.Run("an extension the declaration carries is still created", func(t *testing.T) {
		c := qt.New(t)

		declaring := filepath.Join(workDir, "declaring")
		c.Assert(os.MkdirAll(declaring, 0o750), qt.IsNil)
		c.Assert(os.WriteFile(
			filepath.Join(declaring, "schema.go"),
			[]byte(ignoreExtensionEntities+declaredExtensionEntity),
			0o600,
		), qt.IsNil)

		got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
			"migrations", "plan", "--db-url", scopedURL, "--root-dir", declaring,
			"--ignore-extension", "pg_trgm")

		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
		c.Assert(got.Stdout, qt.Contains, `CREATE EXTENSION`)
		c.Assert(got.Stdout, qt.Contains, "btree_gin")
		c.Assert(got.Stdout, qt.Not(qt.Contains), "DROP EXTENSION")
	})

	// The destructive gate is what a continuous-integration job reads, and the
	// drop is the only destructive statement in this plan.
	t.Run("the destructive gate fails on the drop", func(t *testing.T) {
		c := qt.New(t)

		got := plan(c, "--check-destructive")

		c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", got.Stdout))
	})

	t.Run("and passes once the extension is ignored", func(t *testing.T) {
		c := qt.New(t)

		got := plan(c, "--check-destructive", "--ignore-extension", "pg_trgm")

		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	})
}

// TestSchemaDriftIgnoreExtensionE2E holds the same rule on the verb a drift
// gate runs. `migrations plan` and `schema drift` build their comparison
// options in different packages, so one honoring the flag says nothing about
// the other.
func TestSchemaDriftIgnoreExtensionE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { adminDB.Close() })

	databaseName := fmt.Sprintf("ptah_ignore_ext_drift_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, databaseName)
	t.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, databaseName) })
	scopedURL := replaceDatabaseName(c, dbURL, databaseName)

	targetDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { targetDB.Close() })
	execExitCodeSQL(c, ctx, targetDB, `CREATE EXTENSION IF NOT EXISTS pg_trgm`)

	workDir := c.TempDir()
	root := filepath.Join(workDir, "entities")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(ignoreExtensionEntities), 0o600), qt.IsNil)

	drift := func(c *qt.C, extra ...string) clirun.Result {
		c.Helper()
		args := append([]string{
			"schema", "drift",
			"--db-url", scopedURL,
			"--root-dir", root,
			"--format", "json",
			"--exit-code=false",
		}, extra...)
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir}, args...)
	}

	t.Run("the removed extension is reported", func(t *testing.T) {
		c := qt.New(t)

		got := drift(c)

		c.Assert(driftFindingCategories(c, got.Stdout), qt.DeepEquals,
			[]string{"extensions_removed", "tables_added"})
	})

	t.Run("the flag removes that finding and nothing else", func(t *testing.T) {
		c := qt.New(t)

		got := drift(c, "--ignore-extension", "pg_trgm")

		c.Assert(driftFindingCategories(c, got.Stdout), qt.DeepEquals,
			[]string{"tables_added"})
	})
}

// driftFindingCategories reads the categories out of a drift report.
//
// The findings are the subject and the whole report is not: the `diff` block
// carries an `extensions_removed` key whatever the outcome, empty when nothing
// was removed, so a substring assertion on the report text can never fail.
func driftFindingCategories(c *qt.C, report string) []string {
	c.Helper()

	var decoded struct {
		Findings []struct {
			Category string `json:"category"`
		} `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(report), &decoded), qt.IsNil, qt.Commentf("report:\n%s", report))

	categories := make([]string, 0, len(decoded.Findings))
	for _, finding := range decoded.Findings {
		categories = append(categories, finding.Category)
	}
	return categories
}
