//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// PostgreSQL stores a generated column's expression parsed and prints it with
// the casts its parser inserted. Over a varchar column, `coalesce("title", '')`
// is stored as `(COALESCE(title, ''::character varying))::text`, a text which
// no declaration spells. Each test here holds a table in a live database and a
// schema file, and runs three paths on PostgreSQL 18 and 16: `ptah schema apply
// --dry-run`, `ptah-compat schema apply --dry-run`, and `ptah-compat migrate
// diff` over a directory whose one migration creates the live table.
//
// PostgreSQL 17 added ALTER COLUMN SET EXPRESSION. So a changed expression is
// that statement on 18 and a manual-migration warning on 16, and the same
// comparison decides both.

// storedFormServers are the release lines each test runs on. changed is the
// text a plan carries for a changed generated expression on that line.
var storedFormServers = []struct {
	name    string
	engine  dbtarget.Engine
	changed string
}{
	{name: "postgres-18", engine: dbtarget.PostgreSQL, changed: `" SET EXPRESSION AS (`},
	{
		name:    "postgres-16",
		engine:  dbtarget.PostgreSQL16,
		changed: "requires target capability alter_generated_column_expression, unavailable on this target (PostgreSQL added it in 17); manual migration required.",
	},
}

// storedFormTable is public.prices with the text column derived generated
// from expression.
func storedFormTable(expression string) string {
	return `CREATE TABLE "public"."prices" ("id" integer PRIMARY KEY, "title" varchar(100), "amount" numeric(10,2), ` +
		`"derived" text GENERATED ALWAYS AS (` + expression + `) STORED);` + "\n"
}

// storedFormTSVector is public.prices as the report that found the defect
// declares it, with the weight given.
func storedFormTSVector(title, weight string) string {
	return `CREATE TABLE "public"."prices" ("id" integer PRIMARY KEY, "title" varchar(100), ` +
		`"search_vector" tsvector GENERATED ALWAYS AS (setweight(to_tsvector('simple', coalesce(` + title + `, '')), '` + weight + `')) STORED);` + "\n"
}

// storedFormFixture is a database on one server holding the live table, and
// a schema file declaring the desired one.
type storedFormFixture struct {
	engine           dbtarget.Engine
	database, schema string
}

func newStoredFormFixture(c *qt.C, engine dbtarget.Engine, live, desired string) storedFormFixture {
	c.Helper()
	fixture := storedFormFixture{engine: engine, database: postgresScratchURLOn(c, engine, "ptah_generated_stored")}
	execPostgresFamilySQL(c, c.Context(), fixture.database, live)
	fixture.schema = filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(fixture.schema, []byte(desired), 0o600), qt.IsNil)
	return fixture
}

// dryRun is `ptah schema apply --dry-run`.
func (f storedFormFixture) dryRun(c *qt.C) clirun.Result {
	c.Helper()
	return runPtahIn(c, "schema", "apply", "--db-url", f.database, "--schema-file", f.schema, "--dry-run")
}

// compatDryRun is `ptah-compat schema apply --dry-run`, with a dev database on
// the same server.
func (f storedFormFixture) compatDryRun(c *qt.C) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "apply",
		"-u", f.database, "--to", "file://"+filepath.ToSlash(f.schema),
		"--dev-url", postgresScratchURLOn(c, f.engine, "ptah_generated_stored_dev"), "--dry-run",
	)
}

// storedFormMigrateDiff runs `ptah-compat migrate diff next` over a directory
// whose one migration is live, against desired, with a dev database on
// engine's server. It returns the run and the text of the migration it wrote,
// empty when it wrote none.
func storedFormMigrateDiff(c *qt.C, engine dbtarget.Engine, live, desired string) (clirun.Result, string) {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(live), 0o600), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	dirURL := "file://" + filepath.ToSlash(dir)
	compat := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, args...)
	}
	hashed := compat("migrate", "hash", "--dir", dirURL)
	c.Assert(hashed.ExitCode, qt.Equals, 0, historyComment(hashed))

	diffed := compat("migrate", "diff", "next", "--dir", dirURL, "--to", "file://"+filepath.ToSlash(schema),
		"--dev-url", postgresScratchURLOn(c, engine, "ptah_generated_stored_dev"))

	written, err := filepath.Glob(filepath.Join(dir, "*_next.sql"))
	c.Assert(err, qt.IsNil)
	var text strings.Builder
	for _, path := range written {
		content, err := os.ReadFile(path)
		c.Assert(err, qt.IsNil)
		text.Write(content)
	}
	return diffed, text.String()
}

// storedFormUnchanged are tables whose declaration and live form are one
// expression in PostgreSQL's stored form. A cast to text over a varchar column
// is stored the same with or without the cast in the declaration, so a cast on
// one side only is no change.
var storedFormUnchanged = []struct {
	name    string
	live    string
	desired string
}{
	{
		name:    "identical inputs with a bare varchar reference",
		live:    storedFormTSVector(`"title"`, "B"),
		desired: storedFormTSVector(`"title"`, "B"),
	},
	{
		name:    "a cast to text in the declaration only",
		live:    storedFormTSVector(`"title"`, "B"),
		desired: storedFormTSVector(`"title"::text`, "B"),
	},
	{
		name:    "a cast to text in the live table only",
		live:    storedFormTSVector(`"title"::text`, "B"),
		desired: storedFormTSVector(`"title"`, "B"),
	},
}

// TestGeneratedExpressionInStoredFormPlansNothingE2E holds each path to no
// change on every server.
func TestGeneratedExpressionInStoredFormPlansNothingE2E(t *testing.T) {
	for _, row := range storedFormUnchanged {
		for _, server := range storedFormServers {
			t.Run(row.name+"/"+server.name+"/ptah", func(t *testing.T) {
				c := qt.New(t)
				fixture := newStoredFormFixture(c, server.engine, row.live, row.desired)

				planned := fixture.dryRun(c)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
			t.Run(row.name+"/"+server.name+"/ptah-compat", func(t *testing.T) {
				c := qt.New(t)
				fixture := newStoredFormFixture(c, server.engine, row.live, row.desired)

				planned := fixture.compatDryRun(c)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made\n")
			})
			t.Run(row.name+"/"+server.name+"/migrate-diff", func(t *testing.T) {
				c := qt.New(t)

				diffed, written := storedFormMigrateDiff(c, server.engine, row.live, row.desired)

				c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
				c.Assert(diffed.Stdout, qt.Equals, "The migration directory is synced with the desired state, no changes to be made\n")
				c.Assert(written, qt.Equals, "")
			})
		}
	}
}

// storedFormChanged are real changes of the expression. A cast that can
// change a value is a change even when it is the only difference: amount is
// numeric(10,2), which a cast to integer rounds, and a cast to varchar(3)
// truncates title.
var storedFormChanged = []struct {
	name    string
	live    string
	desired string
}{
	{
		name:    "another weight",
		live:    storedFormTSVector(`"title"`, "B"),
		desired: storedFormTSVector(`"title"`, "A"),
	},
	{
		name:    "a cast that rounds",
		live:    storedFormTable(`"amount"`),
		desired: storedFormTable(`"amount"::integer`),
	},
	{
		name:    "a cast that truncates",
		live:    storedFormTable(`"title"`),
		desired: storedFormTable(`"title"::varchar(3)`),
	},
}

// TestGeneratedExpressionInStoredFormPlansARealChangeE2E holds each path to
// the change on every server: the statement on 18, the warning on 16.
func TestGeneratedExpressionInStoredFormPlansARealChangeE2E(t *testing.T) {
	for _, row := range storedFormChanged {
		for _, server := range storedFormServers {
			t.Run(row.name+"/"+server.name+"/ptah", func(t *testing.T) {
				c := qt.New(t)
				fixture := newStoredFormFixture(c, server.engine, row.live, row.desired)

				planned := fixture.dryRun(c)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Contains, server.changed)
			})
			t.Run(row.name+"/"+server.name+"/ptah-compat", func(t *testing.T) {
				c := qt.New(t)
				fixture := newStoredFormFixture(c, server.engine, row.live, row.desired)

				planned := fixture.compatDryRun(c)

				// The compatibility dry run writes a statement to stdout and a
				// note to stderr.
				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout+planned.Stderr, qt.Contains, server.changed)
			})
			t.Run(row.name+"/"+server.name+"/migrate-diff", func(t *testing.T) {
				c := qt.New(t)

				diffed, written := storedFormMigrateDiff(c, server.engine, row.live, row.desired)

				c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
				c.Assert(written, qt.Contains, server.changed)
			})
		}
	}
}

// TestGeneratedExpressionInStoredFormRebuildsOnARetypeE2E changes the type of
// title, which search_vector reads, from varchar(40) to varchar(100) and
// leaves the expression as it is. PostgreSQL refuses to change the type of a
// column a generated column reads, so every path plans the rebuild: drop the
// generated column, change title, add it back. The expression is unchanged,
// so no path plans SET EXPRESSION or warns about it.
func TestGeneratedExpressionInStoredFormRebuildsOnARetypeE2E(t *testing.T) {
	live := strings.Replace(storedFormTSVector(`"title"`, "B"), "varchar(100)", "varchar(40)", 1)
	desired := storedFormTSVector(`"title"`, "B")
	for _, server := range storedFormServers {
		t.Run(server.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newStoredFormFixture(c, server.engine, live, desired)

			planned := fixture.dryRun(c)

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Contains, `DROP COLUMN "search_vector"`)
			c.Assert(planned.Stdout, qt.Contains, `ALTER COLUMN "title" TYPE varchar(100)`)
			c.Assert(planned.Stdout, qt.Contains, `ADD COLUMN "search_vector"`)
			c.Assert(planned.Stdout+planned.Stderr, qt.Not(qt.Contains), "SET EXPRESSION")
		})
		t.Run(server.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newStoredFormFixture(c, server.engine, live, desired)

			planned := fixture.compatDryRun(c)

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Contains, `DROP COLUMN "search_vector"`)
			c.Assert(planned.Stdout, qt.Contains, `ALTER COLUMN "title" TYPE character varying(100)`)
			c.Assert(planned.Stdout, qt.Contains, `ADD COLUMN "search_vector"`)
			c.Assert(planned.Stdout+planned.Stderr, qt.Not(qt.Contains), "SET EXPRESSION")
		})
		t.Run(server.name+"/migrate-diff", func(t *testing.T) {
			c := qt.New(t)

			diffed, written := storedFormMigrateDiff(c, server.engine, live, desired)

			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(written, qt.Contains, `DROP COLUMN "search_vector"`)
			c.Assert(written, qt.Contains, `ALTER COLUMN "title" TYPE character varying(100)`)
			c.Assert(written, qt.Contains, `ADD COLUMN "search_vector"`)
			c.Assert(written, qt.Not(qt.Contains), "SET EXPRESSION")
		})
	}
}

// TestGeneratedExpressionInStoredFormOnYugabyteDBE2E runs the same tables
// through `ptah schema apply --dry-run` on YugabyteDB, which stores a
// generated expression as PostgreSQL 15 does and has no SET EXPRESSION. Each
// unchanged table plans nothing, and each changed one warns.
func TestGeneratedExpressionInStoredFormOnYugabyteDBE2E(t *testing.T) {
	for _, row := range storedFormUnchanged {
		t.Run("unchanged/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			database := newPolicyClauseDatabase(c, dbtarget.YugabyteDB)
			execPostgresFamilySQL(c, c.Context(), database, row.live)

			planned := runPtahIn(c, "schema", "apply", "--db-url", database,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", row.desired), "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
	for _, row := range storedFormChanged {
		t.Run("changed/"+row.name, func(t *testing.T) {
			c := qt.New(t)
			database := newPolicyClauseDatabase(c, dbtarget.YugabyteDB)
			execPostgresFamilySQL(c, c.Context(), database, row.live)

			planned := runPtahIn(c, "schema", "apply", "--db-url", database,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", row.desired), "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Contains, "manual migration required.")
		})
	}
}
