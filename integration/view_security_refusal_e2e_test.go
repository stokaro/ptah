//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreference"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// viewSecurityEngine is one engine of the PostgreSQL family that holds a view's
// security_invoker. The engines answer alike about what the option does: the
// reader sees one row with it and two without it, measured on PostgreSQL 18,
// CockroachDB 26.3 and YugabyteDB 2026.1. They differ in what they store.
type viewSecurityEngine struct {
	name   string
	engine dbtarget.Engine
}

// viewSecurityPolicy is the row-level security condition that shows each role
// its own row.
const viewSecurityPolicy = "owner_name = current_user"

var viewSecurityEngines = []viewSecurityEngine{
	{name: "postgresql", engine: dbtarget.PostgreSQL},
	{name: "cockroachdb", engine: dbtarget.CockroachDB},
	{name: "yugabytedb", engine: dbtarget.YugabyteDB},
}

const (
	viewSecurityBody   = "SELECT id, owner_name FROM docs"
	viewSecurityWide   = "SELECT id, owner_name, 1 AS one FROM docs"
	viewSecurityNarrow = "SELECT id FROM docs"
)

// viewSecurityPath is a change that loses the option if the declaration leaves
// it out. statements says, by engine, what the plan writes for the view when the
// declaration states the option false, which is how a row proves it reaches the
// statement it is about. CockroachDB takes the drop for a new column where the
// other engines replace the view in place, so a row varies by engine.
type viewSecurityPath struct {
	name string
	// body is the view's declared definition. The database holds
	// viewSecurityBody.
	body       string
	statements map[string]string
}

const (
	viewSecurityAlterStatement   = `ALTER VIEW "my_docs" SET (security_invoker=false)`
	viewSecurityReplaceStatement = `CREATE OR REPLACE VIEW "my_docs" WITH (security_invoker=false)`
	viewSecurityDropStatement    = `DROP VIEW "my_docs"`
)

var viewSecurityPaths = []viewSecurityPath{
	{name: "the option alone", body: viewSecurityBody, statements: map[string]string{
		"postgresql": viewSecurityAlterStatement, "cockroachdb": viewSecurityAlterStatement, "yugabytedb": viewSecurityAlterStatement,
	}},
	{name: "a new column", body: viewSecurityWide, statements: map[string]string{
		"postgresql": viewSecurityReplaceStatement, "cockroachdb": viewSecurityDropStatement, "yugabytedb": viewSecurityReplaceStatement,
	}},
	{name: "a lost column", body: viewSecurityNarrow, statements: map[string]string{
		"postgresql": viewSecurityDropStatement, "cockroachdb": viewSecurityDropStatement, "yugabytedb": viewSecurityDropStatement,
	}},
}

// viewSecurityTarget is a database holding a table with row-level security, a
// policy that shows each role its own row, and the view my_docs, which a role
// with no other rights reads.
type viewSecurityTarget struct {
	url    string
	reader string
	db     *sql.DB
}

// newViewSecurityTarget creates the database and the reader role, dropped when
// the test ends, with the view holding options, or none for an empty string.
func newViewSecurityTarget(c *qt.C, engine viewSecurityEngine, options string) viewSecurityTarget {
	c.Helper()
	adminURL := dbtarget.URL(c, engine.engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	suffix := time.Now().UnixNano()
	name, reader := fmt.Sprintf("ptah_view_security_%d", suffix), fmt.Sprintf("ptah_view_reader_%d", suffix)
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+reader)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP ROLE "+reader)
		c.Assert(err, qt.IsNil)
	})
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })

	target := viewSecurityTarget{url: replaceDatabaseName(c, adminURL, name), reader: reader}
	// One statement each: YugabyteDB answered a conflict to the whole fixture
	// sent as one call and kept part of it.
	for _, statement := range []string{
		"CREATE TABLE docs (id integer PRIMARY KEY, owner_name text NOT NULL)",
		fmt.Sprintf("INSERT INTO docs VALUES (1, '%s'), (2, 'someone_else')", reader),
		"ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
		"CREATE POLICY docs_owner ON docs USING (" + viewSecurityPolicy + ")",
		"GRANT SELECT ON docs TO " + reader,
		"CREATE VIEW my_docs" + withList(options) + " AS " + viewSecurityBody,
		"GRANT SELECT ON my_docs TO " + reader,
	} {
		execPostgresFamilySQL(c, c.Context(), target.url, statement)
	}
	target.db, err = sql.Open("pgx", postgresFamilyDriverURL(c, target.url))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(target.db.Close(), qt.IsNil) })
	return target
}

// schemaFile writes a SQL schema that declares what the database holds, with
// the view's definition and options as given, and returns its path.
func (t viewSecurityTarget) schemaFile(c *qt.C, body, options string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	schema := "CREATE TABLE docs (id integer PRIMARY KEY, owner_name text NOT NULL);\n" +
		"ALTER TABLE docs ENABLE ROW LEVEL SECURITY;\n" +
		"CREATE POLICY docs_owner ON docs USING (" + viewSecurityPolicy + ");\n" +
		"CREATE VIEW my_docs" + withList(options) + " AS " + body + ";\n"
	c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
	return path
}

// held reads back what pg_class.reloptions holds for the view.
func (t viewSecurityTarget) held(c *qt.C) string {
	c.Helper()
	var options string
	c.Assert(t.db.QueryRowContext(c.Context(),
		`SELECT COALESCE(array_to_string(reloptions, ','), '') FROM pg_class WHERE relname = 'my_docs'`).Scan(&options), qt.IsNil)
	return options
}

// definition reads back the view's definition, which a plan that ran would
// have changed on every path but the reset.
func (t viewSecurityTarget) definition(c *qt.C) string {
	c.Helper()
	var definition string
	c.Assert(t.db.QueryRowContext(c.Context(),
		`SELECT pg_get_viewdef(oid) FROM pg_class WHERE relname = 'my_docs'`).Scan(&definition), qt.IsNil)
	return definition
}

// rowsReaderSees counts the rows the reader role sees through the view, on one
// connection, which is the difference the option makes: the view applies the
// policy to the reader with it, and to its owner without it.
func (t viewSecurityTarget) rowsReaderSees(c *qt.C) int {
	c.Helper()
	conn, err := t.db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), "SET ROLE "+t.reader)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, err := conn.ExecContext(c.Context(), "RESET ROLE")
		c.Assert(err, qt.IsNil)
	}()
	var rows int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM my_docs").Scan(&rows), qt.IsNil)
	return rows
}

// assertViewSecurityUnchanged asserts that the view still holds
// security_invoker=true, reads as it did and still shows the reader one row.
func assertViewSecurityUnchanged(c *qt.C, target viewSecurityTarget, definition string) {
	c.Helper()
	c.Assert(target.held(c), qt.Equals, "security_invoker=true")
	c.Assert(target.definition(c), qt.Equals, definition)
	c.Assert(target.rowsReaderSees(c), qt.Equals, 1)
}

// viewSecurityRefused asserts on a refusal: it names the view, as the schema
// spells it, and the option, says what removing it does, and gives both
// remedies.
func viewSecurityRefused(c *qt.C, got clirun.Result, view string) {
	c.Helper()
	for _, part := range []string{
		"view " + view + " holds security_invoker=true and the schema does not state it",
		"the view runs with its owner's rights",
		"Declare security_invoker=true to keep it, or security_invoker=false to remove it",
	} {
		c.Assert(got.Stderr, qt.Contains, part, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	}
}

// TestSchemaApplyRefusesToLoseAViewSecurityOptionE2E applies a schema that
// leaves out the security_invoker a view holds, on each engine and each path
// that would lose it. No statement in the plan would fail: the reader would
// simply see the other role's row. So the proof the refusal came first is the
// server: reloptions, the definition and the rows the reader sees are what they
// were, under --auto-approve.
func TestSchemaApplyRefusesToLoseAViewSecurityOptionE2E(t *testing.T) {
	for _, engine := range viewSecurityEngines {
		for _, path := range viewSecurityPaths {
			t.Run(engine.name+"/"+path.name, func(t *testing.T) {
				c := qt.New(t)
				target := newViewSecurityTarget(c, engine, "security_invoker = true")
				definition := target.definition(c)
				c.Assert(target.rowsReaderSees(c), qt.Equals, 1)

				got := runPtahIn(c, "schema", "apply", "--db-url", target.url,
					"--schema-file", target.schemaFile(c, path.body, ""), "--auto-approve")

				c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				viewSecurityRefused(c, got, "my_docs")
				assertViewSecurityUnchanged(c, target, definition)
			})
		}
	}
}

// TestSchemaApplyRemovesAViewSecurityOptionDeclaredFalseE2E pins the remedy that
// removes the option. An explicit false plans on every path and does change the
// server: the reader sees the other role's row. A second apply plans nothing,
// and the option held false may then be left out of the declaration, which
// removes nothing a reader can see.
func TestSchemaApplyRemovesAViewSecurityOptionDeclaredFalseE2E(t *testing.T) {
	for _, engine := range viewSecurityEngines {
		for _, path := range viewSecurityPaths {
			t.Run(engine.name+"/"+path.name, func(t *testing.T) {
				c := qt.New(t)
				target := newViewSecurityTarget(c, engine, "security_invoker = true")
				apply := func(options string) clirun.Result {
					return runPtahIn(c, "schema", "apply", "--db-url", target.url,
						"--schema-file", target.schemaFile(c, path.body, options), "--auto-approve")
				}

				removed := apply("security_invoker = false")

				c.Assert(removed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", removed.Stdout, removed.Stderr))
				c.Assert(removed.Stdout, qt.Contains, path.statements[engine.name])
				c.Assert(target.held(c), qt.Equals, "security_invoker=false")
				c.Assert(target.rowsReaderSees(c), qt.Equals, 2)

				again := apply("security_invoker = false")

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")

				leftOut := apply("")

				c.Assert(leftOut.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", leftOut.Stdout, leftOut.Stderr))
				c.Assert(target.held(c), qt.Equals, "")
				c.Assert(target.rowsReaderSees(c), qt.Equals, 2)
			})
		}
	}
}

// TestSchemaApplyKeepsAViewSecurityOptionTheSchemaStatesE2E pins the remedy that
// keeps the option, which is what a database upgrading from an undeclared
// option does: the plan is empty and the reader sees what the reader saw.
func TestSchemaApplyKeepsAViewSecurityOptionTheSchemaStatesE2E(t *testing.T) {
	for _, engine := range viewSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, engine, "security_invoker = true")
			definition := target.definition(c)

			got := runPtahIn(c, "schema", "apply", "--db-url", target.url,
				"--schema-file", target.schemaFile(c, viewSecurityBody, "security_invoker = true"), "--auto-approve")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			assertViewSecurityUnchanged(c, target, definition)
		})
	}
}

// TestSchemaDriftReportsAViewSecurityOptionTheSchemaLeavesOutE2E pins that the
// refusal hides nothing from drift, which reads the comparison and plans
// nothing: the view is reported as modified, at the severity any view change
// has, and the verb's own exit status says so.
func TestSchemaDriftReportsAViewSecurityOptionTheSchemaLeavesOutE2E(t *testing.T) {
	for _, engine := range viewSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, engine, "security_invoker = true")
			definition := target.definition(c)

			got := runPtahIn(c, "schema", "drift", "--db-url", target.url, "--schema-file", target.schemaFile(c, viewSecurityBody, ""))

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "views_modified")
			c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), "does not state it")
			assertViewSecurityUnchanged(c, target, definition)
		})
	}
}

// viewSecurityStatedOptions is a list each engine holds, in spellings it stores
// as written where it stores any but true and false.
var viewSecurityStatedOptions = map[string]string{
	"postgresql":  "security_invoker = on, security_barrier = 1",
	"cockroachdb": "security_invoker = true",
	"yugabytedb":  "security_invoker = yes, security_barrier = on",
}

// TestViewSecurityOptionsSurviveARoundTripE2E reads a view's options into each
// form a schema can take and plans from it against the same database. Each form
// states the options, so the refusal has nothing to read, and the plan is empty
// with the options as the server stored them.
func TestViewSecurityOptionsSurviveARoundTripE2E(t *testing.T) {
	for _, engine := range viewSecurityEngines {
		t.Run(engine.name+"/hcl", func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, engine, viewSecurityStatedOptions[engine.name])
			held := target.held(c)
			path := filepath.Join(c.TempDir(), "schema.hcl")

			inspected := runPtahIn(c, "schema", "inspect", "--db-url", target.url)
			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(inspected.Stdout, qt.Contains, "security {")
			c.Assert(os.WriteFile(path, []byte(inspected.Stdout), 0o600), qt.IsNil)
			planned := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--schema-file", path, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(target.held(c), qt.Equals, held)
		})
		t.Run(engine.name+"/sql", func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, engine, viewSecurityStatedOptions[engine.name])
			held := target.held(c)
			path := filepath.Join(c.TempDir(), "schema.sql")

			inspected := runPtahIn(c, "schema", "inspect", "--db-url", target.url, "--format", "sql")
			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(inspected.Stdout, qt.Contains, "security_invoker")
			c.Assert(os.WriteFile(path, []byte(inspected.Stdout), 0o600), qt.IsNil)
			planned := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--schema-file", path, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(target.held(c), qt.Equals, held)
		})
		t.Run(engine.name+"/go annotations", func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, engine, viewSecurityStatedOptions[engine.name])
			held := target.held(c)
			dir := c.TempDir()

			written := runPtahIn(c, "introspect", "--db-url", target.url, "--out", dir)
			c.Assert(written.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", written.Stderr))
			planned := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--root-dir", dir, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(target.held(c), qt.Equals, held)
		})
		t.Run(engine.name+"/compat hcl", func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, engine, viewSecurityStatedOptions[engine.name])
			held := target.held(c)
			path := filepath.Join(c.TempDir(), "schema.hcl")

			inspected := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "inspect", "-u", target.url)
			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(inspected.Stdout, qt.Contains, "security {")
			c.Assert(os.WriteFile(path, []byte(inspected.Stdout), 0o600), qt.IsNil)
			planned := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
				"schema", "apply", "-u", target.url, "--to", "file://"+filepath.ToSlash(path), "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Contains, "Schema is synced")
			c.Assert(target.held(c), qt.Equals, held)
		})
	}
}

// TestHCLSecurityBlockDecidesTheViewSecurityOptionE2E edits the document `schema
// inspect` wrote. Without the block the document leaves the option out, which
// is refused; with `invoker = false` the document removes it.
func TestHCLSecurityBlockDecidesTheViewSecurityOptionE2E(t *testing.T) {
	const block = "  security {\n    invoker = true\n  }\n"
	for _, engine := range viewSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, engine, "security_invoker = true")
			definition := target.definition(c)
			inspected := runPtahIn(c, "schema", "inspect", "--db-url", target.url)
			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(strings.Count(inspected.Stdout, block), qt.Equals, 1, qt.Commentf("%s", inspected.Stdout))
			path := filepath.Join(c.TempDir(), "schema.hcl")

			c.Assert(os.WriteFile(path, []byte(strings.Replace(inspected.Stdout, block, "", 1)), 0o600), qt.IsNil)
			refused := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--schema-file", path, "--auto-approve")

			c.Assert(refused.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
			viewSecurityRefused(c, refused, "public.my_docs")
			c.Assert(refused.Stderr, qt.Contains, "security block")
			assertViewSecurityUnchanged(c, target, definition)

			c.Assert(os.WriteFile(path, []byte(strings.Replace(inspected.Stdout, block,
				"  security {\n    invoker = false\n  }\n", 1)), 0o600), qt.IsNil)
			removed := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--schema-file", path, "--auto-approve")

			c.Assert(removed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", removed.Stdout, removed.Stderr))
			c.Assert(target.held(c), qt.Equals, "security_invoker=false")
			c.Assert(target.rowsReaderSees(c), qt.Equals, 2)
		})
	}
}

// TestPlanningVerbsRefuseToLoseAViewSecurityOptionE2E holds each other verb that
// plans to the same refusal. Each one reads the database and a schema that
// leaves the option out; none may write a migration or change the server. The
// compatibility surface is stricter here than the community binary, which
// leaves the view alone and reports the schema synced.
func TestPlanningVerbsRefuseToLoseAViewSecurityOptionE2E(t *testing.T) {
	type verb struct {
		name     string
		target   clirun.Target
		wantExit int
		args     func(url, schema, dev, dir string) []string
	}
	tests := []verb{
		{name: "schema compare", target: clirun.Ptah, wantExit: 2, args: func(url, schema, _, _ string) []string {
			return []string{"schema", "compare", "--db-url", url, "--schema-file", schema}
		}},
		{name: "migrations generate", target: clirun.Ptah, wantExit: 2, args: func(url, schema, _, dir string) []string {
			return []string{"migrations", "generate", "--db-url", url, "--schema-file", schema, "--migrations-dir", dir}
		}},
		{name: "compat schema apply", target: clirun.Compat, wantExit: 1, args: func(url, schema, dev, _ string) []string {
			return []string{"schema", "apply", "-u", url, "--to", "file://" + schema, "--dev-url", dev, "--auto-approve"}
		}},
		{name: "compat schema diff", target: clirun.Compat, wantExit: 1, args: func(url, schema, dev, _ string) []string {
			return []string{"schema", "diff", "--from", url, "--to", "file://" + schema, "--dev-url", dev}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, viewSecurityEngines[0], "security_invoker = true")
			definition := target.definition(c)
			adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
			admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
			devName := fmt.Sprintf("ptah_view_security_dev_%d", time.Now().UnixNano())
			createE2EDatabase(c, c.Context(), admin, devName)
			c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, devName) })
			dir := c.TempDir()

			got := clirun.Run(c, test.target, clirun.Options{Dir: c.TempDir()},
				test.args(target.url, filepath.ToSlash(target.schemaFile(c, viewSecurityBody, "")), replaceDatabaseName(c, adminURL, devName), dir)...)

			c.Assert(got.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			viewSecurityRefused(c, got, "my_docs")
			assertViewSecurityUnchanged(c, target, definition)
			written, err := os.ReadDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(written, qt.HasLen, 0)
		})
	}
}

// TestMigrationRollbackRemovesAViewSecurityOptionItAddedE2E generates a
// migration that adds the option, on each engine. Its down file resets the
// option because the view had none before, which is the earlier state rather
// than an omission, so generating it is not refused and running it gives back
// what the reader saw.
func TestMigrationRollbackRemovesAViewSecurityOptionItAddedE2E(t *testing.T) {
	for _, engine := range viewSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, engine, "")
			c.Assert(target.rowsReaderSees(c), qt.Equals, 2)
			dir := c.TempDir()

			generated := runPtahIn(c, "migrations", "generate", "--db-url", target.url,
				"--schema-file", target.schemaFile(c, viewSecurityBody, "security_invoker = true"), "--migrations-dir", dir)

			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			down := readFirstMatchingFile(c, dir, "*.down.sql")
			c.Assert(down, qt.Contains, "RESET (security_invoker)")

			up := runPtahIn(c, "migrations", "up", "--db-url", target.url, "--migrations-dir", dir)

			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(target.held(c), qt.Equals, "security_invoker=true")
			c.Assert(target.rowsReaderSees(c), qt.Equals, 1)

			rolledBack := runPtahIn(c, "migrations", "down", "--db-url", target.url, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(rolledBack.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", rolledBack.Stdout, rolledBack.Stderr))
			c.Assert(target.held(c), qt.Equals, "")
			c.Assert(target.rowsReaderSees(c), qt.Equals, 2)
		})
	}
}

// TestCompatMigrateDiffRollbackRemovesAViewSecurityOptionItAddedE2E is the same
// direction on the compatibility surface, which writes a reverse plan for a
// directory format that carries one. The down file of the migration that adds
// the option resets it, and the next migration, whose schema leaves the option
// out, is refused. The directory's replay on the dev database is the current
// side here, so no target database takes part.
func TestCompatMigrateDiffRollbackRemovesAViewSecurityOptionItAddedE2E(t *testing.T) {
	c := qt.New(t)
	engine := viewSecurityEngines[0]
	adminURL := dbtarget.URL(c, engine.engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	devName := fmt.Sprintf("ptah_view_security_dev_%d", time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, devName)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, devName) })
	dev := replaceDatabaseName(c, adminURL, devName)
	schema := viewSecurityTarget{}
	dir := filepath.ToSlash(c.TempDir())
	diff := func(name, options string) clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "migrate", "diff", name,
			"--dir", "file://"+dir+"?format=golang-migrate",
			"--to", "file://"+filepath.ToSlash(schema.schemaFile(c, viewSecurityBody, options)), "--dev-url", dev)
	}

	base := diff("base", "")

	c.Assert(base.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", base.Stdout, base.Stderr))

	added := diff("add_barrier", "security_barrier = true")

	c.Assert(added.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", added.Stdout, added.Stderr))
	c.Assert(readFirstMatchingFile(c, dir, "*_add_barrier.up.sql"), qt.Contains, "SET (security_barrier=true)")
	c.Assert(readFirstMatchingFile(c, dir, "*_add_barrier.down.sql"), qt.Contains, "RESET (security_barrier)")

	lost := diff("lose_barrier", "")

	c.Assert(lost.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", lost.Stdout, lost.Stderr))
	c.Assert(lost.Stderr, qt.Contains, "holds security_barrier=true and the schema does not state it")
	lostFiles, err := filepath.Glob(filepath.Join(filepath.FromSlash(dir), "*_lose_barrier.*"))
	c.Assert(err, qt.IsNil)
	c.Assert(lostFiles, qt.HasLen, 0)
}

// atlasReferenceBinary is the pinned community binary, or a skip where none is
// configured, which the integration contour reports as a failure.
func atlasReferenceBinary(t *testing.T) string {
	t.Helper()
	binary, ok := atlasreference.Binary()
	if !ok {
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s)", atlasreference.EnvVar, atlasreference.Version)
	}
	return binary
}

// TestViewSecurityDocumentOnTheCommunityBinaryE2E holds the measurement behind
// the compatibility surface's stricter answer. The pinned community binary
// manages no view: it reads a `view` block and plans nothing, whatever the block
// says about security, so the option stays. ptah-compat manages the view, so the
// block decides. Each row runs both binaries for real on one document written by
// `ptah-compat schema inspect`, and reads the server back after each.
func TestViewSecurityDocumentOnTheCommunityBinaryE2E(t *testing.T) {
	const block = "  security {\n    invoker = true\n  }\n"
	tests := []struct {
		name     string
		block    string
		wantExit int
		wantHeld string
		wantRows int
	}{
		{name: "the block as inspected", block: block, wantExit: 0, wantHeld: "security_invoker=true", wantRows: 1},
		{name: "no block", block: "", wantExit: 1, wantHeld: "security_invoker=true", wantRows: 1},
		{
			name: "the option turned off", block: "  security {\n    invoker = false\n  }\n",
			wantExit: 0, wantHeld: "security_invoker=false", wantRows: 2,
		},
	}
	reference := atlasReferenceBinary(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewSecurityTarget(c, viewSecurityEngines[0], "security_invoker = true")
			inspected := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "inspect", "-u", target.url)
			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(strings.Count(inspected.Stdout, block), qt.Equals, 1, qt.Commentf("%s", inspected.Stdout))
			path := filepath.Join(c.TempDir(), "schema.hcl")
			c.Assert(os.WriteFile(path, []byte(strings.Replace(inspected.Stdout, block, test.block, 1)), 0o600), qt.IsNil)
			apply := []string{"schema", "apply", "-u", target.url, "--to", "file://" + filepath.ToSlash(path), "--auto-approve"}

			community, err := exec.CommandContext(c.Context(), reference, apply...).CombinedOutput() // #nosec G204 G702 -- operator-provided pinned reference binary

			c.Assert(err, qt.IsNil, qt.Commentf("%s", community))
			c.Assert(string(community), qt.Contains, "Schema is synced")
			c.Assert(target.held(c), qt.Equals, "security_invoker=true")
			c.Assert(target.rowsReaderSees(c), qt.Equals, 1)

			compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, apply...)

			c.Assert(compat.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
			c.Assert(target.held(c), qt.Equals, test.wantHeld)
			c.Assert(target.rowsReaderSees(c), qt.Equals, test.wantRows)
		})
	}
}
