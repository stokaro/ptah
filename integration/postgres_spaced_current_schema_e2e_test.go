//go:build integration

package integration_test

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A URL with search_path=%22%20a%22 sends the server the quoted identifier
// " a", so ` a` is the session's current schema and the catalog reports its
// tables without a schema. Each test below runs against a database holding
// `a.t_plain` and ` a`.t_space, inspects it through that URL, and applies a
// document of ` a` back: the plan has to pair the live ` a`.t_space with the
// declared one. With the current schema trimmed, the read keyed the live table
// as a.t_space, and the plan was CREATE TABLE " a"."t_space" and DROP TABLE
// "t_space".

// newSpacedCurrentSchemaDatabase creates the database and returns its URL.
func newSpacedCurrentSchemaDatabase(c *qt.C) string {
	c.Helper()
	database := newEmptyReadScopeDatabase(c, "spaced_current")
	for _, statement := range []string{
		"CREATE SCHEMA a", `CREATE SCHEMA " a"`,
		"CREATE TABLE a.t_plain (id integer)", `CREATE TABLE " a".t_space (id integer)`,
	} {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	return database
}

// spacedCurrentSchemaURL is database's URL with ` a` as the current schema.
func spacedCurrentSchemaURL(database string) string {
	return withURLParameter(database, "search_path", "%22%20a%22")
}

// spacedCurrentSchemaSurfaces are the two surfaces with the commands that
// inspect a URL and apply a document to it, and the line a plan that changes
// nothing prints.
var spacedCurrentSchemaSurfaces = []struct {
	name    string
	program clirun.Target
	inspect func(target string) []string
	apply   func(target, document string) []string
	synced  string
}{
	{
		name:    "native",
		program: clirun.Ptah,
		inspect: func(target string) []string { return []string{"schema", "inspect", "--db-url", target} },
		apply: func(target, document string) []string {
			return []string{"schema", "apply", "--db-url", target, "--schema-file", document, "--dry-run"}
		},
		synced: "Schema is synced, no changes to be made.",
	},
	{
		name:    "the default compatibility surface",
		program: clirun.Compat,
		inspect: func(target string) []string { return []string{"schema", "inspect", "-u", target} },
		apply: func(target, document string) []string {
			return []string{"schema", "apply", "-u", target, "--to", "file://" + filepath.ToSlash(document), "--dry-run"}
		},
		synced: "Schema is synced, no changes to be made",
	},
}

// TestSpacedCurrentSchemaRoundTripE2E inspects the database on each surface and
// applies the document it printed. The document places t_space in ` a`, and the
// plan changes nothing.
func TestSpacedCurrentSchemaRoundTripE2E(t *testing.T) {
	for _, surface := range spacedCurrentSchemaSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target := spacedCurrentSchemaURL(newSpacedCurrentSchemaDatabase(c))
			dir := c.TempDir()

			inspected := clirun.Run(c, surface.program, clirun.Options{Dir: dir}, surface.inspect(target)...)

			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(inspected.Stdout, qt.Contains, "schema \" a\" {\n}")
			c.Assert(inspected.Stdout, qt.Contains, "table \"t_space\" {\n  schema = schema[\" a\"]")
			c.Assert(inspected.Stdout, qt.Not(qt.Contains), "schema.a\n")
			c.Assert(inspected.Stdout, qt.Not(qt.Contains), "t_plain")
			document := writeSpacedSchemaFile(c, "inspected.hcl", inspected.Stdout)

			applied := clirun.Run(c, surface.program, clirun.Options{Dir: dir}, surface.apply(target, document)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, surface.synced)
		})
	}
}

// TestSpacedCurrentSchemaAppliesTheReferenceInspectE2E applies the document the
// pinned community binary prints for ` a` on each surface, and the plan changes
// nothing. The binary reads search_path=%20a as the one schema " a", so its
// document is taken through that URL; Ptah reads the server's grammar, which
// trims an unquoted name, so it connects through the quoted one.
func TestSpacedCurrentSchemaAppliesTheReferenceInspectE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, surface := range spacedCurrentSchemaSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			database := newSpacedCurrentSchemaDatabase(c)
			target := spacedCurrentSchemaURL(database)
			dir := c.TempDir()
			inspected := referenceInspectOutput(c, reference, dir, withURLParameter(database, "search_path", "%20a"))
			c.Assert(inspected, qt.Contains, "schema = schema[\" a\"]")
			document := writeSpacedSchemaFile(c, "reference.hcl", inspected)

			applied := clirun.Run(c, surface.program, clirun.Options{Dir: dir}, surface.apply(target, document)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, surface.synced)
		})
	}
}

// spacedCurrentSchemaAddsHCL declares t_space in ` a` and a new table in the
// schema `a`, which the database does not hold.
const spacedCurrentSchemaAddsHCL = `schema " a" {}
schema "a" {}
table "t_space" {
  schema = schema[" a"]
  column "id" {
    type = integer
    null = true
  }
}
table "t_new" {
  schema = schema.a
  column "id" {
    type = integer
    null = true
  }
}
`

// TestSpacedCurrentSchemaCreatesTheSchemaWithoutTheSpaceE2E applies a document
// that adds the schema `a` to a database whose default search_path is the
// quoted identifier " a", so ` a` is the current schema of a URL that pins
// none and the run reads every schema. The plan creates `a` before its table,
// because `a` is not the connected schema. With the connected schema trimmed,
// the plan took `a` for it and created no schema, and the server refused
// CREATE TABLE "a"."t_new" because `a` does not exist.
func TestSpacedCurrentSchemaCreatesTheSchemaWithoutTheSpaceE2E(t *testing.T) {
	for _, surface := range []struct {
		name    string
		program clirun.Target
		args    func(target, document string) []string
		// tables is what the catalog holds after the apply. The native
		// surface also keeps its apply log in the current schema.
		tables string
	}{
		{
			name: "native", program: clirun.Ptah,
			args: func(target, document string) []string {
				return []string{"schema", "apply", "--db-url", target, "--schema-file", document, "--auto-approve"}
			},
			tables: "[ a].ptah_schema_apply_log, [ a].t_space, [a].t_new",
		},
		{
			name: "the default compatibility surface", program: clirun.Compat,
			args: func(target, document string) []string {
				return []string{"schema", "apply", "-u", target, "--to", "file://" + filepath.ToSlash(document), "--auto-approve"}
			},
			tables: "[ a].t_space, [a].t_new",
		},
	} {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			database := newEmptyReadScopeDatabase(c, "spaced_current_adds")
			parsed, err := url.Parse(database)
			c.Assert(err, qt.IsNil)
			name := strings.TrimPrefix(parsed.Path, "/")
			for _, statement := range []string{
				`CREATE SCHEMA " a"`, `CREATE TABLE " a".t_space (id integer)`,
				// An identifier, not a string: the server quotes each string
				// of a list setting, so '" a"' would store """ a""".
				fmt.Sprintf(`ALTER DATABASE %q SET search_path = " a"`, name),
			} {
				execPostgresFamilySQL(c, c.Context(), database, statement)
			}
			document := writeSpacedSchemaFile(c, "adds.hcl", spacedCurrentSchemaAddsHCL)

			applied := clirun.Run(c, surface.program, clirun.Options{Dir: c.TempDir()}, surface.args(database, document)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, `CREATE SCHEMA IF NOT EXISTS "a";`)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), `CREATE TABLE "a"."t_space"`)
			c.Assert(spacedCurrentSchemaTables(c, database), qt.Equals, surface.tables)
		})
	}
}

// referenceInspectOutput runs `schema inspect -u target` on the pinned binary in
// dir and returns what it printed on stdout.
func referenceInspectOutput(c *qt.C, reference, dir, target string) string {
	c.Helper()
	// #nosec G204 G702 -- the reference path is operator-provided via PTAH_ATLAS_REFERENCE
	command := exec.CommandContext(c.Context(), reference, "schema", "inspect", "-u", target)
	command.Dir = dir
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	c.Assert(command.Run(), qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	return stdout.String()
}

// TestSpacedCurrentSchemaMigratesE2E writes a migration through a dev database
// and applies it to a target, each reached through search_path=%22%20a%22 and
// each holding a table in `a` beside an empty ` a`. The dev database's clean
// check and the target's read ` a`, and the revision table is created and read
// there. With the current schema trimmed, the clean check read `a` and refused
// the dev database with `found table "t_plain" in connected schema`, and the
// apply created " a".atlas_schema_revisions, read "a".atlas_schema_revisions
// and failed with 42P01.
func TestSpacedCurrentSchemaMigratesE2E(t *testing.T) {
	c := qt.New(t)
	dev := newSpacedCurrentSchemaMigrateDatabase(c, "spaced_current_dev")
	target := newSpacedCurrentSchemaMigrateDatabase(c, "spaced_current_target")
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE t_new (id integer);\n"), 0o600), qt.IsNil)
	apply := func() clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
			"migrate", "apply", "--dir", "file://migrations", "--url", spacedCurrentSchemaURL(target))
	}

	diffed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
		"migrate", "diff", "init", "--dir", "file://migrations", "--to", "file://schema.sql", "--dev-url", spacedCurrentSchemaURL(dev))

	c.Assert(diffed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diffed.Stdout, diffed.Stderr))
	c.Assert(spacedCurrentSchemaTables(c, dev), qt.Equals, "[a].t_plain")

	applied := apply()

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(spacedCurrentSchemaTables(c, target), qt.Equals, "[ a].atlas_schema_revisions, [ a].t_new, [a].t_plain")

	again := apply()

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "No migration files to execute")
}

// newSpacedCurrentSchemaMigrateDatabase creates a database holding the table
// a.t_plain and the empty schema ` a`, and returns its URL.
func newSpacedCurrentSchemaMigrateDatabase(c *qt.C, role string) string {
	c.Helper()
	database := newEmptyReadScopeDatabase(c, role)
	for _, statement := range []string{"CREATE SCHEMA a", `CREATE SCHEMA " a"`, "CREATE TABLE a.t_plain (id integer)"} {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	return database
}

// spacedCurrentSchemaTables lists the tables of database outside the system
// schemas as [schema].table, in byte order. The brackets show the space.
func spacedCurrentSchemaTables(c *qt.C, database string) string {
	c.Helper()
	var tables string
	c.Assert(openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg('[' || table_schema || '].' || table_name, ', '
			ORDER BY table_schema COLLATE "C", table_name COLLATE "C"), '')
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`).Scan(&tables), qt.IsNil)
	return tables
}
