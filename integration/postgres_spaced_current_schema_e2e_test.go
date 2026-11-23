//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
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

// spacedCurrentSchemaSeedHCL declares what the seeded target holds in ` a`
// and a column on t_space whose default draws from seq_x, so the plan is
// rehearsed on the dev database with the target's objects copied into it.
const spacedCurrentSchemaSeedHCL = `schema " a" {
}

sequence "seq_x" {
  schema = schema[" a"]
  type = "bigint"
  start = 1
  increment = 1
  min_value = 1
  max_value = 9223372036854775807
  cache = 1
}

table "shared" {
  schema = schema[" a"]
  column "id" {
    type = integer
    null = true
  }
}

table "t_space" {
  schema = schema[" a"]
  column "id" {
    type = integer
    null = true
  }
  column "n" {
    type = bigint
    null = true
    default = sql("nextval('seq_x')")
  }
}
`

// TestSpacedCurrentSchemaSeedsADevDatabaseThatHoldsTheSchemaWithoutTheSpaceE2E
// rehearses a plan on each surface through a dev database that holds both `a`
// and ` a`, with ` a` the current schema of the target and of the dev URL.
// The target holds a table and a sequence of the same names in each. The
// rehearsal copies the target's objects of ` a` into the dev database's ` a`,
// plans the new column and leaves the dev database empty. With the current
// schema trimmed, the seed places those objects in `a`; with the search path
// trimmed, it copies the objects of `a`. Either way the dev database held `a`
// before the run, and the seed refuses it: `the dev database held schema "a"
// before the run`.
func TestSpacedCurrentSchemaSeedsADevDatabaseThatHoldsTheSchemaWithoutTheSpaceE2E(t *testing.T) {
	for _, surface := range spacedCurrentSchemaSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target := newEmptyReadScopeDatabase(c, "spaced_seed_target")
			for _, statement := range []string{
				`CREATE SCHEMA " a"`, `CREATE TABLE " a".t_space (id integer)`, `CREATE TABLE " a".shared (id integer)`,
				`CREATE SEQUENCE " a".seq_x`, "CREATE SCHEMA a", "CREATE TABLE a.shared (id integer)", "CREATE SEQUENCE a.seq_x",
			} {
				execPostgresFamilySQL(c, c.Context(), target, statement)
			}
			dev := newEmptyReadScopeDatabase(c, "spaced_seed_dev")
			for _, statement := range []string{"CREATE SCHEMA a", `CREATE SCHEMA " a"`} {
				execPostgresFamilySQL(c, c.Context(), dev, statement)
			}
			document := writeSpacedSchemaFile(c, "seed.hcl", spacedCurrentSchemaSeedHCL)
			args := append(surface.apply(spacedCurrentSchemaURL(target), document), "--dev-url", spacedCurrentSchemaURL(dev))

			applied := clirun.Run(c, surface.program, clirun.Options{Dir: c.TempDir()}, args...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, `ALTER TABLE " a"."t_space" ADD COLUMN "n" bigint`)
			c.Assert(spacedCurrentSchemaTables(c, dev), qt.Equals, "")
			c.Assert(spacedCurrentSchemaTables(c, target), qt.Equals, "[ a].shared, [ a].t_space, [a].shared")
		})
	}
}

// TestSpacedCurrentSchemaKeepsTheDevDatabasesDefaultPrivilegeE2E diffs a
// migration directory through a dev database whose current schema is ` a`
// and which holds a default privilege defined in ` a`. The cleanup between
// replays keeps the default privileges of the connection's own schema, so the
// row is still there after the run. With that schema trimmed, the cleanup kept
// the default privileges of `a` and dropped the one in ` a` without a word.
func TestSpacedCurrentSchemaKeepsTheDevDatabasesDefaultPrivilegeE2E(t *testing.T) {
	c := qt.New(t)
	admin := openReadScopeDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL))
	role := fmt.Sprintf("ptah_spaced_reader_%d", time.Now().UnixNano())
	_, err := admin.ExecContext(c.Context(), "CREATE ROLE "+quoteE2EIdent(role)+" NOLOGIN")
	c.Assert(err, qt.IsNil)
	// Registered before the database is created, so it runs after the database
	// is dropped and no default privilege names the role any more.
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
		c.Check(dropErr, qt.IsNil)
	})
	dev := newSpacedCurrentSchemaMigrateDatabase(c, "spaced_current_acl")
	execPostgresFamilySQL(c, c.Context(), dev, `ALTER DEFAULT PRIVILEGES IN SCHEMA " a" GRANT SELECT ON TABLES TO `+quoteE2EIdent(role))
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE t_new (id integer);\n"), 0o600), qt.IsNil)
	want := "[ a] r " + role + ":SELECT"
	c.Assert(spacedCurrentSchemaDefaultPrivileges(c, dev), qt.Equals, want)

	diffed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
		"migrate", "diff", "init", "--dir", "file://migrations", "--to", "file://schema.sql", "--dev-url", spacedCurrentSchemaURL(dev))

	c.Assert(diffed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diffed.Stdout, diffed.Stderr))
	c.Assert(spacedCurrentSchemaDefaultPrivileges(c, dev), qt.Equals, want)
}

// spacedCurrentSchemaDefaultPrivileges lists the default privileges of
// database as [schema] object type grantee:privilege, in byte order.
func spacedCurrentSchemaDefaultPrivileges(c *qt.C, database string) string {
	c.Helper()
	var privileges string
	c.Assert(openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg('[' || n.nspname || '] ' || d.defaclobjtype::text || ' ' || g.rolname || ':' || a.privilege_type, ', '
			ORDER BY n.nspname COLLATE "C", g.rolname COLLATE "C", a.privilege_type COLLATE "C"), '')
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		CROSS JOIN LATERAL aclexplode(d.defaclacl) a
		JOIN pg_roles g ON g.oid = a.grantee`).Scan(&privileges), qt.IsNil)
	return privileges
}

// spacedCurrentSchemaDataSource declares rows for events, which it names
// without a schema, so the table resolves to the current schema.
const spacedCurrentSchemaDataSource = `package entities

//ptah:schema:data table="events" key="id" file="rows.yaml"
//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
	//ptah:schema:field name="at" type="TEXT"
	At string
}
`

// TestSpacedCurrentSchemaMigrationsDataReadsTheCurrentSchemaE2E generates a
// data migration through a URL whose current schema is ` a`, for rows declared
// on events, which ` a` and `a` both hold with different columns. The live row
// in ` a` already matches, so nothing is written. With the current schema
// trimmed, the column read is scoped to `a` and the run refuses with `managed
// table "events" does not have declared column(s) "at"`.
func TestSpacedCurrentSchemaMigrationsDataReadsTheCurrentSchemaE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "spaced_current_data")
	for _, statement := range []string{
		"CREATE SCHEMA a", `CREATE SCHEMA " a"`,
		`CREATE TABLE " a".events (id integer PRIMARY KEY, at text)`, `INSERT INTO " a".events VALUES (1, 'x')`,
		"CREATE TABLE a.events (id integer PRIMARY KEY, note text)", "INSERT INTO a.events VALUES (1, 'y')",
	} {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	dir := c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(dir, "entities"), 0o750), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(dir, "out"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "entities", "schema.go"), []byte(spacedCurrentSchemaDataSource), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "entities", "rows.yaml"), []byte("- id: 1\n  at: \"x\"\n"), 0o600), qt.IsNil)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "data", "--db-url", spacedCurrentSchemaURL(database), "--root-dir", "entities", "--migrations-dir", "out")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Equals, "no data changes\n")
	written, err := os.ReadDir(filepath.Join(dir, "out"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
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
