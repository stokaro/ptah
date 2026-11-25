//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// The tests below hold a PostgreSQL schema whose name begins with a space
// beside the schema of the same name without it, and ask whether a reference
// to the second is read as one to the first: the qualifier of a view body, and
// a grant on " public".

// spacedQualifierSQL builds a view in " a" that reads the table src of the
// other schema, a.
const spacedQualifierSQL = `CREATE SCHEMA a;
CREATE TABLE a.src (id integer);
CREATE SCHEMA " a";
CREATE TABLE " a".src (id integer);
CREATE VIEW " a".v AS SELECT id FROM a.src;`

// spacedQualifierHCL declares the view to read the src of its own schema.
const spacedQualifierHCL = `schema " a" {}
table "src" {
  schema = schema[" a"]
  column "id" {
    null = true
    type = integer
  }
}
view "v" {
  schema = schema[" a"]
  column "id" {}
  as = "SELECT id FROM src"
}
`

// spacedQualifierSource names the schema of the src the view " a".v reads, as
// the server's dependency record holds it.
const spacedQualifierSource = `SELECT string_agg(DISTINCT n.nspname, ',')
	FROM pg_depend d
	JOIN pg_rewrite r ON r.oid = d.objid
	JOIN pg_class c ON c.oid = d.refobjid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE r.ev_class = '" a".v'::regclass AND c.relname = 'src'`

// TestSpacedSchemaViewQualifierE2E applies the declared view through a URL
// whose search_path is " a". The server reads the live body back as
// `SELECT id FROM a.src`, qualified because a is not on the path. The
// qualifier names the other schema, so the body differs from the declared one:
// the apply replaces the view, which then reads " a".src, and a second run
// plans nothing.
func TestSpacedSchemaViewQualifierE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "spaced_qualifier")
	execPostgresFamilySQL(c, c.Context(), database, spacedQualifierSQL)
	target := withURLParameter(database, "search_path", "%22%20a%22")
	schema := writeSpacedSchemaFile(c, "schema.hcl", spacedQualifierHCL)
	args := []string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve"}
	c.Assert(spacedSchemaScalar(c, database, spacedQualifierSource), qt.Equals, "a")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, `CREATE VIEW " a"."v"`)
	c.Assert(spacedSchemaScalar(c, database, spacedQualifierSource), qt.Equals, " a")

	again := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// spacedPublicGrantSQL gives the schema " public" the owner and the grant
// PostgreSQL gives public in every database it creates, so the two schemas
// hold the same privileges and differ in their name alone. Owned by the
// connecting role instead, the schema's privileges name that role once the
// grant is made, and strict mode refuses the role before it reaches the grant
// when the role is not one it reads as the server's own.
const spacedPublicGrantSQL = `CREATE SCHEMA " public" AUTHORIZATION pg_database_owner;
GRANT USAGE ON SCHEMA " public" TO PUBLIC;`

// spacedSecurityFinding is the part of a `ptah schema security --format json`
// finding the tests read.
type spacedSecurityFinding struct {
	Code    string `json:"code"`
	Subject struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"subject"`
}

// spacedSecurityFindings runs `ptah schema security` over public and " public"
// and returns the findings of PRV03, the rule over grants to PUBLIC, as
// kind:[name].
func spacedSecurityFindings(c *qt.C, database string) []string {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"schema", "security", "--db-url", database, "--schemas", `" public",public`, "--format", "json", "--fail-on", "none")
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	var report struct {
		Findings []spacedSecurityFinding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(result.Stdout), &report), qt.IsNil, qt.Commentf("%s", result.Stdout))
	findings := make([]string, 0, len(report.Findings))
	for _, finding := range report.Findings {
		if finding.Code == "PRV03" {
			findings = append(findings, finding.Code+" "+finding.Subject.Kind+":["+finding.Subject.Name+"]")
		}
	}
	return findings
}

// TestSpacedPublicSchemaGrantSecurityE2E reports the USAGE on " public" that
// PUBLIC holds. The same grant on public is the one PostgreSQL ships, and the
// report leaves it out; the control proves the database the first test reads
// holds that one too.
func TestSpacedPublicSchemaGrantSecurityE2E(t *testing.T) {
	t.Run("the grant on the spaced schema", func(t *testing.T) {
		c := qt.New(t)
		database := newEmptyReadScopeDatabase(c, "spaced_public_grant")
		execPostgresFamilySQL(c, c.Context(), database, spacedPublicGrantSQL)

		c.Assert(spacedSecurityFindings(c, database), qt.DeepEquals, []string{"PRV03 schema:[ public]"})
	})
	t.Run("the shipped grant alone", func(t *testing.T) {
		c := qt.New(t)
		database := newEmptyReadScopeDatabase(c, "spaced_public_control")
		execPostgresFamilySQL(c, c.Context(), database, `CREATE SCHEMA " public"`)

		c.Assert(spacedSecurityFindings(c, database), qt.HasLen, 0)
		c.Assert(spacedSchemaScalar(c, database,
			`SELECT has_schema_privilege('public', 'public', 'USAGE')::text`), qt.Equals, "true")
	})
}

// TestSpacedPublicSchemaGrantStrictInspectE2E inspects " public" under
// PTAH_ATLAS_STRICT_COMPAT=1, which refuses an inspected grant other than the
// one PostgreSQL ships. The control inspects the schema without the grant.
func TestSpacedPublicSchemaGrantStrictInspectE2E(t *testing.T) {
	strict := append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")
	t.Run("the grant on the spaced schema", func(t *testing.T) {
		c := qt.New(t)
		database := newEmptyReadScopeDatabase(c, "spaced_strict_grant")
		execPostgresFamilySQL(c, c.Context(), database, spacedPublicGrantSQL)

		result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: strict},
			"schema", "inspect", "-u", database, "--schema", " public")

		c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
		c.Assert(result.Stderr, qt.Contains, "Atlas Community Edition strict compatibility does not support inspected schema grants")
		c.Assert(result.Stdout, qt.Equals, "")
	})
	t.Run("the spaced schema without the grant", func(t *testing.T) {
		c := qt.New(t)
		database := newEmptyReadScopeDatabase(c, "spaced_strict_control")
		execPostgresFamilySQL(c, c.Context(), database, `CREATE SCHEMA " public"`)

		result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: strict},
			"schema", "inspect", "-u", database, "--schema", " public")

		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
		c.Assert(result.Stdout, qt.Contains, "\nschema \" public\" {\n}\n")
	})
}

// spacedSchemaScalar runs a query answering one text value on the database.
func spacedSchemaScalar(c *qt.C, database, query string) string {
	c.Helper()
	var value string
	c.Assert(openReadScopeDatabase(c, database).QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil, qt.Commentf("%s", query))
	return value
}

// spacedPublicDefaultSQL makes " public" the schema a connection to the
// database starts in, which is the schema `schema clean` cleans. Strict mode
// refuses a search_path naming it in the URL, so the database names it.
const spacedPublicDefaultSQL = `CREATE TABLE " public".x (id integer);
DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET search_path = %I', current_database(), ' public'); END $$;`

// spacedPublicTables counts the tables of " public".
const spacedPublicTables = `SELECT count(*)::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = ' public' AND c.relkind = 'r'`

// TestSpacedPublicSchemaGrantStrictCleanE2E cleans " public" under
// PTAH_ATLAS_STRICT_COMPAT=1, which refuses to clean a schema holding a grant
// other than the one PostgreSQL ships: the table stays. The control cleans the
// same schema without the grant, and the table goes.
func TestSpacedPublicSchemaGrantStrictCleanE2E(t *testing.T) {
	strict := append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")
	t.Run("the grant on the spaced schema", func(t *testing.T) {
		c := qt.New(t)
		database := newEmptyReadScopeDatabase(c, "spaced_clean_grant")
		execPostgresFamilySQL(c, c.Context(), database, spacedPublicGrantSQL)
		execPostgresFamilySQL(c, c.Context(), database, spacedPublicDefaultSQL)

		result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: strict},
			"schema", "clean", "-u", database, "--auto-approve")

		c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
		c.Assert(result.Stderr, qt.Contains, "Atlas Community Edition strict compatibility does not support cleaning live schema grants")
		c.Assert(spacedSchemaScalar(c, database, spacedPublicTables), qt.Equals, "1")
	})
	t.Run("the spaced schema without the grant", func(t *testing.T) {
		c := qt.New(t)
		database := newEmptyReadScopeDatabase(c, "spaced_clean_control")
		execPostgresFamilySQL(c, c.Context(), database, `CREATE SCHEMA " public"`)
		execPostgresFamilySQL(c, c.Context(), database, spacedPublicDefaultSQL)

		result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: strict},
			"schema", "clean", "-u", database, "--auto-approve")

		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
		c.Assert(spacedSchemaScalar(c, database, spacedPublicTables), qt.Equals, "0")
	})
}
