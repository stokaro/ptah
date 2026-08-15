package schemafile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/schemafile"
)

// postgresObjectsHCL is the "PostgreSQL Schema Objects" example from
// docs/atlas_hcl_schema.md, verbatim. Every block in it is one Ptah's HCL
// frontend fully supports, so whatever it renders is SQL that Ptah itself
// produced for a schema it can represent.
const postgresObjectsHCL = `schema "public" {}

extension "pg_trgm" {
	schema        = schema.public
  if_not_exists = true
  version       = "1.6"
  comment       = "trigram search"
}

sequence "order_number_seq" {
  schema    = schema.public
  type      = bigint
  start     = 1000
  increment = 1
  cache     = 10
  cycle     = false
}

domain "email" {
  schema = schema.public
  type   = text
  null   = false
  check  = "VALUE ~ '@'"
}

composite "address" {
  schema = schema.public
  field "street" {
    type = text
  }
  field "zip" {
    type = integer
  }
}

range "floatrange" {
  schema       = schema.public
  subtype      = float8
  subtype_diff = float8mi
}

role "app_user" {
  login   = true
  inherit = true
  comment = "application role"
}

table "users" {
  schema = schema.public

  column "id" {
    type = int
  }

  row_security {
    enabled = true
    comment = "tenant isolation"
  }
}

function "get_current_tenant" {
  schema     = schema.public
  lang       = SQL
  return     = text
  security   = INVOKER
  volatility = STABLE
  as         = "SELECT current_setting('app.tenant_id', true)"
}

view "active_users" {
  schema  = schema.public
  as      = "SELECT id FROM users WHERE deleted_at IS NULL"
  comment = "active users"
}

materialized "user_stats" {
  schema           = schema.public
  as               = "SELECT count(*) FROM users"
  refresh_strategy = "manual"
}

trigger "users_set_updated_at" {
  on = table.users
  before {
    update = true
  }
  for = ROW
  as  = "NEW.updated_at = now(); RETURN NEW;"
}

policy "users_tenant_policy" {
  on    = table.users
  for   = SELECT
  to    = [role.app_user, PUBLIC]
  using = "get_current_tenant() IS NOT NULL"
}

permission {
  to         = role.app_user
  for        = table.users
  privileges = [SELECT, INSERT]
  grantable  = true
}

permission {
  to         = PUBLIC
  for        = schema.public
  privileges = [USAGE]
}
`

func writeSchemaFile(c *qt.C, dir, name, body string) string {
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

func renderPostgres(c *qt.C, db *goschema.Database) []string {
	statements, err := renderer.GetOrderedCreateStatements(db, platform.Postgres)
	c.Assert(err, qt.IsNil)
	return statements
}

// dropLeadingComments removes the leading `-- text` lines a statement carries.
// Ptah renders an object's comment as a SQL line comment, and a SQL line
// comment belongs to no object when it is read back, so those lines are the one
// thing a rendered schema loses on its first trip through the SQL frontend.
func dropLeadingComments(statements []string) []string {
	stripped := make([]string, 0, len(statements))
	for _, statement := range statements {
		lines := strings.Split(statement, "\n")
		for len(lines) > 0 && strings.HasPrefix(strings.TrimSpace(lines[0]), "--") {
			lines = lines[1:]
		}
		stripped = append(stripped, strings.Join(lines, "\n"))
	}
	return stripped
}

// TestLoadAll_RenderedPostgresSQLReadsBackAsAFixedPoint is the acceptance check
// for issue #932: Ptah must be able to read back the SQL it renders.
//
// Before the fix, feeding the rendered SQL back exited 2 at
// "unsupported CREATE target: SEQUENCE", and of the 16 statements only
// CREATE SCHEMA and CREATE TABLE survived: 8 were refused outright and 5 more
// parsed and were then dropped on the floor by the AST-to-IR converter.
func TestLoadAll_RenderedPostgresSQLReadsBackAsAFixedPoint(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	hclPath := writeSchemaFile(c, dir, "schema.hcl", postgresObjectsHCL)
	fromHCL, err := schemafile.LoadAll([]string{hclPath}, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	first := renderPostgres(c, fromHCL)
	c.Assert(first, qt.HasLen, 16)

	sqlPath := writeSchemaFile(c, dir, "first.sql", strings.Join(first, ";\n")+";\n")
	fromSQL, err := schemafile.LoadAll([]string{sqlPath}, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	second := renderPostgres(c, fromSQL)
	c.Assert(second, qt.HasLen, 16)

	// Every statement comes back identical apart from the object comments that
	// were rendered as bare `--` lines.
	c.Assert(dropLeadingComments(second), qt.DeepEquals, dropLeadingComments(first))

	// From the second render on, the SQL is a true fixed point: reading it and
	// rendering it again reproduces it byte for byte.
	secondPath := writeSchemaFile(c, dir, "second.sql", strings.Join(second, ";\n")+";\n")
	fromSecondSQL, err := schemafile.LoadAll([]string{secondPath}, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	c.Assert(renderPostgres(c, fromSecondSQL), qt.DeepEquals, second)
}

// TestLoadAll_RenderedPostgresSQLKeepsEveryObjectKind names the objects the
// fixed-point check above depends on, so a regression says which kind was lost
// rather than only that a statement count moved.
func TestLoadAll_RenderedPostgresSQLKeepsEveryObjectKind(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	hclPath := writeSchemaFile(c, dir, "schema.hcl", postgresObjectsHCL)
	fromHCL, err := schemafile.LoadAll([]string{hclPath}, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	rendered := renderPostgres(c, fromHCL)

	sqlPath := writeSchemaFile(c, dir, "schema.sql", strings.Join(rendered, ";\n")+";\n")
	got, err := schemafile.LoadAll([]string{sqlPath}, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)

	c.Assert(got.Schemas, qt.HasLen, 1)
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Extensions, qt.HasLen, 1)
	c.Assert(got.Extensions[0].Schema, qt.Equals, "public")
	c.Assert(got.Sequences, qt.HasLen, 1)
	c.Assert(got.Domains, qt.HasLen, 1)
	c.Assert(got.CompositeTypes, qt.HasLen, 1)
	c.Assert(got.Ranges, qt.HasLen, 1)
	c.Assert(got.Roles, qt.HasLen, 1)
	c.Assert(got.Views, qt.HasLen, 1)
	c.Assert(got.MaterializedViews, qt.HasLen, 1)
	c.Assert(got.Triggers, qt.HasLen, 1)
	c.Assert(got.RLSPolicies, qt.HasLen, 1)
	c.Assert(got.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(got.Grants, qt.HasLen, 2)

	// The role comment survives because PostgreSQL spells it as a separate
	// COMMENT ON ROLE statement, which is real SQL rather than a line comment.
	c.Assert(got.Roles[0].Name, qt.Equals, "app_user")
	c.Assert(got.Roles[0].Comment, qt.Equals, "application role")

	// A trigger renders as a function plus a trigger; reading it back folds the
	// function into the trigger instead of leaving both, so the only function
	// left is the standalone one the schema declared.
	c.Assert(got.Functions, qt.HasLen, 1)
	c.Assert(got.Functions[0].Name, qt.Equals, "public.get_current_tenant")
	c.Assert(got.Triggers[0].Body, qt.Equals, "NEW.updated_at = now(); RETURN NEW;")
	c.Assert(got.Triggers[0].ExecuteFunction, qt.Equals, "")
}
