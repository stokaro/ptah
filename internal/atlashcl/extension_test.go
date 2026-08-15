package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/atlashcl"
)

func TestParseExtensionIfNotExists(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
extension "pg_trgm" {
  if_not_exists = true
  version       = "1.6"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Extensions, qt.HasLen, 1)
	c.Assert(db.Extensions[0].IfNotExists, qt.IsTrue)
	c.Assert(db.Extensions[0].Version, qt.Equals, "1.6")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE EXTENSION IF NOT EXISTS pg_trgm VERSION '1.6';`)
}

func TestParseExtensionDefaultsWithoutIfNotExists(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
extension "pg_trgm" {}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Extensions, qt.HasLen, 1)
	c.Assert(db.Extensions[0].IfNotExists, qt.IsFalse)

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE EXTENSION pg_trgm;`)
}

func TestParseExtensionSchemaAttributePresence(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		wantSchema string
		wantName   string
	}{
		{
			name:       "one label explicit default",
			source:     `extension "citext" { schema = "" }`,
			wantSchema: "",
			wantName:   "citext",
		},
		{
			name:       "two labels without attribute",
			source:     `extension "extensions" "citext" {}`,
			wantSchema: "extensions",
			wantName:   "citext",
		},
		{
			name:       "two labels with matching attribute",
			source:     `extension "extensions" "citext" { schema = "extensions" }`,
			wantSchema: "extensions",
			wantName:   "citext",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse([]byte(test.source), "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(db.Extensions, qt.DeepEquals, []goschema.Extension{{
				Schema: test.wantSchema,
				Name:   test.wantName,
			}})
		})
	}
}

func TestParseTwoLabelExtensionRejectsExplicitDefaultSchema(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
extension "extensions" "citext" {
  schema = ""
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches,
		`.*extension "citext" schema label conflicts with schema attribute "".*`)
}

func TestParseExplicitExtensionSystemSchemaReferenceIsRefused(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "pg_catalog" {}
schema "PG_CATALOG" {}

extension "plpgsql" {
  schema  = schema.pg_catalog
  version = "1.0"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Extensions, qt.HasLen, 1)
	c.Assert(db.Extensions[0].Schema, qt.Equals, "pg_catalog")
	c.Assert(db.Schemas, qt.DeepEquals, []goschema.Schema{
		{Name: "pg_catalog"},
		{Name: "PG_CATALOG"},
	})

	statements, err := renderer.GetOrderedCreateStatements(db, "postgres")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "pg_catalog".*`)
	c.Assert(statements, qt.IsNil)
}

func TestParseLiteralExtensionSystemSchemaNeedsNoDeclaration(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
extension "plpgsql" {
  schema  = "pg_catalog"
  version = "1.0"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Schemas, qt.HasLen, 0)
	c.Assert(db.Extensions, qt.DeepEquals, []goschema.Extension{{
		Name:    "plpgsql",
		Schema:  "pg_catalog",
		Version: "1.0",
	}})

	sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
	c.Assert(sql, qt.Not(qt.Contains), `CREATE SCHEMA IF NOT EXISTS "pg_catalog"`)
	c.Assert(sql, qt.Contains, `CREATE EXTENSION "plpgsql" WITH SCHEMA "pg_catalog" VERSION '1.0';`)
}

func TestParseStandaloneSystemSchemaIsNotTreatedAsAnExtensionReference(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "pg_catalog" {}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Schemas, qt.DeepEquals, []goschema.Schema{{Name: "pg_catalog"}})

	statements, err := renderer.GetOrderedCreateStatements(db, "postgres")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "pg_catalog".*`)
	c.Assert(statements, qt.IsNil)
}

func TestParseSystemSchemaUsedByAnObjectIsNotTreatedAsAnExtensionReference(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "pg_catalog" {}

extension "plpgsql" {
  schema = schema.pg_catalog
}

table "blocked" {
  schema = schema.pg_catalog

  column "id" {
    type = integer
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Schemas, qt.DeepEquals, []goschema.Schema{{Name: "pg_catalog"}})

	statements, err := renderer.GetOrderedCreateStatements(db, "postgres")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "pg_catalog".*`)
	c.Assert(statements, qt.IsNil)
}

func TestParseTwoLabelExtensionDoesNotMakeASystemSchemaReferenceOnly(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "pg_catalog" {}
extension "pg_catalog" "plpgsql" {}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Schemas, qt.DeepEquals, []goschema.Schema{{Name: "pg_catalog"}})

	statements, err := renderer.GetOrderedCreateStatements(db, "postgres")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "pg_catalog".*`)
	c.Assert(statements, qt.IsNil)
}

func TestParsePermissionTargetKeepsItsSystemSchemaSemantic(t *testing.T) {
	tests := []struct {
		name   string
		target string
	}{
		{name: "table", target: "table.pg_catalog.pg_type"},
		{name: "sequence", target: "sequence.pg_catalog.server_seq"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse([]byte(`
schema "pg_catalog" {}

extension "plpgsql" {
  schema = schema.pg_catalog
}

permission {
  to         = PUBLIC
  for        = `+test.target+`
  privileges = [SELECT]
}
`), "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(db.Schemas, qt.DeepEquals, []goschema.Schema{{Name: "pg_catalog"}})

			statements, err := renderer.GetOrderedCreateStatements(db, "postgres")
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*declares server-owned PostgreSQL schema "pg_catalog".*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestParseExtensionRejectsNonBoolIfNotExists(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
extension "pg_trgm" {
  if_not_exists = "yes"
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*extension attribute "if_not_exists" must be a bool.*`)
}
