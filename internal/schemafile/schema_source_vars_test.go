package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemafile"
)

// TestLoadSourcesScopesVarsPerSource pins the boundary stokaro/ptah#934 item 4
// turns on: a source that carries its own variable scope sees that scope and
// nothing else, and an unscoped source in the SAME load still sees the run's
// `--var` values.
//
// The rule is the pinned Atlas community binary v1.3.0's, measured with
// `schema apply --env local --dry-run` against a schema file declaring
// `variable "tenant" { type = string }` with no default, exit codes read
// directly from unpiped invocations:
//
//	src = data.hcl_schema.app.url with --var tenant=acme  -> 1  missing value for
//	                                                            required variable
//	                                                            "tenant"
//	src = "file://s.hcl"          with --var tenant=acme  -> 0  DEFAULT 'acme'
//
// The second row is the control that makes the first mean something: same flag,
// same file, same command, and only the way the env names the source differs.
// Without it, row one would be satisfied by a binary that has no --var at all.
func TestLoadSourcesScopesVarsPerSource(t *testing.T) {
	tests := []struct {
		name    string
		sources func(dir string) []schemafile.Source
		opts    schemafile.Options
		assert  func(c *qt.C, db *goschema.Database, err error)
	}{
		{
			name: "a scoped source uses its own values",
			sources: func(dir string) []schemafile.Source {
				return []schemafile.Source{{
					URL:        filepath.Join(dir, "tenant.hcl"),
					VarValues:  map[string]string{"tenant": "acme"},
					VarsScoped: true,
				}}
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(columnDefault(db, "tenant"), qt.Equals, "acme")
			},
		},
		{
			name: "a scoped source with no values refuses the run's --var",
			sources: func(dir string) []schemafile.Source {
				return []schemafile.Source{{
					URL:        filepath.Join(dir, "tenant.hcl"),
					VarsScoped: true,
				}}
			},
			opts: schemafile.Options{Vars: []string{"tenant=acme"}},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches, `.*missing value for required variable "tenant".*`)
				c.Assert(db, qt.IsNil)
			},
		},
		{
			// The control. Same file, same flag, unscoped source.
			name: "an unscoped source takes the run's --var",
			sources: func(dir string) []schemafile.Source {
				return []schemafile.Source{{URL: filepath.Join(dir, "tenant.hcl")}}
			},
			opts: schemafile.Options{Vars: []string{"tenant=acme"}},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(columnDefault(db, "tenant"), qt.Equals, "acme")
			},
		},
		{
			// Both shapes in ONE load, which is what a parallel Vars field on
			// the whole load could never express.
			name: "a scoped and an unscoped source in one load keep their own values",
			sources: func(dir string) []schemafile.Source {
				return []schemafile.Source{
					{
						URL:        filepath.Join(dir, "tenant.hcl"),
						VarValues:  map[string]string{"tenant": "scoped"},
						VarsScoped: true,
					},
					{URL: filepath.Join(dir, "region.hcl")},
				}
			},
			opts: schemafile.Options{Vars: []string{"region=global"}},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(columnDefault(db, "tenant"), qt.Equals, "scoped")
				c.Assert(columnDefault(db, "region"), qt.Equals, "global")
			},
		},
		{
			// The scoped source's values must not leak the other way either.
			name: "a scoped source's values do not reach an unscoped one",
			sources: func(dir string) []schemafile.Source {
				return []schemafile.Source{
					{
						URL:        filepath.Join(dir, "tenant.hcl"),
						VarValues:  map[string]string{"tenant": "scoped", "region": "scoped"},
						VarsScoped: true,
					},
					{URL: filepath.Join(dir, "region.hcl")},
				}
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches, `.*missing value for required variable "region".*`)
				c.Assert(db, qt.IsNil)
			},
		},
		{
			// A comma is the reason VarValues is a decoded map rather than
			// `--var` text: the flag grammar reads one occurrence as a CSV
			// record, so this value would come back cut in half.
			name: "a value containing a comma survives whole",
			sources: func(dir string) []schemafile.Source {
				return []schemafile.Source{{
					URL:        filepath.Join(dir, "tenant.hcl"),
					VarValues:  map[string]string{"tenant": "acme,inc"},
					VarsScoped: true,
				}}
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(columnDefault(db, "tenant"), qt.Equals, "acme,inc")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaVarFixtures(c)

			db, err := schemafile.LoadSources(test.sources(dir), test.opts)

			test.assert(c, db, err)
		})
	}
}

// writeSchemaVarFixtures writes two HCL schema files, each declaring one
// required variable and using it as a column default so the value is readable
// off the parsed schema.
func writeSchemaVarFixtures(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "tenant.hcl"), []byte(`variable "tenant" {
  type = string
}

schema "main" {
}

table "users" {
  schema = schema.main
  column "tenant" {
    type    = text
    default = var.tenant
  }
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "region.hcl"), []byte(`variable "region" {
  type = string
}

table "orders" {
  schema = schema.main
  column "region" {
    type    = text
    default = var.region
  }
}
`), 0o600), qt.IsNil)
	return dir
}

// columnDefault reads the default of the one column with the given name.
func columnDefault(db *goschema.Database, column string) string {
	for _, field := range db.Fields {
		if field.Name == column {
			return field.Default
		}
	}
	return ""
}
