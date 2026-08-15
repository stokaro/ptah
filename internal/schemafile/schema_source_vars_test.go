package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemafile"
)

// TestLoadSourcesScopesVarsPerSource pins the half of the boundary
// stokaro/ptah#934 item 4 turns on that RESOLVES: a source carrying its own
// variable scope loads with that scope's values, and an unscoped source in the
// SAME load still sees the run's `--var` values. The refusals that make the
// scope a scope are in TestLoadSourcesRefusesValuesFromOutsideTheScope.
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
		// wantDefaults is the column default each variable ended up as, which
		// is where the value a source resolved is readable off the parse.
		wantDefaults map[string]string
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
			wantDefaults: map[string]string{"tenant": "acme"},
		},
		{
			// The control. Same file, same flag, unscoped source.
			name: "an unscoped source takes the run's --var",
			sources: func(dir string) []schemafile.Source {
				return []schemafile.Source{{URL: filepath.Join(dir, "tenant.hcl")}}
			},
			opts:         schemafile.Options{Vars: []string{"tenant=acme"}},
			wantDefaults: map[string]string{"tenant": "acme"},
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
			opts:         schemafile.Options{Vars: []string{"region=global"}},
			wantDefaults: map[string]string{"tenant": "scoped", "region": "global"},
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
			wantDefaults: map[string]string{"tenant": "acme,inc"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaVarFixtures(c)

			db, err := schemafile.LoadSources(test.sources(dir), test.opts)

			c.Assert(err, qt.IsNil)
			c.Assert(test.wantDefaults, qt.Not(qt.HasLen), 0,
				qt.Commentf("a row asserting no default would pass over any value at all"))
			for column, want := range test.wantDefaults {
				c.Check(columnDefault(db, column), qt.Equals, want,
					qt.Commentf("column %q", column))
			}
		})
	}
}

// TestLoadSourcesRefusesValuesFromOutsideTheScope is the other half of the
// boundary: a scoped source sees its OWN values and nothing else, in both
// directions. The run's `--var` does not reach in, and the source's VarValues
// do not reach out to an unscoped sibling in the same load.
//
// Both rows fail on the variable the leaking values would have satisfied, so a
// scope that stopped holding turns each of them green — which is what makes
// them measurements rather than the absence of a feature.
func TestLoadSourcesRefusesValuesFromOutsideTheScope(t *testing.T) {
	tests := []struct {
		name    string
		sources func(dir string) []schemafile.Source
		opts    schemafile.Options
		wantErr string
	}{
		{
			name: "a scoped source with no values refuses the run's --var",
			sources: func(dir string) []schemafile.Source {
				return []schemafile.Source{{
					URL:        filepath.Join(dir, "tenant.hcl"),
					VarsScoped: true,
				}}
			},
			opts:    schemafile.Options{Vars: []string{"tenant=acme"}},
			wantErr: `.*missing value for required variable "tenant".*`,
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
			wantErr: `.*missing value for required variable "region".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaVarFixtures(c)

			db, err := schemafile.LoadSources(test.sources(dir), test.opts)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
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
