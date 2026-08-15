package schemaload_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaload"
)

// tenantHCL declares one required variable and uses it as a column default, so
// a load that never delivers a value is visible in the result rather than only
// in an error.
const tenantHCL = `variable "tenant" {
  type    = string
  default = "fallback"
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
`

// requiredTenantHCL is the same file with the default removed, so a load that
// delivers no value cannot fall back and has to refuse.
const requiredTenantHCL = `variable "tenant" {
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
`

// TestLoad_DeliversVariableValuesToAnHCLSchema pins the gap stokaro/ptah#1533
// records: schemaload had nowhere to put variable values, so every native verb
// that resolves its desired state through it -- `schema apply`, `schema plan`,
// `compare`, `generate`, `migrate generate`, `schema export` and `schema test`
// -- loaded an HCL schema with its declared defaults whatever the caller
// passed, and refused a file whose variable had none.
//
// The rows separate the two spellings a caller has. `Vars` is the `--var`
// form, already parsed by the flag; `VarValues` is the decoded map a project
// file's data source scope resolves to. Both must arrive, because the compat
// surface reaches this loader through each of them.
func TestLoad_DeliversVariableValuesToAnHCLSchema(t *testing.T) {
	tests := []struct {
		name string
		file string
		opts func(path string) schemaload.Options
		want string
	}{
		{
			name: "the --var spelling reaches the file",
			file: tenantHCL,
			opts: func(path string) schemaload.Options {
				return schemaload.Options{SchemaFiles: []string{path}, Vars: []string{"tenant=acme"}}
			},
			want: "acme",
		},
		{
			name: "the decoded spelling reaches the file",
			file: tenantHCL,
			opts: func(path string) schemaload.Options {
				return schemaload.Options{
					SchemaFiles: []string{path},
					VarValues:   map[string]string{"tenant": "acme"},
				}
			},
			want: "acme",
		},
		{
			name: "a variable with no value declared keeps its default",
			file: tenantHCL,
			opts: func(path string) schemaload.Options {
				return schemaload.Options{SchemaFiles: []string{path}}
			},
			want: "fallback",
		},
		{
			name: "a value reaches a variable that has no default",
			file: requiredTenantHCL,
			opts: func(path string) schemaload.Options {
				return schemaload.Options{SchemaFiles: []string{path}, Vars: []string{"tenant=acme"}}
			},
			want: "acme",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "s.hcl")
			c.Assert(os.WriteFile(path, []byte(test.file), 0o600), qt.IsNil)

			db, err := schemaload.Load(test.opts(path))

			c.Assert(err, qt.IsNil)
			c.Assert(tenantColumnDefault(c, db), qt.Equals, test.want)
		})
	}
}

// TestLoad_RefusesARequiredVariableNobodyGaveAValue is the control that keeps
// the rows above from being satisfied by a loader that ignores the file's
// variables entirely and hands back a constant.
func TestLoad_RefusesARequiredVariableNobodyGaveAValue(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "s.hcl")
	c.Assert(os.WriteFile(path, []byte(requiredTenantHCL), 0o600), qt.IsNil)

	_, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})

	c.Assert(err, qt.ErrorMatches, `.*missing value for required variable "tenant".*`)
}

// tenantColumnDefault returns the default the loaded schema gives the tenant
// column, with the quoting the renderer applies stripped.
func tenantColumnDefault(c *qt.C, db *goschema.Database) string {
	c.Helper()
	for _, field := range db.Fields {
		if field.Name == "tenant" {
			return trimQuotes(field.Default)
		}
	}
	c.Fatalf("the loaded schema declares no tenant column")
	return ""
}

// trimQuotes removes one layer of single quotes.
func trimQuotes(value string) string {
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		return value[1 : len(value)-1]
	}
	return value
}
