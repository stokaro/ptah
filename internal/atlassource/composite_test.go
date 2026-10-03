package atlassource_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/envbool/envbooltest"
)

// authHCL declares the `auth` schema and one table in it, in Atlas's HCL.
const authHCL = `schema "auth" {}

table "users" {
  schema = schema.auth
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`

// compositeProject writes files and an atlas.hcl into a fresh directory and
// returns the evaluated env "dev" of that project.
func compositeProject(c *qt.C, atlasHCL string, files map[string]string) atlassource.ProjectEnv {
	c.Helper()
	dir := c.TempDir()
	for name, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
	configPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(configPath, []byte(atlasHCL), 0o600), qt.IsNil)
	cfg, err := projectconfig.ParseAtlas([]byte(atlasHCL), configPath, "dev")
	c.Assert(err, qt.IsNil)
	return atlassource.ProjectEnv{Loaded: true, Config: cfg, BaseDir: dir}
}

// compositeHCL wraps the given schema blocks in a composite_schema named
// project that env "dev" takes as its desired state.
func compositeHCL(parts string) string {
	return `data "composite_schema" "project" {
` + parts + `}

env "dev" {
  src = data.composite_schema.project.url
}
`
}

// TestClassifySetCompositeSchema_HappyPath expands env://src into one
// composite source whose parts keep their labels and their order, each
// classified as a source of its own.
func TestClassifySetCompositeSchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	env := compositeProject(c, compositeHCL(`  schema "auth" {
    url = "file://auth.hcl"
  }
  schema {
    url = "public.sql"
  }
  schema "app" {
    url = "file://app"
  }
`), map[string]string{
		"auth.hcl":   authHCL,
		"public.sql": "CREATE TABLE posts (id int PRIMARY KEY);\n",
		"app/a.sql":  "CREATE TABLE app.a (id int PRIMARY KEY);\n",
	})

	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)

	c.Assert(err, qt.IsNil)
	c.Assert(set.Kind, qt.Equals, atlassource.KindCompositeSchema)
	c.Assert(set.Sources, qt.HasLen, 1)
	parts := set.Sources[0].Composite
	c.Assert(parts, qt.HasLen, 3)
	c.Assert(parts[0].Schema, qt.Equals, "auth")
	c.Assert(parts[0].Source.Kind, qt.Equals, atlassource.KindLocalFile)
	c.Assert(filepath.Base(parts[0].Source.Path), qt.Equals, "auth.hcl")
	c.Assert(parts[1].Schema, qt.Equals, "")
	c.Assert(filepath.Base(parts[1].Source.Path), qt.Equals, "public.sql")
	c.Assert(parts[2].Schema, qt.Equals, "app")
	c.Assert(filepath.Base(parts[2].Source.Path), qt.Equals, "app")
	c.Assert(parts[2].Location, qt.Matches, `.*atlas\.hcl:8`)
}

// TestClassifySetCompositeSchema_FailurePath refuses a part Ptah cannot read
// without a database, and the marker spelled on a flag.
func TestClassifySetCompositeSchema_FailurePath(t *testing.T) {
	t.Run("a database URL part", func(t *testing.T) {
		c := qt.New(t)
		env := compositeProject(c, compositeHCL(`  schema "public" {
    url = "postgres://localhost:5432/app"
  }
`), nil)

		set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)

		c.Assert(err, qt.ErrorMatches, `--to "env://src": atlas\.hcl schema source: data\.composite_schema "project" `+
			`schema "public" at .*atlas\.hcl:2: "postgres://localhost:5432/app" is a database URL, which cannot be a `+
			`composite part; a part is a schema file or directory, or a data\.hcl_schema, data\.external_schema or `+
			`data\.remote_schema value`)
		c.Assert(set.Sources, qt.IsNil)
	})

	t.Run("a migration directory part", func(t *testing.T) {
		c := qt.New(t)
		env := compositeProject(c, compositeHCL(`  schema {
    url = "file://migrations"
  }
`), map[string]string{"migrations/atlas.sum": "h1:x=\n"})

		set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)

		c.Assert(err, qt.ErrorMatches, `.*unlabeled schema block at .*atlas\.hcl:2: "file://migrations" is a `+
			`migration directory, which cannot be a composite part.*`)
		c.Assert(set.Sources, qt.IsNil)
	})

	t.Run("an external schema part without the opt-in", func(t *testing.T) {
		envbooltest.Unset(atlassource.AllowExternalSchemaEnvVar)(t)
		c := qt.New(t)
		env := compositeProject(c, `data "external_schema" "orm" {
  program = ["./export.sh"]
}

`+compositeHCL(`  schema "public" {
    url = data.external_schema.orm.url
  }
`), nil)

		set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)

		c.Assert(err, qt.ErrorMatches, `.*schema "public" at .*: atlas\.hcl data\.external_schema executes a `+
			`repository-controlled program and is disabled by default; set PTAH_ALLOW_EXTERNAL_SCHEMA=1 to allow it`)
		c.Assert(set.Sources, qt.IsNil)
	})

	t.Run("the marker on a flag", func(t *testing.T) {
		c := qt.New(t)

		set, err := atlassource.ClassifySet("--to", []string{projectconfig.CompositeSchemaMarkerScheme + "://project"},
			atlassource.ProjectEnv{})

		c.Assert(err, qt.ErrorMatches, `--to "ptah-composite-schema://project": ptah-composite-schema:// is a `+
			`reserved internal marker scheme; reference data\.composite_schema\.<name>\.url from an atlas\.hcl env `+
			`src instead`)
		c.Assert(set.Sources, qt.IsNil)
	})
}

// TestSetValidateLocalSchemaSources_ReachesCompositeParts holds a composition's
// local parts to the policy every other local source meets, so a policy that
// refuses a format cannot be stepped around by wrapping the file in a part.
func TestSetValidateLocalSchemaSources_ReachesCompositeParts(t *testing.T) {
	c := qt.New(t)
	env := compositeProject(c, compositeHCL(`  schema "public" {
    url = "file://schema.sql"
  }
  schema "auth" {
    url = "file://auth.hcl"
  }
`), map[string]string{"schema.sql": "CREATE TABLE t (id int);\n", "auth.hcl": authHCL})
	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)
	c.Assert(err, qt.IsNil)
	var seen []string

	err = set.ValidateLocalSchemaSources(func(path string) error {
		seen = append(seen, filepath.Base(path))
		return nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(seen, qt.DeepEquals, []string{"schema.sql", "auth.hcl"})
}

// tableNames lists a schema's tables by qualified name, sorted.
func tableNames(db *schemamodel.Database) []string {
	names := make([]string, 0, len(db.Tables))
	for _, table := range db.Tables {
		names = append(names, table.QualifiedName())
	}
	slices.Sort(names)
	return names
}

// schemaNames lists the schemas a desired state declares, sorted.
func schemaNames(db *schemamodel.Database) []string {
	names := make([]string, 0, len(db.Schemas))
	for _, schema := range db.Schemas {
		names = append(names, schema.Name)
	}
	slices.Sort(names)
	return names
}

// TestResolveCompositeSchema_HappyPath loads an HCL part in `auth` and a SQL
// part in `public` whose table references the first, and merges them. The SQL
// part does not create its schema, and the composition declares it, as Atlas's
// documentation says a labeled part does.
func TestResolveCompositeSchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	env := compositeProject(c, compositeHCL(`  schema "auth" {
    url = "file://auth.hcl"
  }
  schema "public" {
    url = "file://public.sql"
  }
`), map[string]string{
		"auth.hcl":   authHCL,
		"public.sql": "CREATE TABLE posts (id int PRIMARY KEY, author_id int REFERENCES auth.users(id));\n",
	})
	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)
	c.Assert(err, qt.IsNil)

	state, err := set.Resolve(c.Context(), atlassource.ResolveOptions{Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(state.Kind, qt.Equals, atlassource.KindCompositeSchema)
	c.Assert(tableNames(state.Schema), qt.DeepEquals, []string{"auth.users", "posts"})
	c.Assert(schemaNames(state.Schema), qt.DeepEquals, []string{"auth", "public"})
}

// TestResolveCompositeSchema_FailurePath names the part a refusal came from.
func TestResolveCompositeSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		parts   string
		files   map[string]string
		wantErr string
	}{
		{
			name: "an unqualified table in a part labeled with another schema",
			parts: `  schema "auth" {
    url = "file://auth.sql"
  }
`,
			files: map[string]string{"auth.sql": "CREATE TABLE sessions (id int PRIMARY KEY);\n"},
			wantErr: `--to composite schema "auth" at .*atlas\.hcl:2: table "sessions" is not in schema "auth": ` +
				`an unqualified name belongs to the run's default schema, and Ptah does not move it; qualify it ` +
				`as auth\.sessions`,
		},
		{
			name: "a table in another schema",
			parts: `  schema "auth" {
    url = "file://auth.sql"
  }
`,
			files:   map[string]string{"auth.sql": "CREATE TABLE billing.invoices (id int PRIMARY KEY);\n"},
			wantErr: `--to composite schema "auth" at .*: table "invoices" is in schema "billing", outside schema "auth"`,
		},
		{
			name: "two parts declaring one table",
			parts: `  schema "public" {
    url = "file://a.sql"
  }
  schema "public" {
    url = "file://b.sql"
  }
`,
			files: map[string]string{
				"a.sql": "CREATE TABLE t (id int PRIMARY KEY);\n",
				"b.sql": "CREATE TABLE t (id bigint PRIMARY KEY);\n",
			},
			wantErr: `--to "ptah-composite-schema://project": merging the composite schema: .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			env := compositeProject(c, compositeHCL(test.parts), test.files)
			set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)
			c.Assert(err, qt.IsNil)

			state, err := set.Resolve(c.Context(), atlassource.ResolveOptions{Dialect: "postgres"})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(state.Schema, qt.IsNil)
		})
	}
}

// TestPlaceCompositePart_HappyPath accepts a part whose objects all lie in its
// schema, and declares the schema when the part did not.
func TestPlaceCompositePart_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		part          *schemamodel.Database
		schema        string
		defaultSchema string
		wantSchemas   []string
	}{
		{
			name:          "an unlabeled part is returned as it is",
			part:          &schemamodel.Database{Tables: []schemamodel.Table{{Name: "t", Schema: "other"}}},
			schema:        "",
			defaultSchema: "public",
			wantSchemas:   make([]string, 0),
		},
		{
			name: "qualified objects in the label's schema",
			part: &schemamodel.Database{
				Tables:    []schemamodel.Table{{StructName: "Users", Name: "users", Schema: "auth"}},
				Views:     []schemamodel.View{{Name: "auth.active"}},
				Functions: []schemamodel.Function{{Name: "auth.uid"}},
				Enums:     []schemamodel.Enum{{Name: "role", Schema: "auth"}},
				Sequences: []schemamodel.Sequence{{Name: "ids", Schema: "auth"}},
			},
			schema:        "auth",
			defaultSchema: "public",
			wantSchemas:   []string{"auth"},
		},
		{
			name: "unqualified objects when the label is the run's default schema",
			part: &schemamodel.Database{
				Tables:  []schemamodel.Table{{StructName: "Posts", Name: "posts"}},
				Domains: []schemamodel.Domain{{Name: "email"}},
			},
			schema:        "public",
			defaultSchema: "public",
			wantSchemas:   []string{"public"},
		},
		{
			name: "a schema the part already declares is not declared twice",
			part: &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "auth", Comment: "kept"}},
				Tables:  []schemamodel.Table{{StructName: "Users", Name: "users", Schema: "auth"}},
			},
			schema:        "auth",
			defaultSchema: "public",
			wantSchemas:   []string{"auth"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := len(test.part.Schemas)

			placed, err := atlassource.PlaceCompositePart(test.part, test.schema, test.defaultSchema)

			c.Assert(err, qt.IsNil)
			c.Assert(schemaNames(placed), qt.DeepEquals, test.wantSchemas)
			c.Assert(placed.Tables, qt.DeepEquals, test.part.Tables)
			c.Assert(test.part.Schemas, qt.HasLen, before)
		})
	}
}

// TestPlaceCompositePart_FailurePath refuses each family whose object lies
// outside the label's schema, naming the object.
func TestPlaceCompositePart_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		part    *schemamodel.Database
		wantErr string
	}{
		{
			name:    "a declared schema",
			part:    &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "billing"}}},
			wantErr: `the part declares schema "billing", outside schema "auth"`,
		},
		{
			name:    "a view",
			part:    &schemamodel.Database{Views: []schemamodel.View{{Name: "billing.open"}}},
			wantErr: `view "open" is in schema "billing", outside schema "auth"`,
		},
		{
			name:    "a materialized view",
			part:    &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "billing.totals"}}},
			wantErr: `materialized view "totals" is in schema "billing", outside schema "auth"`,
		},
		{
			name: "an unqualified function",
			part: &schemamodel.Database{Functions: []schemamodel.Function{{Name: "uid"}}},
			wantErr: `function "uid" is not in schema "auth": an unqualified name belongs to the run's default ` +
				`schema, and Ptah does not move it; qualify it as auth\.uid`,
		},
		{
			name:    "an enum",
			part:    &schemamodel.Database{Enums: []schemamodel.Enum{{Name: "state", Schema: "billing"}}},
			wantErr: `enum "state" is in schema "billing", outside schema "auth"`,
		},
		{
			name:    "a domain",
			part:    &schemamodel.Database{Domains: []schemamodel.Domain{{Name: "money", Schema: "billing"}}},
			wantErr: `domain "money" is in schema "billing", outside schema "auth"`,
		},
		{
			name:    "a composite type",
			part:    &schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{{Name: "addr", Schema: "billing"}}},
			wantErr: `composite type "addr" is in schema "billing", outside schema "auth"`,
		},
		{
			name:    "a range type",
			part:    &schemamodel.Database{Ranges: []schemamodel.Range{{Name: "span", Schema: "billing"}}},
			wantErr: `range type "span" is in schema "billing", outside schema "auth"`,
		},
		{
			name:    "a sequence",
			part:    &schemamodel.Database{Sequences: []schemamodel.Sequence{{Name: "ids", Schema: "billing"}}},
			wantErr: `sequence "ids" is in schema "billing", outside schema "auth"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			placed, err := atlassource.PlaceCompositePart(test.part, "auth", "public")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(placed, qt.IsNil)
		})
	}
}

// TestResolveCompositeSchema_CarriesHCLSchemaVars loads a part minted by
// `data "hcl_schema"`, whose file declares a variable only that block's vars
// supply. The value reaches the file through the composition; without the
// scope the load refuses the missing value.
func TestResolveCompositeSchema_CarriesHCLSchemaVars(t *testing.T) {
	c := qt.New(t)
	env := compositeProject(c, `data "hcl_schema" "auth" {
  path = "auth.hcl"
  vars = {
    tenant = "acme"
  }
}

`+compositeHCL(`  schema "auth" {
    url = data.hcl_schema.auth.url
  }
`), map[string]string{"auth.hcl": `variable "tenant" {
  type = string
}

schema "auth" {}

table "users" {
  schema  = schema.auth
  comment = var.tenant
  column "id" {
    type = int
  }
}
`})
	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)
	c.Assert(err, qt.IsNil)

	state, err := set.Resolve(c.Context(), atlassource.ResolveOptions{Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(state.Schema.Tables, qt.HasLen, 1)
	c.Assert(state.Schema.Tables[0].Comment, qt.Equals, "acme")
}
