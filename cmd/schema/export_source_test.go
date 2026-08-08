package schema_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// The four spellings of one desired schema. They declare the same two tables,
// the same foreign key, and the same named enum, so an export taken from any of
// them describes the same contract and must produce the same bytes. The enum and
// the foreign key are what make the comparison worth making: they are the two
// places where an API export invents names of its own.
const (
	gridGoModels = `package models

//ptah:schema:table name="categories"
type Category struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(120)" not_null="true"
	Name string
}

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//ptah:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price float64

	//ptah:schema:field name="description" type="TEXT"
	Description string

	//ptah:schema:field name="category_id" type="INTEGER" not_null="true" foreign="categories(id)"
	CategoryID int64

	//ptah:schema:field name="status" type="product_status" not_null="true"
	Status string
}

//ptah:schema:enum name="product_status" values="active,inactive"
type ProductStatus struct{}
`

	gridYAMLSchema = `tables:
  categories:
    columns:
      id:
        type: SERIAL
        primary: true
      name:
        type: VARCHAR(120)
        not_null: true
  products:
    columns:
      id:
        type: SERIAL
        primary: true
      name:
        type: VARCHAR(255)
        not_null: true
      price:
        type: DECIMAL(10,2)
        not_null: true
      description:
        type: TEXT
      category_id:
        type: INTEGER
        not_null: true
        foreign: categories(id)
      status:
        type: product_status
        not_null: true
enums:
  product_status:
    values: [active, inactive]
`

	gridHCLSchema = `schema "public" {}

enum "product_status" {
  values = ["active", "inactive"]
}

table "categories" {
  schema = schema.public

  column "id" {
    type = serial
  }

  column "name" {
    type = varchar(120)
    null = false
  }

  primary_key {
    columns = [column.id]
  }
}

table "products" {
  schema = schema.public

  column "id" {
    type = serial
  }

  column "name" {
    type = varchar(255)
    null = false
  }

  column "price" {
    type = decimal(10, 2)
    null = false
  }

  column "description" {
    type = text
    null = true
  }

  column "category_id" {
    type = integer
    null = false
  }

  column "status" {
    type = product_status
    null = false
  }

  primary_key {
    columns = [column.id]
  }

  foreign_key "products_category_id_fkey" {
    columns     = [column.category_id]
    ref_columns = [table.categories.column.id]
  }
}
`

	gridSQLSchema = `CREATE TYPE product_status AS ENUM ('active', 'inactive');

CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(120) NOT NULL
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    description TEXT,
    category_id INTEGER NOT NULL REFERENCES categories(id),
    status product_status NOT NULL
);
`

	// strayGoModel is annotated Go in a directory nobody named as a source.
	strayGoModel = `package stray

//ptah:schema:table name="audit_log"
type AuditLog struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="actor" type="VARCHAR(64)" not_null="true"
	Actor string
}
`
)

// gridFixture writes the four source spellings into one directory and returns
// it. The Go models live in a "models" subdirectory so a schema-file export can
// be shown not to pick them up.
func gridFixture(c *qt.C) string {
	c.Helper()
	dir := resolvedTempDir(c)
	modelsDir := filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "model.go"), []byte(gridGoModels), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.yaml"), []byte(gridYAMLSchema), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.hcl"), []byte(gridHCLSchema), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(gridSQLSchema), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not a schema\n"), 0o600), qt.IsNil)
	return dir
}

// exportTarget is one API export target: how to run it, and a token its output
// must carry. Without the token a cell could pass by comparing one empty
// artifact with another.
type exportTarget struct {
	name   string
	marker string
	run    func(c *qt.C, sourceArgs []string) string
}

func apiExportTargets() []exportTarget {
	return []exportTarget{
		{name: "openapi-v3", marker: "        - inactive\n", run: stdoutExport("openapi-v3")},
		{name: "graphql", marker: "enum ProductStatus {\n", run: stdoutExport("graphql")},
		{name: "protobuf", marker: "PRODUCT_STATUS_INACTIVE = 2;\n", run: protobufExport},
	}
}

// stdoutExport runs a stateless target with --out omitted, which writes the
// schema itself to stdout.
func stdoutExport(target string) func(c *qt.C, sourceArgs []string) string {
	return func(c *qt.C, sourceArgs []string) string {
		c.Helper()
		stdout, stderr, err := runSchemaExport(append([]string{"--to", target}, sourceArgs...)...)
		c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
		return stdout
	}
}

// protobufExport runs the stateful target, which requires --out. Every run gets
// its own output directory so no run reads another run's compatibility state.
func protobufExport(c *qt.C, sourceArgs []string) string {
	c.Helper()
	outPath := filepath.Join(c.TempDir(), "schema.proto")
	args := append([]string{
		"--to", "protobuf",
		"--out", outPath,
		"--proto-package", "acme.inventory.v1",
	}, sourceArgs...)

	stdout, stderr, err := runSchemaExport(args...)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	data, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	return string(data)
}

// schemaSource is one non-Go desired-schema source: the --from value that names
// its format, and the file that carries it.
type schemaSource struct {
	name string
	path string
}

func nonGoSchemaSources(dir string) []schemaSource {
	return []schemaSource{
		{name: "yaml", path: filepath.Join(dir, "schema.yaml")},
		{name: "hcl", path: filepath.Join(dir, "schema.hcl")},
		{name: "sql", path: filepath.Join(dir, "schema.sql")},
	}
}

// TestSchemaExportReadsEveryNonGoSource walks the whole grid of API export
// targets against non-Go desired-schema sources. Each cell is checked in both
// spellings: --schema-file alone, where the extension names the format, and an
// explicit --from beside it.
func TestSchemaExportReadsEveryNonGoSource(t *testing.T) {
	c := qt.New(t)
	dir := gridFixture(c)
	goSource := []string{"--root-dir", filepath.Join(dir, "models")}

	for _, target := range apiExportTargets() {
		for _, source := range nonGoSchemaSources(dir) {
			c.Run(target.name+"/"+source.name, func(c *qt.C) {
				baseline := target.run(c, goSource)
				c.Assert(baseline, qt.Contains, target.marker)

				inferred := target.run(c, []string{"--schema-file", source.path})
				declared := target.run(c, []string{"--from", source.name, "--schema-file", source.path})

				c.Assert(inferred, qt.Equals, baseline)
				c.Assert(declared, qt.Equals, baseline)
			})
		}
	}
}

// TestSchemaExportAcceptsTheYamlExtensionAlias covers .yml, which schemaload
// reads as YAML. An explicit --from yaml has to agree with it, or the alias
// would be readable only by leaving --from unset.
func TestSchemaExportAcceptsTheYamlExtensionAlias(t *testing.T) {
	c := qt.New(t)
	dir := gridFixture(c)
	aliasPath := filepath.Join(dir, "schema.yml")
	c.Assert(os.WriteFile(aliasPath, []byte(gridYAMLSchema), 0o600), qt.IsNil)
	target := apiExportTargets()[1]

	baseline := target.run(c, []string{"--root-dir", filepath.Join(dir, "models")})
	alias := target.run(c, []string{"--from", "yaml", "--schema-file", aliasPath})

	c.Assert(alias, qt.Equals, baseline)
}

// TestSchemaExportComposesGoRootsWithSchemaFiles pins the composite case: an
// explicit --root-dir beside --schema-file exports both, as it does on
// "ptah schema render", which shares this resolver.
func TestSchemaExportComposesGoRootsWithSchemaFiles(t *testing.T) {
	c := qt.New(t)
	dir := gridFixture(c)
	strayDir := filepath.Join(dir, "stray")
	c.Assert(os.MkdirAll(strayDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(strayDir, "stray.go"), []byte(strayGoModel), 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaExport(
		"--to", "graphql",
		"--root-dir", strayDir,
		"--schema-file", filepath.Join(dir, "schema.yaml"),
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "type Product {")
	c.Assert(stdout, qt.Contains, "type AuditLog {")
}

// TestSchemaExportSchemaFileDoesNotAddTheWorkingDirectory holds the --root-dir
// default to Go-only exports. The flag defaults to ".", and passing that default
// to the resolver alongside --schema-file would merge whatever annotated Go
// files happen to sit in the working directory into the published contract.
func TestSchemaExportSchemaFileDoesNotAddTheWorkingDirectory(t *testing.T) {
	c := qt.New(t)
	dir := gridFixture(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "stray.go"), []byte(strayGoModel), 0o600), qt.IsNil)
	t.Chdir(dir)

	stdout, stderr, err := runSchemaExport("--to", "graphql", "--schema-file", "schema.yaml")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "type Product {")
	c.Assert(stdout, qt.Not(qt.Contains), "AuditLog")
}

// TestSchemaExportRefusesSourcesItCannotRead covers every refusal on the source
// selection: the one source deliberately out of scope, a format nothing reads,
// and each way a declared --from can disagree with the file beside it.
func TestSchemaExportRefusesSourcesItCannotRead(t *testing.T) {
	c := qt.New(t)
	dir := gridFixture(c)
	yamlPath := filepath.Join(dir, "schema.yaml")
	sqlPath := filepath.Join(dir, "schema.sql")
	notesPath := filepath.Join(dir, "notes.txt")

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name: "a live database",
			args: []string{"--to", "graphql", "--from", "db"},
			wantErr: `--from db is not supported: an export reads a schema definition, not a live database; ` +
				`run "ptah introspect" to generate annotated Go models from a database URL and export those`,
		},
		{
			name:    "a format nothing reads",
			args:    []string{"--to", "graphql", "--from", "toml"},
			wantErr: `unsupported --from "toml": expected go, yaml, hcl, or sql`,
		},
		{
			name:    "a declared format with no file",
			args:    []string{"--to", "graphql", "--from", "yaml", "--root-dir", filepath.Join(dir, "models")},
			wantErr: `--from yaml requires --schema-file`,
		},
		{
			name:    "a declared format the file contradicts",
			args:    []string{"--to", "graphql", "--from", "yaml", "--schema-file", sqlPath},
			wantErr: fmt.Sprintf(`--from yaml does not match --schema-file %q, which is sql`, sqlPath),
		},
		{
			name: "a declared format on a file with no schema extension",
			args: []string{"--to", "graphql", "--from", "yaml", "--schema-file", notesPath},
			wantErr: fmt.Sprintf(
				`--schema-file %q has no recognized schema file extension: expected .yaml, .yml, .hcl, or .sql`,
				notesPath,
			),
		},
		{
			name: "a declared format on a registry artifact",
			args: []string{"--to", "graphql", "--from", "yaml", "--schema-file", "oci://ghcr.io/acme/app-schema:v1"},
			wantErr: `--from cannot declare the format of the oci:// artifact "oci://ghcr.io/acme/app-schema:v1", ` +
				`which records its own; omit --from`,
		},
		{
			name: "an inferred format on a file with no schema extension",
			args: []string{"--to", "graphql", "--schema-file", notesPath},
			wantErr: `unsupported schema file extension ".txt": ` +
				`only .yaml, .yml, .hcl, and .sql are supported`,
		},
		{
			name: "Go annotations and a schema file at once",
			args: []string{"--to", "graphql", "--from", "go", "--schema-file", yamlPath},
			wantErr: `--from go reads Go annotations from --root-dir: ` +
				`drop --from, or set it to the format of the --schema-file value`,
		},
		{
			// --to defaults to hcl, so a user who names a schema file and forgets
			// --to lands here. The message has to name the way out.
			name: "a schema file with no target named",
			args: []string{"--out", filepath.Join(dir, "out.hcl"), "--schema-file", yamlPath},
			wantErr: `--schema-file is not supported with --to hcl: that target rewrites the Go files it reads ` +
				`(--cleanup-go-annotations removes their annotations), so its source is --root-dir; ` +
				`use --to openapi-v3, graphql, or protobuf to export a schema file`,
		},
		{
			name: "a schema file for the target that rewrites its source",
			args: []string{"--to", "hcl", "--out", filepath.Join(dir, "out.hcl"), "--schema-file", yamlPath},
			wantErr: `--schema-file is not supported with --to hcl: that target rewrites the Go files it reads ` +
				`(--cleanup-go-annotations removes their annotations), so its source is --root-dir; ` +
				`use --to openapi-v3, graphql, or protobuf to export a schema file`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			stdout, stderr, err := runSchemaExport(tt.args...)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(tt.wantErr), qt.Commentf("stdout:\n%s", stdout))
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error: "+tt.wantErr+"\n")
			// A refused source must not have produced a schema either.
			c.Assert(stdout, qt.Equals, "")
		})
	}
}
