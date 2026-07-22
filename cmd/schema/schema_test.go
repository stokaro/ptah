package schema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/schema"
	"github.com/stokaro/ptah/internal/atlashcl"
)

func TestSchemaExportCommandWritesAtlasHCL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)
	outPath := filepath.Join(dir, "schema.hcl")

	cmd := schema.NewSchemaCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"export",
		"--from", "go",
		"--to", "atlas-hcl",
		"--root-dir", dir,
		"--out", outPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "Exported Atlas HCL schema")
	content, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, `table "users"`)
	c.Assert(string(content), qt.Contains, `column "created_at"`)
	c.Assert(string(content), qt.Contains, `primary_key {`)
	parsed, err := atlashcl.Parse(content, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("schema.hcl:\n%s", string(content)))
	c.Assert(parsed.Tables, qt.HasLen, 1)
	c.Assert(parsed.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})
}

func TestSchemaExportCommandWritesAPISchemas(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)

	for _, tc := range []struct{ format, contains string }{
		{"openapi-v3", "openapi: 3.0.3"},
		{"graphql", "type Query {"},
	} {
		cmd := schema.NewSchemaCommand()
		var stdout, stderr bytes.Buffer
		cmd.SetOut(&stdout)
		cmd.SetErr(&stderr)
		// No --out: the schema is written to stdout.
		cmd.SetArgs([]string{"export", "--to", tc.format, "--root-dir", dir})

		err := cmd.Execute()

		c.Assert(err, qt.IsNil, qt.Commentf("format %s stderr:\n%s", tc.format, stderr.String()))
		c.Assert(stdout.String(), qt.Contains, tc.contains)
	}
}

func TestSchemaExportCommandTrimsFormatSelector(t *testing.T) {
	// Regression: a whitespace-padded --to must route to the real format rather
	// than fall through routing (which previously could run annotation cleanup
	// without exporting, losing source data).
	c := qt.New(t)
	dir := t.TempDir()
	writeModel(c, dir)
	outPath := filepath.Join(dir, "schema.hcl")

	cmd := schema.NewSchemaCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"export", "--to", "atlas-hcl ", "--root-dir", dir, "--out", outPath})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	content, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, `table "users"`)
}

func TestSchemaExportCommandPreservesSchemaObjects(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fixtureDir, err := filepath.Abs("../../integration/fixtures/entities/023-go-annotations-objects")
	c.Assert(err, qt.IsNil)
	outPath := filepath.Join(dir, "schema.hcl")

	cmd := schema.NewSchemaCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"export",
		"--from", "go",
		"--to", "atlas-hcl",
		"--root-dir", fixtureDir,
		"--out", outPath,
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "2 export warning(s) reported")
	c.Assert(stderr.String(), qt.Contains, "extension if_not_exists")
	c.Assert(stderr.String(), qt.Contains, "RLS enablement comments cannot be represented")
	for _, oldObjectWarning := range []string{
		"extensions cannot be represented",
		"functions contain raw SQL bodies",
		"views contain raw SQL bodies",
		"materialized views contain raw SQL bodies",
		"triggers contain raw SQL bodies",
		"RLS policies cannot be represented",
		"RLS enablement cannot be represented",
		"roles cannot be represented",
		"grants cannot be represented",
	} {
		c.Assert(stderr.String(), qt.Not(qt.Contains), oldObjectWarning)
	}

	content, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(content, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("schema.hcl:\n%s", string(content)))
	c.Assert(parsed.Extensions, qt.HasLen, 1)
	c.Assert(parsed.Functions, qt.HasLen, 1)
	c.Assert(parsed.Views, qt.HasLen, 1)
	c.Assert(parsed.MaterializedViews, qt.HasLen, 1)
	c.Assert(parsed.Triggers, qt.HasLen, 1)
	c.Assert(parsed.RLSPolicies, qt.HasLen, 1)
	c.Assert(parsed.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(parsed.Roles, qt.HasLen, 1)
	c.Assert(parsed.Grants, qt.HasLen, 3)
}

func TestSchemaCommand_RegistersNativePaths(t *testing.T) {
	c := qt.New(t)

	cmd := schema.NewSchemaCommand()
	for _, path := range [][]string{
		{"annotations"},
		{"export"},
		{"render"},
		{"compare"},
		{"drift"},
	} {
		found, _, err := cmd.Find(path)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsNotNil)
	}
}

func TestSchemaAnnotationsCommandWritesJSONSchema(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	outPath := filepath.Join(dir, "migrator-annotations.schema.json")

	cmd := schema.NewSchemaCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"annotations",
		"--format", "json-schema",
		"--out", outPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "Exported annotation JSON Schema")
	content, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, `"migrator.schema.field"`)
	c.Assert(string(content), qt.Not(qt.Contains), `"defaul"`)
}

func TestSchemaCommand_RenderHelpShowsNativePath(t *testing.T) {
	c := qt.New(t)

	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"render", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Usage:\n  schema render [flags]")
	c.Assert(out.String(), qt.Not(qt.Contains), "Usage:\n  generate")
	c.Assert(out.String(), qt.Contains, "--dialect")
	c.Assert(out.String(), qt.Contains, "--schema-file")
}

func TestSchemaExportCleanupDryRunAndWrite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	modelPath := writeModel(c, dir)
	outPath := filepath.Join(dir, "schema.hcl")

	cmd := schema.NewSchemaCommand()
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"export",
		"--root-dir", dir,
		"--out", outPath,
		"--cleanup-go-annotations",
		"--cleanup-diff",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, "-//migrator:schema:table")
	content, err := os.ReadFile(modelPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, "migrator:schema")

	cmd = schema.NewSchemaCommand()
	stdout.Reset()
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{
		"export",
		"--root-dir", dir,
		"--out", outPath,
		"--cleanup-go-annotations",
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	content, err = os.ReadFile(modelPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Not(qt.Contains), "migrator:schema")
	c.Assert(string(content), qt.Not(qt.Contains), "migrator:embedded")
	c.Assert(string(content), qt.Contains, "// User is business documentation.")
}

func writeModel(c *qt.C, dir string) string {
	path := filepath.Join(dir, "model.go")
	content := `package models

type Timestamps struct {
	//migrator:schema:field name="created_at" type="TIMESTAMP" default_expr="CURRENT_TIMESTAMP"
	CreatedAt string
}

// User is business documentation.
//migrator:schema:table name="users"
type User struct {
	//migrator:embedded mode="inline"
	Timestamps

	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string
}
`
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}
