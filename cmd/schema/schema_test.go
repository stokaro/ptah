package schema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/schema"
	"github.com/stokaro/ptah/internal/atlashcl"
	"github.com/stokaro/ptah/internal/goannotationexport"
)

func TestSchemaExportCommandWritesHCL(t *testing.T) {
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
		"--to", "hcl",
		"--root-dir", dir,
		"--out", outPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "Exported HCL schema")
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

func TestSchemaExportCommandAcceptsLegacyHCLAlias(t *testing.T) {
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
	c.Assert(stdout.String(), qt.Contains, "Exported HCL schema")
	content, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, `table "users"`)
}

func TestSchemaExportHelpUsesNeutralHCLName(t *testing.T) {
	c := qt.New(t)
	cmd := schema.NewSchemaCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"export", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "Target schema format: hcl, openapi-v3, graphql, or protobuf")
	c.Assert(stdout.String(), qt.Contains, "ptah schema export --to hcl")
	c.Assert(stdout.String(), qt.Not(qt.Contains), "atlas-hcl, openapi-v3")
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
	cmd.SetArgs([]string{"export", "--to", "hcl ", "--root-dir", dir, "--out", outPath})

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
		"--to", "hcl",
		"--root-dir", fixtureDir,
		"--out", outPath,
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "4 export warning(s) reported")
	c.Assert(stderr.String(), qt.Equals, `warning: functions.get_fixture_tenant_id: raw SQL body is emitted as opaque HCL text and cannot be structurally interpreted; review it before treating the export as semantically complete
warning: materialized_views.user_stats: raw SQL body is emitted as opaque HCL text and cannot be structurally interpreted; review it before treating the export as semantically complete
warning: triggers["users"]["users_set_updated_at"]: raw SQL body is emitted as opaque HCL text and cannot be structurally interpreted; review it before treating the export as semantically complete
warning: views.active_users: raw SQL body is emitted as opaque HCL text and cannot be structurally interpreted; review it before treating the export as semantically complete
`)

	content, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(content, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("schema.hcl:\n%s", string(content)))
	c.Assert(parsed.Extensions, qt.HasLen, 1)
	c.Assert(parsed.Extensions[0].IfNotExists, qt.IsTrue)
	c.Assert(parsed.Sequences, qt.HasLen, 1)
	c.Assert(parsed.Domains, qt.HasLen, 1)
	c.Assert(parsed.CompositeTypes, qt.HasLen, 1)
	c.Assert(parsed.Ranges, qt.HasLen, 1)
	c.Assert(parsed.Functions, qt.HasLen, 1)
	c.Assert(parsed.Views, qt.HasLen, 1)
	c.Assert(parsed.MaterializedViews, qt.HasLen, 1)
	c.Assert(parsed.Triggers, qt.HasLen, 1)
	c.Assert(parsed.RLSPolicies, qt.HasLen, 1)
	c.Assert(parsed.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(parsed.RLSEnabledTables[0].Comment, qt.Equals, "Enable RLS for fixture users")
	c.Assert(parsed.Roles, qt.HasLen, 1)
	c.Assert(parsed.Grants, qt.HasLen, 4)
	c.Assert(parsed.Grants[1].OnSequence, qt.Equals, "fixture_order_seq")
	c.Assert(parsed.ManagedData, qt.HasLen, 1)
}

func TestSchemaCommand_RegistersNativePaths(t *testing.T) {
	c := qt.New(t)

	cmd := schema.NewSchemaCommand()
	for _, path := range [][]string{
		{"annotations"},
		{"export"},
		{"push"},
		{"pull"},
		{"render"},
		{"compare"},
		{"drift"},
		{"test"},
	} {
		found, _, err := cmd.Find(path)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsNotNil)
	}
}

func TestSchemaAnnotationsCommandWritesJSONSchema(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	outPath := filepath.Join(dir, "ptah-annotations.schema.json")

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
	c.Assert(string(content), qt.Contains, `"ptah.schema.field"`)
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
	c.Assert(stdout.String(), qt.Contains, "-//ptah:schema:table")
	content, err := os.ReadFile(modelPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, "ptah:schema")

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
	c.Assert(string(content), qt.Not(qt.Contains), "ptah:schema")
	c.Assert(string(content), qt.Not(qt.Contains), "ptah:embedded")
	c.Assert(string(content), qt.Contains, "// User is business documentation.")
}

func TestSchemaExportCommand_FailurePath_LossyCleanupPreservesSourcesAndOutput(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.go")
	outPath := filepath.Join(dir, "schema.hcl")
	modelData := []byte(`package models

//ptah:schema:rls:enable table="users"
type SecurityMarker struct{}
`)
	outData := []byte("previous schema\n")
	c.Assert(os.WriteFile(modelPath, modelData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(outPath, outData, 0o600), qt.IsNil)

	cmd := schema.NewSchemaCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"export",
		"--root-dir", dir,
		"--out", outPath,
		"--cleanup-go-annotations",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrLossyCleanup)
	c.Assert(stderr.String(), qt.Contains, "RLS enablement cannot be rendered because the target table is absent")
	modelAfter, err := os.ReadFile(modelPath)
	c.Assert(err, qt.IsNil)
	c.Assert(modelAfter, qt.DeepEquals, modelData)
	outAfter, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(outAfter, qt.DeepEquals, outData)
}

func TestSchemaExportCommand_FailurePath_RepeatCleanupPreservesExistingOutput(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.go")
	outPath := filepath.Join(dir, "schema.hcl")
	modelData := []byte("package models\n\ntype User struct{}\n")
	outData := []byte("previous useful schema\n")
	c.Assert(os.WriteFile(modelPath, modelData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(outPath, outData, 0o600), qt.IsNil)

	cmd := schema.NewSchemaCommand()
	cmd.SetArgs([]string{
		"export",
		"--root-dir", dir,
		"--out", outPath,
		"--cleanup-go-annotations",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrNoRemovableAnnotations)
	modelAfter, err := os.ReadFile(modelPath)
	c.Assert(err, qt.IsNil)
	c.Assert(modelAfter, qt.DeepEquals, modelData)
	outAfter, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(outAfter, qt.DeepEquals, outData)
}

func TestSchemaExportCommand_FailurePath_NoAnnotationsPreservesExistingOutput(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.go")
	outPath := filepath.Join(dir, "schema.hcl")
	modelData := []byte("package models\n\ntype User struct{}\n")
	outData := []byte("previous useful schema\n")
	c.Assert(os.WriteFile(modelPath, modelData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(outPath, outData, 0o600), qt.IsNil)

	cmd := schema.NewSchemaCommand()
	cmd.SetArgs([]string{
		"export",
		"--root-dir", dir,
		"--out", outPath,
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrNoAnnotations)
	modelAfter, err := os.ReadFile(modelPath)
	c.Assert(err, qt.IsNil)
	c.Assert(modelAfter, qt.DeepEquals, modelData)
	outAfter, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(outAfter, qt.DeepEquals, outData)
}

func writeModel(c *qt.C, dir string) string {
	path := filepath.Join(dir, "model.go")
	content := `package models

type Timestamps struct {
	//ptah:schema:field name="created_at" type="TIMESTAMP" default_expr="CURRENT_TIMESTAMP"
	CreatedAt string
}

// User is business documentation.
//ptah:schema:table name="users"
type User struct {
	//ptah:embedded mode="inline"
	Timestamps

	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string
}
`
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}
