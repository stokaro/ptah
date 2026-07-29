package goannotationexport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/atlashcl"
	"github.com/stokaro/ptah/internal/goannotationexport"
)

func TestExport_HappyPath_WritesValidatedHCLAndCleansAnnotations(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeSimpleModel(c, root)
	output := filepath.Join(root, "schema.hcl")
	c.Assert(os.WriteFile(output, []byte("old schema\n"), 0o600), qt.IsNil)
	c.Assert(os.Chmod(output, 0o640), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})

	c.Assert(err, qt.IsNil)
	resolvedOutput, err := filepath.EvalSymlinks(output)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OutputPath, qt.Equals, resolvedOutput)
	c.Assert(result.Tables, qt.Equals, 1)
	c.Assert(result.Fields, qt.Equals, 1)
	c.Assert(result.Enums, qt.Equals, 0)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Cleanup, qt.HasLen, 1)
	c.Assert(result.Cleanup[0].RemovedLines, qt.Equals, 2)
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	c.Assert(string(sourceData), qt.Equals, "package models\n\ntype User struct {\n\tID int64\n}\n")
	outputData, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(outputData, output)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Tables, qt.HasLen, 1)
	c.Assert(parsed.Fields, qt.HasLen, 1)
	info, err := os.Stat(output)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o640))
}

func TestExport_HappyPath_TightensPermissionsForRolePasswords(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "model.go")
	output := filepath.Join(root, "schema.hcl")
	sourceData := []byte(`package models

//migrator:schema:role name="app_user" login="true" password="SCRAM-SHA-256$fixture"
	type AppRole struct{}
`)
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(output, []byte("old schema\n"), 0o600), qt.IsNil)
	c.Assert(os.Chmod(output, 0o644), qt.IsNil)

	_, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
	})

	c.Assert(err, qt.IsNil)
	info, err := os.Stat(output)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o600))
	content, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, `password = "SCRAM-SHA-256$fixture"`)
}

func TestExport_HappyPath_DiffWritesOutputWithoutChangingSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeSimpleModel(c, root)
	output := filepath.Join(root, "schema.hcl")
	original, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
		Diff:       true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Cleanup, qt.HasLen, 1)
	c.Assert(result.Cleanup[0].Diff, qt.Contains, "-//migrator:schema:table")
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	c.Assert(sourceData, qt.DeepEquals, original)
	outputData, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	c.Assert(string(outputData), qt.Contains, `table "users"`)
}

func TestExport_HappyPath_CleanupModesReportTheSamePlan(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeSimpleModel(c, root)
	output := filepath.Join(root, "schema.hcl")
	original, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)

	dryRun, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
		DryRun:     true,
	})
	c.Assert(err, qt.IsNil)
	assertFileBytes(c, source, original)

	diff, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
		Diff:       true,
	})
	c.Assert(err, qt.IsNil)
	assertFileBytes(c, source, original)

	write, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})
	c.Assert(err, qt.IsNil)

	c.Assert(dryRun.Cleanup, qt.HasLen, 1)
	c.Assert(diff.Cleanup, qt.HasLen, 1)
	c.Assert(write.Cleanup, qt.HasLen, 1)
	c.Assert(dryRun.Cleanup[0].Path, qt.Equals, diff.Cleanup[0].Path)
	c.Assert(dryRun.Cleanup[0].Path, qt.Equals, write.Cleanup[0].Path)
	c.Assert(dryRun.Cleanup[0].RemovedLines, qt.Equals, diff.Cleanup[0].RemovedLines)
	c.Assert(dryRun.Cleanup[0].RemovedLines, qt.Equals, write.Cleanup[0].RemovedLines)
	c.Assert(dryRun.Cleanup[0].Diff, qt.Equals, "")
	c.Assert(diff.Cleanup[0].Diff, qt.Contains, "-//migrator:schema:table")
	c.Assert(write.Cleanup[0].Diff, qt.Equals, "")
}

func TestExport_HappyPath_IsDeterministicAcrossRepeatedExports(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	writeSimpleModel(c, root)
	output := filepath.Join(root, "schema.hcl")

	_, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
	})
	c.Assert(err, qt.IsNil)
	first, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)

	_, err = goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
	})
	c.Assert(err, qt.IsNil)
	second, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)

	c.Assert(second, qt.DeepEquals, first)
}

func TestExport_HappyPath_RebasesManagedDataRelativeToHCL(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	sourceDir := filepath.Join(root, "models")
	c.Assert(os.MkdirAll(sourceDir, 0o755), qt.IsNil)
	source := filepath.Join(sourceDir, "model.go")
	sourceData := []byte(`package models

//migrator:schema:table name="countries"
//migrator:schema:data table="countries" key="code" file="countries.yaml"
type Country struct {
	//migrator:schema:field name="code" type="TEXT" primary="true"
	Code string
}
`)
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(sourceDir, "countries.yaml"),
		[]byte("- code: CZ\n"),
		0o600,
	), qt.IsNil)
	output := filepath.Join(root, "schema", "schema.hcl")

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	outputData, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(outputData, output)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.ManagedData, qt.HasLen, 1)
	c.Assert(parsed.ManagedData[0].File, qt.Equals, "../models/countries.yaml")
	rows, err := goschema.LoadManagedRows("", parsed.ManagedData[0])
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.DeepEquals, []map[string]any{{"code": "CZ"}})
}

func TestExport_HappyPath_NonDestructiveExportPreservesCustomSQL(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeCustomModel(c, root)
	output := filepath.Join(root, "schema.hcl")
	original, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	c.Assert(sourceData, qt.DeepEquals, original)
	outputData, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(outputData, output)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Tables, qt.HasLen, 1)
	c.Assert(parsed.Tables[0].CustomSQL, qt.Equals, "WITHOUT OIDS")
}

func TestExport_FailurePath_RepeatCleanupPreservesExistingOutput(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "model.go")
	output := filepath.Join(root, "schema.hcl")
	sourceData := []byte("package models\n\ntype User struct{}\n")
	outputData := []byte("previous useful schema\n")
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(output, outputData, 0o600), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrNoAnnotations)
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	assertFileBytes(c, output, outputData)
}

func TestExport_HappyPath_CleanupPreservesCustomSQL(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeCustomModel(c, root)
	output := filepath.Join(root, "schema.hcl")

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	c.Assert(string(sourceData), qt.Equals, "package models\n\ntype User struct {\n\tID int64\n}\n")
	outputData, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(outputData, output)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Tables[0].CustomSQL, qt.Equals, "WITHOUT OIDS")
}

func TestExport_HappyPath_CleanupPreservesMultiwordSQLType(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "model.go")
	output := filepath.Join(root, "schema.hcl")
	sourceData := []byte(`package models

//migrator:schema:table name="measurements"
type Measurement struct {
	//migrator:schema:field name="value" type="DOUBLE PRECISION"
	Value float64
}
`)
	outputData := []byte("previous schema\n")
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(output, outputData, 0o600), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	sourceAfter, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	c.Assert(string(sourceAfter), qt.Equals, "package models\n\ntype Measurement struct {\n\tValue float64\n}\n")
	outputAfter, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(outputAfter, output)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Fields, qt.HasLen, 1)
	c.Assert(parsed.Fields[0].Type, qt.Equals, "DOUBLE PRECISION")
}

func TestExport_FailurePath_OutputCannotAliasGoSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeSimpleModel(c, root)
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: source,
		Cleanup:    true,
	})

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrOutputAliasesSource)
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
}

func TestExport_FailurePath_OutputCannotAliasManagedData(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "model.go")
	dataPath := filepath.Join(root, "countries.yaml")
	sourceData := []byte(`package models

//migrator:schema:table name="countries"
//migrator:schema:data table="countries" key="code" file="countries.yaml"
type Country struct {
	//migrator:schema:field name="code" type="TEXT" primary="true"
	Code string
}
`)
	data := []byte("- code: CZ\n")
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(dataPath, data, 0o600), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: dataPath,
	})

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrOutputAliasesManagedData)
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	assertFileBytes(c, dataPath, data)
}

func TestExport_FailurePath_OutputWriteFailurePreservesSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeSimpleModel(c, root)
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	parentFile := filepath.Join(root, "not-a-directory")
	c.Assert(os.WriteFile(parentFile, []byte("block directory creation"), 0o600), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: filepath.Join(parentFile, "schema.hcl"),
		Cleanup:    true,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "output")
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
}

func TestExport_FailurePath_RejectsRemovedIndexPlatformOverride(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "model.go")
	output := filepath.Join(root, "schema.hcl")
	sourceData := []byte(`package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="BIGINT"
	ID int64

	//migrator:schema:index name="idx_users_id" fields="id" platform.mysql.type="HASH"
	_ int
}
`)
	outputData := []byte("previous schema\n")
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(output, outputData, 0o600), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "platform.mysql.type")
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	assertFileBytes(c, output, outputData)
}

func TestExport_FailurePath_RejectsCleanupModesWithoutCleanup(t *testing.T) {
	c := qt.New(t)

	result, err := goannotationexport.Export(goannotationexport.Options{DryRun: true})

	c.Assert(err, qt.ErrorMatches, "cleanup dry-run and diff require cleanup")
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
}

func writeSimpleModel(c *qt.C, root string) string {
	path := filepath.Join(root, "model.go")
	data := []byte(`package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="BIGINT"
	ID int64
}
`)
	c.Assert(os.WriteFile(path, data, 0o600), qt.IsNil)
	return path
}

func writeCustomModel(c *qt.C, root string) string {
	path := filepath.Join(root, "model.go")
	data := []byte(`package models

//migrator:schema:table name="users" custom="WITHOUT OIDS"
type User struct {
	//migrator:schema:field name="id" type="BIGINT"
	ID int64
}
`)
	c.Assert(os.WriteFile(path, data, 0o600), qt.IsNil)
	return path
}

func assertFileBytes(c *qt.C, path string, want []byte) {
	c.Helper()
	got, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, want)
}
