package goannotationexport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

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

func TestExport_HappyPath_NonDestructiveExportReportsLoss(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeLossyModel(c, root)
	output := filepath.Join(root, "schema.hcl")
	original, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Message, qt.Contains, "custom SQL")
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	c.Assert(sourceData, qt.DeepEquals, original)
	outputData, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	c.Assert(string(outputData), qt.Contains, `table "users"`)
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

func TestExport_FailurePath_LossyCleanupPreservesOutputAndSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := writeLossyModel(c, root)
	output := filepath.Join(root, "schema.hcl")
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	outputData := []byte("previous schema\n")
	c.Assert(os.WriteFile(output, outputData, 0o600), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrLossyCleanup)
	c.Assert(err.Error(), qt.Contains, "custom SQL")
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	assertFileBytes(c, output, outputData)
}

func TestExport_FailurePath_UnstableHCLPreservesOutputAndSource(t *testing.T) {
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

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrInvalidHCL)
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	assertFileBytes(c, output, outputData)
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

func TestExport_FailurePath_UnretainedPlatformOverrideBlocksCleanup(t *testing.T) {
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

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrLossyCleanup)
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

func writeLossyModel(c *qt.C, root string) string {
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
