//go:build unix

package goannotationexport

// White-box testing required: this file replaces the HCL output ancestor after
// staging to verify that publication aborts without reporting a stale output
// path or mutating either the selected or replacement directory.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestExport_FailurePath_AncestorSwapAbortsHCLPublication(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	source := filepath.Join(root, "model.go")
	outputDir := filepath.Join(root, "output")
	capturedDir := filepath.Join(root, "captured-output")
	outsideDir := c.TempDir()
	output := filepath.Join(outputDir, "schema.hcl")
	outsideOutput := filepath.Join(outsideDir, "schema.hcl")
	sourceData := []byte(
		"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
	)
	previousOutput := []byte("previous schema\n")
	outsideData := []byte("outside schema\n")
	c.Assert(os.MkdirAll(outputDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(output, previousOutput, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(outsideOutput, outsideData, 0o600), qt.IsNil)
	result, err := export(Options{
		RootDir:    root,
		OutputPath: output,
	}, exportHooks{afterOutputStage: func() {
		c.Assert(os.Rename(outputDir, capturedDir), qt.IsNil)
		c.Assert(os.Symlink(outsideDir, outputDir), qt.IsNil)
	}})

	c.Assert(err, qt.ErrorIs, ErrOutputChanged)
	c.Assert(result, qt.DeepEquals, Result{})
	assertExportInternalFileBytes(c.TB, filepath.Join(capturedDir, "schema.hcl"), previousOutput)
	assertExportInternalFileBytes(c.TB, outsideOutput, outsideData)
	assertExportInternalFileBytes(c.TB, source, sourceData)
	entries, err := os.ReadDir(capturedDir)
	c.Assert(err, qt.IsNil)
	c.Assert(exportInternalEntryNames(entries), qt.DeepEquals, []string{"schema.hcl"})
}
