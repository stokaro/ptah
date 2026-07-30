//go:build unix

package goannotationexport

// White-box testing required: this file changes output identity after HCL
// staging to verify the unexported managed-data alias publication barrier
// without timing-dependent filesystem polling.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestExport_FailurePath_RevalidatesManagedDataAliasAfterOutputStaging(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "model.go")
	dataPath := filepath.Join(root, "countries.yaml")
	output := filepath.Join(root, "schema.hcl")
	sourceData := []byte(`package models

//ptah:schema:table name="countries"
//ptah:schema:data table="countries" key="code" file="countries.yaml"
type Country struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string
}
`)
	data := []byte("- code: CZ\n")
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(dataPath, data, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(output, []byte("previous schema\n"), 0o600), qt.IsNil)

	result, err := export(Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	}, func() {
		c.Assert(os.Remove(output), qt.IsNil)
		c.Assert(os.Link(dataPath, output), qt.IsNil)
	})

	c.Assert(err, qt.ErrorIs, ErrOutputAliasesManagedData)
	c.Assert(result, qt.DeepEquals, Result{})
	assertExportInternalFileBytes(c, source, sourceData)
	assertExportInternalFileBytes(c, dataPath, data)
	assertExportInternalFileBytes(c, output, data)
	entries, err := os.ReadDir(root)
	c.Assert(err, qt.IsNil)
	c.Assert(exportInternalEntryNames(entries), qt.DeepEquals, []string{
		"countries.yaml",
		"model.go",
		"schema.hcl",
	})
}
