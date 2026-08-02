//go:build unix

package goannotationexport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/goannotationexport"
)

func TestExport_FailurePath_OutputSymlinkAliasCannotOverwriteGoSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	source := writeSimpleModel(c, root)
	output := filepath.Join(outside, "schema.hcl")
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Symlink(source, output), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrOutputAliasesSource)
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	info, err := os.Lstat(output)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode()&os.ModeSymlink, qt.Equals, os.ModeSymlink)
}

func TestExport_FailurePath_OutputHardLinkAliasCannotOverwriteGoSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	source := writeSimpleModel(c, root)
	output := filepath.Join(outside, "schema.hcl")
	sourceData, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Link(source, output), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrOutputAliasesSource)
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	assertFileBytes(c, output, sourceData)
}

func TestExport_FailurePath_OutputHardLinkAliasCannotOverwriteManagedData(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	source := filepath.Join(root, "model.go")
	dataPath := filepath.Join(root, "countries.yaml")
	output := filepath.Join(outside, "schema.hcl")
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
	c.Assert(os.Link(dataPath, output), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
	})

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrOutputAliasesManagedData)
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	assertFileBytes(c, dataPath, data)
	assertFileBytes(c, output, data)
}

func TestExport_FailurePath_OutputSymlinkAliasCannotOverwriteManagedData(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	source := filepath.Join(root, "model.go")
	dataPath := filepath.Join(root, "countries.yaml")
	output := filepath.Join(outside, "schema.hcl")
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
	c.Assert(os.Symlink(dataPath, output), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
	})

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrOutputAliasesManagedData)
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	assertFileBytes(c, dataPath, data)
	info, err := os.Lstat(output)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode()&os.ModeSymlink, qt.Equals, os.ModeSymlink)
}
