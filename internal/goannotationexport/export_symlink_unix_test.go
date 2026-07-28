//go:build unix

package goannotationexport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/goannotationexport"
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
