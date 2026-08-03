package goannotationexport

// White-box testing required: this file injects source mutations after HCL
// staging but before the unexported publication barrier, a timing boundary that
// public APIs cannot exercise deterministically without filesystem polling.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
	"go.5x5.cz/ptah/internal/goannotationsource"
)

func TestExport_FailurePath_RevalidatesSourcesAfterOutputStaging(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name           string
		mutate         func(c *qt.C, root, source string, original []byte)
		wantSource     []byte
		wantEntryNames []string
	}{
		{
			name: "same-size edit",
			mutate: func(c *qt.C, _ string, source string, _ []byte) {
				c.Assert(os.WriteFile(
					source,
					[]byte("package models\n\n//ptah:schema:table name=\"roles\"\ntype User struct{}\n"),
					0o600,
				), qt.IsNil)
			},
			wantSource: []byte(
				"package models\n\n//ptah:schema:table name=\"roles\"\ntype User struct{}\n",
			),
			wantEntryNames: []string{"model.go", "schema.hcl"},
		},
		{
			name: "identity replacement",
			mutate: func(c *qt.C, _ string, source string, original []byte) {
				replacement := source + ".replacement"
				c.Assert(os.WriteFile(replacement, original, 0o600), qt.IsNil)
				c.Assert(os.Remove(source), qt.IsNil)
				c.Assert(os.Rename(replacement, source), qt.IsNil)
			},
			wantSource: []byte(
				"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
			),
			wantEntryNames: []string{"model.go", "schema.hcl"},
		},
		{
			name: "source addition",
			mutate: func(c *qt.C, root, _ string, _ []byte) {
				c.Assert(os.WriteFile(
					filepath.Join(root, "added.go"),
					[]byte("package models\n"),
					0o600,
				), qt.IsNil)
			},
			wantSource: []byte(
				"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
			),
			wantEntryNames: []string{"added.go", "model.go", "schema.hcl"},
		},
	}
	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			root := c.TempDir()
			source := filepath.Join(root, "model.go")
			output := filepath.Join(root, "schema.hcl")
			original := []byte(
				"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
			)
			previousOutput := []byte("previous schema\n")
			c.Assert(os.WriteFile(source, original, 0o600), qt.IsNil)
			c.Assert(os.WriteFile(output, previousOutput, 0o600), qt.IsNil)

			result, err := export(Options{
				RootDir:    root,
				OutputPath: output,
				Cleanup:    true,
			}, exportHooks{afterOutputStage: func() {
				test.mutate(c, root, source, original)
			}})

			c.Assert(err, qt.ErrorIs, goannotationsource.ErrChanged)
			c.Assert(result, qt.DeepEquals, Result{})
			assertExportInternalFileBytes(c, source, test.wantSource)
			assertExportInternalFileBytes(c, output, previousOutput)
			entries, err := os.ReadDir(root)
			c.Assert(err, qt.IsNil)
			c.Assert(exportInternalEntryNames(entries), qt.DeepEquals, test.wantEntryNames)
		})
	}
}

func TestExport_FailurePath_PreservesConcurrentOutputChangeAfterStaging(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		prepare func(c *qt.C, output string)
	}{
		{
			name: "existing output edited",
			prepare: func(c *qt.C, output string) {
				c.Assert(os.WriteFile(output, []byte("previous schema\n"), 0o600), qt.IsNil)
			},
		},
		{
			name:    "missing output created",
			prepare: func(*qt.C, string) {},
		},
	}
	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			root := c.TempDir()
			source := filepath.Join(root, "model.go")
			output := filepath.Join(root, "schema.hcl")
			sourceData := []byte(
				"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
			)
			concurrentOutput := []byte("concurrent schema\n")
			c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
			test.prepare(c, output)

			result, err := export(Options{
				RootDir:    root,
				OutputPath: output,
				Cleanup:    true,
			}, exportHooks{afterOutputStage: func() {
				c.Assert(os.WriteFile(output, concurrentOutput, 0o600), qt.IsNil)
			}})

			c.Assert(err, qt.ErrorIs, ErrOutputChanged)
			c.Assert(result, qt.DeepEquals, Result{})
			assertExportInternalFileBytes(c, source, sourceData)
			assertExportInternalFileBytes(c, output, concurrentOutput)
			entries, err := os.ReadDir(root)
			c.Assert(err, qt.IsNil)
			c.Assert(exportInternalEntryNames(entries), qt.DeepEquals, []string{
				"model.go",
				"schema.hcl",
			})
		})
	}
}

func TestExport_FailurePath_RejectsReplacedStagedOutput(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	source := filepath.Join(root, "model.go")
	output := filepath.Join(root, "schema.hcl")
	sourceData := []byte(
		"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
	)
	previousOutput := []byte("previous schema\n")
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(output, previousOutput, 0o600), qt.IsNil)

	result, err := export(Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	}, exportHooks{afterOutputStage: func() {
		staged, err := filepath.Glob(filepath.Join(root, ".schema.hcl.tmp-*"))
		c.Assert(err, qt.IsNil)
		c.Assert(staged, qt.HasLen, 1)
		c.Assert(os.Remove(staged[0]), qt.IsNil)
		c.Assert(os.WriteFile(staged[0], []byte("attacker-controlled\n"), 0o600), qt.IsNil)
	}})

	c.Assert(err, qt.ErrorIs, fsdurable.ErrStagedFileChanged)
	c.Assert(result, qt.DeepEquals, Result{})
	assertExportInternalFileBytes(c, source, sourceData)
	assertExportInternalFileBytes(c, output, previousOutput)
	entries, err := os.ReadDir(root)
	c.Assert(err, qt.IsNil)
	c.Assert(exportInternalEntryNames(entries), qt.DeepEquals, []string{
		"model.go",
		"schema.hcl",
	})
}

// TestExport_FailurePath_RefusesDestinationChangedInsideCommitWindow measures
// the gap between the last destination barrier and the rename. Before the
// commit became conditional, every commit-window row returned a nil error and
// left the staged HCL at the destination, so the CLI reported a successful
// export while the other writer's file was gone. Each row therefore asserts the
// rival's bytes, not merely a nonzero exit.
//
// The control rows run the identical mutation one step earlier, where
// validateDestination already caught it. Same mutation, opposite side of the
// last barrier: that is what makes this fixture measure the window rather than
// "any concurrent write".
func TestExport_FailurePath_RefusesDestinationChangedInsideCommitWindow(t *testing.T) {
	c := qt.New(t)
	previousOutput := []byte("previous schema\n")
	rival := []byte("concurrent writer bytes\n")
	writePrevious := func(c *qt.C, output string) {
		c.Assert(os.WriteFile(output, previousOutput, 0o600), qt.IsNil)
	}
	leaveAbsent := func(*qt.C, string) {}
	editInPlace := func(c *qt.C, output string) {
		c.Assert(os.WriteFile(output, rival, 0o600), qt.IsNil)
	}
	replaceByRename := func(c *qt.C, output string) {
		replacement := output + ".rival"
		c.Assert(os.WriteFile(replacement, rival, 0o600), qt.IsNil)
		c.Assert(os.Rename(replacement, output), qt.IsNil)
	}
	commitWindow := func(inject func()) exportHooks {
		return exportHooks{beforeCommit: inject}
	}
	stagingWindow := func(inject func()) exportHooks {
		return exportHooks{afterOutputStage: inject}
	}
	tests := []struct {
		name    string
		prepare func(c *qt.C, output string)
		inject  func(c *qt.C, output string)
		hooks   func(inject func()) exportHooks
	}{
		{
			name:    "existing destination edited in place inside the commit window",
			prepare: writePrevious,
			inject:  editInPlace,
			hooks:   commitWindow,
		},
		{
			name:    "existing destination replaced by rename inside the commit window",
			prepare: writePrevious,
			inject:  replaceByRename,
			hooks:   commitWindow,
		},
		{
			name:    "absent destination created inside the commit window",
			prepare: leaveAbsent,
			inject:  editInPlace,
			hooks:   commitWindow,
		},
		{
			name:    "control: existing destination edited before the last barrier",
			prepare: writePrevious,
			inject:  editInPlace,
			hooks:   stagingWindow,
		},
		{
			name:    "control: absent destination created before the last barrier",
			prepare: leaveAbsent,
			inject:  editInPlace,
			hooks:   stagingWindow,
		},
	}
	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			root := c.TempDir()
			source := filepath.Join(root, "model.go")
			output := filepath.Join(root, "schema.hcl")
			sourceData := []byte(
				"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
			)
			c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
			test.prepare(c, output)

			result, err := export(Options{
				RootDir:    root,
				OutputPath: output,
				Cleanup:    true,
			}, test.hooks(func() {
				test.inject(c, output)
			}))

			c.Assert(err, qt.ErrorIs, ErrOutputChanged)
			c.Assert(err, qt.Not(qt.ErrorIs), fsdurable.ErrReplacementCommitted)
			c.Assert(result, qt.DeepEquals, Result{})
			assertExportInternalFileBytes(c, output, rival)
			assertExportInternalFileBytes(c, source, sourceData)
			entries, err := os.ReadDir(root)
			c.Assert(err, qt.IsNil)
			c.Assert(exportInternalEntryNames(entries), qt.DeepEquals, []string{
				"model.go",
				"schema.hcl",
			})
		})
	}
}

func assertExportInternalFileBytes(c *qt.C, path string, want []byte) {
	c.Helper()
	got, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, want)
}

func exportInternalEntryNames(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}
