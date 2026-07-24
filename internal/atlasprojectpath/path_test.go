package atlasprojectpath_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasprojectpath"
	"github.com/stokaro/ptah/internal/pathguard"
)

func TestLocalDir_HappyPath(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	want, err := pathguard.ResolveWithinRoot(filepath.Join(baseDir, "migrations"), baseDir)
	c.Assert(err, qt.IsNil)

	resolved, err := atlasprojectpath.LocalDir("file://migrations", baseDir)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, want)
}

func TestLocalDir_PreservesAbsolutePath(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	absoluteDir := filepath.Join(baseDir, "absolute-migrations")
	want, err := pathguard.ResolveWithinRoot(absoluteDir, "")
	c.Assert(err, qt.IsNil)

	resolved, err := atlasprojectpath.LocalDir(absoluteDir, t.TempDir())

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, want)
}

func TestLocalDir_AllowsParentRelativePath(t *testing.T) {
	c := qt.New(t)
	rootDir := t.TempDir()
	baseDir := filepath.Join(rootDir, "project")
	c.Assert(os.MkdirAll(baseDir, 0o755), qt.IsNil)
	want, err := pathguard.ResolveWithinRoot(filepath.Join(rootDir, "migrations"), "")
	c.Assert(err, qt.IsNil)

	resolved, err := atlasprojectpath.LocalDir("file://../migrations", baseDir)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, want)
}

func TestLocalDir_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := atlasprojectpath.LocalDir("postgres://localhost/db", t.TempDir())

	c.Assert(err, qt.ErrorMatches, `only local file:// migration directories are supported`)
}

func TestSchemaFileURLs_HappyPath(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	schemaHCL, err := pathguard.ResolveWithinRoot(filepath.Join(baseDir, "schema.hcl"), baseDir)
	c.Assert(err, qt.IsNil)
	schemaSQL, err := pathguard.ResolveWithinRoot(filepath.Join(baseDir, "schema.sql"), baseDir)
	c.Assert(err, qt.IsNil)

	resolved, err := atlasprojectpath.SchemaFileURLs([]string{
		"file://schema.hcl",
		"schema.sql",
	}, baseDir)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.DeepEquals, []string{
		"file://" + filepath.ToSlash(schemaHCL),
		"file://" + filepath.ToSlash(schemaSQL),
	})
}

func TestSchemaFileURL_RejectsQuery(t *testing.T) {
	c := qt.New(t)

	_, err := atlasprojectpath.SchemaFileURL("file://schema.hcl?format=hcl", t.TempDir())

	c.Assert(err, qt.ErrorMatches, `schema file URL query parameters are not supported yet`)
}

func TestSchemaFileURL_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := atlasprojectpath.SchemaFileURL("env://src", t.TempDir())

	c.Assert(err, qt.ErrorMatches, `only local file:// schema files are supported`)
}
