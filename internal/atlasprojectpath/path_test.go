package atlasprojectpath_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasprojectpath"
	"go.5x5.cz/ptah/internal/pathguard"
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

func TestLocalDirWithQuery_HappyPath(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	want, err := pathguard.ResolveWithinRoot(filepath.Join(baseDir, "migration files"), baseDir)
	c.Assert(err, qt.IsNil)

	resolved, query, err := atlasprojectpath.LocalDirWithQuery(
		"file://migration%20files?format=atlas",
		baseDir,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, want)
	c.Assert(query, qt.DeepEquals, url.Values{"format": []string{"atlas"}})
}

func TestLocalDir_AllowsAbsolutePathInsideProjectRoot(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	absoluteDir := filepath.Join(baseDir, "absolute-migrations")
	want, err := pathguard.ResolveWithinRoot(absoluteDir, baseDir)
	c.Assert(err, qt.IsNil)

	resolved, err := atlasprojectpath.LocalDir(absoluteDir, baseDir)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, want)
}

func TestLocalDir_RejectsAbsolutePathOutsideProjectRoot(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	absoluteDir := filepath.Join(t.TempDir(), "absolute-migrations")

	_, err := atlasprojectpath.LocalDir(absoluteDir, baseDir)

	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
}

func TestLocalDir_PreservesPlainPathURLCharacters(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	want, err := pathguard.ResolveWithinRoot(filepath.Join(baseDir, "migrations%2Farchive"), "")
	c.Assert(err, qt.IsNil)

	resolved, err := atlasprojectpath.LocalDir("migrations%2Farchive", baseDir)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, want)
}

func TestLocalDir_RejectsParentRelativePath(t *testing.T) {
	c := qt.New(t)
	rootDir := t.TempDir()
	baseDir := filepath.Join(rootDir, "project")
	c.Assert(os.MkdirAll(baseDir, 0o755), qt.IsNil)

	_, err := atlasprojectpath.LocalDir("file://../migrations", baseDir)

	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
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
