//go:build !windows

package projectconfig_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

func TestParseAtlasFSWithOptionsRejectsFileSymlinkEscape(t *testing.T) {
	c := qt.New(t)
	projectDir := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "database-url.txt")
	c.Assert(os.WriteFile(outsidePath, []byte("sqlite://outside.db"), 0o600), qt.IsNil)
	c.Assert(os.Symlink(outsidePath, filepath.Join(projectDir, "database-url.txt")), qt.IsNil)
	root, err := os.OpenRoot(projectDir)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})
	raw := []byte(`env "local" {
  url = file("database-url.txt")
}
`)

	_, err = projectconfig.ParseAtlasFSWithOptions(
		raw,
		"atlas.hcl",
		root.FS(),
		projectconfig.AtlasLoadOptions{EnvName: "local"},
	)

	// The refusal is the same one this test was written for. Its wording moved
	// from the filesystem's ("openat database-url.txt: path escapes from
	// parent") to the sandbox's own, which names the link and the rule --
	// stokaro/ptah#1042. The rooted filesystem still refuses whatever the
	// sandbox cannot resolve for itself; that path is pinned by the
	// "chain longer than the sandbox resolves" row in
	// TestAtlasFileSandboxRefusesReadsOutsideTheConfigDirectory.
	c.Assert(err, qt.ErrorMatches,
		`cannot evaluate atlas\.hcl "url" at atlas\.hcl:2: .*path escapes atlas\.hcl directory: database-url\.txt: `+
			`database-url\.txt is a symbolic link pointing outside it.*`)
}

func TestParseAtlasFSWithOptionsRejectsFilesetSymlinkEscape(t *testing.T) {
	c := qt.New(t)
	projectDir := t.TempDir()
	schemaDir := filepath.Join(projectDir, "schema")
	c.Assert(os.Mkdir(schemaDir, 0o700), qt.IsNil)
	outsidePath := filepath.Join(t.TempDir(), "outside.hcl")
	c.Assert(os.WriteFile(outsidePath, []byte(`schema "main" {}`), 0o600), qt.IsNil)
	c.Assert(os.Symlink(outsidePath, filepath.Join(schemaDir, "escape.hcl")), qt.IsNil)
	root, err := os.OpenRoot(projectDir)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})
	raw := []byte(`data "hcl_schema" "app" {
  paths = fileset("schema/**/*.hcl")
}

env "local" {
  src = data.hcl_schema.app.url
}
`)

	_, err = projectconfig.ParseAtlasFSWithOptions(
		raw,
		"atlas.hcl",
		root.FS(),
		projectconfig.AtlasLoadOptions{EnvName: "local"},
	)

	c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "paths" at atlas\.hcl:2: .*`)
}
