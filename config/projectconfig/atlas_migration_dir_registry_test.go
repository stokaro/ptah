package projectconfig_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasregistry"
)

// TestAtlasMigrationDir_RegistryReference pins what an `atlas://` migration.dir
// becomes.
//
// It resolves against the namespace PTAH_ATLAS_REGISTRY names and is registered
// as an in-memory read-only directory -- the same shape `data "remote_dir"`
// produces, which is what lets every consumer that already accepts one accept
// this without a second pull path (stokaro/ptah#1210).
func TestAtlasMigrationDir_RegistryReference(t *testing.T) {
	tests := []struct {
		name string
		// dir is the migration.dir attribute value.
		dir string
		// wantPath is the reference the registered source displays, empty when
		// the value is an ordinary local directory. It is what the PROJECT
		// wrote: nothing has resolved at parse time, and a message naming
		// oci:// for a project that says atlas:// sends the reader to the
		// wrong line.
		wantPath string
		// wantDir is the value migration.dir resolves to when it stays local.
		wantDir string
	}{
		{
			name:     "a tag",
			dir:      "atlas://app?tag=prod",
			wantPath: "atlas://app?tag=prod",
		},
		{
			name:     "an immutable version",
			dir:      "atlas://app?version=20260806123000",
			wantPath: "atlas://app?version=20260806123000",
		},
		{
			name:     "no tag resolves to latest",
			dir:      "atlas://app",
			wantPath: "atlas://app",
		},
		{
			// A local directory is untouched: this reads the scheme, not every
			// value that happens to contain a slash.
			name:    "a local directory is left alone",
			dir:     "file://migrations",
			wantDir: "migrations",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(atlasregistry.NamespaceEnvVar, "registry.example/team")
			path := writeAtlasMigrationDirProject(c, test.dir)

			cfg, err := projectconfig.LoadAtlasFile(path, "local")

			c.Assert(err, qt.IsNil)
			dir := cfg.StringValue(projectconfig.StringMigrationDir)
			c.Assert(dir.Present, qt.IsTrue)
			source, registered := cfg.MigrationDirectorySource(dir.Value)
			c.Assert(source.Path, qt.Equals, test.wantPath)
			c.Assert(registered, qt.Equals, test.wantPath != "")
			c.Assert(source.ReadOnly, qt.Equals, test.wantPath != "")
			c.Assert(dir.Value, qt.Equals, orResolved(test.wantDir, dir.Value))
		})
	}
}

// TestAtlasMigrationDir_NothingIsFetchedWhileParsing is the property that makes
// this safe to put in a project file.
//
// A project file is read by EVERY verb. The namespace below names a host that
// does not resolve, so a parse that contacted the registry could not succeed --
// and `schema inspect --env prod` would fail over a directory it never opens.
func TestAtlasMigrationDir_NothingIsFetchedWhileParsing(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.NamespaceEnvVar, "registry.invalid.test/team")
	path := writeAtlasMigrationDirProject(c, "atlas://app?tag=prod")

	cfg, err := projectconfig.LoadAtlasFile(path, "local")

	c.Assert(err, qt.IsNil)
	dir := cfg.StringValue(projectconfig.StringMigrationDir)
	source, registered := cfg.MigrationDirectorySource(dir.Value)
	c.Assert(registered, qt.IsTrue)
	c.Assert(source.Path, qt.Equals, "atlas://app?tag=prod")
	// The filesystem exists and has not resolved anything. Reading it is what
	// contacts the registry, and that is the failure a verb which needs the
	// directory should get.
	_, readErr := source.FileSystem.Open("atlas.sum")
	c.Assert(readErr, qt.IsNotNil)
}

// TestAtlasMigrationDir_WithoutANamespaceIsRefusedOnRead pins where the refusal
// lands and what it names.
//
// Not at parse time: a project file is read by every verb, and a command that
// never opens the migration directory must not fail because a namespace is
// unset. On read, and naming the variable rather than the scheme alone.
func TestAtlasMigrationDir_WithoutANamespaceIsRefusedOnRead(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.NamespaceEnvVar, "")
	path := writeAtlasMigrationDirProject(c, "atlas://app?tag=prod")

	cfg, err := projectconfig.LoadAtlasFile(path, "local")

	// The project PARSES: a verb that never opens the directory must not fail
	// because a namespace is unset. The refusal belongs to the read.
	c.Assert(err, qt.IsNil)
	dir := cfg.StringValue(projectconfig.StringMigrationDir)
	source, registered := cfg.MigrationDirectorySource(dir.Value)
	c.Assert(registered, qt.IsTrue)
	_, readErr := source.FileSystem.Open("atlas.sum")
	c.Assert(readErr, qt.ErrorMatches, `.*PTAH_ATLAS_REGISTRY.*`)
}

// writeAtlasMigrationDirProject writes an atlas.hcl whose env names dir, and
// returns its path.
func writeAtlasMigrationDirProject(c *qt.C, dir string) string {
	c.Helper()
	path := filepath.Join(c.TB.TempDir(), "atlas.hcl")
	c.Assert(os.WriteFile(path, []byte(`env "local" {
  url = "sqlite://app.db"

  migration {
    dir = "`+dir+`"
  }
}
`), 0o600), qt.IsNil)
	return path
}

// orResolved returns want when a row states one, and the observed value when it
// does not, so a row asserts the local case without restating the mem URL the
// registry case mints.
func orResolved(want, got string) string {
	if want == "" {
		return got
	}
	return want
}
