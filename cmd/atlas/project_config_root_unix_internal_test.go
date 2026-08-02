//go:build !windows

package atlas

// White-box testing required: this test verifies the unexported Atlas argument
// mapper's snapshot boundary between project evaluation and native consumption;
// no exported API exposes a deterministic hook at that point.

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestAtlasArgMapperPreservesProjectFilesSnapshot(t *testing.T) {
	c := qt.New(t)
	parentDir := t.TempDir()
	projectDir := filepath.Join(parentDir, "project")
	c.Assert(os.Mkdir(projectDir, 0o700), qt.IsNil)
	t.Chdir(projectDir)
	c.Assert(os.Mkdir("migrations", 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile("migrations/20240101000000_original.sql", []byte("SELECT 1;\n"), 0o600),
		qt.IsNil,
	)
	c.Assert(os.WriteFile("ptah.yaml", []byte(`env:
  local:
    migration:
      pre_down_hook: "generation-one-hook"
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://generation-one.db"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	migrationPath, err := filepath.EvalSymlinks(filepath.Join(projectDir, "migrations"))
	c.Assert(err, qt.IsNil)

	mapperCommand := &cobra.Command{Use: "down"}
	mapperCommand.SetContext(t.Context())
	mapper := atlasArgMapper("migrate", atlasMigrateDownVerb())
	cleanup := &cmdadapter.CleanupScope{}
	_, snapshotContext, err := mapper(mapperCommand, []string{
		"--config", "file://atlas.hcl",
		"--env", "local",
	}, cleanup)
	c.Assert(err, qt.IsNil)

	preservedDir := filepath.Join(parentDir, "preserved")
	c.Assert(os.Rename(projectDir, preservedDir), qt.IsNil)
	c.Assert(os.Mkdir(projectDir, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(projectDir, "migrations"), 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join(projectDir, "migrations", "20240101000000_replacement.sql"),
			[]byte("SELECT 2;\n"),
			0o600,
		),
		qt.IsNil,
	)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "ptah.yaml"), []byte(`env:
  local:
    migration:
      pre_down_hook: "generation-two-hook"
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://generation-two.db"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	nativeCommand := &cobra.Command{Use: "down"}
	nativeCommand.SetContext(snapshotContext)
	loaded, err := dbcli.LoadProjectConfig(nativeCommand, "ptah.yaml")
	c.Assert(err, qt.IsNil)
	c.Assert(loaded.DatabaseURL, qt.Equals, "sqlite://generation-one.db")
	c.Assert(loaded.Migration.Dir, qt.Equals, "migrations")
	c.Assert(loaded.Migration.PreDownHook, qt.Equals, "generation-one-hook")

	source, err := migrationsource.Resolve(snapshotContext, migrationPath, migrationsource.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	original, err := fs.ReadFile(source.FileSystem, "20240101000000_original.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(original), qt.Equals, "SELECT 1;\n")
	_, err = fs.ReadFile(source.FileSystem, "20240101000000_replacement.sql")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	c.Assert(cleanup.Close(), qt.IsNil)
}

func TestAtlasProjectCaptureLocalPreservesProjectRoot(t *testing.T) {
	c := qt.New(t)
	parentDir := t.TempDir()
	projectDir := filepath.Join(parentDir, "project")
	c.Assert(os.Mkdir(projectDir, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(projectDir, "migrations"), 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join(projectDir, "migrations", "20240101000000_original.sql"),
			[]byte("SELECT 1;\n"),
			0o600,
		),
		qt.IsNil,
	)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	project, _, err := openAtlasProject(atlasProjectFlagValues{
		configPath: "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		envName:    "local",
	}, requiredAtlasProject)
	c.Assert(err, qt.IsNil)

	preservedDir := filepath.Join(parentDir, "preserved")
	c.Assert(os.Rename(projectDir, preservedDir), qt.IsNil)
	c.Assert(os.Mkdir(projectDir, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(projectDir, "migrations"), 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join(projectDir, "migrations", "20240101000000_replacement.sql"),
			[]byte("SELECT 2;\n"),
			0o600,
		),
		qt.IsNil,
	)

	localDir, err := project.localDirWithQuery(project.Migration.Dir)
	c.Assert(err, qt.IsNil)
	source, err := project.captureLocal(localDir)
	c.Assert(err, qt.IsNil)
	original, err := fs.ReadFile(source.FileSystem, "20240101000000_original.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(original), qt.Equals, "SELECT 1;\n")
	_, err = fs.ReadFile(source.FileSystem, "20240101000000_replacement.sql")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	c.Assert(project.Close(), qt.IsNil)
}

func TestAtlasMigrateDownFormatPreservesProjectRoot(t *testing.T) {
	c := qt.New(t)
	parentDir := t.TempDir()
	projectDir := filepath.Join(parentDir, "project")
	c.Assert(os.Mkdir(projectDir, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(projectDir, "migrations"), 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join(projectDir, "migrations", "20240101000000_original.sql"),
			[]byte("SELECT 1;\n"),
			0o600,
		),
		qt.IsNil,
	)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://target.db"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	opts, err := parseAtlasMigrateDownFormatArgs(
		atlasMigrateDownVerb(),
		[]string{"--format", "{{ .Dir }}"},
	)
	c.Assert(err, qt.IsNil)
	project, err := applyAtlasMigrateDownFormatProjectConfig(opts, atlasProjectArgValues{
		flags: atlasProjectFlagValues{
			configPath: "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
			envName:    "local",
		},
		changed: true,
	})
	c.Assert(err, qt.IsNil)

	preservedDir := filepath.Join(parentDir, "preserved")
	c.Assert(os.Rename(projectDir, preservedDir), qt.IsNil)
	c.Assert(os.Mkdir(projectDir, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(projectDir, "migrations"), 0o700), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join(projectDir, "migrations", "20240101000000_replacement.sql"),
			[]byte("SELECT 2;\n"),
			0o600,
		),
		qt.IsNil,
	)

	source, err := migrationsource.CaptureLocal(opts.dir, opts.dirOptions)
	c.Assert(err, qt.IsNil)
	original, err := fs.ReadFile(source.FileSystem, "20240101000000_original.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(original), qt.Equals, "SELECT 1;\n")
	_, err = fs.ReadFile(source.FileSystem, "20240101000000_replacement.sql")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	c.Assert(project.Close(), qt.IsNil)
}
