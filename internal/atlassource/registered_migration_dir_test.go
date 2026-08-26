package atlassource_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlassource"
)

// projectWithMigrationDir writes an atlas.hcl naming one migration directory
// and loads it, which is what registers a registry reference.
func projectWithMigrationDir(c *qt.C, dir string) (projectconfig.Config, string) {
	c.Helper()

	baseDir := c.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(baseDir, "migrations"), 0o755), qt.IsNil)
	path := filepath.Join(baseDir, "atlas.hcl")
	document := `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
  migration {
    dir = "` + dir + `"
  }
}
`
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)

	config, err := projectconfig.Load(projectconfig.LoadOptions{
		Context: c.Context(), AtlasPath: path, EnvName: "local", Verb: "test",
	})
	c.Assert(err, qt.IsNil)
	return config, baseDir
}

// TestClassifySet_ARegisteredMigrationDirIsNotResolvedAsAPath is the defect.
//
// A `migration.dir` naming a registry reference has no local path: the loader
// registers a lazily-pulled read-only filesystem and leaves an in-memory URL in
// its place. `env://migration.dir` sent that URL through the local-path
// resolver, so a project entitled to write `atlas://acme-migrations` was told
// "only local file:// migration directories are supported" -- about a value it
// never wrote, naming a rule that does not apply to it (stokaro/ptah#1215).
func TestClassifySet_ARegisteredMigrationDirIsNotResolvedAsAPath(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_ATLAS_REGISTRY", "ghcr.io/acme")
	config, baseDir := projectWithMigrationDir(c, "atlas://acme-migrations")

	set, err := atlassource.ClassifySet("--to", []string{"env://migration.dir"},
		atlassource.ProjectEnv{Loaded: true, BaseDir: baseDir, Config: config})

	c.Assert(err, qt.IsNil)
	c.Assert(set.Sources, qt.HasLen, 1)
	c.Assert(set.Sources[0].Kind, qt.Equals, atlassource.KindMigrationDir)
	// The reference the PROJECT wrote, so a diagnostic sends the reader to the
	// line they can edit rather than to an in-memory URL nothing names.
	c.Assert(set.Sources[0].Raw, qt.Equals, "atlas://acme-migrations")
	c.Assert(set.Sources[0].FileSystem, qt.IsNotNil)
	// A registered directory has no local path, and reporting one would send
	// the capture at a directory that is not there.
	c.Assert(set.Sources[0].Path, qt.Equals, "")
}

// TestClassifySet_ALocalMigrationDirStillResolvesToAPath is the control.
//
// A change that returned the registered branch for everything would satisfy the
// test above and would stop every ordinary project working.
func TestClassifySet_ALocalMigrationDirStillResolvesToAPath(t *testing.T) {
	tests := []struct {
		name string
		dir  string
	}{
		{name: "a file:// URL", dir: "file://migrations"},
		{name: "a bare path", dir: "migrations"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			config, baseDir := projectWithMigrationDir(c, test.dir)

			set, err := atlassource.ClassifySet("--to", []string{"env://migration.dir"},
				atlassource.ProjectEnv{Loaded: true, BaseDir: baseDir, Config: config})

			c.Assert(err, qt.IsNil)
			c.Assert(set.Sources[0].FileSystem, qt.IsNil)
			// Compared through EvalSymlinks because the temporary directory is
			// reached by one on macOS -- /var is a link to /private/var -- and a
			// literal comparison would fail there for a path that is right.
			want, err := filepath.EvalSymlinks(filepath.Join(baseDir, "migrations"))
			c.Assert(err, qt.IsNil)
			got, err := filepath.EvalSymlinks(set.Sources[0].Path)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, want)
		})
	}
}

// TestCaptureVerifiedMigrationFS_ReadsADirectoryWithNoLocalPath pins the half
// the capture needed.
//
// CaptureVerifiedMigrationDir opens os.DirFS on a path, which a registered
// directory does not have. The FS form is what the registry case reaches, and
// the fetch a lazily-pulled filesystem defers happens on the first read this
// performs.
func TestCaptureVerifiedMigrationFS_ReadsADirectoryWithNoLocalPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
		[]byte("CREATE TABLE captured (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	snapshot, err := atlassource.CaptureVerifiedMigrationFS(os.DirFS(dir))

	c.Assert(err, qt.IsNil)
	body, err := os.ReadFile(filepath.Join(dir, "1_init.sql"))
	c.Assert(err, qt.IsNil)
	captured, err := readSnapshotFile(snapshot, "1_init.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(captured, qt.Equals, string(body))
}

// readSnapshotFile reads one file out of a captured snapshot.
func readSnapshotFile(fsys fs.FS, name string) (string, error) {
	body, err := fs.ReadFile(fsys, name)
	return string(body), err
}
