package migrateup

// White-box testing required: lintPendingDestructive is the native command's
// final safety-gate adapter and its compatibility profile is not observable
// through an exported API without opening a real database connection.

import (
	"context"
	"io/fs"
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestLintPendingDestructive_DoesNotApplyAtlasFileSuppression(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_drop.up.sql": {
			Data: []byte("-- atlas:nolint destructive\n\nDROP TABLE users;\n"),
		},
		"0000000001_drop.down.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
	}

	findings, err := lintPendingDestructive(fsys, []int64{1}, "sqlite", "")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
}

func TestLintPendingDestructive_HonorsDirectoryPrefixedExclusion(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {
			Data: []byte("rules:\n  DS101:\n    exclude:\n      - migrations/legacy/**\n"),
		},
		"legacy/0000000001_drop.up.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"legacy/0000000001_drop.down.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
	}

	findings, err := lintPendingDestructive(fsys, []int64{1}, "sqlite", "migrations")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

func TestLintPathPrefixForSource_PreservesLocalSpelling(t *testing.T) {
	c := qt.New(t)

	got := lintPathPrefixForSource("db/migrations", migrationsource.Source{
		Display: "/absolute/db/migrations",
	})

	c.Assert(got, qt.Equals, "db/migrations")
}

func TestLintPathPrefixForSource_UsesCanonicalOCIDisplay(t *testing.T) {
	c := qt.New(t)

	got := lintPathPrefixForSource("oci://REGISTRY.EXAMPLE/acme/migrations:latest", migrationsource.Source{
		Display: "oci://registry.example/acme/migrations:latest",
		OCI:     &migrationsource.OCI{},
	})

	c.Assert(got, qt.Equals, "oci://registry.example/acme/migrations:latest")
}

func TestCapturedMigrationsFeedProviderAndDestructiveGateFromSameBytes(t *testing.T) {
	c := qt.New(t)
	source := &changingMigrationFS{
		files: fstest.MapFS{
			"0000000001_drop.up.sql": {
				Data: []byte("DROP TABLE users;\n"),
			},
			"0000000001_drop.down.sql": {
				Data: []byte("CREATE TABLE users (id INTEGER);\n"),
			},
		},
		reads: map[string]int{},
	}

	snapshot, err := migrationsnapshot.Capture(source)
	c.Assert(err, qt.IsNil)
	provider, err := migrator.NewFSMigrationProvider(
		snapshot,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatPtah),
	)
	c.Assert(err, qt.IsNil)
	findings, err := lintPendingDestructive(snapshot, []int64{1}, "sqlite", "")
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].UpSQL, qt.Equals, "DROP TABLE users;\n")
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
	c.Assert(source.reads, qt.DeepEquals, map[string]int{
		"0000000001_drop.down.sql": 1,
		"0000000001_drop.up.sql":   1,
	})
}

func TestLockedDestructiveLintHookUsesLockedPlanVersions(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_create.up.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
		"0000000001_create.down.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"0000000002_drop.up.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"0000000002_drop.down.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
	}
	hook := lockedDestructiveLintHook(fsys, "sqlite", "")

	err := hook(context.Background(), migrator.MigrationPlan{
		Versions: []int64{2},
	})

	c.Assert(err, qt.ErrorMatches, "(?s)pending migrations contain destructive statements.*0000000002_drop.up.sql.*DS101.*")
}

func TestLockedDestructiveLintHookAllowsSafeLockedPlan(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"0000000001_create.up.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
		"0000000001_create.down.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"0000000002_drop.up.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"0000000002_drop.down.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
	}
	hook := lockedDestructiveLintHook(fsys, "sqlite", "")

	err := hook(context.Background(), migrator.MigrationPlan{
		Versions: []int64{1},
	})

	c.Assert(err, qt.IsNil)
}

type changingMigrationFS struct {
	files fstest.MapFS
	reads map[string]int
}

func (f *changingMigrationFS) Open(name string) (fs.File, error) {
	return f.files.Open(name)
}

func (f *changingMigrationFS) ReadFile(name string) ([]byte, error) {
	f.reads[name]++
	contents, err := fs.ReadFile(f.files, name)
	responses := [][]byte{contents, []byte("SELECT 1;\n")}
	return slices.Clone(responses[min(f.reads[name]-1, 1)]), err
}
