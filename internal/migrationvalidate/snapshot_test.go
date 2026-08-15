package migrationvalidate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationvalidate"
	"go.5x5.cz/ptah/migration/migrator"
)

// This file covers [migrationvalidate.Options.FS]: validation of a migration
// directory that has no path, which is what a registry artifact is
// (stokaro/ptah#1499).
//
// Every fixture here deliberately pairs the snapshot with a Dir that CANNOT be
// read — an `oci://` reference is not a path on any filesystem. A build that
// read Dir instead of FS therefore cannot accidentally agree with one that
// read FS: it fails at stat, which is precisely the failure the issue reported
// from the command surface.

const unreadableArtifactDir = "oci://registry.invalid/demo/migrations:v1"

// hashedSnapshot returns a two-file ptah directory plus the integrity file that
// matches it, as an in-memory filesystem.
func hashedSnapshot(c *qt.C) fstest.MapFS {
	c.Helper()
	files := fstest.MapFS{
		"0000000001_init.up.sql":   {Data: []byte("CREATE TABLE snapshot_widgets (id INTEGER PRIMARY KEY);\n")},
		"0000000001_init.down.sql": {Data: []byte("DROP TABLE snapshot_widgets;\n")},
	}
	sum, err := migratesum.ComputeWithFormat(files, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	files[migratesum.FileName] = &fstest.MapFile{Data: sum.Bytes()}
	return files
}

func TestValidate_VerifiesTheSuppliedSnapshotRatherThanTheDirPath(t *testing.T) {
	c := qt.New(t)

	result, err := migrationvalidate.Validate(context.Background(), migrationvalidate.Options{
		Dir:       unreadableArtifactDir,
		FS:        hashedSnapshot(c),
		DirFormat: migrator.MigrationDirFormatPtah,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Integrity.OK(), qt.IsTrue)
	c.Assert(result.Integrity.SumFileName, qt.Equals, migratesum.FileName)
}

func TestValidate_ReportsDriftInsideTheSuppliedSnapshot(t *testing.T) {
	c := qt.New(t)
	files := hashedSnapshot(c)
	files["0000000001_init.up.sql"] = &fstest.MapFile{
		Data: []byte("CREATE TABLE snapshot_widgets (id TEXT PRIMARY KEY);\n"),
	}

	result, err := migrationvalidate.Validate(context.Background(), migrationvalidate.Options{
		Dir:       unreadableArtifactDir,
		FS:        files,
		DirFormat: migrator.MigrationDirFormatPtah,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Integrity.OK(), qt.IsFalse)
	c.Assert(result.Integrity.Describe(), qt.Contains, "changed: 0000000001_init.up.sql")
}

func TestValidate_ReportsAMissingSumInsideTheSuppliedSnapshot(t *testing.T) {
	c := qt.New(t)
	files := hashedSnapshot(c)
	delete(files, migratesum.FileName)

	_, err := migrationvalidate.Validate(context.Background(), migrationvalidate.Options{
		Dir:       unreadableArtifactDir,
		FS:        files,
		DirFormat: migrator.MigrationDirFormatPtah,
	})

	c.Assert(err, qt.ErrorIs, migratesum.ErrSumFileMissing)
}

// TestValidate_ReplaysTheSuppliedSnapshotOnTheDevDatabase pins the second half:
// the snapshot has to reach the replay too, not only the integrity check.
//
// Without it a build could verify the pulled bytes and then replay whatever
// Dir happens to name, which for an artifact is nothing at all — and the
// integrity assertions above would still pass.
func TestValidate_ReplaysTheSuppliedSnapshotOnTheDevDatabase(t *testing.T) {
	c := qt.New(t)
	devDBPath := filepath.Join(t.TempDir(), "dev.db")

	result, err := migrationvalidate.Validate(context.Background(), migrationvalidate.Options{
		Dir:       unreadableArtifactDir,
		FS:        hashedSnapshot(c),
		DirFormat: migrator.MigrationDirFormatPtah,
		DevURL:    "sqlite://" + devDBPath,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Integrity.OK(), qt.IsTrue)
	c.Assert(result.DevSQLValidated, qt.IsTrue)
	assertSQLiteTableCount(c, devDBPath, "snapshot_widgets", 0)
}

// TestValidate_WithoutSnapshotStillReadsTheDirPath is the non-interference
// control: the local path remains the source when no snapshot is supplied.
func TestValidate_WithoutSnapshotStillReadsTheDirPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeSnapshotToDisk(c, dir, hashedSnapshot(c))

	result, err := migrationvalidate.Validate(context.Background(), migrationvalidate.Options{
		Dir:       dir,
		DirFormat: migrator.MigrationDirFormatPtah,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Integrity.OK(), qt.IsTrue)
}

func writeSnapshotToDisk(c *qt.C, dir string, files fstest.MapFS) {
	c.Helper()
	for name, file := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), file.Data, 0o600), qt.IsNil)
	}
	// Guard the fixture's premise rather than assuming the copy landed: a
	// silently empty directory would make the control pass for the wrong
	// reason, since a directory with no sum is a failure and not a pass.
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, len(files))
}
