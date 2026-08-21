package migrator_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// These are the end-to-end half of stokaro/ptah#1702. The message tests hold
// what the error says; these hold that the raise site sets the flag at all,
// which is what a run against a converted directory actually hits.

const convertedRepeatableVersionedBody = "CREATE TABLE t (id INTEGER PRIMARY KEY);\n"

// newConvertedRepeatableMigrator builds a migrator over an Atlas-format
// directory shaped the way `migrate import --dir-format flyway` writes one: one
// ordinary versioned migration, and the repeatable on the reserved slot at the
// top of int64.
func newConvertedRepeatableMigrator(
	c *qt.C,
	dbPath, repeatableBody string,
) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	repeatable := fmt.Sprintf("%d_view.sql", migrator.ConvertedFlywayRepeatableVersion)
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"0000000001_init.sql": {Data: []byte(convertedRepeatableVersionedBody)},
			repeatable:            {Data: []byte(repeatableBody)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	return conn, mig.WithRevisionTableFormat(migrator.RevisionTableFormatPtah)
}

// mismatchFromApply runs a migrator over an edited directory and returns the
// checksum mismatch it refuses with.
func mismatchFromApply(c *qt.C, dbPath, repeatableBody string) *migrator.ChecksumMismatchError {
	c.Helper()
	_, edited := newConvertedRepeatableMigrator(c, dbPath, repeatableBody)

	err := edited.MigrateUp(c.Context())

	c.Assert(err, qt.IsNotNil)
	var mismatch *migrator.ChecksumMismatchError
	c.Assert(err, qt.ErrorAs, &mismatch)
	return mismatch
}

func TestMigrateUp_EditedConvertedRepeatableNamesTheConversion(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "repeatable.db")

	_, first := newConvertedRepeatableMigrator(c, dbPath, "CREATE VIEW v AS SELECT id FROM t;\n")
	c.Assert(first.MigrateUp(c.Context()), qt.IsNil)
	mismatch := mismatchFromApply(c, dbPath, "CREATE VIEW v AS SELECT id, 1 AS extra FROM t;\n")

	c.Assert(mismatch.ConvertedRepeatable, qt.IsTrue)
	c.Assert(mismatch.Version, qt.Equals, migrator.ConvertedFlywayRepeatableVersion)
	c.Assert(mismatch.Error(), qt.Contains, "was a Flyway repeatable")
}

// TestMigrateUp_EditedConvertedRepeatableWritesNothing is the other half of the
// issue's "refused before any state is written": the refusal has to come before
// the body runs, or the directory is wedged with a half-applied change.
func TestMigrateUp_EditedConvertedRepeatableWritesNothing(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "repeatable.db")

	conn, first := newConvertedRepeatableMigrator(c, dbPath, "CREATE VIEW v AS SELECT id FROM t;\n")
	c.Assert(first.MigrateUp(c.Context()), qt.IsNil)
	mismatchFromApply(c, dbPath, "CREATE VIEW v AS SELECT id, 1 AS extra FROM t;\n")

	var definition string
	c.Assert(
		conn.QueryRowContext(c.Context(), "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = 'v'").
			Scan(&definition),
		qt.IsNil,
	)
	c.Assert(definition, qt.Contains, "SELECT id FROM t")
	c.Assert(definition, qt.Not(qt.Contains), "extra")
}

// TestMigrateUp_EditedVersionedMigrationIsStillRefusedPlainly is the control
// the issue asks for. The repeatable case must not be satisfied by weakening
// the checksum for everything, so the versioned file is edited in the same
// directory shape and its refusal must carry none of the repeatable sentence.
func TestMigrateUp_EditedVersionedMigrationIsStillRefusedPlainly(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "versioned.db")
	repeatable := "CREATE VIEW v AS SELECT id FROM t;\n"

	_, first := newConvertedRepeatableMigrator(c, dbPath, repeatable)
	c.Assert(first.MigrateUp(c.Context()), qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	edited, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"0000000001_init.sql": {Data: []byte("CREATE TABLE t (id INTEGER PRIMARY KEY, extra INTEGER);\n")},
			fmt.Sprintf("%d_view.sql", migrator.ConvertedFlywayRepeatableVersion): {Data: []byte(repeatable)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)

	applyErr := edited.WithRevisionTableFormat(migrator.RevisionTableFormatPtah).MigrateUp(c.Context())

	c.Assert(applyErr, qt.IsNotNil)
	var mismatch *migrator.ChecksumMismatchError
	c.Assert(applyErr, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.ConvertedRepeatable, qt.IsFalse)
	c.Assert(mismatch.Error(), qt.Not(qt.Contains), "repeatable")
}
