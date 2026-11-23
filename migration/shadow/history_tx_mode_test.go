package shadow_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// The rollback rehearsal, the derived rollback and the baseline verification
// each replay a target's applied history, and each replays it in the
// transaction mode the caller states. SQLite refuses VACUUM inside a
// transaction, so migration 1 applies under none and its replay fails in a
// transaction.

// vacuumHistory is a history whose migration 1 holds a VACUUM beside a
// statement a transaction accepts.
var vacuumHistory = fstest.MapFS{
	"0000000001_items.up.sql":   {Data: []byte("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\nVACUUM;\n")},
	"0000000001_items.down.sql": {Data: []byte("DROP TABLE items;\n")},
	"0000000002_note.up.sql":    {Data: []byte("ALTER TABLE items ADD COLUMN note TEXT;\n")},
	"0000000002_note.down.sql":  {Data: []byte("ALTER TABLE items DROP COLUMN note;\n")},
}

// vacuumReplayRefusal is the refusal of migration 1 replayed in a transaction.
const vacuumReplayRefusal = `(?s).*0000000001_items\.up\.sql cannot run inside a transaction: line 2 VACUUM is refused inside a transaction block.*`

// vacuumTarget opens a SQLite target holding the schema vacuumHistory makes at
// version 2, and returns it with the URL of a shadow database beside it.
func vacuumTarget(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	dir := c.TempDir()
	target, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	_, err = target.ExecContext(c.Context(),
		"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL); ALTER TABLE items ADD COLUMN note TEXT;")
	c.Assert(err, qt.IsNil)
	return target, atlasurl.SQLiteURLFromPath(filepath.Join(dir, "shadow.db"))
}

func vacuumRollbackOptions(target *dbschema.DatabaseConnection, shadowURL string, txMode migrator.MigrationTxMode) shadow.RollbackVerifyOptions {
	return shadow.RollbackVerifyOptions{
		TargetConnection:  target,
		ShadowDatabaseURL: shadowURL,
		FS:                vacuumHistory,
		CurrentVersion:    2,
		TargetVersion:     1,
		TxMode:            txMode,
	}
}

func vacuumDynamicRollbackOptions(target *dbschema.DatabaseConnection, devURL string, txMode migrator.MigrationTxMode) shadow.DynamicRollbackOptions {
	return shadow.DynamicRollbackOptions{
		Runtime:          builtintest.Runtime(),
		TargetConnection: target,
		DevDatabaseURL:   devURL,
		FS:               vacuumHistory,
		TargetVersion:    1,
		TxMode:           txMode,
	}
}

func vacuumBaselineOptions(target *dbschema.DatabaseConnection, shadowURL string, txMode migrator.MigrationTxMode) shadow.BaselineVerifyOptions {
	return shadow.BaselineVerifyOptions{
		Runtime:           builtintest.Runtime(),
		ShadowDatabaseURL: shadowURL,
		TargetConn:        target,
		MigrationsFS:      vacuumHistory,
		Version:           2,
		Dialect:           target.Info().Dialect,
		TxMode:            txMode,
	}
}

// TestVerifyRollback_HappyPathReplaysInTheTxMode rehearses the rollback of
// migration 2 on a history migration 1 of which ran under none.
func TestVerifyRollback_HappyPathReplaysInTheTxMode(t *testing.T) {
	c := qt.New(t)
	target, shadowURL := vacuumTarget(c)

	err := shadow.VerifyRollback(c.Context(), vacuumRollbackOptions(target, shadowURL, migrator.MigrationTxModeNone))

	c.Assert(err, qt.IsNil)
}

// TestVerifyRollback_FailurePathReplaysInTheTxMode is the control: without a
// mode the history replays a file to a transaction, and migration 1 is refused.
func TestVerifyRollback_FailurePathReplaysInTheTxMode(t *testing.T) {
	c := qt.New(t)
	target, shadowURL := vacuumTarget(c)

	err := shadow.VerifyRollback(c.Context(), vacuumRollbackOptions(target, shadowURL, ""))

	c.Assert(err, qt.ErrorMatches, `rollback verification failed: replay migrations: `+vacuumReplayRefusal)
}

// TestPlanDynamicRollback_HappyPathReplaysInTheTxMode derives the rollback to
// version 1 on a history migration 1 of which ran under none.
func TestPlanDynamicRollback_HappyPathReplaysInTheTxMode(t *testing.T) {
	c := qt.New(t)
	target, devURL := vacuumTarget(c)

	statements, err := shadow.PlanDynamicRollback(c.Context(), vacuumDynamicRollbackOptions(target, devURL, migrator.MigrationTxModeNone))

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"PRAGMA foreign_keys = off",
		`CREATE TABLE "new_items" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)`,
		`INSERT INTO "new_items" ("id", "name") SELECT "id", "name" FROM "items"`,
		`DROP TABLE "items"`,
		`ALTER TABLE "new_items" RENAME TO "items"`,
		"PRAGMA foreign_keys = on",
	})
}

// TestPlanDynamicRollback_FailurePathReplaysInTheTxMode is its control.
func TestPlanDynamicRollback_FailurePathReplaysInTheTxMode(t *testing.T) {
	c := qt.New(t)
	target, devURL := vacuumTarget(c)

	statements, err := shadow.PlanDynamicRollback(c.Context(), vacuumDynamicRollbackOptions(target, devURL, ""))

	c.Assert(err, qt.ErrorMatches, `dynamic rollback planning failed: replay migrations: `+vacuumReplayRefusal)
	c.Assert(statements, qt.IsNil)
}

// TestVerifyBaseline_HappyPathReplaysInTheTxMode verifies a baseline at
// version 2 of a history migration 1 of which ran under none.
func TestVerifyBaseline_HappyPathReplaysInTheTxMode(t *testing.T) {
	c := qt.New(t)
	target, shadowURL := vacuumTarget(c)

	err := shadow.VerifyBaseline(c.Context(), vacuumBaselineOptions(target, shadowURL, migrator.MigrationTxModeNone))

	c.Assert(err, qt.IsNil)
}

// TestVerifyBaseline_FailurePathReplaysInTheTxMode is its control.
func TestVerifyBaseline_FailurePathReplaysInTheTxMode(t *testing.T) {
	c := qt.New(t)
	target, shadowURL := vacuumTarget(c)

	err := shadow.VerifyBaseline(c.Context(), vacuumBaselineOptions(target, shadowURL, ""))

	c.Assert(err, qt.ErrorMatches, `.*`+vacuumReplayRefusal)
}
