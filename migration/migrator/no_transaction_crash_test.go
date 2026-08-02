package migrator_test

import (
	"os/exec"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestNoTransactionCrash_PersistsProgressBeforeObserver(t *testing.T) {
	c := qt.New(t)
	helperPath := filepath.Join(t.TempDir(), "no-transaction-crash-helper")
	build := exec.Command("go", "build", "-o", helperPath, "./testdata/no_transaction_crash")
	build.Dir = "."
	c.Assert(build.Run(), qt.IsNil)

	c.Run("Ptah revision table", func(c *qt.C) {
		databasePath := filepath.Join(c.TempDir(), "ptah-crash.db")
		runNoTransactionCrashHelper(c, helperPath, databasePath, "ptah", "up", "after-checkpoint")
		conn := openNoTransactionCrashDatabase(c, databasePath)
		defer dbschema.CloseAndWarn(conn)
		c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
		c.Assert(noTransactionTableExists(c, conn, "posts"), qt.IsFalse)

		var state string
		var applied, total int
		c.Assert(
			conn.QueryRow("SELECT state, applied, total FROM schema_migrations WHERE version = 1").Scan(&state, &applied, &total),
			qt.IsNil,
		)
		c.Assert(state, qt.Equals, "pending")
		c.Assert(applied, qt.Equals, 1)
		c.Assert(total, qt.Equals, 2)
	})

	c.Run("Atlas revision table", func(c *qt.C) {
		databasePath := filepath.Join(c.TempDir(), "atlas-crash.db")
		runNoTransactionCrashHelper(c, helperPath, databasePath, "atlas", "up", "after-checkpoint")
		conn := openNoTransactionCrashDatabase(c, databasePath)
		defer dbschema.CloseAndWarn(conn)
		c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
		c.Assert(noTransactionTableExists(c, conn, "posts"), qt.IsFalse)

		var applied, total int
		var failure string
		c.Assert(
			conn.QueryRow("SELECT applied, total, COALESCE(error, '') FROM atlas_schema_revisions WHERE version = '1'").Scan(&applied, &total, &failure),
			qt.IsNil,
		)
		c.Assert(applied, qt.Equals, 1)
		c.Assert(total, qt.Equals, 2)
		c.Assert(failure, qt.Equals, "")
	})

	c.Run("Ptah down revision", func(c *qt.C) {
		databasePath := filepath.Join(c.TempDir(), "ptah-down-crash.db")
		runNoTransactionCrashHelper(c, helperPath, databasePath, "ptah", "down", "after-checkpoint")
		conn := openNoTransactionCrashDatabase(c, databasePath)
		defer dbschema.CloseAndWarn(conn)
		c.Assert(noTransactionTableExists(c, conn, "posts"), qt.IsFalse)
		c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)

		var state string
		var applied, total int
		c.Assert(
			conn.QueryRow("SELECT state, applied, total FROM schema_migrations WHERE version = 1").Scan(&state, &applied, &total),
			qt.IsNil,
		)
		c.Assert(state, qt.Equals, "pending")
		c.Assert(applied, qt.Equals, 1)
		c.Assert(total, qt.Equals, 2)
	})

	c.Run("Ptah in-flight statement", func(c *qt.C) {
		databasePath := filepath.Join(c.TempDir(), "ptah-in-flight.db")
		runNoTransactionCrashHelper(c, helperPath, databasePath, "ptah", "up", "after-execution")
		conn := openNoTransactionCrashDatabase(c, databasePath)
		defer dbschema.CloseAndWarn(conn)
		c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
		c.Assert(noTransactionTableExists(c, conn, "posts"), qt.IsFalse)

		var state, failure, failureStatement string
		var applied, total int
		c.Assert(conn.QueryRow(`
			SELECT state, applied, total, COALESCE(error, ''), COALESCE(error_stmt, '')
			FROM schema_migrations WHERE version = 1
		`).Scan(&state, &applied, &total, &failure, &failureStatement), qt.IsNil)
		c.Assert(state, qt.Equals, "pending")
		c.Assert(applied, qt.Equals, 0)
		c.Assert(total, qt.Equals, 2)
		c.Assert(failure, qt.Contains, "statement execution outcome is unknown")
		c.Assert(failureStatement, qt.Contains, "CREATE TABLE users")

		mig := migrator.NewMigrator(conn, noTransactionCrashProvider(c))
		err := mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 1})
		c.Assert(err, qt.ErrorMatches, `migration 1 has an unknown statement outcome.*omit --resume-from.*`)
	})

	c.Run("Atlas in-flight statement", func(c *qt.C) {
		databasePath := filepath.Join(c.TempDir(), "atlas-in-flight.db")
		runNoTransactionCrashHelper(c, helperPath, databasePath, "atlas", "up", "after-execution")
		conn := openNoTransactionCrashDatabase(c, databasePath)
		defer dbschema.CloseAndWarn(conn)
		c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
		c.Assert(noTransactionTableExists(c, conn, "posts"), qt.IsFalse)

		var failure, failureStatement string
		var applied, total int
		c.Assert(conn.QueryRow(`
			SELECT applied, total, COALESCE(error, ''), COALESCE(error_stmt, '')
			FROM atlas_schema_revisions WHERE version = '1'
		`).Scan(&applied, &total, &failure, &failureStatement), qt.IsNil)
		c.Assert(applied, qt.Equals, 0)
		c.Assert(total, qt.Equals, 2)
		c.Assert(failure, qt.Contains, "statement execution outcome is unknown")
		c.Assert(failureStatement, qt.Contains, "CREATE TABLE users")

		mig := migrator.NewMigrator(conn, noTransactionCrashProvider(c)).
			WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
		err := mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 1})
		c.Assert(err, qt.ErrorMatches, `migration 1 has an unknown statement outcome.*omit --resume-from.*`)
	})

	c.Run("Atlas down preserves compatibility bookkeeping", func(c *qt.C) {
		databasePath := filepath.Join(c.TempDir(), "atlas-down-crash.db")
		runNoTransactionCrashHelper(c, helperPath, databasePath, "atlas", "down", "after-checkpoint")
		conn := openNoTransactionCrashDatabase(c, databasePath)
		defer dbschema.CloseAndWarn(conn)
		c.Assert(noTransactionTableExists(c, conn, "posts"), qt.IsFalse)
		c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)

		var failure string
		var applied, total int
		c.Assert(
			conn.QueryRow("SELECT applied, total, COALESCE(error, '') FROM atlas_schema_revisions WHERE version = '1'").Scan(&applied, &total, &failure),
			qt.IsNil,
		)
		c.Assert(applied, qt.Equals, 2)
		c.Assert(total, qt.Equals, 2)
		c.Assert(failure, qt.Equals, "")
	})
}

func runNoTransactionCrashHelper(
	c *qt.C,
	helperPath, databasePath, revisionFormat, direction, crashPoint string,
) {
	c.Helper()
	run := exec.Command(helperPath, databasePath, revisionFormat, direction, crashPoint)
	err := run.Run()
	var exitErr *exec.ExitError
	c.Assert(err, qt.ErrorAs, &exitErr)
	c.Assert(exitErr.ExitCode(), qt.Equals, 73)
}

func noTransactionCrashProvider(c *qt.C) migrator.MigrationProvider {
	c.Helper()
	provider, err := migrator.NewFSMigrationProvider(fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("-- +ptah no_transaction\nCREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE posts (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("-- +ptah no_transaction\nDROP TABLE posts;\nDROP TABLE users;"),
		},
	})
	c.Assert(err, qt.IsNil)
	return provider
}

func openNoTransactionCrashDatabase(c *qt.C, databasePath string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+databasePath)
	c.Assert(err, qt.IsNil)
	return conn
}
