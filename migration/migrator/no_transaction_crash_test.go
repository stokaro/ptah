package migrator_test

import (
	"crypto/sha256"
	"encoding/base64"
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

	t.Run("Ptah revision table", func(t *testing.T) {
		c := qt.New(t)
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

	t.Run("Atlas revision table", func(t *testing.T) {
		c := qt.New(t)
		databasePath := filepath.Join(c.TempDir(), "atlas-crash.db")
		runNoTransactionCrashHelper(c, helperPath, databasePath, "atlas", "up", "after-checkpoint")
		conn := openNoTransactionCrashDatabase(c, databasePath)
		defer dbschema.CloseAndWarn(conn)
		c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
		c.Assert(noTransactionTableExists(c, conn, "posts"), qt.IsFalse)

		var applied, total int
		var failure, partialHashes string
		c.Assert(
			conn.QueryRow(`
				SELECT applied, total, COALESCE(error, ''), COALESCE(partial_hashes, '')
				FROM atlas_schema_revisions WHERE version = '1'
			`).Scan(&applied, &total, &failure, &partialHashes),
			qt.IsNil,
		)
		c.Assert(applied, qt.Equals, 1)
		c.Assert(total, qt.Equals, 2)
		c.Assert(failure, qt.Equals, "")
		// The counter alone cannot be resumed from: a resume verifies the
		// committed prefix against the digest chain, so the durable checkpoint
		// has to carry it too. A `null` here would leave the interrupted run
		// unresumable by anything that checks (#887).
		c.Assert(
			partialHashes,
			qt.Equals,
			`["h1:`+crashCheckpointDigest("CREATE TABLE users (id INTEGER PRIMARY KEY);")+`"]`,
		)
	})

	// The down crash records the same progress numbers as the up crash above --
	// applied=1, total=2 -- and used to record the same state, so nothing could
	// tell the two rows apart. The recorded state now carries the direction, and
	// the operator can finish the interrupted rollback from where it stopped.
	t.Run("Ptah down revision", func(t *testing.T) {
		c := qt.New(t)
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
		c.Assert(state, qt.Equals, "pending:down")
		c.Assert(applied, qt.Equals, 1)
		c.Assert(total, qt.Equals, 2)

		mig := migrator.NewMigrator(conn, noTransactionCrashProvider(c))
		status, err := mig.GetMigrationStatus(c.Context())
		c.Assert(err, qt.IsNil)
		c.Assert(status.DirtyRevision, qt.IsNotNil)
		c.Assert(status.DirtyRevision.State, qt.Equals, "pending")
		c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)

		// Resuming runs only the down statement the crash never reached, and the
		// finished rollback removes the revision instead of recording it applied.
		c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2}), qt.IsNil)
		c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsFalse)
		c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(0))
	})

	// A rollback interrupted between the in-flight marker and the checkpoint
	// cannot say whether its statement committed. Recording the migration
	// applied over that is the one thing repair must not do quietly, and
	// resuming it would repeat SQL that may already be committed.
	t.Run("Ptah down in-flight statement", func(t *testing.T) {
		c := qt.New(t)
		databasePath := filepath.Join(c.TempDir(), "ptah-down-in-flight.db")
		runNoTransactionCrashHelper(c, helperPath, databasePath, "ptah", "down", "after-execution")
		conn := openNoTransactionCrashDatabase(c, databasePath)
		defer dbschema.CloseAndWarn(conn)

		var state, failure string
		var applied, total int
		c.Assert(conn.QueryRow(`
			SELECT state, applied, total, COALESCE(error, '')
			FROM schema_migrations WHERE version = 1
		`).Scan(&state, &applied, &total, &failure), qt.IsNil)
		c.Assert(state, qt.Equals, "pending:down")
		c.Assert(applied, qt.Equals, 0)
		c.Assert(total, qt.Equals, 2)
		c.Assert(failure, qt.Contains, "statement execution outcome is unknown")

		mig := migrator.NewMigrator(conn, noTransactionCrashProvider(c))
		c.Assert(
			mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1}),
			qt.ErrorMatches, `migration 1 stopped while rolling back and the outcome of .* is unknown.*`,
		)
		c.Assert(
			mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 1}),
			qt.ErrorMatches, `migration 1 has an unknown statement outcome.*omit --resume-from.*`,
		)
		c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(1))
	})

	t.Run("Ptah in-flight statement", func(t *testing.T) {
		c := qt.New(t)
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

	t.Run("Atlas in-flight statement", func(t *testing.T) {
		c := qt.New(t)
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

	t.Run("Atlas down persists rollback progress", func(t *testing.T) {
		c := qt.New(t)
		databasePath := filepath.Join(c.TempDir(), "atlas-down-crash.db")
		runNoTransactionCrashHelper(c, helperPath, databasePath, "atlas", "down", "after-checkpoint")
		conn := openNoTransactionCrashDatabase(c, databasePath)
		defer dbschema.CloseAndWarn(conn)
		c.Assert(noTransactionTableExists(c, conn, "posts"), qt.IsFalse)
		c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)

		var failure, operatorVersion string
		var applied, total int
		c.Assert(
			conn.QueryRow("SELECT applied, total, COALESCE(error, ''), operator_version FROM atlas_schema_revisions WHERE version = '1'").
				Scan(&applied, &total, &failure, &operatorVersion),
			qt.IsNil,
		)
		c.Assert(applied, qt.Equals, 1)
		c.Assert(total, qt.Equals, 2)
		c.Assert(failure, qt.Equals, "")
		c.Assert(operatorVersion, qt.Equals, "Ptah/down")

		mig := migrator.NewMigrator(conn, noTransactionCrashProvider(c)).
			WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
		status, err := mig.GetMigrationStatus(c.Context())
		c.Assert(err, qt.IsNil)
		c.Assert(status.DirtyRevision, qt.IsNotNil)
		c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	})
}

// crashCheckpointDigest is the digest Atlas records for a one-statement
// committed prefix: base64 of the SHA-256 of that statement's source text.
// Spelling it out here rather than reusing the production helper keeps the
// expectation independent of the code that writes the column.
func crashCheckpointDigest(statement string) string {
	sum := sha256.Sum256([]byte(statement))
	return base64.StdEncoding.EncodeToString(sum[:])
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
