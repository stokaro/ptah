package migrator_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// The repeatable migration tests run a directory whose versioned migration
// creates a table and an audit table, and whose repeatable migrations rebuild a
// view and append their own identity to the audit table. The audit rows are
// the measurement: a repeatable migration that ran adds one, and one that did
// not adds none, whatever the log says.

const repeatableVersionedUp = `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE repeatable_runs (name TEXT NOT NULL);
`

const repeatableViewUp = `DROP VIEW IF EXISTS user_names;
CREATE VIEW user_names AS SELECT name FROM users;
INSERT INTO repeatable_runs (name) VALUES ('user_names');
`

const repeatableViewChangedUp = `DROP VIEW IF EXISTS user_names;
CREATE VIEW user_names AS SELECT id, name FROM users;
INSERT INTO repeatable_runs (name) VALUES ('user_names');
`

const repeatableAuditUp = `INSERT INTO repeatable_runs (name) VALUES ('a_first');
`

// repeatableFailingUp records its run and then fails, so a transaction that
// rolled the body back leaves no audit row and one that did not leaves it.
const repeatableFailingUp = `INSERT INTO repeatable_runs (name) VALUES ('user_names');
THIS IS NOT SQL;
`

// repeatableFiles is a directory with one versioned migration and the given
// repeatable migrations, keyed by file name.
func repeatableFiles(repeatables map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{
		"0000000001_users.up.sql":   &fstest.MapFile{Data: []byte(repeatableVersionedUp)},
		"0000000001_users.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE repeatable_runs;\nDROP TABLE users;\n")},
	}
	for name, body := range repeatables {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// openRepeatableMigrator connects to the SQLite database at dbPath and builds a
// migrator that loads the repeatable migrations of fsys. Each call models one
// run of the tool.
func openRepeatableMigrator(c *qt.C, dbPath string, fsys fstest.MapFS) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	mig, err := migrator.NewFSMigrator(conn, fsys, migrator.WithRepeatableMigrations())
	c.Assert(err, qt.IsNil)
	return conn, mig
}

// runRepeatableUp runs one up against dbPath and returns the plan the run
// selected under its lock.
func runRepeatableUp(c *qt.C, dbPath string, fsys fstest.MapFS, opts migrator.MigrateUpOptions) (migrator.MigrationPlan, error) {
	c.Helper()
	_, mig := openRepeatableMigrator(c, dbPath, fsys)
	var plan migrator.MigrationPlan
	opts.PlanObserver = func(_ context.Context, selected migrator.MigrationPlan) { plan = selected }
	err := mig.MigrateUpWithOptions(c.Context(), opts)
	return plan, err
}

// repeatableAudit reads the audit rows in the order they were written.
func repeatableAudit(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT name FROM repeatable_runs ORDER BY rowid")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// repeatableStatus reads the status the native status command prints.
func repeatableStatus(c *qt.C, dbPath string, fsys fstest.MapFS) *migrator.MigrationStatus {
	c.Helper()
	_, mig := openRepeatableMigrator(c, dbPath, fsys)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	return status
}

// repeatableTableCount counts the SQLite tables named table.
func repeatableTableCount(c *qt.C, dbPath, table string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count), qt.IsNil)
	return count
}

// h1 is the digest a repeatable migration is recorded under, computed here
// from the bytes rather than taken from the code under test.
func h1(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "h1:" + base64.StdEncoding.EncodeToString(sum[:])
}

func TestRepeatableMigrations_HappyPath(t *testing.T) {
	t.Run("a first run applies the versioned migration and then the repeatable one", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})

		plan, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(plan.Versions, qt.DeepEquals, []int64{1})
		c.Assert(plan.Repeatables, qt.DeepEquals, []migrator.RepeatableWork{{
			Identity: "user_names",
			Path:     "R__user_names.sql",
			Checksum: h1(repeatableViewUp),
			Reason:   migrator.RepeatableReasonNew,
		}})
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})

		status := repeatableStatus(c, dbPath, fsys)
		c.Assert(status.HasPendingChanges, qt.IsFalse)
		c.Assert(status.Repeatables, qt.HasLen, 1)
		c.Assert(status.Repeatables[0].State, qt.Equals, migrator.RepeatableStateApplied)
		c.Assert(status.Repeatables[0].AppliedChecksum, qt.Equals, h1(repeatableViewUp))
		c.Assert(status.Repeatables[0].AttemptOrder, qt.Equals, 1)
		c.Assert(status.Repeatables[0].NoTransaction, qt.IsFalse)
	})

	t.Run("an unchanged repeatable migration does not run again", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		plan, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(plan.Versions, qt.HasLen, 0)
		c.Assert(plan.Repeatables, qt.HasLen, 0)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})
	})

	t.Run("a changed repeatable migration runs once and then not again", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		changed := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewChangedUp})
		c.Assert(repeatableStatus(c, dbPath, changed).Repeatables[0].State, qt.Equals, migrator.RepeatableStateChanged)
		c.Assert(repeatableStatus(c, dbPath, changed).HasPendingChanges, qt.IsTrue)

		plan, err := runRepeatableUp(c, dbPath, changed, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(plan.Repeatables, qt.DeepEquals, []migrator.RepeatableWork{{
			Identity:        "user_names",
			Path:            "R__user_names.sql",
			Checksum:        h1(repeatableViewChangedUp),
			AppliedChecksum: h1(repeatableViewUp),
			Reason:          migrator.RepeatableReasonChanged,
		}})
		_, err = runRepeatableUp(c, dbPath, changed, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names", "user_names"})
		c.Assert(repeatableStatus(c, dbPath, changed).Repeatables[0].AppliedChecksum, qt.Equals, h1(repeatableViewChangedUp))
	})

	t.Run("repeatable migrations run in byte order of their identities", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{
			"R__user_names.sql":     repeatableViewUp,
			"nested/R__a_first.sql": repeatableAuditUp,
		})

		plan, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(plan.Repeatables, qt.HasLen, 2)
		c.Assert(plan.Repeatables[0].Path, qt.Equals, "nested/R__a_first.sql")
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"a_first", "user_names"})
		c.Assert(repeatableStatus(c, dbPath, fsys).Repeatables[1].AttemptOrder, qt.Equals, 2)
	})

	t.Run("a dry run predicts the work and changes nothing", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(nil), migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})

		conn, mig := openRepeatableMigrator(c, dbPath, fsys)
		conn.SchemaWriter().SetDryRun(true)
		var plan migrator.MigrationPlan
		err = mig.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{
			PlanObserver: func(_ context.Context, selected migrator.MigrationPlan) { plan = selected },
		})
		c.Assert(err, qt.IsNil)
		c.Assert(plan.Repeatables, qt.HasLen, 1)
		c.Assert(plan.Repeatables[0].Checksum, qt.Equals, h1(repeatableViewUp))
		c.Assert(repeatableAudit(c, dbPath), qt.HasLen, 0)
		c.Assert(repeatableTableCount(c, dbPath, "schema_migrations_repeatable"), qt.Equals, 0)

		applied, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(applied.Repeatables, qt.DeepEquals, plan.Repeatables)
	})

	t.Run("a pending repeatable migration counts as a pending change", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(nil), migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		status := repeatableStatus(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}))
		c.Assert(status.PendingMigrations, qt.HasLen, 0)
		c.Assert(status.HasPendingChanges, qt.IsTrue)
		c.Assert(status.Repeatables[0].State, qt.Equals, migrator.RepeatableStatePending)
		c.Assert(status.Repeatables[0].AttemptedAt, qt.IsNil)
	})

	t.Run("a provider that does not load repeatable migrations leaves the file alone", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { _ = conn.Close() })
		mig, err := migrator.NewFSMigrator(conn, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}))
		c.Assert(err, qt.IsNil)

		c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
		c.Assert(repeatableAudit(c, dbPath), qt.HasLen, 0)
		c.Assert(repeatableTableCount(c, dbPath, "schema_migrations_repeatable"), qt.Equals, 0)
	})

	t.Run("a run that retries dirty work runs the fixed repeatable migration", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableFailingUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableMigrationError))
		fixed := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})

		plan, err := runRepeatableUp(c, dbPath, fixed, migrator.MigrateUpOptions{AllowDirty: true})
		c.Assert(err, qt.IsNil)
		c.Assert(plan.Repeatables, qt.HasLen, 1)
		c.Assert(plan.Repeatables[0].Reason, qt.Equals, migrator.RepeatableReasonRetry)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})
		c.Assert(repeatableStatus(c, dbPath, fixed).Repeatables[0].State, qt.Equals, migrator.RepeatableStateApplied)
	})

	t.Run("the run document names the repeatable work it applied", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		plan, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		result := migrator.NewRunResult(migrator.RunEvidence{Plan: &plan, After: repeatableStatus(c, dbPath, fsys)})
		c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeApplied)
		c.Assert(result.PlannedRepeatables, qt.DeepEquals, plan.Repeatables)
		c.Assert(result.AppliedRepeatables, qt.DeepEquals, []string{"user_names"})
	})

	t.Run("the record of a deleted file is reported as orphaned, and an up run leaves it alone", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		deleted := repeatableFiles(nil)

		plan, err := runRepeatableUp(c, dbPath, deleted, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(plan.Repeatables, qt.HasLen, 0)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})
		status := repeatableStatus(c, dbPath, deleted)
		c.Assert(status.HasPendingChanges, qt.IsFalse)
		c.Assert(status.Repeatables, qt.HasLen, 1)
		c.Assert(status.Repeatables[0].Identity, qt.Equals, "user_names")
		c.Assert(status.Repeatables[0].State, qt.Equals, migrator.RepeatableStateOrphaned)
		c.Assert(status.Repeatables[0].Orphaned, qt.IsTrue)
		c.Assert(status.Repeatables[0].Path, qt.Equals, "")
		c.Assert(status.Repeatables[0].AppliedChecksum, qt.Equals, h1(repeatableViewUp))
	})

	t.Run("a renamed file runs under its new identity and leaves the old record orphaned", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		renamed := repeatableFiles(map[string]string{"R__names.sql": repeatableViewUp})

		plan, err := runRepeatableUp(c, dbPath, renamed, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(plan.Repeatables, qt.HasLen, 1)
		c.Assert(plan.Repeatables[0].Identity, qt.Equals, "names")
		c.Assert(plan.Repeatables[0].Reason, qt.Equals, migrator.RepeatableReasonNew)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names", "user_names"})
		records := repeatableStatus(c, dbPath, renamed).Repeatables
		c.Assert(records, qt.HasLen, 2)
		c.Assert(records[0].Identity, qt.Equals, "names")
		c.Assert(records[0].State, qt.Equals, migrator.RepeatableStateApplied)
		c.Assert(records[1].Identity, qt.Equals, "user_names")
		c.Assert(records[1].State, qt.Equals, migrator.RepeatableStateOrphaned)
	})

	t.Run("a statement that cannot run in a transaction runs in a file marked no_transaction", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__vacuum.sql": "-- +ptah no_transaction\nVACUUM;\n"})

		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		record := repeatableStatus(c, dbPath, fsys).Repeatables[0]
		c.Assert(record.State, qt.Equals, migrator.RepeatableStateApplied)
		c.Assert(record.NoTransaction, qt.IsTrue)
	})
}

func TestRepeatableMigrations_FailurePath(t *testing.T) {
	t.Run("a body that fails in its transaction takes no effect and blocks the next run", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		failing := repeatableFiles(map[string]string{"R__user_names.sql": repeatableFailingUp})

		_, err := runRepeatableUp(c, dbPath, failing, migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorMatches, `(?s)repeatable migration user_names failed and took no effect: .*`)
		c.Assert(repeatableAudit(c, dbPath), qt.HasLen, 0)
		record := repeatableStatus(c, dbPath, failing).Repeatables[0]
		c.Assert(record.State, qt.Equals, migrator.RepeatableStateFailed)
		c.Assert(record.AppliedChecksum, qt.Equals, "")
		c.Assert(record.AttemptChecksum, qt.Equals, h1(repeatableFailingUp))
		c.Assert(record.Error, qt.Not(qt.Equals), "")

		_, err = runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorAs, new(*migrator.DirtyRepeatableError))
		c.Assert(repeatableAudit(c, dbPath), qt.HasLen, 0)
	})

	t.Run("a body that fails outside a transaction says part of it may have run", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		failing := repeatableFiles(map[string]string{
			"R__user_names.sql": "-- +ptah no_transaction\n" + repeatableFailingUp,
		})

		_, err := runRepeatableUp(c, dbPath, failing, migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorMatches, `(?s)repeatable migration user_names failed outside a transaction, so part of it may have taken effect: .*`)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})
		record := repeatableStatus(c, dbPath, failing).Repeatables[0]
		c.Assert(record.State, qt.Equals, migrator.RepeatableStateFailed)
		c.Assert(record.NoTransaction, qt.IsTrue)
	})

	t.Run("a failed repeatable after applied versions is a partial run", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		failing := repeatableFiles(map[string]string{"R__user_names.sql": repeatableFailingUp})
		plan, err := runRepeatableUp(c, dbPath, failing, migrator.MigrateUpOptions{})

		result := migrator.NewRunResult(migrator.RunEvidence{Plan: &plan, After: repeatableStatus(c, dbPath, failing), Err: err})
		c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomePartial)
		c.Assert(result.Applied, qt.DeepEquals, []int64{1})
		c.Assert(result.AppliedRepeatables, qt.HasLen, 0)
	})

	t.Run("a bounded run that stops below the latest version refuses pending repeatable work", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		fsys["0000000002_more.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE more (id INTEGER);\n")}
		fsys["0000000002_more.down.sql"] = &fstest.MapFile{Data: []byte("DROP TABLE more;\n")}

		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{TargetVersion: 1})
		c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableBoundError))
		c.Assert(err, qt.ErrorMatches, `cannot run up to target version 1: .* repeatable migrations user_names are pending.*`)
		c.Assert(repeatableTableCount(c, dbPath, "users"), qt.Equals, 0)
	})

	t.Run("a rollback over a recorded repeatable migration is refused", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		_, mig := openRepeatableMigrator(c, dbPath, fsys)
		err = mig.MigrateDownTo(c.Context(), 0)
		c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableRollbackError))
		c.Assert(err, qt.ErrorMatches, `cannot roll back: this database records repeatable migrations user_names, .*`)
		c.Assert(repeatableTableCount(c, dbPath, "users"), qt.Equals, 1)
	})

	t.Run("MigrateTo refuses a directory that holds repeatable migrations", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}))

		err := mig.MigrateTo(c.Context(), 1)
		c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableMigrationsUnsupportedError))
		c.Assert(repeatableTableCount(c, dbPath, "users"), qt.Equals, 0)
	})

	t.Run("the Atlas-compatible revision format refuses repeatable migrations", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}))

		err := mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).MigrateUpWithOptions(
			c.Context(), migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorMatches, `this directory holds repeatable migrations, which this run cannot execute: the Atlas-compatible revision format .*`)
		c.Assert(repeatableTableCount(c, dbPath, "users"), qt.Equals, 0)
	})

	t.Run("a repeatable migration takes no check directive", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{
			"R__user_names.sql": "-- +ptah check name=\"seeded\" assert=\"SELECT 1\"\n" + repeatableViewUp,
		})

		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorMatches, `repeatable migration R__user_names.sql declares a -- \+ptah check directive.*`)
		c.Assert(repeatableTableCount(c, dbPath, "users"), qt.Equals, 0)
	})

	t.Run("an edited applied versioned migration is refused before any repeatable runs", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		edited := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewChangedUp})
		edited["0000000001_users.up.sql"] = &fstest.MapFile{Data: []byte(repeatableVersionedUp + "-- edited\n")}

		_, err = runRepeatableUp(c, dbPath, edited, migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorAs, new(*migrator.ChecksumMismatchError))
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names"})
	})

	t.Run("a rollback is refused while the record holds a row no file carries", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(nil))
		err = mig.MigrateDownTo(c.Context(), 0)
		c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableRollbackError))
		c.Assert(err, qt.ErrorMatches, `cannot roll back: this database records repeatable migrations user_names, .*`)
		c.Assert(repeatableTableCount(c, dbPath, "users"), qt.Equals, 1)
	})

	t.Run("a rollback through a provider that does not load repeatable migrations is refused too", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { _ = conn.Close() })
		mig, err := migrator.NewFSMigrator(conn, repeatableFiles(nil))
		c.Assert(err, qt.IsNil)

		err = mig.MigrateDownTo(c.Context(), 0)
		c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableRollbackError))
		c.Assert(repeatableTableCount(c, dbPath, "users"), qt.Equals, 1)
	})

	t.Run("a dirty record no file carries blocks every up run, a retry included", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableFailingUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableMigrationError))
		deleted := repeatableFiles(map[string]string{"R__other.sql": repeatableAuditUp})

		_, err = runRepeatableUp(c, dbPath, deleted, migrator.MigrateUpOptions{AllowDirty: true})
		c.Assert(err, qt.ErrorAs, new(*migrator.DirtyRepeatableError))
		c.Assert(err, qt.ErrorMatches,
			`(?s)repeatable migrations did not finish: user_names .*no file of the directory carries it, so it cannot run again.*"schema_migrations_repeatable".*`)
		c.Assert(repeatableAudit(c, dbPath), qt.HasLen, 0)
		status := repeatableStatus(c, dbPath, deleted)
		c.Assert(status.HasPendingChanges, qt.IsTrue)
		c.Assert(status.Repeatables[1].State, qt.Equals, migrator.RepeatableStateFailed)
		c.Assert(status.Repeatables[1].Orphaned, qt.IsTrue)
	})

	t.Run("a statement that cannot run in a transaction is refused before any migration runs", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")

		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__vacuum.sql": "VACUUM;\n"}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorMatches, `(?s).*R__vacuum.sql cannot run inside a transaction: .*VACUUM.*`)
		c.Assert(repeatableTableCount(c, dbPath, "users"), qt.Equals, 0)
		c.Assert(repeatableTableCount(c, dbPath, "schema_migrations_repeatable"), qt.Equals, 0)
	})
}

// The engine refusal names the engine, since the remedy depends on it; a
// refusal for another reason has no engine to name.
func TestRepeatableMigrationsUnsupportedError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *migrator.RepeatableMigrationsUnsupportedError
		want string
	}{
		{
			name: "engine",
			err:  &migrator.RepeatableMigrationsUnsupportedError{Dialect: "mysql", Reason: "Ptah runs repeatable migrations on PostgreSQL and SQLite only"},
			want: "this directory holds repeatable migrations, which this run cannot execute on mysql: Ptah runs repeatable migrations on PostgreSQL and SQLite only",
		},
		{
			name: "another reason",
			err:  &migrator.RepeatableMigrationsUnsupportedError{Reason: "the Atlas-compatible revision format keeps no record of them"},
			want: "this directory holds repeatable migrations, which this run cannot execute: the Atlas-compatible revision format keeps no record of them",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tc.err.Error(), qt.Equals, tc.want)
		})
	}
}
