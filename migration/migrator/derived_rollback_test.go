package migrator_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A derived rollback runs statements its caller derived from the database at
// one set of applied versions. MigrateDownDerived runs them only while that
// set is still the applied one, which it reads under the migration lock, and
// moves the revision boundary after them.

// errDerivedHook and errDerivedApply stand for a pre-flight hook and a
// statement that refuse.
var (
	errDerivedHook  = errors.New("the pre-flight hook refused")
	errDerivedApply = errors.New("the derived statement failed")
)

// derivedRollbackFixture is a SQLite database migrated up through the
// migrations of a three-version directory that versions names, and the
// migrator over the whole directory.
type derivedRollbackFixture struct {
	conn     *dbschema.DatabaseConnection
	migrator *migrator.Migrator
}

func newDerivedRollbackFixture(c *qt.C, versions ...int64) derivedRollbackFixture {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "derived.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	all := map[int64]*migrator.Migration{
		1: migrator.CreateMigrationFromSQL(1, "one", "CREATE TABLE one (id INTEGER PRIMARY KEY);\n", "DROP TABLE one;\n"),
		2: migrator.CreateMigrationFromSQL(2, "two", "CREATE TABLE two (id INTEGER PRIMARY KEY);\n", "DROP TABLE two;\n"),
		3: migrator.CreateMigrationFromSQL(3, "three", "CREATE TABLE three (id INTEGER PRIMARY KEY);\n", "DROP TABLE three;\n"),
	}
	applied := make([]*migrator.Migration, 0, len(versions))
	for _, version := range versions {
		applied = append(applied, all[version])
	}
	c.Assert(migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(applied...)).MigrateUp(c.Context()), qt.IsNil)
	return derivedRollbackFixture{
		conn:     conn,
		migrator: migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(all[1], all[2], all[3])),
	}
}

// drop is the Apply of a derived rollback that drops tables.
func (f derivedRollbackFixture) drop(calls *int, tables ...string) func(context.Context) error {
	return func(ctx context.Context) error {
		*calls++
		for _, table := range tables {
			if err := f.conn.Writer().ExecuteSQL(ctx, "DROP TABLE "+table); err != nil {
				return err
			}
		}
		return nil
	}
}

// applied reads the applied versions back from the revision table.
func (f derivedRollbackFixture) applied(c *qt.C) []int64 {
	c.Helper()
	versions, err := f.migrator.GetAppliedMigrations(c.Context())
	c.Assert(err, qt.IsNil)
	return versions
}

// tables reads the user tables back from the catalog, in name order.
func (f derivedRollbackFixture) tables(c *qt.C) []string {
	c.Helper()
	rows, err := f.conn.QueryContext(c.Context(), `SELECT name FROM sqlite_master
WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'schema_migrations%' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// recordPlan is a pre-flight hook that keeps the plan it was handed.
func recordPlan(into *[]migrator.MigrationPlan) migrator.PreMigrationHook {
	return func(_ context.Context, plan migrator.MigrationPlan) error {
		*into = append(*into, plan)
		return nil
	}
}

// TestMigrateDownDerived_HappyPath runs the statements once, hands the hook
// the versions newest first whatever order the caller gave, and leaves the
// revision table at the target with a log attempt settled for each version.
func TestMigrateDownDerived_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		target       int64
		versions     []int64
		drop         []string
		wantPlan     migrator.MigrationPlan
		wantApplied  []int64
		wantTables   []string
		wantOutcomes []string
	}{
		{
			name:     "one version",
			target:   1,
			versions: []int64{2},
			drop:     []string{"two"},
			wantPlan: migrator.MigrationPlan{
				Direction: migrator.MigrationDirectionDown, CurrentVersion: 2, TargetVersion: 1, Versions: []int64{2},
			},
			wantApplied:  []int64{1},
			wantTables:   []string{"one"},
			wantOutcomes: []string{"v1:applied", "v2:applied", "v2:rolled_back"},
		},
		{
			name:     "two versions given oldest first",
			target:   0,
			versions: []int64{1, 2},
			drop:     []string{"two", "one"},
			wantPlan: migrator.MigrationPlan{
				Direction: migrator.MigrationDirectionDown, CurrentVersion: 2, TargetVersion: 0, Versions: []int64{2, 1},
			},
			wantApplied:  make([]int64, 0),
			wantOutcomes: []string{"v1:applied", "v2:applied", "v2:rolled_back", "v1:rolled_back"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newDerivedRollbackFixture(c, 1, 2)
			var calls int
			var plans []migrator.MigrationPlan

			err := f.migrator.MigrateDownDerived(c.Context(), migrator.DerivedRollback{
				TargetVersion: test.target,
				Versions:      test.versions,
				Apply:         f.drop(&calls, test.drop...),
			}, recordPlan(&plans))

			c.Assert(err, qt.IsNil)
			c.Assert(calls, qt.Equals, 1)
			c.Assert(plans, qt.DeepEquals, []migrator.MigrationPlan{test.wantPlan})
			c.Assert(f.applied(c), qt.DeepEquals, test.wantApplied)
			c.Assert(f.tables(c), qt.DeepEquals, test.wantTables)
			c.Assert(attemptOutcomes(c, c.Context(), f.migrator), qt.DeepEquals, test.wantOutcomes)
		})
	}
}

// TestMigrateDownDerivedStale_FailurePath derives the rollback for one set of
// applied versions and finds another under the lock: a migration applied
// since, or one rolled back since. The refusal names both sets, the
// statements and the hook never run, and the revisions stay as they were.
func TestMigrateDownDerivedStale_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		applied     []int64
		derivedFor  []int64
		wantErr     string
		wantApplied []int64
	}{
		{
			name:       "a migration applied since",
			applied:    []int64{1, 2, 3},
			derivedFor: []int64{2},
			wantErr: `the rollback to version 1 was derived for the applied migrations \[2\], ` +
				`and the migrations applied above version 1 are now \[3 2\]; nothing was rolled back`,
			wantApplied: []int64{1, 2, 3},
		},
		{
			name:       "a migration rolled back since",
			applied:    []int64{1, 2},
			derivedFor: []int64{3, 2},
			wantErr: `the rollback to version 1 was derived for the applied migrations \[3 2\], ` +
				`and the migrations applied above version 1 are now \[2\]; nothing was rolled back`,
			wantApplied: []int64{1, 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newDerivedRollbackFixture(c, test.applied...)
			before := f.tables(c)
			var calls int
			var plans []migrator.MigrationPlan

			err := f.migrator.MigrateDownDerived(c.Context(), migrator.DerivedRollback{
				TargetVersion: 1,
				Versions:      test.derivedFor,
				Apply:         f.drop(&calls, "two"),
			}, recordPlan(&plans))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			stale, ok := errors.AsType[*migrator.StaleDerivedRollbackError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(stale.TargetVersion, qt.Equals, int64(1))
			c.Assert(calls, qt.Equals, 0)
			c.Assert(plans, qt.HasLen, 0)
			c.Assert(f.applied(c), qt.DeepEquals, test.wantApplied)
			c.Assert(f.tables(c), qt.DeepEquals, before)
		})
	}
}

// TestMigrateDownDerivedHook_FailurePath refuses in the hook. The statements
// never run and nothing is logged for a rollback that did not start.
func TestMigrateDownDerivedHook_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newDerivedRollbackFixture(c, 1, 2)
	var calls int

	err := f.migrator.MigrateDownDerived(c.Context(), migrator.DerivedRollback{
		TargetVersion: 1,
		Versions:      []int64{2},
		Apply:         f.drop(&calls, "two"),
	}, func(context.Context, migrator.MigrationPlan) error { return errDerivedHook })

	c.Assert(err, qt.ErrorIs, errDerivedHook)
	c.Assert(calls, qt.Equals, 0)
	c.Assert(f.applied(c), qt.DeepEquals, []int64{1, 2})
	c.Assert(f.tables(c), qt.DeepEquals, []string{"one", "two"})
	c.Assert(attemptOutcomes(c, c.Context(), f.migrator), qt.DeepEquals, []string{"v1:applied", "v2:applied"})
}

// TestMigrateDownDerivedApply_FailurePath fails a statement. The revision
// boundary does not move, so the revision table still describes a database
// ahead of the target, and the log records the attempt as failed.
func TestMigrateDownDerivedApply_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newDerivedRollbackFixture(c, 1, 2)

	err := f.migrator.MigrateDownDerived(c.Context(), migrator.DerivedRollback{
		TargetVersion: 1,
		Versions:      []int64{2},
		Apply:         func(context.Context) error { return errDerivedApply },
	}, nil)

	c.Assert(err, qt.ErrorIs, errDerivedApply)
	c.Assert(f.applied(c), qt.DeepEquals, []int64{1, 2})
	c.Assert(attemptOutcomes(c, c.Context(), f.migrator), qt.DeepEquals, []string{"v1:applied", "v2:applied", "v2:failed"})
}

// TestMigrateDownDerivedDirty_FailurePath finds a revision a failed run left
// dirty. The rollback refuses as one through the down bodies does, before the
// statements run.
func TestMigrateDownDerivedDirty_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newDerivedRollbackFixture(c, 1, 2)
	failing := migrator.NewMigrator(f.conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "one", "CREATE TABLE one (id INTEGER PRIMARY KEY);\n", "DROP TABLE one;\n"),
		migrator.CreateMigrationFromSQL(2, "two", "CREATE TABLE two (id INTEGER PRIMARY KEY);\n", "DROP TABLE two;\n"),
		migrator.CreateMigrationFromSQL(3, "three", "SELECT * FROM no_such_table;\n", "SELECT 1;\n"),
	))
	c.Assert(failing.MigrateUp(c.Context()), qt.IsNotNil)
	var calls int

	err := f.migrator.MigrateDownDerived(c.Context(), migrator.DerivedRollback{
		TargetVersion: 1,
		Versions:      []int64{2},
		Apply:         f.drop(&calls, "two"),
	}, nil)

	dirty, ok := errors.AsType[*migrator.DirtyMigrationError](err)
	c.Assert(ok, qt.IsTrue, qt.Commentf("%v", err))
	c.Assert(dirty.Revision.Version, qt.Equals, int64(3))
	c.Assert(calls, qt.Equals, 0)
	c.Assert(f.tables(c), qt.DeepEquals, []string{"one", "two"})
}
