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

// derivedRollbackFixture is a SQLite database migrated up through some of the
// migrations of a directory, and the migrator over the whole directory.
type derivedRollbackFixture struct {
	conn     *dbschema.DatabaseConnection
	migrator *migrator.Migrator
}

// newDerivedRollbackFixture is the directory of versions 1, 2 and 3 with the
// migrations versions names applied.
func newDerivedRollbackFixture(c *qt.C, versions ...int64) derivedRollbackFixture {
	c.Helper()
	return newDerivedRollbackDirectory(c, []int64{1, 2, 3}, versions...)
}

// newDerivedRollbackDirectory is the directory of the migrations files names,
// each creating one table, with the migrations applied names applied.
func newDerivedRollbackDirectory(c *qt.C, files []int64, applied ...int64) derivedRollbackFixture {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "derived.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	up := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(derivedMigrations(applied)...))
	c.Assert(up.MigrateUp(c.Context()), qt.IsNil)
	return derivedRollbackFixture{
		conn:     conn,
		migrator: migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(derivedMigrations(files)...)),
	}
}

// derivedMigrations builds the migrations of versions. Each creates the table
// its version is spelled as.
func derivedMigrations(versions []int64) []*migrator.Migration {
	names := map[int64]string{1: "one", 2: "two", 3: "three", 10: "ten", 20: "twenty"}
	migrations := make([]*migrator.Migration, 0, len(versions))
	for _, version := range versions {
		name := names[version]
		migrations = append(migrations, migrator.CreateMigrationFromSQL(version, name,
			"CREATE TABLE "+name+" (id INTEGER PRIMARY KEY);\n", "DROP TABLE "+name+";\n"))
	}
	return migrations
}

// drop is the Apply of a derived rollback that drops tables.
func (f derivedRollbackFixture) drop(calls *int, tables ...string) func(context.Context) error {
	statements := make([]string, 0, len(tables))
	for _, table := range tables {
		statements = append(statements, "DROP TABLE "+table)
	}
	return f.run(calls, statements...)
}

// run is the Apply of a derived rollback that runs statements and counts its
// calls.
func (f derivedRollbackFixture) run(calls *int, statements ...string) func(context.Context) error {
	return func(ctx context.Context) error {
		*calls++
		for _, statement := range statements {
			if err := f.conn.Writer().ExecuteSQL(ctx, statement); err != nil {
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
// revision table at the newest migration at or below the target, with a log
// attempt settled for each version. That migration is the target when a file
// names it. When none does, it is the one below, as for a rollback through the
// down bodies. A migration below the target that was never applied is recorded
// as applied, because the statements were derived against the directory up to
// the target and create what it holds. A dry run hands the hook the same plan
// and runs nothing.
func TestMigrateDownDerived_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		files        []int64
		applied      []int64
		dryRun       bool
		target       int64
		versions     []int64
		statements   []string
		wantPlan     migrator.MigrationPlan
		wantCalls    int
		wantApplied  []int64
		wantTables   []string
		wantOutcomes []string
	}{
		{
			name:       "one version",
			files:      []int64{1, 2, 3},
			applied:    []int64{1, 2},
			target:     1,
			versions:   []int64{2},
			statements: []string{"DROP TABLE two"},
			wantPlan: migrator.MigrationPlan{
				Direction: migrator.MigrationDirectionDown, CurrentVersion: 2, TargetVersion: 1, Versions: []int64{2},
			},
			wantCalls:    1,
			wantApplied:  []int64{1},
			wantTables:   []string{"one"},
			wantOutcomes: []string{"v1:applied", "v2:applied", "v2:rolled_back"},
		},
		{
			name:       "two versions given oldest first",
			files:      []int64{1, 2, 3},
			applied:    []int64{1, 2},
			target:     0,
			versions:   []int64{1, 2},
			statements: []string{"DROP TABLE two", "DROP TABLE one"},
			wantPlan: migrator.MigrationPlan{
				Direction: migrator.MigrationDirectionDown, CurrentVersion: 2, TargetVersion: 0, Versions: []int64{2, 1},
			},
			wantCalls:    1,
			wantApplied:  make([]int64, 0),
			wantOutcomes: []string{"v1:applied", "v2:applied", "v2:rolled_back", "v1:rolled_back"},
		},
		{
			name:       "a target no migration names",
			files:      []int64{10, 20},
			applied:    []int64{10, 20},
			target:     15,
			versions:   []int64{20},
			statements: []string{"DROP TABLE twenty"},
			wantPlan: migrator.MigrationPlan{
				Direction: migrator.MigrationDirectionDown, CurrentVersion: 20, TargetVersion: 10, Versions: []int64{20},
			},
			wantCalls:    1,
			wantApplied:  []int64{10},
			wantTables:   []string{"ten"},
			wantOutcomes: []string{"v10:applied", "v20:applied", "v20:rolled_back"},
		},
		{
			name:       "a migration below the target never applied",
			files:      []int64{1, 2, 3},
			applied:    []int64{1, 3},
			target:     2,
			versions:   []int64{3},
			statements: []string{"DROP TABLE three", "CREATE TABLE two (id INTEGER PRIMARY KEY)"},
			wantPlan: migrator.MigrationPlan{
				Direction: migrator.MigrationDirectionDown, CurrentVersion: 3, TargetVersion: 2, Versions: []int64{3},
			},
			wantCalls:    1,
			wantApplied:  []int64{1, 2},
			wantTables:   []string{"one", "two"},
			wantOutcomes: []string{"v1:applied", "v3:applied", "v3:rolled_back"},
		},
		{
			name:       "a dry run",
			files:      []int64{1, 2, 3},
			applied:    []int64{1, 2},
			dryRun:     true,
			target:     1,
			versions:   []int64{2},
			statements: []string{"DROP TABLE two"},
			wantPlan: migrator.MigrationPlan{
				Direction: migrator.MigrationDirectionDown, CurrentVersion: 2, TargetVersion: 1, Versions: []int64{2},
			},
			wantCalls:    0,
			wantApplied:  []int64{1, 2},
			wantTables:   []string{"one", "two"},
			wantOutcomes: []string{"v1:applied", "v2:applied"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newDerivedRollbackDirectory(c, test.files, test.applied...)
			f.conn.SchemaWriter().SetDryRun(test.dryRun)
			var calls int
			var plans []migrator.MigrationPlan

			err := f.migrator.MigrateDownDerived(c.Context(), migrator.DerivedRollback{
				TargetVersion: test.target,
				Versions:      test.versions,
				Apply:         f.run(&calls, test.statements...),
			}, recordPlan(&plans))

			f.conn.SchemaWriter().SetDryRun(false)
			c.Assert(err, qt.IsNil)
			c.Assert(calls, qt.Equals, test.wantCalls)
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
		dryRun      bool
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
			name:       "a migration applied since, on a dry run",
			applied:    []int64{1, 2, 3},
			dryRun:     true,
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
			f.conn.SchemaWriter().SetDryRun(test.dryRun)
			var calls int
			var plans []migrator.MigrationPlan

			err := f.migrator.MigrateDownDerived(c.Context(), migrator.DerivedRollback{
				TargetVersion: 1,
				Versions:      test.derivedFor,
				Apply:         f.drop(&calls, "two"),
			}, recordPlan(&plans))

			f.conn.SchemaWriter().SetDryRun(false)
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

// TestMigrateDownDerivedNegativeTarget_FailurePath asks for a target below
// zero, which names no state of the database. The rollback refuses before it
// reads anything, so the statements never run.
func TestMigrateDownDerivedNegativeTarget_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newDerivedRollbackFixture(c, 1, 2)
	var calls int

	err := f.migrator.MigrateDownDerived(c.Context(), migrator.DerivedRollback{
		TargetVersion: -1,
		Versions:      []int64{1, 2},
		Apply:         f.drop(&calls, "two", "one"),
	}, nil)

	c.Assert(err, qt.ErrorMatches, `migration version must not be negative`)
	c.Assert(calls, qt.Equals, 0)
	c.Assert(f.applied(c), qt.DeepEquals, []int64{1, 2})
	c.Assert(f.tables(c), qt.DeepEquals, []string{"one", "two"})
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
