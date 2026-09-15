package migrator_test

import (
	"context"
	"maps"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// targetVersionBoundFS is a three-migration ptah-format directory, the
// smallest one where a bound at the middle version leaves work above it.
func targetVersionBoundFS() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_one.up.sql":     &fstest.MapFile{Data: []byte("CREATE TABLE one (id INTEGER PRIMARY KEY);\n")},
		"0000000001_one.down.sql":   &fstest.MapFile{Data: []byte("DROP TABLE one;\n")},
		"0000000002_two.up.sql":     &fstest.MapFile{Data: []byte("CREATE TABLE two (id INTEGER PRIMARY KEY);\n")},
		"0000000002_two.down.sql":   &fstest.MapFile{Data: []byte("DROP TABLE two;\n")},
		"0000000003_three.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE three (id INTEGER PRIMARY KEY);\n")},
		"0000000003_three.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE three;\n")},
	}
}

// mergedTargetVersionFS is the directory a merge leaves behind: version 2
// arrives after version 3 is already applied, so it is pending and out of
// order.
func mergedTargetVersionFS() fstest.MapFS {
	merged := fstest.MapFS{}
	maps.Copy(merged, targetVersionBoundFS())
	delete(merged, "0000000002_two.up.sql")
	delete(merged, "0000000002_two.down.sql")
	return merged
}

func newBoundConnection(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(c.TempDir(), "bound.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newTargetVersionBoundMigrator(c *qt.C) *migrator.Migrator {
	c.Helper()
	m, err := migrator.NewFSMigrator(newBoundConnection(c), targetVersionBoundFS())
	c.Assert(err, qt.IsNil)
	return m
}

// newMergedTargetVersionMigrator applies the directory without version 2 and
// returns a migrator over the directory that has it, against the same database.
// The second migrator sees version 2 as pending below the recorded version 3.
func newMergedTargetVersionMigrator(c *qt.C, execOrder migrator.ExecOrder) *migrator.Migrator {
	c.Helper()
	conn := newBoundConnection(c)
	before, err := migrator.NewFSMigrator(conn, mergedTargetVersionFS())
	c.Assert(err, qt.IsNil)
	c.Assert(before.MigrateUp(context.Background()), qt.IsNil)
	after, err := migrator.NewFSMigrator(conn, targetVersionBoundFS())
	c.Assert(err, qt.IsNil)
	return after.WithExecOrder(execOrder)
}

func currentBoundVersion(c *qt.C, m *migrator.Migrator) int64 {
	c.Helper()
	status, err := m.GetMigrationStatus(context.Background())
	c.Assert(err, qt.IsNil)
	return status.CurrentVersion
}

func TestMigrateUpRefuseTargetVersionAlreadyPassed_HappyPath(t *testing.T) {
	t.Run("a reachable target applies its prefix", func(t *testing.T) {
		c := qt.New(t)
		m := newTargetVersionBoundMigrator(c)

		err := m.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{
			TargetVersion:                    2,
			RefuseTargetVersionAlreadyPassed: true,
		})

		c.Assert(err, qt.IsNil)
		c.Assert(currentBoundVersion(c, m), qt.Equals, int64(2))
	})

	t.Run("a target the history records exactly is reached, not passed", func(t *testing.T) {
		c := qt.New(t)
		m := newTargetVersionBoundMigrator(c)
		c.Assert(m.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{TargetVersion: 2}), qt.IsNil)

		err := m.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{
			TargetVersion:                    2,
			RefuseTargetVersionAlreadyPassed: true,
		})

		c.Assert(err, qt.IsNil)
		c.Assert(currentBoundVersion(c, m), qt.Equals, int64(2))
	})
}

func TestMigrateUpRefuseTargetVersionAlreadyPassed_FailurePath(t *testing.T) {
	c := qt.New(t)
	m := newTargetVersionBoundMigrator(c)
	c.Assert(m.MigrateUp(context.Background()), qt.IsNil)

	err := m.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{
		TargetVersion:                    1,
		RefuseTargetVersionAlreadyPassed: true,
	})

	c.Assert(err, qt.ErrorMatches, `cannot migrate up to version 1: the database already records version 3`)
	var passed *migrator.TargetVersionPassedError
	c.Assert(err, qt.ErrorAs, &passed)
	c.Assert(passed.TargetVersion, qt.Equals, int64(1))
	c.Assert(passed.CurrentVersion, qt.Equals, int64(3))
	c.Assert(currentBoundVersion(c, m), qt.Equals, int64(3))
}

// TestMigrateUpTargetVersionLeftPendingByExecOrder_FailurePath pins what the
// refusal blames. The target is in the directory and still pending, and
// linear-skip is what leaves it there, so a message about the recorded version
// sends the operator to look at the database rather than at --exec-order.
func TestMigrateUpTargetVersionLeftPendingByExecOrder_FailurePath(t *testing.T) {
	c := qt.New(t)
	m := newMergedTargetVersionMigrator(c, migrator.ExecOrderLinearSkip)

	err := m.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{
		TargetVersion:                    2,
		RefuseTargetVersionAlreadyPassed: true,
	})

	c.Assert(err, qt.ErrorMatches,
		`cannot migrate up to version 2: execution order linear-skip leaves it pending because it sorts below the recorded version 3`)
	var skipped *migrator.TargetVersionSkippedError
	c.Assert(err, qt.ErrorAs, &skipped)
	c.Assert(skipped.TargetVersion, qt.Equals, int64(2))
	c.Assert(skipped.CurrentVersion, qt.Equals, int64(3))
	c.Assert(skipped.ExecOrder, qt.Equals, migrator.ExecOrderLinearSkip)
	c.Assert(currentBoundVersion(c, m), qt.Equals, int64(3))
}

// TestMigrateUpTargetVersionReachedByExecOrder_HappyPath is what the refusal
// above points the operator at: the same directory, the same target, and an
// execution order that runs the out-of-order migration.
func TestMigrateUpTargetVersionReachedByExecOrder_HappyPath(t *testing.T) {
	c := qt.New(t)
	m := newMergedTargetVersionMigrator(c, migrator.ExecOrderNonLinear)

	var planned []int64
	err := m.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{
		TargetVersion:                    2,
		RefuseTargetVersionAlreadyPassed: true,
		PlanObserver: func(_ context.Context, plan migrator.MigrationPlan) {
			planned = plan.Versions
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(planned, qt.DeepEquals, []int64{2})
}

// TestMigrateUpTargetVersionWithoutRefusal_HappyPath is the control for the
// refusal above. A caller that leaves the field clear keeps the empty plan and
// the nil error, so the divergence is a policy this caller selects rather than
// a behavior change every caller of TargetVersion inherits -- which is what
// `ptah-compat migrate apply --to-version` depends on.
func TestMigrateUpTargetVersionWithoutRefusal_HappyPath(t *testing.T) {
	c := qt.New(t)
	m := newTargetVersionBoundMigrator(c)
	c.Assert(m.MigrateUp(context.Background()), qt.IsNil)

	var planned []int64
	err := m.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{
		TargetVersion: 1,
		PlanObserver: func(_ context.Context, plan migrator.MigrationPlan) {
			planned = plan.Versions
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(planned, qt.HasLen, 0)
	c.Assert(currentBoundVersion(c, m), qt.Equals, int64(3))
}
