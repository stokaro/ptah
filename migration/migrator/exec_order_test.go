package migrator

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestParseExecOrder(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want ExecOrder
	}{
		{name: "default", in: "", want: ExecOrderLinear},
		{name: "linear", in: "linear", want: ExecOrderLinear},
		{name: "linear skip", in: "linear-skip", want: ExecOrderLinearSkip},
		{name: "non linear", in: "non-linear", want: ExecOrderNonLinear},
		{name: "trim and case", in: " Non-Linear ", want: ExecOrderNonLinear},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ParseExecOrder(tt.in)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestParseExecOrderRejectsUnknownValue(t *testing.T) {
	c := qt.New(t)

	_, err := ParseExecOrder("latest")
	c.Assert(err, qt.ErrorMatches, `invalid exec-order "latest": expected linear, linear-skip, or non-linear`)
}

func TestPendingMigrationVersionsUsesAppliedSet(t *testing.T) {
	c := qt.New(t)

	migrations := testMigrations(1, 2, 3, 5)
	pending := pendingMigrationVersions(migrations, []int64{1, 2, 5})

	c.Assert(pending, qt.DeepEquals, []int64{3})
	c.Assert(outOfOrderMigrationVersions(pending, 5), qt.DeepEquals, []int64{3})
}

func TestMigrationsToApplyExecOrderPolicies(t *testing.T) {
	c := qt.New(t)

	migrations := testMigrations(1, 2, 3, 5)
	applied := []int64{1, 2, 5}

	linear := NewMigrator(nil, NewRegisteredMigrationProvider(migrations...))
	_, err := linear.migrationsToApply(migrations, applied, 0)
	c.Assert(err, qt.IsNotNil)
	var outOfOrderErr *OutOfOrderError
	c.Assert(err, qt.ErrorAs, &outOfOrderErr)
	c.Assert(outOfOrderErr.CurrentVersion, qt.Equals, int64(5))
	c.Assert(outOfOrderErr.Versions, qt.DeepEquals, []int64{3})

	linearSkip := linear.WithExecOrder(ExecOrderLinearSkip)
	got, err := linearSkip.migrationsToApply(migrations, applied, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(migrationVersions(got), qt.DeepEquals, []int64{})

	nonLinear := linear.WithExecOrder(ExecOrderNonLinear)
	got, err = nonLinear.migrationsToApply(migrations, applied, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(migrationVersions(got), qt.DeepEquals, []int64{3})
}

func TestValidateMigrateUpOptionsRejectsAmbiguousTarget(t *testing.T) {
	c := qt.New(t)

	err := validateMigrateUpOptions(MigrateUpOptions{TargetVersion: 2, Amount: 1})

	c.Assert(err, qt.ErrorMatches, `target version and amount cannot both be set`)
}

func TestLimitMigrationsToApply(t *testing.T) {
	c := qt.New(t)

	migrations := testMigrations(1, 2, 3)

	c.Assert(migrationVersions(limitMigrationsToApply(migrations, 0)), qt.DeepEquals, []int64{1, 2, 3})
	c.Assert(migrationVersions(limitMigrationsToApply(migrations, 2)), qt.DeepEquals, []int64{1, 2})
	c.Assert(migrationVersions(limitMigrationsToApply(migrations, 5)), qt.DeepEquals, []int64{1, 2, 3})
}

func TestMergeAppliedVersions(t *testing.T) {
	c := qt.New(t)

	c.Assert(mergeAppliedVersions([]int64{1, 3}, nil), qt.DeepEquals, []int64{1, 3})
	c.Assert(mergeAppliedVersions([]int64{3, 1}, []int64{2, 3}), qt.DeepEquals, []int64{1, 2, 3})
}

func TestMigrationsToRollbackUsesAppliedSet(t *testing.T) {
	c := qt.New(t)

	migrationMap := migrationsByVersion(testMigrations(1, 2, 3, 5))
	got, err := migrationsToRollback(migrationMap, []int64{1, 2, 5}, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(migrationVersions(got), qt.DeepEquals, []int64{5})
}

func TestMigrationsToRollbackRequiresAppliedMigrationFile(t *testing.T) {
	c := qt.New(t)

	migrationMap := migrationsByVersion(testMigrations(1, 2))
	_, err := migrationsToRollback(migrationMap, []int64{1, 2, 5}, 2)
	c.Assert(err, qt.ErrorMatches, `applied migration 5 is above target version 2 but is missing from the migration provider`)
}

func TestUpTargetVersionKeepsCurrentHighWaterForOutOfOrderMigration(t *testing.T) {
	c := qt.New(t)

	c.Assert(upTargetVersion(5, testMigrations(3)), qt.Equals, int64(5))
	c.Assert(upTargetVersion(5, testMigrations(6, 7)), qt.Equals, int64(7))
}

func TestDownTargetVersionUsesFinalAppliedVersion(t *testing.T) {
	c := qt.New(t)

	applied := []int64{1, 3, 5}
	c.Assert(downTargetVersion(applied, 4), qt.Equals, int64(3))
	c.Assert(downTargetVersion(applied, 3), qt.Equals, int64(3))
	c.Assert(downTargetVersion(applied, 2), qt.Equals, int64(1))
	c.Assert(downTargetVersion(applied, 0), qt.Equals, int64(0))
}

func testMigrations(versions ...int64) []*Migration {
	migrations := make([]*Migration, 0, len(versions))
	for _, version := range versions {
		migrations = append(migrations, &Migration{
			Version:     version,
			Description: "test",
			Up:          NoopMigrationFunc,
			Down:        NoopMigrationFunc,
		})
	}
	return migrations
}
