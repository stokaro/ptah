package generator

// White-box testing required: Close's whole job is to drop the migration
// directory handle a plan holds, and that handle is the unexported dir field.
// Observing it directly is what separates a Close that releases from one that
// only marks the plan spent -- on Unix nothing else can tell the two apart,
// because an open directory descriptor does not stop the directory from being
// removed. The platform where it does is the one this cannot run on.

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMigrationPlanClose_ReleasesTheDirectoryHandle(t *testing.T) {
	c := qt.New(t)
	plan := newBoundMigrationPlan(c)

	plan.Close()

	c.Assert(plan.dir, qt.IsNil)
}

func TestMigrationPlanClose_IsIdempotent(t *testing.T) {
	c := qt.New(t)
	plan := newBoundMigrationPlan(c)
	plan.Close()

	plan.Close()

	c.Assert(plan.dir, qt.IsNil)
	c.Assert(plan.closed, qt.IsTrue)
}

func TestMigrationPlanClose_OnNilPlanDoesNothing(t *testing.T) {
	c := qt.New(t)
	var plan *MigrationPlan

	plan.Close()

	c.Assert(plan, qt.IsNil)
}

// newBoundMigrationPlan builds the smallest plan that owns a directory handle.
// It does not go through PlanMigration because the subject here is the handle's
// release and not the planning that produced it.
func newBoundMigrationPlan(c *qt.C) *MigrationPlan {
	c.Helper()
	root := c.TempDir()
	outputDir := filepath.Join(root, "migrations")
	writer, err := bindPlannedMigrationDir(root, outputDir)
	c.Assert(err, qt.IsNil)
	plan := &MigrationPlan{outputDir: outputDir, dir: writer}
	c.Cleanup(plan.Close)
	return plan
}
