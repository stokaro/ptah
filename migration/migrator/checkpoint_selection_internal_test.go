package migrator

// White-box testing required: this file exercises the unexported checkpoint
// selection helpers (checkpointBootstrap, checkpointFloor, checkpointRunnable,
// and the checkpoint-aware pending computation) that determine which migrations
// run, which are not observable through the public migrator API alone.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func checkpointMig(version int64, isCheckpoint bool) *Migration {
	return &Migration{Version: version, IsCheckpoint: isCheckpoint}
}

// A representative directory: ordinary history at 1,2,3, a checkpoint at 5, and
// post-checkpoint history at 7,9.
func checkpointSampleMigrations() []*Migration {
	return []*Migration{
		checkpointMig(1, false),
		checkpointMig(2, false),
		checkpointMig(3, false),
		checkpointMig(5, true),
		checkpointMig(7, false),
		checkpointMig(9, false),
	}
}

func TestCheckpointBootstrap_FreshDatabasePicksNewestCheckpoint(t *testing.T) {
	c := qt.New(t)
	migrations := []*Migration{
		checkpointMig(1, false),
		checkpointMig(5, true),
		checkpointMig(8, true),
		checkpointMig(9, false),
	}
	bootstrap := checkpointBootstrap(migrations, nil, 0)
	c.Assert(bootstrap, qt.IsNotNil)
	c.Assert(bootstrap.Version, qt.Equals, int64(8))
}

func TestCheckpointBootstrap_ExistingDatabaseNeverBootstraps(t *testing.T) {
	c := qt.New(t)
	bootstrap := checkpointBootstrap(checkpointSampleMigrations(), []int64{1}, 0)
	c.Assert(bootstrap, qt.IsNil)
}

func TestCheckpointBootstrap_TargetExcludesNewerCheckpoint(t *testing.T) {
	c := qt.New(t)
	migrations := []*Migration{
		checkpointMig(5, true),
		checkpointMig(8, true),
	}
	bootstrap := checkpointBootstrap(migrations, nil, 6)
	c.Assert(bootstrap, qt.IsNotNil)
	c.Assert(bootstrap.Version, qt.Equals, int64(5))
}

func TestCheckpointBootstrap_NoCheckpointReturnsNil(t *testing.T) {
	c := qt.New(t)
	migrations := []*Migration{checkpointMig(1, false), checkpointMig(2, false)}
	c.Assert(checkpointBootstrap(migrations, nil, 0), qt.IsNil)
}

func TestCheckpointFloor_BootstrapVersion(t *testing.T) {
	c := qt.New(t)
	migrations := checkpointSampleMigrations()
	bootstrap := checkpointBootstrap(migrations, nil, 0)
	c.Assert(checkpointFloor(migrations, nil, bootstrap), qt.Equals, int64(5))
}

func TestCheckpointFloor_HighestAppliedCheckpoint(t *testing.T) {
	c := qt.New(t)
	migrations := checkpointSampleMigrations()
	// Existing database with the checkpoint applied: no bootstrap, floor is the
	// applied checkpoint version.
	floor := checkpointFloor(migrations, []int64{5, 7}, nil)
	c.Assert(floor, qt.Equals, int64(5))
}

func TestCheckpointFloor_NoAppliedCheckpointIsZero(t *testing.T) {
	c := qt.New(t)
	migrations := checkpointSampleMigrations()
	// History applied normally, checkpoint file present but never run.
	c.Assert(checkpointFloor(migrations, []int64{1, 2}, nil), qt.Equals, int64(0))
}

func TestPendingMigrationVersions_FreshDatabaseBootstrapsAndSkipsSquashed(t *testing.T) {
	c := qt.New(t)
	pending := pendingMigrationVersions(checkpointSampleMigrations(), nil)
	// The checkpoint (5) plus post-checkpoint history (7, 9); squashed 1,2,3 are
	// not pending.
	c.Assert(pending, qt.DeepEquals, []int64{5, 7, 9})
}

func TestPendingMigrationVersions_ExistingDatabaseIgnoresCheckpointAndSquashed(t *testing.T) {
	c := qt.New(t)
	// Bootstrapped earlier: checkpoint 5 is applied. Only genuinely new
	// post-checkpoint migrations are pending; the checkpoint and squashed
	// history are not.
	pending := pendingMigrationVersions(checkpointSampleMigrations(), []int64{5})
	c.Assert(pending, qt.DeepEquals, []int64{7, 9})
}

func TestPendingMigrationVersions_NormalHistoryIgnoresUnrunCheckpoint(t *testing.T) {
	c := qt.New(t)
	// Database migrated through ordinary history (1,2,3 applied) with a
	// checkpoint file present but never run. The checkpoint is not pending and
	// nothing below it is squashed.
	pending := pendingMigrationVersions(checkpointSampleMigrations(), []int64{1, 2, 3})
	c.Assert(pending, qt.DeepEquals, []int64{7, 9})
}

func TestCheckpointRunnable(t *testing.T) {
	c := qt.New(t)
	bootstrap := checkpointMig(5, true)
	otherCheckpoint := checkpointMig(8, true)
	// Only the bootstrap checkpoint runs.
	c.Assert(checkpointRunnable(bootstrap, bootstrap, 5), qt.IsTrue)
	c.Assert(checkpointRunnable(otherCheckpoint, bootstrap, 5), qt.IsFalse)
	// Ordinary migration below the floor is squashed; at or above runs.
	c.Assert(checkpointRunnable(checkpointMig(3, false), bootstrap, 5), qt.IsFalse)
	c.Assert(checkpointRunnable(checkpointMig(7, false), bootstrap, 5), qt.IsTrue)
}
