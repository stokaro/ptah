package migrator

// White-box testing required: how a lost migration lock and a failed release
// combine with the run's own outcome is decided by unexported functions, and
// a loss reaches them through MigrateUp only on a live YDB whose lock session
// stops answering for longer than its session timeout. The live tests under
// integration/dbschema/ydb drive that path; these pin each combination.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dblock"
)

var lostMigrationLock = &dblock.LostError{Dialect: "ydb", Name: "ptah_migrate"}

const lostMigrationLockText = `migration lock for up: advisory lock "ptah_migrate" on ydb was lost while it was ` +
	`held: the server no longer confirms that this session owns it, so another session may hold it now; ` +
	`the run stopped there, and the revision table records what it committed`

func TestMigrationLockOutcome_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(migrationLockOutcome("up", nil, nil, nil), qt.IsNil)
}

// A lost lock and a failed release are the run's failure, whatever the run
// itself reported.
func TestMigrationLockOutcome_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		runErr     error
		lostErr    error
		releaseErr error
		wantErr    string
	}{
		{name: "the run failed", runErr: errors.New("boom"), wantErr: "boom"},
		{name: "the lock was lost under a run that succeeded", lostErr: lostMigrationLock,
			wantErr: lostMigrationLockText},
		{name: "the lock was lost and the run stopped on it", runErr: lostMigrationLock, lostErr: lostMigrationLock,
			wantErr: lostMigrationLockText},
		{name: "the lock was lost and the run failed otherwise", runErr: errors.New("boom"),
			lostErr: lostMigrationLock, wantErr: lostMigrationLockText + ` \(the run reported: boom\)`},
		{name: "the release failed after a run that succeeded", releaseErr: errors.New("session expired"),
			wantErr: "failed to release migration lock for up: session expired"},
		{name: "the release failed after a run that failed", runErr: errors.New("boom"),
			releaseErr: errors.New("session expired"),
			wantErr:    "boom; additionally failed to release migration lock: session expired"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationLockOutcome("up", test.runErr, test.lostErr, test.releaseErr), qt.ErrorMatches,
				test.wantErr)
		})
	}
}

// heldLockAnswer is a held lock whose Err answers err.
type heldLockAnswer struct{ err error }

func (h heldLockAnswer) Err() error { return h.err }

// A run asks the lock it holds, and only that lock answers.
func TestMigrationLockLost(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{name: "no lock held", ctx: context.Background()},
		{name: "a lock still held", ctx: withHeldMigrationLock(context.Background(), heldLockAnswer{})},
		{name: "a lock lost", ctx: withHeldMigrationLock(context.Background(), heldLockAnswer{err: lostMigrationLock}),
			wantErr: lostMigrationLock},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationLockLost(test.ctx), qt.ErrorIs, test.wantErr)
		})
	}
}
