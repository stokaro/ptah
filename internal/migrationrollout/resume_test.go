package migrationrollout_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationrollout"
	"ptah.run/migration/migrator"
)

const resumeDefinition = `
targets:
  - {name: canary, url: sqlite://canary.db}
  - {name: t1, url: sqlite://t1.db}
  - {name: t2, url: sqlite://t2.db}
  - {name: t3, url: sqlite://t3.db}
  - {name: t4, url: sqlite://t4.db}
stages:
  - {name: canary, canary: true, targets: [canary]}
  - {name: wave, depends_on: [canary], targets: [t1, t2, t3, t4], concurrency: 2, on_failure: continue}
`

// interruptedRun is the state a run leaves when it stopped with the canary
// succeeded, t1 failed, t2 still running, t3 unknown and t4 not started.
func interruptedRun(c *qt.C) (migrationrollout.Plan, *migrationrollout.State) {
	c.Helper()
	plan := mustPlan(c, resumeDefinition, nil)
	state := newState(plan)
	state.Invocations = 1
	state.Status = migrationrollout.RunInterrupted
	state.Stages[0].Status = migrationrollout.StageSucceeded
	state.Stages[0].Targets[0].Status = migrationrollout.TargetSucceeded
	state.Stages[1].Status = migrationrollout.StageRunning
	state.Stages[1].Targets[0].Status = migrationrollout.TargetFailed
	state.Stages[1].Targets[1].Status = migrationrollout.TargetRunning
	state.Stages[1].Targets[2].Status = migrationrollout.TargetUnknown
	return plan, state
}

// Each started target is read back and decided from what its revision table
// holds; only the targets that are clean and behind run again, and a target
// the run never started is not read until its own run reads it.
func TestResume_ReconcilesStartedTargetsAgainstTheirRevisionTables(t *testing.T) {
	cases := []struct {
		name         string
		inspections  map[string]inspection
		retryFailed  bool
		wantApplied  []string
		wantStatuses map[string]migrationrollout.TargetStatus
	}{
		{
			name: "a failed target is not retried unless asked",
			inspections: map[string]inspection{
				"canary": cleanAt("2"), "t1": cleanPending(), "t2": cleanPending(), "t3": cleanPending(),
			},
			wantApplied: []string{"t2", "t3", "t4"},
			wantStatuses: map[string]migrationrollout.TargetStatus{
				"canary": migrationrollout.TargetSucceeded,
				"t1":     migrationrollout.TargetFailed,
				"t2":     migrationrollout.TargetSucceeded,
				"t3":     migrationrollout.TargetSucceeded,
				"t4":     migrationrollout.TargetSucceeded,
			},
		},
		{
			name: "a failed target with a clean table is retried when asked",
			inspections: map[string]inspection{
				"canary": cleanAt("2"), "t1": cleanPending(), "t2": cleanPending(), "t3": cleanPending(),
			},
			retryFailed: true,
			wantApplied: []string{"t1", "t2", "t3", "t4"},
			wantStatuses: map[string]migrationrollout.TargetStatus{
				"canary": migrationrollout.TargetSucceeded,
				"t1":     migrationrollout.TargetSucceeded,
				"t2":     migrationrollout.TargetSucceeded,
				"t3":     migrationrollout.TargetSucceeded,
				"t4":     migrationrollout.TargetSucceeded,
			},
		},
		{
			name: "a dirty row is never retried, even when asked",
			inspections: map[string]inspection{
				"canary": cleanAt("2"), "t1": dirty(0), "t2": dirty(1), "t3": cleanPending(),
			},
			retryFailed: true,
			wantApplied: []string{"t3", "t4"},
			wantStatuses: map[string]migrationrollout.TargetStatus{
				"canary": migrationrollout.TargetSucceeded,
				"t1":     migrationrollout.TargetFailed,
				"t2":     migrationrollout.TargetPartial,
				"t3":     migrationrollout.TargetSucceeded,
				"t4":     migrationrollout.TargetSucceeded,
			},
		},
		{
			name: "a table that cannot be read leaves the target unknown",
			inspections: map[string]inspection{
				"canary": cleanAt("2"), "t1": cleanPending(), "t2": {err: errors.New("connection refused")}, "t3": cleanPending(),
			},
			wantApplied: []string{"t3", "t4"},
			wantStatuses: map[string]migrationrollout.TargetStatus{
				"canary": migrationrollout.TargetSucceeded,
				"t1":     migrationrollout.TargetFailed,
				"t2":     migrationrollout.TargetUnknown,
				"t3":     migrationrollout.TargetSucceeded,
				"t4":     migrationrollout.TargetSucceeded,
			},
		},
		{
			name: "a target recovered by hand counts as succeeded without running",
			inspections: map[string]inspection{
				"canary": cleanAt("2"), "t1": cleanAt("2"), "t2": cleanAt("2"), "t3": cleanAt("2"),
			},
			wantApplied: []string{"t4"},
			wantStatuses: map[string]migrationrollout.TargetStatus{
				"canary": migrationrollout.TargetSucceeded,
				"t1":     migrationrollout.TargetSucceeded,
				"t2":     migrationrollout.TargetSucceeded,
				"t3":     migrationrollout.TargetSucceeded,
				"t4":     migrationrollout.TargetSucceeded,
			},
		},
		{
			name: "a succeeded target that fell behind runs again",
			inspections: map[string]inspection{
				"canary": cleanPending(), "t1": cleanAt("2"), "t2": cleanAt("2"), "t3": cleanAt("2"),
			},
			wantApplied: []string{"canary", "t4"},
			wantStatuses: map[string]migrationrollout.TargetStatus{
				"canary": migrationrollout.TargetSucceeded,
				"t1":     migrationrollout.TargetSucceeded,
				"t2":     migrationrollout.TargetSucceeded,
				"t3":     migrationrollout.TargetSucceeded,
				"t4":     migrationrollout.TargetSucceeded,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan, state := interruptedRun(c)
			executor := &fakeExecutor{inspections: tc.inspections}

			err := (&migrationrollout.Runner{Executor: executor}).Resume(context.Background(), plan, state,
				migrationrollout.ResumeOptions{RetryFailed: tc.retryFailed})

			c.Assert(err, qt.IsNil)
			c.Assert(sortedNames(executor.inspectedTargets()), qt.DeepEquals, []string{"canary", "t1", "t2", "t3"})
			c.Assert(sortedNames(executor.appliedTargets()), qt.DeepEquals, tc.wantApplied)
			c.Assert(statuses(state), qt.DeepEquals, tc.wantStatuses)
			c.Assert(state.Invocations, qt.Equals, 2)
		})
	}
}

// What a resumed run read back is written beside the status, and the advice
// for a dirty row names the commands that recover it.
func TestResume_RecordsWhatItReadBack(t *testing.T) {
	c := qt.New(t)
	plan, state := interruptedRun(c)
	executor := &fakeExecutor{inspections: map[string]inspection{
		"canary": cleanAt("2"),
		"t1":     cleanPending(),
		"t2":     dirty(1),
		"t3":     {err: errors.New("dial unix /var/run/t3.sock: refused")},
	}}

	err := (&migrationrollout.Runner{Executor: executor}).Resume(context.Background(), plan, state, migrationrollout.ResumeOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(targetState(c, state, "t1").Note, qt.Equals,
		"the revision table is clean with 1 migrations pending; it runs again only when a resume retries failed targets")
	c.Assert(targetState(c, state, "t2").Note, qt.Equals,
		"the revision table holds a dirty row for migration 2, which a rollout never retries; "+
			"recover it with ptah migrations up --allow-dirty or ptah migrations repair against this target, then resume")
	c.Assert(targetState(c, state, "t3").Note, qt.Matches,
		`a resumed run could not read the revision table, so the target does not run until it can: dial .*: refused`)
}

// A stop policy that a carried failure triggered keeps the stage's untried
// members skipped; retrying the failure lets them run.
func TestResume_SkippedMembersRunOnceTheFailureClears(t *testing.T) {
	cases := []struct {
		name        string
		retryFailed bool
		wantApplied []string
		wantT2      migrationrollout.TargetStatus
	}{
		{name: "failure still recorded", retryFailed: false, wantApplied: nil, wantT2: migrationrollout.TargetSkipped},
		{name: "failure retried", retryFailed: true, wantApplied: []string{"t1", "t2", "t3", "t4"}, wantT2: migrationrollout.TargetSucceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan := mustPlan(c, replaceOnce(c, resumeDefinition, "on_failure: continue", "on_failure: stop"), nil)
			state := newState(plan)
			state.Invocations = 1
			state.Stages[0].Status = migrationrollout.StageSucceeded
			state.Stages[0].Targets[0].Status = migrationrollout.TargetSucceeded
			state.Stages[1].Status = migrationrollout.StageFailed
			state.Stages[1].Targets[0].Status = migrationrollout.TargetFailed
			for i := 1; i < 4; i++ {
				state.Stages[1].Targets[i].Status = migrationrollout.TargetSkipped
			}
			executor := &fakeExecutor{inspections: map[string]inspection{"canary": cleanAt("2"), "t1": cleanPending()}}

			err := (&migrationrollout.Runner{Executor: executor}).Resume(context.Background(), plan, state,
				migrationrollout.ResumeOptions{RetryFailed: tc.retryFailed})

			c.Assert(err, qt.IsNil)
			c.Assert(sortedNames(executor.appliedTargets()), qt.DeepEquals, tc.wantApplied)
			c.Assert(statuses(state)["t2"], qt.Equals, tc.wantT2)
		})
	}
}

// A blocked stage runs once the stage it waited for succeeds on resume.
func TestResume_BlockedStageRunsWhenItsDependencyRecovers(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, resumeDefinition, nil)
	state := newState(plan)
	state.Invocations = 1
	state.Status = migrationrollout.RunFailed
	state.Stages[0].Status = migrationrollout.StageFailed
	state.Stages[0].Targets[0].Status = migrationrollout.TargetFailed
	state.Stages[1].Status = migrationrollout.StageBlocked
	for i := range state.Stages[1].Targets {
		state.Stages[1].Targets[i].Status = migrationrollout.TargetBlocked
	}
	executor := &fakeExecutor{inspections: map[string]inspection{"canary": cleanAt("2")}}

	err := (&migrationrollout.Runner{Executor: executor}).Resume(context.Background(), plan, state, migrationrollout.ResumeOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(executor.inspectedTargets(), qt.DeepEquals, []string{"canary"})
	c.Assert(sortedNames(executor.appliedTargets()), qt.DeepEquals, []string{"t1", "t2", "t3", "t4"})
	c.Assert(state.Status, qt.Equals, migrationrollout.RunSucceeded)
	c.Assert(targetState(c, state, "canary").Note, qt.Equals,
		"a resumed run found every migration of the release applied after a recorded failed")
}

func TestResume_FailurePath(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, resumeDefinition, nil)
	other := mustPlan(c, `
targets: [{name: canary, url: sqlite://canary.db}]
stages: [{name: canary, canary: true, targets: [canary]}]
`, nil)
	state := migrationrollout.NewState(other, directoryRelease, "rollout.yaml", fixedNow)
	executor := &fakeExecutor{}

	err := (&migrationrollout.Runner{Executor: executor}).Resume(context.Background(), plan, state, migrationrollout.ResumeOptions{})

	c.Assert(err, qt.ErrorIs, migrationrollout.ErrPlanChanged)
	c.Assert(executor.inspectedTargets(), qt.HasLen, 0)
	c.Assert(executor.appliedTargets(), qt.HasLen, 0)
}

// A dirty row is classified the way the run document classifies one, from
// what the row says it committed.
func TestResume_DirtyRowOutcomes(t *testing.T) {
	cases := []struct {
		name     string
		revision migrator.MigrationRevision
		want     migrationrollout.TargetStatus
	}{
		{name: "committed nothing", revision: migrator.MigrationRevision{Version: 2, Applied: 0, Total: 2, Dirty: true}, want: migrationrollout.TargetFailed},
		{name: "committed part", revision: migrator.MigrationRevision{Version: 2, Applied: 1, Total: 2, Dirty: true}, want: migrationrollout.TargetPartial},
		{name: "says nothing", revision: migrator.MigrationRevision{Version: 2, Dirty: true}, want: migrationrollout.TargetUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan, state := interruptedRun(c)
			revision := tc.revision
			executor := &fakeExecutor{inspections: map[string]inspection{
				"canary": cleanAt("2"),
				"t1":     cleanAt("2"),
				"t2":     {status: &migrator.MigrationStatus{HasPendingChanges: true, DirtyRevision: &revision}},
				"t3":     cleanAt("2"),
			}}

			err := (&migrationrollout.Runner{Executor: executor}).Resume(context.Background(), plan, state, migrationrollout.ResumeOptions{})

			c.Assert(err, qt.IsNil)
			c.Assert(statuses(state)["t2"], qt.Equals, tc.want)
		})
	}
}
