package migrationrollout_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationrollout"
	"ptah.run/migration/migrator"
)

const canaryAndWavesDefinition = `
targets:
  - {name: canary, url: sqlite://canary.db}
  - {name: t1, url: sqlite://t1.db}
  - {name: t2, url: sqlite://t2.db}
  - {name: t3, url: sqlite://t3.db}
  - {name: t4, url: sqlite://t4.db}
  - {name: t5, url: sqlite://t5.db}
stages:
  - {name: canary, canary: true, targets: [canary]}
  - {name: wave-1, depends_on: [canary], targets: [t1, t2, t3, t4], concurrency: 2}
  - {name: wave-2, depends_on: [wave-1], targets: [t5]}
`

func TestRun_HappyPath(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, canaryAndWavesDefinition, nil)
	state := newState(plan)
	executor := &fakeExecutor{results: map[string]migrationrollout.Result{
		"t3": withOutcome(migrator.RunOutcomeUpToDate),
	}}
	var saves int
	runner := &migrationrollout.Runner{
		Executor: executor,
		Save:     func(*migrationrollout.State) error { saves++; return nil },
		Now:      func() time.Time { return fixedNow },
	}

	err := runner.Run(context.Background(), plan, state)

	c.Assert(err, qt.IsNil)
	c.Assert(state.Status, qt.Equals, migrationrollout.RunSucceeded)
	c.Assert(state.Invocations, qt.Equals, 1)
	c.Assert(executor.appliedTargets()[0], qt.Equals, "canary")
	c.Assert(executor.appliedTargets(), qt.HasLen, 6)
	c.Assert(stageStatuses(state), qt.DeepEquals, map[string]migrationrollout.StageStatus{
		"canary": migrationrollout.StageSucceeded,
		"wave-1": migrationrollout.StageSucceeded,
		"wave-2": migrationrollout.StageSucceeded,
	})
	c.Assert(statuses(state)["t3"], qt.Equals, migrationrollout.TargetNoOp)
	canary := targetState(c, state, "canary")
	c.Assert(canary.Status, qt.Equals, migrationrollout.TargetSucceeded)
	c.Assert(canary.Attempts, qt.Equals, 1)
	c.Assert(canary.Planned, qt.DeepEquals, []string{"1", "2"})
	c.Assert(canary.Applied, qt.DeepEquals, []string{"1", "2"})
	c.Assert(canary.Version, qt.Equals, "2")
	c.Assert(*canary.StartedAt, qt.Equals, fixedNow)
	// Every start and every outcome is written down as it happens, so a
	// process that dies leaves the last thing it knew.
	c.Assert(saves >= 1+6*2+3, qt.IsTrue, qt.Commentf("saved %d times", saves))
}

// A failed canary blocks every stage that depends on it, and nothing in those
// stages runs.
func TestRun_CanaryFailureBlocksLaterStages(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, canaryAndWavesDefinition, nil)
	state := newState(plan)
	executor := &fakeExecutor{results: map[string]migrationrollout.Result{
		"canary": failed("no such table: nope"),
	}}
	runner := &migrationrollout.Runner{Executor: executor}

	err := runner.Run(context.Background(), plan, state)

	c.Assert(err, qt.IsNil)
	c.Assert(state.Status, qt.Equals, migrationrollout.RunFailed)
	c.Assert(executor.appliedTargets(), qt.DeepEquals, []string{"canary"})
	c.Assert(stageStatuses(state), qt.DeepEquals, map[string]migrationrollout.StageStatus{
		"canary": migrationrollout.StageFailed,
		"wave-1": migrationrollout.StageBlocked,
		"wave-2": migrationrollout.StageBlocked,
	})
	c.Assert(statuses(state), qt.DeepEquals, map[string]migrationrollout.TargetStatus{
		"canary": migrationrollout.TargetFailed,
		"t1":     migrationrollout.TargetBlocked,
		"t2":     migrationrollout.TargetBlocked,
		"t3":     migrationrollout.TargetBlocked,
		"t4":     migrationrollout.TargetBlocked,
		"t5":     migrationrollout.TargetBlocked,
	})
	c.Assert(state.Stages[1].BlockedBy, qt.DeepEquals, []string{"canary"})
	c.Assert(targetState(c, state, "t1").Note, qt.Equals, "blocked: stage canary did not succeed")
	c.Assert(targetState(c, state, "canary").Error.Message, qt.Equals, "no such table: nope")
}

// No more members of a stage run at once than its concurrency allows, and the
// limit is reached. Inside the bubble every run takes one second of fake time,
// so all the runs the limit admits overlap.
func TestRun_ConcurrencyCapHolds(t *testing.T) {
	cases := []struct {
		name        string
		concurrency string
		want        int
	}{
		{name: "one at a time", concurrency: "1", want: 1},
		{name: "two at a time", concurrency: "2", want: 2},
		{name: "every member at once", concurrency: "5", want: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := qt.New(t)
				plan := mustPlan(c, `
targets:
  - {name: a, url: sqlite://a.db}
  - {name: b, url: sqlite://b.db}
  - {name: c, url: sqlite://c.db}
  - {name: d, url: sqlite://d.db}
  - {name: e, url: sqlite://e.db}
stages:
  - {name: wave, targets: [a, b, c, d, e], concurrency: `+tc.concurrency+`}
`, nil)
				executor := &fakeExecutor{delays: map[string]time.Duration{
					"a": time.Second, "b": time.Second, "c": time.Second, "d": time.Second, "e": time.Second,
				}}
				state := newState(plan)
				var started []string
				runner := &migrationrollout.Runner{
					Executor: executor,
					Observe:  startedTargets(&started),
				}

				err := runner.Run(context.Background(), plan, state)

				c.Assert(err, qt.IsNil)
				c.Assert(executor.maxInFlight, qt.Equals, tc.want)
				c.Assert(started, qt.DeepEquals, []string{"a", "b", "c", "d", "e"})
				c.Assert(state.Status, qt.Equals, migrationrollout.RunSucceeded)
			})
		})
	}
}

// Under on_failure: stop a failure starts nothing further in the stage, and
// the member that was already running keeps the outcome it reported.
func TestRun_StopPreservesWorkInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := qt.New(t)
		plan := mustPlan(c, canaryAndWavesDefinition, nil)
		state := newState(plan)
		executor := &fakeExecutor{
			results: map[string]migrationrollout.Result{"t2": failed("refused")},
			delays:  map[string]time.Duration{"t1": 2 * time.Second},
		}

		var started []string
		runner := &migrationrollout.Runner{Executor: executor, Observe: startedTargets(&started)}

		err := runner.Run(context.Background(), plan, state)

		c.Assert(err, qt.IsNil)
		c.Assert(started, qt.DeepEquals, []string{"canary", "t1", "t2"})
		c.Assert(statuses(state), qt.DeepEquals, map[string]migrationrollout.TargetStatus{
			"canary": migrationrollout.TargetSucceeded,
			"t1":     migrationrollout.TargetSucceeded,
			"t2":     migrationrollout.TargetFailed,
			"t3":     migrationrollout.TargetSkipped,
			"t4":     migrationrollout.TargetSkipped,
			"t5":     migrationrollout.TargetBlocked,
		})
		c.Assert(state.Stages[1].Succeeded, qt.Equals, 1)
		c.Assert(targetState(c, state, "t3").Note, qt.Equals, "skipped: the stage stops after a failure")
	})
}

// Under on_failure: continue every member runs, every outcome is kept, and a
// stage whose successes meet its requirement lets the next one start.
func TestRun_MixedOutcomeMeetsRequirement(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, `
targets:
  - {name: a, url: sqlite://a.db}
  - {name: b, url: sqlite://b.db}
  - {name: c, url: sqlite://c.db}
  - {name: d, url: sqlite://d.db}
  - {name: e, url: sqlite://e.db}
stages:
  - {name: wave-1, targets: [a, b, c, d], on_failure: continue, require: 2, concurrency: 4}
  - {name: wave-2, targets: [e], depends_on: [wave-1]}
`, nil)
	state := newState(plan)
	executor := &fakeExecutor{results: map[string]migrationrollout.Result{
		"b": failed("refused"),
		"c": withOutcome(migrator.RunOutcomePartial),
		"d": {Err: errors.New("the run left no document: signal: killed")},
	}}

	err := (&migrationrollout.Runner{Executor: executor}).Run(context.Background(), plan, state)

	c.Assert(err, qt.IsNil)
	c.Assert(statuses(state), qt.DeepEquals, map[string]migrationrollout.TargetStatus{
		"a": migrationrollout.TargetSucceeded,
		"b": migrationrollout.TargetFailed,
		"c": migrationrollout.TargetPartial,
		"d": migrationrollout.TargetUnknown,
		"e": migrationrollout.TargetBlocked,
	})
	c.Assert(state.Stages[0].Status, qt.Equals, migrationrollout.StageFailed)
	c.Assert(state.Stages[0].Succeeded, qt.Equals, 1)
	c.Assert(targetState(c, state, "d").Error.Message, qt.Equals, "the run left no document: signal: killed")
}

// A stage that does not depend on a failed stage still runs.
func TestRun_IndependentStageRunsAfterAFailure(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, `
targets: [{name: a, url: sqlite://a.db}, {name: b, url: sqlite://b.db}, {name: c, url: sqlite://c.db}]
stages:
  - {name: first, targets: [a]}
  - {name: after-first, targets: [b], depends_on: [first]}
  - {name: elsewhere, targets: [c]}
`, nil)
	state := newState(plan)
	executor := &fakeExecutor{results: map[string]migrationrollout.Result{"a": failed("refused")}}

	err := (&migrationrollout.Runner{Executor: executor}).Run(context.Background(), plan, state)

	c.Assert(err, qt.IsNil)
	c.Assert(executor.appliedTargets(), qt.DeepEquals, []string{"a", "c"})
	c.Assert(stageStatuses(state), qt.DeepEquals, map[string]migrationrollout.StageStatus{
		"first":       migrationrollout.StageFailed,
		"after-first": migrationrollout.StageBlocked,
		"elsewhere":   migrationrollout.StageSucceeded,
	})
	c.Assert(state.Status, qt.Equals, migrationrollout.RunFailed)
}

// What a target's run reports about its database never carries the
// database's credentials into the state.
func TestRun_RedactsCredentials(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, `
targets: [{name: a, url_env: A_URL}]
stages: [{name: only, targets: [a]}]
`, map[string]string{"A_URL": credentialURL("Xk9pass", "db.internal", "app")})
	state := newState(plan)
	executor := &fakeExecutor{results: map[string]migrationrollout.Result{
		"a": failed(`connect to postgres://app:Xk9pass@db.internal/app: password "Xk9pass" rejected`),
	}}

	err := (&migrationrollout.Runner{Executor: executor}).Run(context.Background(), plan, state)

	c.Assert(err, qt.IsNil)
	encoded, err := state.Encode()
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Not(qt.Contains), "Xk9pass")
	a := targetState(c, state, "a")
	c.Assert(a.Database, qt.Equals, "postgres://app:***@db.internal/app")
	c.Assert(a.Error.Message, qt.Equals, `connect to postgres://app:***@db.internal/app: password "***" rejected`)
}

// An interrupt starts nothing further, lets the running member finish and keeps
// its outcome, and leaves the run resumable.
func TestRun_InterruptStopsStartingTargets(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, replaceOnce(c, canaryAndWavesDefinition, "concurrency: 2", "concurrency: 1"), nil)
	state := newState(plan)
	ctx, cancel := context.WithCancel(context.Background())
	c.Cleanup(cancel)
	// The interrupt arrives while t1 runs; t1 still reports its outcome.
	executor := &fakeExecutor{interruptAt: map[string]context.CancelFunc{"t1": cancel}}

	err := (&migrationrollout.Runner{Executor: executor}).Run(ctx, plan, state)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(state.Status, qt.Equals, migrationrollout.RunInterrupted)
	c.Assert(executor.appliedTargets(), qt.DeepEquals, []string{"canary", "t1"})
	c.Assert(statuses(state), qt.DeepEquals, map[string]migrationrollout.TargetStatus{
		"canary": migrationrollout.TargetSucceeded,
		"t1":     migrationrollout.TargetSucceeded,
		"t2":     migrationrollout.TargetNotStarted,
		"t3":     migrationrollout.TargetNotStarted,
		"t4":     migrationrollout.TargetNotStarted,
		"t5":     migrationrollout.TargetNotStarted,
	})
	c.Assert(stageStatuses(state)["wave-1"], qt.Equals, migrationrollout.StageRunning)
	c.Assert(stageStatuses(state)["wave-2"], qt.Equals, migrationrollout.StageNotStarted)
}

// A state that cannot be saved starts nothing further: a run whose outcomes
// cannot be recorded is not one to continue.
func TestRun_SaveFailureHalts(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, canaryAndWavesDefinition, nil)
	state := newState(plan)
	executor := &fakeExecutor{}
	// The run's first save, the canary stage's start and the canary's start
	// succeed; the canary's outcome is the first write that fails.
	disk := &failingSave{limit: 3}
	runner := &migrationrollout.Runner{Executor: executor, Save: disk.save}

	err := runner.Run(context.Background(), plan, state)

	c.Assert(err, qt.ErrorMatches, `save rollout state: no space left on device`)
	c.Assert(executor.appliedTargets(), qt.DeepEquals, []string{"canary"})
	c.Assert(state.Status, qt.Equals, migrationrollout.RunInterrupted)
}

// Events arrive in the order the changes happened.
func TestRun_ObserverSeesEveryChangeInOrder(t *testing.T) {
	c := qt.New(t)
	plan := mustPlan(c, `
targets: [{name: a, url: sqlite://a.db}, {name: b, url: sqlite://b.db}]
stages:
  - {name: first, targets: [a]}
  - {name: second, targets: [b], depends_on: [first]}
`, nil)
	state := newState(plan)
	var seen []string
	runner := &migrationrollout.Runner{
		Executor: &fakeExecutor{results: map[string]migrationrollout.Result{"a": failed("refused")}},
		Observe: func(event migrationrollout.Event) {
			seen = append(seen, strings.Join([]string{string(event.Kind), event.Stage.Name, event.Target.Name, string(event.Target.Status)}, " "))
		},
	}

	err := runner.Run(context.Background(), plan, state)

	c.Assert(err, qt.IsNil)
	c.Assert(seen, qt.DeepEquals, []string{
		"stage-started first  ",
		"target-started first a running",
		"target-finished first a failed",
		"stage-finished first  ",
		"stage-finished second  ",
	})
}

// startedTargets is an observer that records the order targets start in,
// which is the runner's decision; the order their runs reach the executor is
// the scheduler's.
func startedTargets(started *[]string) func(migrationrollout.Event) {
	return func(event migrationrollout.Event) {
		if event.Kind == migrationrollout.EventTargetStarted {
			*started = append(*started, event.Target.Name)
		}
	}
}
