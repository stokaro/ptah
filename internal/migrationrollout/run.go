package migrationrollout

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// EventKind names a change a [Runner] reports while it works.
type EventKind string

const (
	// EventStageStarted is a stage whose targets are about to start.
	EventStageStarted EventKind = "stage-started"
	// EventStageFinished is a stage whose status is decided: succeeded,
	// failed or blocked.
	EventStageFinished EventKind = "stage-finished"
	// EventTargetStarted is a target whose run has started.
	EventTargetStarted EventKind = "target-started"
	// EventTargetFinished is a target whose run has ended, or a target the
	// stage skipped.
	EventTargetFinished EventKind = "target-finished"
	// EventTargetReconciled is a target a resumed run read back.
	EventTargetReconciled EventKind = "target-reconciled"
)

// Event is one change, with copies of the stage and the target it concerns.
// Target is the zero value for a stage event.
type Event struct {
	Kind   EventKind
	Stage  StageState
	Target TargetState
}

// ResumeOptions are the decisions a resumed run takes from its operator.
type ResumeOptions struct {
	// RetryFailed runs a failed or partial target again when its revision
	// table is clean. A dirty revision row is never retried: recovering it is
	// a decision about one database, taken against that database.
	RetryFailed bool
}

// Runner schedules a plan's targets and records their outcomes in a [State].
//
// Stages run one at a time, in plan order. Within a stage at most Concurrency
// targets run at once, and they start in member order. A stage starts only
// when every stage it depends on has succeeded; otherwise it is blocked, and
// so is every target in it that has not run. Stages that do not depend on a
// failed stage still run.
//
// A Runner is used for one Run or Resume at a time.
type Runner struct {
	// Executor does the work on each target.
	Executor Executor
	// Save persists the state. It is called after every change, with no other
	// change in progress. A failure stops the rollout from starting further
	// targets, and is returned once the running ones end.
	Save func(*State) error
	// Observe, when set, receives every change as it happens, in order.
	Observe func(Event)
	// Now, when set, replaces time.Now.
	Now func() time.Time

	mu      sync.Mutex
	saveErr error
}

// stageRun is what the workers of one stage share with the loop that starts
// them.
type stageRun struct {
	stage   Stage
	state   *StageState
	stopped bool
}

// Run executes a plan from a state no target has run in.
func (r *Runner) Run(ctx context.Context, plan Plan, state *State) error {
	if err := state.checkShape(plan); err != nil {
		return err
	}
	return r.execute(ctx, plan, state)
}

// Resume continues a run recorded in state, which must be bound to plan; see
// [State.Bind].
//
// Every target the run has started is read back from its own revision table
// first, through [Executor.Inspect], and its status is decided again from
// what the table holds:
//
//   - unreadable: unknown, and not run until it can be read;
//   - a dirty revision row: failed, partial or unknown as the row says, and
//     never run again by the rollout;
//   - every migration of the release applied: succeeded, whatever was
//     recorded;
//   - clean with migrations pending after a failure: run again only with
//     [ResumeOptions.RetryFailed];
//   - clean with migrations pending after any other recorded status: run
//     again. The target's run selects only what its revision table does not
//     record, under its migration lock, so no statement is run twice.
//
// A target the run never started is read by its own run when its turn comes.
func (r *Runner) Resume(ctx context.Context, plan Plan, state *State, opts ResumeOptions) error {
	if err := state.checkShape(plan); err != nil {
		return err
	}
	r.reconcile(ctx, plan, state, opts)
	return r.execute(ctx, plan, state)
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// update applies change to state, saves it, and reports event, holding the
// lock for all three so the file and the report follow one order.
func (r *Runner) update(state *State, change func() Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	event := change()
	state.UpdatedAt = r.now()
	if r.Save != nil {
		if err := r.Save(state); err != nil && r.saveErr == nil {
			r.saveErr = fmt.Errorf("save rollout state: %w", err)
		}
	}
	if r.Observe != nil && event.Kind != "" {
		r.Observe(event)
	}
}

// halted reports that no further target may start: the run was interrupted or
// its state can no longer be saved.
func (r *Runner) halted(ctx context.Context) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return ctx.Err() != nil || r.saveErr != nil
}

func (r *Runner) execute(ctx context.Context, plan Plan, state *State) error {
	r.update(state, func() Event {
		state.Status = RunRunning
		state.Invocations++
		return Event{}
	})
	for i, stage := range plan.Stages {
		if r.halted(ctx) {
			break
		}
		stageState := &state.Stages[i]
		if blockers := unmetDependencies(stage, state); len(blockers) > 0 {
			r.block(state, stageState, blockers)
			continue
		}
		r.runStage(ctx, state, &stageRun{stage: stage, state: stageState})
	}
	r.update(state, func() Event {
		state.Status = finalStatus(state)
		if (r.saveErr != nil || ctx.Err() != nil) && undecided(state) {
			state.Status = RunInterrupted
		}
		return Event{}
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	// An interrupt that arrives after every stage is decided stopped nothing,
	// so it is not reported as the run's failure.
	if state.Status == RunInterrupted {
		return errors.Join(r.saveErr, ctx.Err())
	}
	return r.saveErr
}

// undecided reports a run with a stage it has not decided: one it never
// reached, or one an interrupt left with members not started.
func undecided(state *State) bool {
	for _, stage := range state.Stages {
		if stage.Status == StageNotStarted || stage.Status == StageRunning {
			return true
		}
	}
	return false
}

// finalStatus is the status of a run whose every stage is decided.
func finalStatus(state *State) RunStatus {
	for _, stage := range state.Stages {
		if stage.Status != StageSucceeded {
			return RunFailed
		}
	}
	return RunSucceeded
}

// unmetDependencies names the stages stage depends on that have not succeeded.
func unmetDependencies(stage Stage, state *State) []string {
	var blockers []string
	for _, dependency := range stage.DependsOn {
		if dependencyState := state.stageStateFor(dependency); dependencyState == nil || dependencyState.Status != StageSucceeded {
			blockers = append(blockers, dependency)
		}
	}
	return blockers
}

// waiting reports a target this invocation may still start.
func waiting(status TargetStatus) bool {
	return status == TargetNotStarted || status == TargetSkipped || status == TargetBlocked
}

func (r *Runner) block(state *State, stageState *StageState, blockers []string) {
	r.update(state, func() Event {
		stageState.Status = StageBlocked
		stageState.BlockedBy = blockers
		stageState.Succeeded = countSuccesses(stageState)
		note := fmt.Sprintf("blocked: stage %s did not succeed", strings.Join(blockers, ", "))
		for i := range stageState.Targets {
			if waiting(stageState.Targets[i].Status) {
				stageState.Targets[i].Status = TargetBlocked
				stageState.Targets[i].Note = note
			}
		}
		return Event{Kind: EventStageFinished, Stage: cloneStage(stageState)}
	})
}

func (r *Runner) runStage(ctx context.Context, state *State, run *stageRun) {
	r.update(state, func() Event {
		run.state.Status = StageRunning
		run.state.BlockedBy = nil
		for i := range run.state.Targets {
			target := &run.state.Targets[i]
			if waiting(target.Status) {
				target.Status = TargetNotStarted
				target.Note = ""
			}
			// A failure carried from an earlier invocation stops the stage
			// as one observed now would: the policy is about the stage, not
			// about one invocation of it.
			if run.stage.OnFailure == FailureStop && target.Status.Failure() {
				run.stopped = true
			}
		}
		return Event{Kind: EventStageStarted, Stage: cloneStage(run.state)}
	})
	slots := make(chan struct{}, run.stage.Concurrency)
	var workers sync.WaitGroup
	for i, target := range run.stage.Targets {
		targetState := &run.state.Targets[i]
		if !r.startable(targetState) {
			continue
		}
		if !acquire(ctx, slots) {
			break
		}
		if r.halted(ctx) {
			<-slots
			break
		}
		if r.skipIfStopped(state, run, targetState) {
			<-slots
			continue
		}
		r.start(state, run, targetState)
		workers.Go(func() {
			result := r.Executor.Apply(ctx, target)
			r.finishTarget(ctx, state, run, targetState, target, result)
			// Released after the outcome is recorded, so a failure that sets
			// the stop flag is seen before the slot lets another target in.
			<-slots
		})
	}
	workers.Wait()
	r.closeStage(state, run.state)
}

func (r *Runner) startable(target *TargetState) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return target.Status == TargetNotStarted
}

// acquire takes a slot, and reports false when ctx ends first.
func acquire(ctx context.Context, slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *Runner) skipIfStopped(state *State, run *stageRun, target *TargetState) bool {
	skipped := false
	r.update(state, func() Event {
		if !run.stopped {
			return Event{}
		}
		skipped = true
		target.Status = TargetSkipped
		target.Note = "skipped: the stage stops after a failure"
		return Event{Kind: EventTargetFinished, Stage: cloneStage(run.state), Target: *target}
	})
	return skipped
}

func (r *Runner) start(state *State, run *stageRun, target *TargetState) {
	r.update(state, func() Event {
		started := r.now()
		target.Status = TargetRunning
		target.Attempts++
		target.StartedAt = &started
		target.FinishedAt = nil
		target.Note = ""
		return Event{Kind: EventTargetStarted, Stage: cloneStage(run.state), Target: *target}
	})
}

func (r *Runner) finishTarget(ctx context.Context, state *State, run *stageRun, targetState *TargetState, target Target, result Result) {
	r.update(state, func() Event {
		recordResult(targetState, target, result)
		interrupted(ctx, targetState)
		finished := r.now()
		targetState.FinishedAt = &finished
		if run.stage.OnFailure == FailureStop && targetState.Status.Failure() {
			run.stopped = true
		}
		run.state.Succeeded = countSuccesses(run.state)
		return Event{Kind: EventTargetFinished, Stage: cloneStage(run.state), Target: *targetState}
	})
}

// interrupted records a target that ended in failure after the rollout was
// interrupted as unknown. The interrupt reaches the target's own run, so its
// failure may be the interrupt's rather than the migration's: a pre-flight
// hook killed by the signal reports failed exactly as one that refused. A
// recorded failure waits for a resume that retries failed targets, which would
// leave a plain resume unable to continue what an interrupt stopped. Unknown
// sends the target to its revision table instead, where a dirty row still
// stops it and a clean table with pending migrations runs again.
func interrupted(ctx context.Context, target *TargetState) {
	if ctx.Err() == nil || (target.Status != TargetFailed && target.Status != TargetPartial) {
		return
	}
	note := fmt.Sprintf("the rollout was interrupted while the target ran, so its %s outcome may be the interrupt's; "+
		"a resumed rollout reads the revision table", target.Status)
	if target.Note != "" {
		note += ": " + target.Note
	}
	target.Status, target.Note = TargetUnknown, note
}

// closeStage decides a stage whose every member has an outcome. A stage an
// interrupt left with members not started stays running.
func (r *Runner) closeStage(state *State, stageState *StageState) {
	r.update(state, func() Event {
		stageState.Succeeded = countSuccesses(stageState)
		for _, target := range stageState.Targets {
			if target.Status == TargetNotStarted || target.Status == TargetRunning {
				return Event{}
			}
		}
		stageState.Status = StageFailed
		if stageState.Succeeded >= stageState.Required {
			stageState.Status = StageSucceeded
		}
		return Event{Kind: EventStageFinished, Stage: cloneStage(stageState)}
	})
}

func countSuccesses(stageState *StageState) int {
	count := 0
	for _, target := range stageState.Targets {
		if target.Status.Success() {
			count++
		}
	}
	return count
}

// cloneStage copies a stage state for an event, so an observer reading it
// later sees the stage as it was.
func cloneStage(stageState *StageState) StageState {
	clone := *stageState
	clone.DependsOn = slices.Clone(stageState.DependsOn)
	clone.BlockedBy = slices.Clone(stageState.BlockedBy)
	clone.Targets = slices.Clone(stageState.Targets)
	return clone
}

// checkShape refuses a state whose stages and members are not the plan's, in
// the plan's order. The digest comparison in [State.Bind] already covers it
// for a state this package wrote; this is what keeps a hand-edited file from
// pairing one plan's targets with another's records.
func (s *State) checkShape(plan Plan) error {
	if len(s.Stages) != len(plan.Stages) {
		return fmt.Errorf("%w: the state holds %d stages and the plan %d", ErrPlanChanged, len(s.Stages), len(plan.Stages))
	}
	for i, stage := range plan.Stages {
		recorded := s.Stages[i]
		if recorded.Name != stage.Name || len(recorded.Targets) != len(stage.Targets) {
			return fmt.Errorf("%w: stage %d is %q in the state and %q in the plan", ErrPlanChanged, i+1, recorded.Name, stage.Name)
		}
		for j, target := range stage.Targets {
			if recorded.Targets[j].Name != target.Name {
				return fmt.Errorf("%w: stage %q lists %q in the state where the plan has %q",
					ErrPlanChanged, stage.Name, recorded.Targets[j].Name, target.Name)
			}
		}
	}
	return nil
}
