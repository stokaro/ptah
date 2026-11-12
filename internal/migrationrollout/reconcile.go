package migrationrollout

import (
	"context"
	"fmt"
	"sync"

	"ptah.run/migration/migrator"
)

// reconcile reads back every target the run has started, a stage at a time and
// at most the stage's concurrency at once, and decides its status again from
// its revision table.
func (r *Runner) reconcile(ctx context.Context, plan Plan, state *State, opts ResumeOptions) {
	for i, stage := range plan.Stages {
		stageState := &state.Stages[i]
		slots := make(chan struct{}, stage.Concurrency)
		var workers sync.WaitGroup
		for j, target := range stage.Targets {
			targetState := &stageState.Targets[j]
			if !targetState.Status.attempted() {
				continue
			}
			if !acquire(ctx, slots) {
				break
			}
			workers.Go(func() {
				status, err := r.Executor.Inspect(ctx, target)
				r.update(state, func() Event {
					reconcileTarget(targetState, target, status, err, opts)
					stageState.Succeeded = countSuccesses(stageState)
					return Event{Kind: EventTargetReconciled, Stage: cloneStage(stageState), Target: *targetState}
				})
				<-slots
			})
		}
		workers.Wait()
	}
}

// reconcileTarget decides a started target's status from its revision table.
// See [Runner.Resume] for the rules.
func reconcileTarget(state *TargetState, target Target, status *migrator.MigrationStatus, err error, opts ResumeOptions) {
	recorded := state.Status
	switch {
	case err != nil:
		state.Status = TargetUnknown
		state.Note = fmt.Sprintf("a resumed run could not read the revision table, so the target does not run until it can: %s",
			redact(target, err.Error()))
	case status.DirtyRevision != nil:
		run := migrator.NewRunResult(migrator.RunEvidence{After: status})
		state.Status, _ = statusOf(&run)
		state.Version = currentVersion(status)
		state.Note = fmt.Sprintf("the revision table holds a dirty row for migration %s, which a rollout never retries; "+
			"recover it with ptah migrations up --allow-dirty or ptah migrations repair against this target, then resume",
			dirtyVersion(status.DirtyRevision))
	case releaseApplied(status):
		state.Version = currentVersion(status)
		if !recorded.Success() {
			state.Status = TargetSucceeded
			state.Note = fmt.Sprintf("a resumed run found every migration of the release applied after a recorded %s", recorded)
		}
	case recorded == TargetFailed || recorded == TargetPartial:
		state.Version = currentVersion(status)
		if opts.RetryFailed {
			state.Status = TargetNotStarted
			state.Note = fmt.Sprintf("runs again: the revision table is clean after a recorded %s, and the resume retries failed targets", recorded)
			return
		}
		state.Note = fmt.Sprintf("the revision table is clean with %d migrations pending; it runs again only when a resume retries failed targets",
			len(status.PendingMigrations))
	default:
		state.Version = currentVersion(status)
		state.Status = TargetNotStarted
		state.Note = fmt.Sprintf("runs again: the revision table is clean with %d migrations pending after a recorded %s",
			len(status.PendingMigrations), recorded)
	}
}

// releaseApplied reports a revision table that records every migration of the
// release as applied, with nothing dirty, pending, changed or unknown to the
// release.
func releaseApplied(status *migrator.MigrationStatus) bool {
	if status.HasPendingChanges || status.DirtyRevision != nil || len(status.PendingMigrations) > 0 ||
		len(status.MissingMigrations) > 0 || len(status.OutOfOrderMigrations) > 0 {
		return false
	}
	for _, record := range status.Migrations {
		if record.State != migrator.MigrationStateApplied && record.State != migrator.MigrationStateCheckpointCovered {
			return false
		}
	}
	return true
}

func dirtyVersion(revision *migrator.MigrationRevision) string {
	if revision.AtlasVersion != "" {
		return revision.AtlasVersion
	}
	return fmt.Sprintf("%d", revision.Version)
}
