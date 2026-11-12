package migrationrollout

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"ptah.run/internal/dburldisplay"
	"ptah.run/migration/migrator"
)

// Executor does the work on one target. The rollout decides which target runs
// when; the executor runs it and reads it back.
type Executor interface {
	// Apply runs the release's pending migrations against target and returns
	// what the run reported. It returns when the run has ended, including
	// after ctx is canceled: the run is asked to stop, and its own report of
	// where it stopped is the result.
	Apply(ctx context.Context, target Target) Result
	// Inspect reads target's revision state against the release without
	// changing anything.
	Inspect(ctx context.Context, target Target) (*migrator.MigrationStatus, error)
}

// Result is what one target run reported.
type Result struct {
	// Run is the run's own document, and nil when it left none.
	Run *migrator.RunResult
	// Err says why there is no document, or adds to it.
	Err error
	// NotStarted reports that the run could not be started at all, so nothing
	// reached the target.
	NotStarted bool
}

// statusOf classifies a run document. The run document reads the target's
// revision table after the run, so its outcome is evidence; the process exit
// status is not consulted.
func statusOf(run *migrator.RunResult) (TargetStatus, string) {
	if run.ContractVersion != migrator.RunContractVersion {
		return TargetUnknown, fmt.Sprintf("the run document has contract version %d, and this build reads %d",
			run.ContractVersion, migrator.RunContractVersion)
	}
	switch run.Outcome {
	case migrator.RunOutcomeApplied:
		return TargetSucceeded, ""
	case migrator.RunOutcomeUpToDate:
		return TargetNoOp, ""
	case migrator.RunOutcomeFailed:
		return TargetFailed, ""
	case migrator.RunOutcomePartial:
		return TargetPartial, ""
	case migrator.RunOutcomeUnknown:
		return TargetUnknown, ""
	default:
		return TargetUnknown, fmt.Sprintf("the run reported the outcome %q, which a rollout does not count", run.Outcome)
	}
}

// recordResult writes result into state, with every credential of target
// taken out of the text it carries.
func recordResult(state *TargetState, target Target, result Result) {
	state.Planned, state.Applied, state.Version = nil, nil, ""
	state.Error, state.Failure, state.Note = nil, nil, ""
	switch {
	case result.Run != nil:
		status, note := statusOf(result.Run)
		state.Status = status
		state.Note = note
		state.Planned = versionKeys(result.Run.Planned, result.Run.PlannedKeys)
		state.Applied = versionKeys(result.Run.Applied, result.Run.AppliedKeys)
		state.Version = currentVersion(result.Run.Status)
		if result.Run.Error != nil {
			state.Error = &migrator.ErrorReport{
				Kind:    result.Run.Error.Kind,
				Message: redact(target, result.Run.Error.Message),
			}
		}
		state.Failure = result.Run.Failure
	case result.NotStarted:
		state.Status = TargetFailed
	default:
		state.Status = TargetUnknown
		state.Note = "the run left no document, so nothing says what it did; a resumed rollout reads the revision table"
	}
	if result.Err != nil && state.Error == nil {
		state.Error = &migrator.ErrorReport{Message: redact(target, result.Err.Error())}
	}
}

// redact takes target's credentials out of text: the whole of each secret the
// URL carries and every part of one, as dburldisplay does for a driver error.
func redact(target Target, text string) string {
	return dburldisplay.RedactError(target.URL, errors.New(text)).Error()
}

func versionKeys(versions []int64, keys []string) []string {
	if len(keys) == len(versions) && len(keys) > 0 {
		return keys
	}
	if len(versions) == 0 {
		return nil
	}
	spelled := make([]string, 0, len(versions))
	for _, version := range versions {
		spelled = append(spelled, strconv.FormatInt(version, 10))
	}
	return spelled
}

func currentVersion(status *migrator.MigrationStatus) string {
	if status == nil {
		return ""
	}
	if status.CurrentVersionKey != "" {
		return status.CurrentVersionKey
	}
	if status.CurrentVersion == 0 {
		return ""
	}
	return strconv.FormatInt(status.CurrentVersion, 10)
}
