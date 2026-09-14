package migrator

import "slices"

// RunContractVersion is the version of the machine-readable migration run
// document.
//
// It rises when a field changes meaning or leaves; a new field a reader may
// ignore is not a new version. A consumer that does not know the version it
// reads refuses the document rather than reading the fields it recognizes out
// of a shape that means something else.
const RunContractVersion = 1

// RunOutcome is what a run's own evidence says happened.
//
// The evidence is the revision table, not the exit code: a lost process and a
// lost answer look identical from outside, and the database is the only witness
// that was there.
type RunOutcome string

const (
	// RunOutcomeUpToDate means the run selected nothing because nothing was
	// pending.
	RunOutcomeUpToDate RunOutcome = "up-to-date"
	// RunOutcomeApplied means every selected migration is recorded applied and
	// no revision row is dirty.
	RunOutcomeApplied RunOutcome = "applied"
	// RunOutcomeDryRun means the run was asked to change nothing, and did not.
	RunOutcomeDryRun RunOutcome = "dry-run"
	// RunOutcomeFailed means the run stopped and the database reflects only the
	// migrations it records as applied. The migration that failed committed
	// nothing: its dirty row counts no applied statements.
	RunOutcomeFailed RunOutcome = "failed"
	// RunOutcomePartial means the migration that failed committed some of its
	// statements and not the rest. Recovery is Ptah's own resume path, which
	// has the row's partial digest to prove which statements it may skip; it is
	// never a retry of the whole file.
	RunOutcomePartial RunOutcome = "partial"
	// RunOutcomeUnknown means the evidence could not be read, or a dirty row
	// does not say how far it got. Nothing may decide from this that the
	// migration did or did not run.
	RunOutcomeUnknown RunOutcome = "unknown"
)

// RunResult is the machine-readable record of one migration run.
type RunResult struct {
	// ContractVersion is [RunContractVersion].
	ContractVersion int `json:"contract_version"`
	// Direction is the direction the run was asked for.
	Direction MigrationDirection `json:"direction"`
	// Outcome is what the evidence says happened.
	Outcome RunOutcome `json:"outcome"`
	// Planned and PlannedKeys are the migrations the run selected while holding
	// the migration lock, which is not the same as the pending list it started
	// from: a limit, a target version or a checkpoint narrows it.
	Planned     []int64  `json:"planned,omitempty"`
	PlannedKeys []string `json:"planned_keys,omitempty"`
	// Applied and AppliedKeys are the selected migrations the history records
	// as applied after the run, in selection order.
	Applied     []int64  `json:"applied,omitempty"`
	AppliedKeys []string `json:"applied_keys,omitempty"`
	// Error is the run's error message, present whenever the run failed. It is
	// the message the operator already prints; it is not evidence about the
	// database.
	Error string `json:"error,omitempty"`
	// Status is the history after the run, so one document answers both what
	// was asked and what the database now holds. It is absent when the status
	// could not be read, which is itself why the outcome is unknown.
	Status *MigrationStatus `json:"status,omitempty"`
}

// RunEvidence is what a caller saw of one run.
type RunEvidence struct {
	// Direction is the direction the run was asked for.
	Direction MigrationDirection
	// Plan is the plan the migrator selected, and nil when the run never
	// selected one.
	Plan *MigrationPlan
	// After is the status read once the run returned, and nil when it could not
	// be read.
	After *MigrationStatus
	// Err is the run's error, and nil when it returned cleanly.
	Err error
	// DryRun reports that the run was asked to change nothing.
	DryRun bool
}

// NewRunResult classifies one run from its own evidence.
//
// The classification never reads the process's exit status, because a run that
// died between its last statement and its answer produces the same exit status
// as one that never started. What the revision table holds afterwards is the
// only account of the run that was present for it.
func NewRunResult(evidence RunEvidence) RunResult {
	result := RunResult{
		ContractVersion: RunContractVersion,
		Direction:       evidence.Direction,
		Status:          evidence.After,
	}
	if result.Direction == "" {
		result.Direction = MigrationDirectionUp
	}
	if evidence.Err != nil {
		result.Error = evidence.Err.Error()
	}
	if evidence.Plan != nil {
		result.Planned = slices.Clone(evidence.Plan.Versions)
		result.PlannedKeys = slices.Clone(evidence.Plan.VersionKeys)
	}
	result.Applied, result.AppliedKeys = appliedFromPlan(result.Planned, result.PlannedKeys, evidence.After)
	result.Outcome = runOutcome(evidence, result)
	return result
}

func runOutcome(evidence RunEvidence, result RunResult) RunOutcome {
	if evidence.After == nil {
		return RunOutcomeUnknown
	}
	if dirty := evidence.After.DirtyRevision; dirty != nil {
		switch {
		case dirty.Total <= 0:
			return RunOutcomeUnknown
		case dirty.Applied <= 0:
			return RunOutcomeFailed
		default:
			return RunOutcomePartial
		}
	}
	if evidence.DryRun {
		return RunOutcomeDryRun
	}
	if evidence.Err != nil {
		return RunOutcomeFailed
	}
	if len(result.Planned) == 0 {
		return RunOutcomeUpToDate
	}
	if len(result.Applied) == len(result.Planned) {
		return RunOutcomeApplied
	}
	return RunOutcomePartial
}

// appliedFromPlan names the selected migrations the history records as
// applied, in selection order.
//
// It reads the applied identities rather than the version numbers, because a
// version does not identify a row that carries an opaque Atlas token, and it
// falls back to the version list for a plan that carries no keys.
func appliedFromPlan(planned []int64, plannedKeys []string, after *MigrationStatus) ([]int64, []string) {
	if after == nil || len(planned) == 0 {
		return nil, nil
	}
	appliedVersions := make(map[int64]struct{}, len(after.AppliedMigrations))
	for _, version := range after.AppliedMigrations {
		appliedVersions[version] = struct{}{}
	}
	appliedKeys := make(map[string]struct{}, len(after.AppliedMigrationKeys))
	for _, key := range after.AppliedMigrationKeys {
		appliedKeys[key] = struct{}{}
	}
	versions := make([]int64, 0, len(planned))
	keys := make([]string, 0, len(plannedKeys))
	for index, version := range planned {
		key := ""
		if index < len(plannedKeys) {
			key = plannedKeys[index]
		}
		if key != "" {
			if _, applied := appliedKeys[key]; !applied {
				continue
			}
		} else if _, applied := appliedVersions[version]; !applied {
			continue
		}
		versions = append(versions, version)
		if key != "" {
			keys = append(keys, key)
		}
	}
	if len(versions) == 0 {
		return nil, nil
	}
	return versions, keys
}
