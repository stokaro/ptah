package migrator

import (
	"errors"
	"slices"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

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
	// Failure names the statement the run stopped at and the code the database
	// answered with. It is present only when a statement of a migration was
	// sent and failed; a run that stopped before any statement ran has none.
	Failure *RunFailure `json:"failure,omitempty"`
	// Status is the history after the run, so one document answers both what
	// was asked and what the database now holds. It is absent when the status
	// could not be read, which is itself why the outcome is unknown.
	Status *MigrationStatus `json:"status,omitempty"`
}

// RunFailure identifies the statement a failed run stopped at, and the code
// the database answered with.
//
// It carries no text. [RunResult.Error] quotes what the database said, and that
// can include a row of data or the statement itself; a caller that must not
// publish data reads this instead.
type RunFailure struct {
	// Version is the version of the migration that failed.
	Version int64 `json:"version"`
	// VersionKey is that migration's exact revision identity, spelled as
	// [RunResult.PlannedKeys] spells it. It is always present, because an
	// empty string is itself an identity a Flyway repeatable migration records.
	VersionKey string `json:"version_key"`
	// Statement is the 1-based position of the failed statement in the
	// migration body, and Statements is how many statements the body holds.
	Statement  int `json:"statement"`
	Statements int `json:"statements"`
	// SQLState is the SQLSTATE the server answered with, read from a
	// PostgreSQL-protocol or MySQL-protocol driver error. It is empty when the
	// server gave no answer, such as after a lost connection or a canceled
	// statement, and on other engines.
	SQLState string `json:"sqlstate,omitempty"`
	// ErrorNumber is the server's error number on the MySQL protocol, such as
	// 1146 for a table that does not exist. It is zero on other engines.
	ErrorNumber int `json:"error_number,omitempty"`
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
		result.Failure = runFailure(evidence.Err)
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

// runFailure reads the failed statement out of a run's error.
//
// Both halves have to be in the chain: the migration the migrator was running,
// and a statement it sent. A refusal raised before any statement ran -- the
// transaction preflight, a statement validator -- names a statement too, but
// the database never saw it, so it is not a failure the database answered.
func runFailure(err error) *RunFailure {
	migration, ok := errors.AsType[*failedMigrationError](err)
	if !ok {
		return nil
	}
	statement, ok := errors.AsType[*MigrationExecutionError](migration.err)
	if !ok || !statement.sent {
		return nil
	}
	failure := &RunFailure{
		Version:    migration.version,
		VersionKey: migration.versionKey,
		Statement:  statement.StatementIndex,
		Statements: statement.Total,
	}
	if serverErr, ok := errors.AsType[*pgconn.PgError](statement.Err); ok {
		failure.SQLState = serverErr.Code
	}
	if serverErr, ok := errors.AsType[*mysqldriver.MySQLError](statement.Err); ok {
		failure.ErrorNumber = int(serverErr.Number)
		if serverErr.SQLState != [5]byte{} {
			failure.SQLState = string(serverErr.SQLState[:])
		}
	}
	return failure
}
