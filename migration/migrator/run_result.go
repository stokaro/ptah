package migrator

import (
	"errors"
	"slices"

	"ptah.run/internal/errorkind"
	"ptah.run/internal/servercode"
)

// RunContractVersion is the version of the machine-readable migration run
// document.
//
// It rises when a field changes meaning or leaves; a new field a reader may
// ignore is not a new version, and neither is a new error kind. A consumer
// that does not know the version it reads refuses the document rather than
// reading the fields it recognizes out of a shape that means something else.
//
// Version 2 carries error as an object with a kind and a message, where
// version 1 carried the message alone, and reports a run that stopped before
// it selected any migration as failed with no status.
const RunContractVersion = 2

// RunOutcome is what a run's own evidence says happened.
//
// The evidence is the revision table, not the exit code: a lost process and a
// lost answer look identical from outside, and the database is the only witness
// that was there.
type RunOutcome string

const (
	// RunOutcomeUpToDate means the run selected nothing because nothing was
	// pending, or for a rollback, because nothing above the target is applied.
	RunOutcomeUpToDate RunOutcome = "up-to-date"
	// RunOutcomeApplied means every selected migration took effect and no
	// revision row is dirty: an up run's migrations are recorded applied, and a
	// rollback's are no longer recorded applied.
	RunOutcomeApplied RunOutcome = "applied"
	// RunOutcomeDryRun means the run was asked to change nothing, and did not.
	RunOutcomeDryRun RunOutcome = "dry-run"
	// RunOutcomeFailed means the run stopped and the database reflects only the
	// migrations it records as applied. The migration that failed committed
	// nothing: its dirty row counts no applied statements. A run that stopped
	// before it selected any migration, such as one that could not connect,
	// is failed too, with nothing planned and no status.
	RunOutcomeFailed RunOutcome = "failed"
	// RunOutcomePartial means the migration that failed committed some of its
	// statements and not the rest, which a rollback can leave too on an engine
	// whose schema statements do not wait for the end of the transaction.
	// Recovery is Ptah's own resume path, which has the row's partial digest to
	// prove which statements it may skip; it is never a retry of the whole file.
	RunOutcomePartial RunOutcome = "partial"
	// RunOutcomeUnknown means the evidence could not be read, or a dirty row
	// does not say how far it got. Nothing may decide from this that the
	// migration did or did not run.
	RunOutcomeUnknown RunOutcome = "unknown"
)

// RunResult is the machine-readable record of one migration run.
type RunResult struct {
	// Inputs records the value-free references approved for exact statement
	// digests before execution. Values are never part of a run document.
	Inputs []MigrationInput `json:"inputs,omitempty"`
	// ContractVersion is [RunContractVersion].
	ContractVersion int `json:"contract_version"`
	// Command names the command that printed the document, such as
	// "migrations up", and is empty when [RunEvidence.Command] is.
	Command string `json:"command,omitempty"`
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
	// as applied after an up run, in selection order. A rollback leaves them
	// empty and reports RolledBack.
	Applied     []int64  `json:"applied,omitempty"`
	AppliedKeys []string `json:"applied_keys,omitempty"`
	// RolledBack and RolledBackKeys are the selected migrations the history no
	// longer records as applied after a rollback, in selection order. An up run
	// leaves them empty.
	RolledBack     []int64  `json:"rolled_back,omitempty"`
	RolledBackKeys []string `json:"rolled_back_keys,omitempty"`
	// Error is the run's error, present whenever the run failed. It is not
	// evidence about the database.
	Error *ErrorReport `json:"error,omitempty"`
	// Failure names the statement or the commit the run stopped at and the code
	// the database answered with. It is present only when a statement of a
	// migration was sent and failed, or the commit that ends them did. A run that
	// stopped before any statement ran has none, and neither does one that stopped
	// at something else: a write to the revision table, a check, a lost lock.
	Failure *RunFailure `json:"failure,omitempty"`
	// MigrationLog says which of the run's entries reached the migration log and
	// which did not. It is absent when the run keeps no log: the log is turned
	// off, the revision format is the Atlas-compatible one, the run is a dry run,
	// or it failed before the migrator ran, as [RunEvidence.NotStarted] says. A
	// lost entry does not change Outcome, which reads the revision table and not
	// the log.
	MigrationLog *MigrationLogReport `json:"migration_log,omitempty"`
	// Status is the history after the run, so one document answers both what
	// was asked and what the database now holds. It is absent when the status
	// could not be read, which is itself why the outcome is unknown.
	Status *MigrationStatus `json:"status,omitempty"`
}

// ErrorReport is the error a run document reports.
type ErrorReport struct {
	// Kind names why the run could not open its connection to the target
	// database, from a closed vocabulary: database_unreachable,
	// authentication_failed, database_not_found, permission_denied or
	// tls_failed. It is absent for every other failure, and only the command
	// line, which knows which connection is the target, sets it. A kind added
	// later is read as no kind by a caller that does not know it.
	Kind string `json:"kind,omitempty"`
	// Message is the message written for a person. It can quote what the
	// database said, including a row of data or the statement itself.
	Message string `json:"message"`
}

// RunFailure identifies the statement or the commit a failed run stopped at,
// and the code the database answered with.
//
// It carries no text. The message of [RunResult.Error] quotes what the
// database said, and that can include a row of data or the statement itself; a
// caller that must not publish data reads this instead.
type RunFailure struct {
	// Version is the version of the migration that failed. One transaction for
	// the whole run has one commit, and a failure of that commit names the last
	// migration in it.
	Version int64 `json:"version"`
	// VersionKey is that migration's exact revision identity, spelled as
	// [RunResult.PlannedKeys] spells it. It is always present, because an
	// empty string is itself an identity a Flyway repeatable migration records.
	VersionKey string `json:"version_key"`
	// Statement is the 1-based position of the failed statement in the
	// migration body, and Statements is how many statements the body holds.
	// Statement is zero when Commit is set: no statement failed.
	Statement  int `json:"statement"`
	Statements int `json:"statements"`
	// Commit reports that the statements ran and the commit that ends them
	// failed, as a deferred constraint does.
	Commit bool `json:"commit,omitempty"`
	// SQLState is the SQLSTATE the server answered with, read from a
	// PostgreSQL-protocol or MySQL-protocol driver error. It is empty when the
	// server gave no answer, such as after a lost connection or a canceled
	// statement, and on an engine with no SQLSTATE, such as SQL Server.
	SQLState string `json:"sqlstate,omitempty"`
	// ErrorNumber is the server's error number, such as 1146 for a table that
	// does not exist on MySQL and 208 on SQL Server. It is read from a
	// MySQL-protocol, SQL Server, Oracle or ClickHouse driver error, means
	// something only on that engine, and is zero on other engines.
	ErrorNumber int `json:"error_number,omitempty"`
}

// RunEvidence is what a caller saw of one run.
type RunEvidence struct {
	// Command names the command whose document the result is.
	Command string
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
	// NotStarted reports that the run failed before the migrator selected
	// anything, so it sent no migration statement: the target could not be
	// opened, or the history could not be read before the run. Its outcome is
	// failed whether or not a status follows. The zero value claims nothing,
	// and a failure with no status is then unknown.
	NotStarted bool
	// MigrationLog is what the run's writes to the migration log came to, as
	// [Migrator.MigrationLogReport] returned it. It is nil when the run keeps no
	// log.
	MigrationLog *MigrationLogReport
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
		Command:         evidence.Command,
		Direction:       evidence.Direction,
		MigrationLog:    evidence.MigrationLog,
		Status:          evidence.After,
	}
	if result.Direction == "" {
		result.Direction = MigrationDirectionUp
	}
	if evidence.Err != nil {
		result.Error = &ErrorReport{
			Kind:    string(errorkind.Classify(evidence.Err)),
			Message: evidence.Err.Error(),
		}
		result.Failure = runFailure(evidence.Err)
	}
	if evidence.Plan != nil {
		result.Inputs = cloneMigrationInputs(evidence.Plan.Inputs)
		result.Planned = slices.Clone(evidence.Plan.Versions)
		result.PlannedKeys = slices.Clone(evidence.Plan.VersionKeys)
	}
	if result.Direction == MigrationDirectionDown {
		result.RolledBack, result.RolledBackKeys = rolledBackFromPlan(result.Planned, result.PlannedKeys, evidence.After)
	} else {
		result.Applied, result.AppliedKeys = appliedFromPlan(result.Planned, result.PlannedKeys, evidence.After)
	}
	result.Outcome = runOutcome(evidence, result)
	return result
}

func runOutcome(evidence RunEvidence, result RunResult) RunOutcome {
	if evidence.NotStarted && evidence.Err != nil {
		return RunOutcomeFailed
	}
	if evidence.After == nil {
		return RunOutcomeUnknown
	}
	if dirty := evidence.After.DirtyRevision; dirty != nil {
		switch {
		case dirty.Total <= 0, dirty.StatementOutcomeUnknown():
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
	if len(result.Applied)+len(result.RolledBack) == len(result.Planned) {
		return RunOutcomeApplied
	}
	return RunOutcomePartial
}

// rolledBackFromPlan names the selected migrations the history no longer
// records as applied, in selection order: what a rollback took effect on. It
// reads identities as [appliedFromPlan] does, and is its complement over the
// plan, less the migration a dirty row names: a rollback that stopped there
// left its row in place, so that migration was not rolled back.
func rolledBackFromPlan(planned []int64, plannedKeys []string, after *MigrationStatus) ([]int64, []string) {
	if after == nil || len(planned) == 0 {
		return nil, nil
	}
	stillVersions, stillKeys := appliedFromPlan(planned, plannedKeys, after)
	versions := make([]int64, 0, len(planned))
	keys := make([]string, 0, len(plannedKeys))
	for index, version := range planned {
		key := ""
		if index < len(plannedKeys) {
			key = plannedKeys[index]
		}
		if namesDirtyRow(after.DirtyRevision, version, key) {
			continue
		}
		if key != "" {
			if slices.Contains(stillKeys, key) {
				continue
			}
			keys = append(keys, key)
		} else if slices.Contains(stillVersions, version) {
			continue
		}
		versions = append(versions, version)
	}
	if len(versions) == 0 {
		return nil, nil
	}
	if len(keys) == 0 {
		return versions, nil
	}
	return versions, keys
}

// namesDirtyRow reports whether dirty is the row of the migration with version
// and key. A row that records an exact identity is matched by it, and one that
// records none by its version.
func namesDirtyRow(dirty *MigrationRevision, version int64, key string) bool {
	if dirty == nil {
		return false
	}
	if dirty.AtlasVersion != "" && key != "" {
		return dirty.AtlasVersion == key
	}
	return dirty.Version == version
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

// runFailure reads the failed statement or commit out of a run's error.
//
// Both halves have to be in the chain: the migration the migrator was running,
// and a statement it sent or a commit it asked for. A refusal raised before any
// statement ran -- the transaction preflight, a statement validator -- names a
// statement too, but the database never saw it, so it is not a failure the
// database answered.
func runFailure(err error) *RunFailure {
	migration, ok := errors.AsType[*failedMigrationError](err)
	if !ok {
		return nil
	}
	failure := &RunFailure{Version: migration.version, VersionKey: migration.versionKey}
	if commit, ok := errors.AsType[*failedCommitError](migration.err); ok {
		failure.Statements = commit.statements
		failure.Commit = true
		failure.SQLState, failure.ErrorNumber = servercode.Read(commit.err)
		return failure
	}
	statement, ok := errors.AsType[*MigrationExecutionError](migration.err)
	if !ok || !statement.sent {
		return nil
	}
	failure.Statement = statement.StatementIndex
	failure.Statements = statement.Total
	failure.SQLState, failure.ErrorNumber = servercode.Read(statement.Err)
	return failure
}
