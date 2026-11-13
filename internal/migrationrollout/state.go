package migrationrollout

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"ptah.run/internal/fsdurable"
	"ptah.run/migration/migrator"
)

// StateContractVersion is the version of the state document. It rises when a
// field changes meaning or leaves; a reader refuses a version it does not
// know.
const StateContractVersion = 1

// ErrPlanChanged is the refusal of a resumed run whose definition resolves to
// a plan other than the one the run started with.
var ErrPlanChanged = errors.New("the rollout definition no longer resolves to the plan this rollout started with")

// TargetStatus is what the rollout knows about one target.
type TargetStatus string

const (
	// TargetNotStarted is a target the rollout has not started.
	TargetNotStarted TargetStatus = "not-started"
	// TargetRunning is a target whose run is in progress, or was when the
	// rollout stopped. A resumed run reads its revision table.
	TargetRunning TargetStatus = "running"
	// TargetSucceeded is a target whose run applied every migration it
	// selected, or that a resumed run found holding every migration of the
	// release.
	TargetSucceeded TargetStatus = "succeeded"
	// TargetNoOp is a target whose run found nothing pending.
	TargetNoOp TargetStatus = "no-op"
	// TargetFailed is a target whose run stopped, either before it sent a
	// statement or at a migration that committed nothing.
	TargetFailed TargetStatus = "failed"
	// TargetPartial is a target whose failed migration committed some of its
	// statements and not the rest.
	TargetPartial TargetStatus = "partial"
	// TargetUnknown is a target with no evidence of what happened: its run
	// left no readable document, its revision table could not be read, or a
	// dirty row does not say how far the migration got.
	TargetUnknown TargetStatus = "unknown"
	// TargetSkipped is a target not started because its stage stopped after a
	// failure.
	TargetSkipped TargetStatus = "skipped"
	// TargetBlocked is a target not started because a stage its stage depends
	// on did not meet its requirement.
	TargetBlocked TargetStatus = "blocked"
)

// Success reports a status that counts toward a stage's requirement.
func (s TargetStatus) Success() bool {
	return s == TargetSucceeded || s == TargetNoOp
}

// Failure reports a status a stage's failure policy reacts to.
func (s TargetStatus) Failure() bool {
	return s == TargetFailed || s == TargetPartial || s == TargetUnknown
}

// attempted reports a status only a started run produces, so the target's
// revision table is read before a resumed run decides about it.
func (s TargetStatus) attempted() bool {
	return s == TargetRunning || s.Success() || s.Failure()
}

// StageStatus is what the rollout knows about one stage.
type StageStatus string

const (
	// StageNotStarted is a stage the rollout has not reached.
	StageNotStarted StageStatus = "not-started"
	// StageRunning is a stage with targets running, or one an interrupt left
	// with targets not started.
	StageRunning StageStatus = "running"
	// StageSucceeded is a stage whose successes meet its requirement.
	StageSucceeded StageStatus = "succeeded"
	// StageFailed is a stage that ran and whose successes do not meet its
	// requirement.
	StageFailed StageStatus = "failed"
	// StageBlocked is a stage that did not run because a stage it depends on
	// did not succeed.
	StageBlocked StageStatus = "blocked"
)

// RunStatus is what the rollout knows about the run as a whole.
type RunStatus string

const (
	// RunRunning is a run in progress, or one whose process ended without
	// recording an outcome.
	RunRunning RunStatus = "running"
	// RunSucceeded is a run whose every stage succeeded.
	RunSucceeded RunStatus = "succeeded"
	// RunFailed is a run that reached every stage it could and in which a
	// stage failed or was blocked.
	RunFailed RunStatus = "failed"
	// RunInterrupted is a run stopped before every stage was decided.
	RunInterrupted RunStatus = "interrupted"
)

// State is the rollout's record: the plan and release it is bound to, and the
// outcome of every stage and target. It is written after every change, so an
// interrupted process leaves the last thing it knew.
type State struct {
	ContractVersion int `json:"contract_version"`
	// Status is the run's status.
	Status RunStatus `json:"status"`
	// Definition is the definition path the last invocation read.
	Definition string `json:"definition"`
	// Plan is the [Plan.Digest] the run is bound to.
	Plan string `json:"plan"`
	// Release is the release the run is bound to.
	Release Release `json:"release"`
	// PtahVersion is the version of the ptah binary of the last invocation.
	// Every target run of one invocation uses that binary.
	PtahVersion string `json:"ptah_version,omitempty"`
	// Invocations counts the run and its resumptions.
	Invocations int       `json:"invocations"`
	StartedAt   time.Time `json:"started_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	// Stages holds every stage, in execution order.
	Stages []StageState `json:"stages"`
}

// StageState is one stage's settings and outcome.
type StageState struct {
	Name        string        `json:"name"`
	Canary      bool          `json:"canary,omitempty"`
	DependsOn   []string      `json:"depends_on,omitempty"`
	Concurrency int           `json:"concurrency"`
	OnFailure   FailurePolicy `json:"on_failure"`
	Require     string        `json:"require"`
	Required    int           `json:"required"`
	Status      StageStatus   `json:"status"`
	// Succeeded counts the members whose status is a success.
	Succeeded int `json:"succeeded"`
	// BlockedBy names the dependencies that did not succeed.
	BlockedBy []string      `json:"blocked_by,omitempty"`
	Targets   []TargetState `json:"targets"`
}

// TargetState is one target's settings and outcome.
type TargetState struct {
	TargetRecord
	Status TargetStatus `json:"status"`
	// Attempts counts the runs the rollout started for this target.
	Attempts   int        `json:"attempts"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// Planned and Applied are the migrations the last run selected under its
	// migration lock and the ones the revision table records as applied
	// afterwards, by version key.
	Planned []string `json:"planned,omitempty"`
	Applied []string `json:"applied,omitempty"`
	// Version is the version key the revision table records as current after
	// the last run or reconciliation.
	Version string `json:"version,omitempty"`
	// Error is the last run's error, with credentials taken out.
	Error *migrator.ErrorReport `json:"error,omitempty"`
	// Failure locates the statement the last run stopped at, when one did.
	Failure *migrator.RunFailure `json:"failure,omitempty"`
	// Note says why the status is what it is when the run's own outcome does
	// not: a skip, a block, or what a resumed run read back.
	Note string `json:"note,omitempty"`
}

// NewState returns the state of a run that has started nothing.
func NewState(plan Plan, release Release, definition string, now time.Time) *State {
	state := &State{
		ContractVersion: StateContractVersion,
		Status:          RunRunning,
		Definition:      definition,
		Plan:            plan.Digest,
		Release:         release,
		StartedAt:       now.UTC(),
		UpdatedAt:       now.UTC(),
		Stages:          make([]StageState, 0, len(plan.Stages)),
	}
	for _, stage := range plan.Stages {
		record := stage.record()
		targets := make([]TargetState, 0, len(record.Targets))
		for _, target := range record.Targets {
			targets = append(targets, TargetState{TargetRecord: target, Status: TargetNotStarted})
		}
		state.Stages = append(state.Stages, StageState{
			Name:        record.Name,
			Canary:      record.Canary,
			DependsOn:   record.DependsOn,
			Concurrency: record.Concurrency,
			OnFailure:   record.OnFailure,
			Require:     record.Require,
			Required:    record.Required,
			Status:      StageNotStarted,
			Targets:     targets,
		})
	}
	return state
}

// Bind refuses to resume s with a plan or a release it was not started with.
// The plan is compared by digest, which covers every stage and target setting
// and each database's display form; the release by kind and digest.
func (s *State) Bind(plan Plan, release Release) error {
	if s.Plan != plan.Digest {
		return fmt.Errorf("%w: the state records %s and the definition resolves to %s; "+
			"start a new rollout with a new state file to run a changed definition",
			ErrPlanChanged, s.Plan, plan.Digest)
	}
	return release.Matches(s.Release)
}

// stageStateFor returns the state of the stage named name.
func (s *State) stageStateFor(name string) *StageState {
	for i := range s.Stages {
		if s.Stages[i].Name == name {
			return &s.Stages[i]
		}
	}
	return nil
}

// Encode writes s as indented JSON.
func (s *State) Encode() ([]byte, error) {
	encoded, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode rollout state: %w", err)
	}
	return append(encoded, '\n'), nil
}

// ReadState reads a state file strictly and refuses a contract version this
// build does not know.
func ReadState(path string) (*State, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- the path is the operator's own --state-file
	if err != nil {
		return nil, fmt.Errorf("read rollout state: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state State
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("read rollout state %s: %w", path, err)
	}
	if decoder.More() {
		return nil, fmt.Errorf("read rollout state %s: the file holds more than one document", path)
	}
	if state.ContractVersion != StateContractVersion {
		return nil, fmt.Errorf("read rollout state %s: contract version %d is not %d, the one this build writes",
			path, state.ContractVersion, StateContractVersion)
	}
	return &state, nil
}

// CreateState writes the first version of a state file and refuses a path
// that already exists, so a new run never overwrites the record of another.
func CreateState(path string, state *State) error {
	encoded, err := state.Encode()
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- the operator's own --state-file
	if err != nil {
		return fmt.Errorf("create rollout state: %w", err)
	}
	_, writeErr := file.Write(encoded)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("write rollout state %s: %w", path, err)
	}
	return nil
}

// WriteState replaces the state file with state. The new content is written
// beside it and moved over it, so a reader sees the old file or the new one
// and never a torn write.
func WriteState(path string, state *State) error {
	encoded, err := state.Encode()
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("write rollout state: %w", err)
	}
	tempPath := file.Name()
	_, writeErr := file.Write(encoded)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return errors.Join(fmt.Errorf("write rollout state %s: %w", path, err), removeIfPresent(tempPath))
	}
	if err := fsdurable.ReplaceFile(tempPath, path); err != nil {
		return errors.Join(fmt.Errorf("write rollout state %s: %w", path, err), removeIfPresent(tempPath))
	}
	return nil
}

// LockState takes the lock file beside a state file, so a second invocation on
// the same state refuses rather than writing over the first one's record. The
// returned function removes the lock; calling it twice is harmless.
//
// The lock is a file created exclusively, which every platform offers. A
// process killed while holding it leaves the file behind, and the refusal
// names it so an operator who knows no rollout is running can remove it.
func LockState(path string) (func() error, error) {
	lockPath := path + ".lock"
	file, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- beside the operator's own --state-file
	if errors.Is(err, os.ErrExist) {
		holder, _ := os.ReadFile(lockPath) // #nosec G304 -- the lock file named above
		return nil, fmt.Errorf("another rollout holds %s (%s); if no rollout is running on this state, remove the file",
			lockPath, bytes.TrimSpace(holder))
	}
	if err != nil {
		return nil, fmt.Errorf("lock rollout state: %w", err)
	}
	host, _ := os.Hostname()
	_, writeErr := fmt.Fprintf(file, "pid %d on %s since %s\n", os.Getpid(), host, time.Now().UTC().Format(time.RFC3339))
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return nil, errors.Join(fmt.Errorf("lock rollout state: %w", err), removeIfPresent(lockPath))
	}
	released := false
	return func() error {
		if released {
			return nil
		}
		released = true
		return removeIfPresent(lockPath)
	}, nil
}

func removeIfPresent(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
