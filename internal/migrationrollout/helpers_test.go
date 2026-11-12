package migrationrollout_test

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"sync"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp"

	"ptah.run/internal/migrationrollout"
	"ptah.run/migration/migrator"
)

// requirementComparer compares requirements by their spelling, which is all a
// definition can say about one.
var requirementComparer = cmp.Comparer(func(left, right migrationrollout.Requirement) bool {
	return left.String() == right.String()
})

// fixedNow is the clock every runner in these tests reads.
var fixedNow = time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

// mustPlan resolves a definition written in YAML, with url_env read from env.
func mustPlan(c *qt.C, definition string, env map[string]string) migrationrollout.Plan {
	c.Helper()
	decoded, err := migrationrollout.Decode([]byte(definition))
	c.Assert(err, qt.IsNil)
	plan, err := decoded.Resolve(migrationrollout.ResolveOptions{
		BaseDir: c.TempDir(),
		LookupEnv: func(name string) (string, bool) {
			value, ok := env[name]
			return value, ok
		},
	})
	c.Assert(err, qt.IsNil)
	return plan
}

// directoryRelease is a release the tests bind runs to.
var directoryRelease = migrationrollout.Release{
	Kind:   migrationrollout.ReleaseDirectory,
	Source: "migrations",
	Digest: "sha256:0000000000000000000000000000000000000000000000000000000000000001",
}

// newState is a fresh state for plan.
func newState(plan migrationrollout.Plan) *migrationrollout.State {
	return migrationrollout.NewState(plan, directoryRelease, "rollout.yaml", fixedNow)
}

// applied is the document of a run that applied every migration it selected.
func applied(keys ...string) migrationrollout.Result {
	return migrationrollout.Result{Run: &migrator.RunResult{
		ContractVersion: migrator.RunContractVersion,
		Command:         "migrations up",
		Direction:       migrator.MigrationDirectionUp,
		Outcome:         migrator.RunOutcomeApplied,
		PlannedKeys:     keys,
		Planned:         versions(keys),
		AppliedKeys:     keys,
		Applied:         versions(keys),
		Status:          &migrator.MigrationStatus{CurrentVersionKey: keys[len(keys)-1]},
	}}
}

// failed is the document of a run that stopped with message.
func failed(message string) migrationrollout.Result {
	return migrationrollout.Result{Run: &migrator.RunResult{
		ContractVersion: migrator.RunContractVersion,
		Command:         "migrations up",
		Direction:       migrator.MigrationDirectionUp,
		Outcome:         migrator.RunOutcomeFailed,
		Error:           &migrator.ErrorReport{Message: message},
	}}
}

// withOutcome is a run document reporting outcome and nothing else.
func withOutcome(outcome migrator.RunOutcome) migrationrollout.Result {
	return migrationrollout.Result{Run: &migrator.RunResult{
		ContractVersion: migrator.RunContractVersion,
		Direction:       migrator.MigrationDirectionUp,
		Outcome:         outcome,
	}}
}

func versions(keys []string) []int64 {
	out := make([]int64, 0, len(keys))
	for i := range keys {
		out = append(out, int64(i+1))
	}
	return out
}

// inspection is what Inspect answers for one target.
type inspection struct {
	status *migrator.MigrationStatus
	err    error
}

// cleanAt is a clean revision table at the release's head.
func cleanAt(key string) inspection {
	return inspection{status: &migrator.MigrationStatus{
		CurrentVersionKey: key,
		AppliedMigrations: []int64{1, 2},
		PendingMigrations: make([]int64, 0),
		Migrations: []migrator.MigrationRecord{
			{Version: 1, VersionKey: "1", State: migrator.MigrationStateApplied},
			{Version: 2, VersionKey: "2", State: migrator.MigrationStateApplied},
		},
	}}
}

// cleanPending is a clean revision table with the release's second migration
// pending.
func cleanPending() inspection {
	return inspection{status: &migrator.MigrationStatus{
		CurrentVersionKey: "1",
		AppliedMigrations: []int64{1},
		PendingMigrations: []int64{2},
		HasPendingChanges: true,
		Migrations: []migrator.MigrationRecord{
			{Version: 1, VersionKey: "1", State: migrator.MigrationStateApplied},
			{Version: 2, VersionKey: "2", State: migrator.MigrationStatePending},
		},
	}}
}

// dirty is a revision table holding a dirty row for migration 2 that applied
// appliedStatements of two.
func dirty(appliedStatements int) inspection {
	return inspection{status: &migrator.MigrationStatus{
		CurrentVersionKey: "2",
		AppliedMigrations: []int64{1},
		PendingMigrations: []int64{2},
		HasPendingChanges: true,
		DirtyRevision: &migrator.MigrationRevision{
			Version: 2,
			State:   "failed",
			Applied: appliedStatements,
			Total:   2,
			Dirty:   true,
		},
	}}
}

// fakeExecutor answers from tables and records what it was asked. Apply
// sleeps for the target's delay first, which inside a synctest bubble is fake
// time, so overlapping runs are measured without a real clock.
type fakeExecutor struct {
	results     map[string]migrationrollout.Result
	inspections map[string]inspection
	delays      map[string]time.Duration
	// interruptAt cancels a context when the named target's run starts, the
	// way an interrupt arriving during that run would.
	interruptAt map[string]context.CancelFunc

	mu          sync.Mutex
	applied     []string
	inspected   []string
	inFlight    int
	maxInFlight int
}

func (f *fakeExecutor) Apply(_ context.Context, target migrationrollout.Target) migrationrollout.Result {
	f.mu.Lock()
	f.applied = append(f.applied, target.Name)
	f.inFlight++
	f.maxInFlight = max(f.maxInFlight, f.inFlight)
	interrupt := f.interruptAt[target.Name]
	f.mu.Unlock()
	if interrupt != nil {
		interrupt()
	}
	time.Sleep(f.delays[target.Name])
	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()
	result, ok := f.results[target.Name]
	if !ok {
		return applied("1", "2")
	}
	return result
}

func (f *fakeExecutor) Inspect(_ context.Context, target migrationrollout.Target) (*migrator.MigrationStatus, error) {
	f.mu.Lock()
	f.inspected = append(f.inspected, target.Name)
	f.mu.Unlock()
	answer, ok := f.inspections[target.Name]
	if !ok {
		return nil, errors.New("fake executor has no inspection for " + target.Name)
	}
	return answer.status, answer.err
}

func (f *fakeExecutor) appliedTargets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.applied...)
}

func (f *fakeExecutor) inspectedTargets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.inspected...)
}

// statuses maps every target of state to its status.
func statuses(state *migrationrollout.State) map[string]migrationrollout.TargetStatus {
	out := make(map[string]migrationrollout.TargetStatus)
	for _, stage := range state.Stages {
		for _, target := range stage.Targets {
			out[target.Name] = target.Status
		}
	}
	return out
}

// stageStatuses maps every stage of state to its status.
func stageStatuses(state *migrationrollout.State) map[string]migrationrollout.StageStatus {
	out := make(map[string]migrationrollout.StageStatus)
	for _, stage := range state.Stages {
		out[stage.Name] = stage.Status
	}
	return out
}

// targetState returns the state of the target named name.
func targetState(c *qt.C, state *migrationrollout.State, name string) migrationrollout.TargetState {
	c.Helper()
	for _, stage := range state.Stages {
		for _, target := range stage.Targets {
			if target.Name == name {
				return target
			}
		}
	}
	c.Fatalf("no target %q in the state", name)
	return migrationrollout.TargetState{}
}

// sortedNames returns names sorted, for a set whose order the scheduler
// decides.
func sortedNames(names []string) []string {
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	return sorted
}

// credentialURL is a PostgreSQL URL for user app with password on host,
// built rather than written out, for the tests that hold the password out of
// every report.
func credentialURL(password, host, database string) string {
	return (&url.URL{Scheme: "postgres", User: url.UserPassword("app", password), Host: host, Path: "/" + database}).String()
}

// failingSave is a Save that succeeds for its first writes and then fails, as
// a disk that fills during a run does.
type failingSave struct {
	mu        sync.Mutex
	succeeded int
	limit     int
}

func (f *failingSave) save(*migrationrollout.State) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.succeeded >= f.limit {
		return errors.New("no space left on device")
	}
	f.succeeded++
	return nil
}
