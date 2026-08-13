package devdocker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/devdocker"
)

// The rows below are the pinned community binary v1.3.0's own answers,
// measured through ptah-atlas-conformance/bin/atlas on 2026-08-13 with each
// exit status read from an unpiped invocation of
// `atlas schema diff --from file://schema.sql --to file://schema2.sql --dev-url <value>`:
//
//	docker://postgres/16/dev     exit 0, provisioned postgres:16
//	docker://postgres            exit 0, provisioned a default tag
//	docker://postgres/dev        exit 1, `Unable to find image 'postgres:dev' locally`
//	docker://postgres:16/dev     exit 1, `unsupported docker image "postgres:16"`
//	docker://sqlite/dev          exit 1, `unsupported docker image "sqlite"`
//	docker://nosuchengine/1/dev  exit 1, `unsupported docker image "nosuchengine"`
//	docker:///dev                exit 1, `unsupported docker image ""`
//
// Three of those rows are the reason this parser exists at all rather than
// reusing [atlasurl.DialectFromURL], which answers a dialect for
// `docker://sqlite` and for `docker://postgres:16/dev`. Provisioning either
// would make ptah-compat exit 0 where the pinned binary exits 1 -- the one
// direction AGENTS.md compatibility rule (a) forbids outright. The
// `docker://postgres/dev` row is why a one-segment path is a TAG: reading it as
// a database name would run `postgres:latest` for a URL the pinned binary
// resolves to the image `postgres:dev`.

func TestParseAcceptsTheURLFormsTheCommunityBinaryProvisions(t *testing.T) {
	tests := []struct {
		name         string
		rawURL       string
		wantImage    string
		wantDatabase string
		wantDialect  string
	}{
		{
			name:         "engine tag and database",
			rawURL:       "docker://postgres/16/dev",
			wantImage:    "postgres:16",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			// Measured: the pinned binary reads the single segment as an image
			// tag and looks for `postgres:dev`, not for a database called dev.
			name:         "single path segment is the image tag",
			rawURL:       "docker://postgres/dev",
			wantImage:    "postgres:dev",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			name:         "bare engine takes both defaults",
			rawURL:       "docker://postgres",
			wantImage:    "postgres:latest",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			// Measured: the pinned binary exits 0 on this, so the trailing
			// separator is the tag-only form and not an empty database name.
			name:         "trailing separator is the tag-only form",
			rawURL:       "docker://postgres/16/",
			wantImage:    "postgres:16",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			name:         "mysql",
			rawURL:       "docker://mysql/8/dev",
			wantImage:    "mysql:8",
			wantDatabase: "dev",
			wantDialect:  "mysql",
		},
		{
			// Both spellings were measured on the pinned binary and both
			// resolved to its MariaDB image, so both resolve here.
			name:         "maria short spelling",
			rawURL:       "docker://maria/11/dev",
			wantImage:    "mariadb:11",
			wantDatabase: "dev",
			wantDialect:  "mariadb",
		},
		{
			name:         "mariadb long spelling",
			rawURL:       "docker://mariadb/11/app",
			wantImage:    "mariadb:11",
			wantDatabase: "app",
			wantDialect:  "mariadb",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec, err := devdocker.Parse(tc.rawURL)
			qt.Assert(t, err, qt.IsNil)
			qt.Check(t, spec.Image, qt.Equals, tc.wantImage)
			qt.Check(t, spec.Database, qt.Equals, tc.wantDatabase)
			qt.Check(t, spec.Dialect, qt.Equals, tc.wantDialect)
		})
	}
}

func TestParseRefusesTheURLFormsTheCommunityBinaryRefuses(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		// wantErr is the whole message, byte for byte. Three of these rows are
		// the pinned binary's own wording and are asserted exactly so a reworded
		// refusal is a test failure rather than a silent divergence.
		wantErr string
	}{
		{
			// The single most important row in this file. `sqlite` is a dialect
			// Ptah has and an image the pinned binary refuses; a build that
			// provisioned it would exit 0 where the pinned binary exits 1.
			name:    "sqlite is a dialect but not an image",
			rawURL:  "docker://sqlite/dev",
			wantErr: `unsupported docker image "sqlite"`,
		},
		{
			// The second. Ptah's own dialect parser reads `postgres:16` as an
			// engine with a port and answers `postgres`; the pinned binary
			// refuses the whole host. The value quoted is the host as WRITTEN.
			name:    "colon form is refused whole",
			rawURL:  "docker://postgres:16/dev",
			wantErr: `unsupported docker image "postgres:16"`,
		},
		{
			name:    "unknown engine",
			rawURL:  "docker://nosuchengine/1/dev",
			wantErr: `unsupported docker image "nosuchengine"`,
		},
		{
			name:    "missing engine",
			rawURL:  "docker:///dev",
			wantErr: `unsupported docker image ""`,
		},
		{
			name:    "more than tag and database",
			rawURL:  "docker://postgres/16/dev/extra",
			wantErr: `docker --dev-url path "/16/dev/extra" has more than <tag>/<database>`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := devdocker.Parse(tc.rawURL)
			qt.Assert(t, err, qt.IsNotNil)
			qt.Check(t, err.Error(), qt.Equals, tc.wantErr)
		})
	}
}

// fakeRunner is a container runtime that starts nothing.
//
// It exists so the lifecycle can be asserted on a machine with no container
// runtime at all, and so the failure paths -- a runtime that is unavailable, a
// start that fails, a server that never becomes ready -- can be exercised
// deterministically. Those are exactly the paths a live test cannot reach on
// demand, and they are the ones where a leaked container would come from.
type fakeRunner struct {
	mu sync.Mutex

	availableErr error
	startErr     error
	hostPort     string
	// removeFailures[i] is returned by the (i+1)-th Remove call. Calls past the
	// end succeed, so a queue of one error models a removal that fails once and
	// then works -- the transient `docker rm` the retry exists for.
	removeFailures []error

	started []string
	removed []string
}

func (f *fakeRunner) Available(context.Context) error {
	return f.availableErr
}

func (f *fakeRunner) Start(_ context.Context, name, _, _ string, _ []string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, name)
	return f.hostPort, f.startErr
}

func (f *fakeRunner) Remove(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, name)
	return errorAt(f.removeFailures, len(f.removed)-1)
}

// errorAt reads errs[i], treating a short slice as trailing nils. It pads
// rather than testing the length so the fake stays free of branches.
func errorAt(errs []error, i int) error {
	padded := make([]error, max(len(errs), i+1))
	copy(padded, errs)
	return padded[i]
}

func (f *fakeRunner) calls() (started, removed []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.started...), append([]string{}, f.removed...)
}

// alwaysReady and neverReady stand in for the connection probe.
func alwaysReady(context.Context, string) error { return nil }

var errNotListening = errors.New("connection refused")

func neverReady(context.Context, string) error { return errNotListening }

func TestResolveLeavesANonDockerURLAloneAndStartsNothing(t *testing.T) {
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	resolved, release, err := devdocker.Resolve(t.Context(), "postgres://u:p@localhost:5432/db", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	qt.Assert(t, err, qt.IsNil)
	t.Cleanup(release)

	qt.Check(t, resolved, qt.Equals, "postgres://u:p@localhost:5432/db")
	started, removed := runner.calls()
	// Both halves matter. A build that provisioned for every dev URL would pass
	// the URL assertion above by accident whenever the fake handed back the
	// same string, and would still be starting a container per invocation.
	qt.Check(t, started, qt.HasLen, 0)
	qt.Check(t, removed, qt.HasLen, 0)
}

func TestResolveProvisionsADockerURLAndReleaseRemovesTheContainer(t *testing.T) {
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	resolved, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	qt.Assert(t, err, qt.IsNil)

	qt.Check(t, resolved, qt.Equals, "postgres://postgres:ptah-dev@127.0.0.1:15432/dev?sslmode=disable")
	started, removed := runner.calls()
	qt.Assert(t, started, qt.HasLen, 1)
	// Nothing is removed until the caller releases: a release that removed
	// eagerly would hand back a URL to a container that is already gone.
	qt.Check(t, removed, qt.HasLen, 0)

	release()
	started, removed = runner.calls()
	qt.Check(t, removed, qt.DeepEquals, started)
}

func TestResolveRemovesTheContainerWhenTheServerNeverBecomesReady(t *testing.T) {
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner:       runner,
		Ready:        neverReady,
		ReadyTimeout: 10 * time.Millisecond,
	})
	qt.Assert(t, err, qt.IsNotNil)
	t.Cleanup(release)

	qt.Check(t, err, qt.ErrorIs, errNotListening)
	qt.Check(t, err.Error(), qt.Contains, "did not become ready")
	started, removed := runner.calls()
	// The whole point of this row: a failed wait must not leave the container
	// behind, and the caller is handed an error rather than a releasable
	// instance, so nothing else can remove it.
	qt.Assert(t, started, qt.HasLen, 1)
	qt.Check(t, removed, qt.DeepEquals, started)
}

func TestResolveRemovesTheContainerWhenTheRuntimeFailsToStartIt(t *testing.T) {
	runner := &fakeRunner{startErr: errors.New("no such image")}
	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	qt.Assert(t, err, qt.IsNotNil)
	t.Cleanup(release)

	started, removed := runner.calls()
	// A `docker run` that fails after creating the container leaves one behind,
	// so removal is attempted on this path too rather than assumed unnecessary.
	qt.Assert(t, started, qt.HasLen, 1)
	qt.Check(t, removed, qt.DeepEquals, started)
}

func TestResolveReportsAnUnavailableRuntimeWithoutStartingAnything(t *testing.T) {
	runner := &fakeRunner{availableErr: errors.New("docker daemon is not running")}
	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	qt.Assert(t, err, qt.IsNotNil)
	t.Cleanup(release)

	qt.Check(t, err.Error(), qt.Equals, "docker daemon is not running")
	started, _ := runner.calls()
	qt.Check(t, started, qt.HasLen, 0)
}

func TestConcurrentProvisioningPicksDistinctContainerNames(t *testing.T) {
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	const parallel = 8

	var wg sync.WaitGroup
	releases := make([]func(), parallel)
	errs := make([]error, parallel)
	for i := range parallel {
		wg.Go(func() {
			_, release, err := devdocker.Resolve(context.Background(), "docker://postgres/16/dev", devdocker.Options{
				Runner: runner,
				Ready:  alwaysReady,
			})
			releases[i], errs[i] = release, err
		})
	}
	wg.Wait()
	for _, release := range releases {
		t.Cleanup(release)
	}
	qt.Assert(t, errors.Join(errs...), qt.IsNil)

	started, _ := runner.calls()
	qt.Assert(t, started, qt.HasLen, parallel)
	unique := make(map[string]struct{}, parallel)
	for _, name := range started {
		unique[name] = struct{}{}
	}
	// A name derived from the URL rather than from randomness would collide on
	// every one of these, and two concurrent runs would fight over one
	// container -- the "parallel invocations do not collide" half of #844.
	qt.Check(t, unique, qt.HasLen, parallel)
}

func TestReleaseIsSafeToCallTwice(t *testing.T) {
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	qt.Assert(t, err, qt.IsNil)

	release()
	release()
	_, removed := runner.calls()
	// Consumers defer the release and some also call it on an early return, so
	// a second call must not issue a second removal against a name the daemon
	// may by then have given to nobody.
	qt.Check(t, removed, qt.HasLen, 1)
}

// The rows below pin that connection parameters written on a docker dev URL
// survive into the provisioned URL.
//
// Measured against the pinned community binary v1.3.0 on 2026-08-13, exit
// status read from an unpiped invocation of
// `atlas schema inspect -u file://schema.sql --dev-url <value>`:
//
//	docker://postgres/16/dev                     exit 0, inspects the container
//	docker://postgres/16/dev?search_path=app     exit 1, `schema "app" was not found`
//	docker://postgres/16/dev?search_path=public  exit 0
//
// That binary HONORS the parameter: it provisions, then scopes to the named
// schema, and fails when a fresh container does not have it. While the query
// was being parsed and discarded here, the middle row answered exit 0 and
// inspected `public` — accepting what the operator wrote and then ignoring it,
// and exiting 0 where the pinned binary exits 1, which compatibility rule (a)
// forbids outright.
func TestParseCarriesConnectionParametersIntoTheProvisionedURL(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantURL string
	}{
		{ //nolint:gosec // G101: this package's own throwaway dev password, asserted verbatim
			name:    "no parameters leaves the engine defaults",
			rawURL:  "docker://postgres/16/dev",
			wantURL: "postgres://postgres:ptah-dev@127.0.0.1:15432/dev?sslmode=disable",
		},
		{ //nolint:gosec // G101: this package's own throwaway dev password, asserted verbatim
			// The row the pinned binary answers exit 1 on. The parameter has to
			// reach the connection for Ptah to answer it at all.
			name:    "search_path survives alongside the defaults",
			rawURL:  "docker://postgres/16/dev?search_path=app",
			wantURL: "postgres://postgres:ptah-dev@127.0.0.1:15432/dev?search_path=app&sslmode=disable",
		},
		{ //nolint:gosec // G101: this package's own throwaway dev password, asserted verbatim
			// The operator wins on a key the engine also sets. Defaults exist to
			// make a throwaway container reachable; an operator who writes one
			// has a reason the container cannot know.
			name:    "an operator sslmode replaces the default",
			rawURL:  "docker://postgres/16/dev?sslmode=require",
			wantURL: "postgres://postgres:ptah-dev@127.0.0.1:15432/dev?sslmode=require",
		},
		{ //nolint:gosec // G101: this package's own throwaway dev password, asserted verbatim
			// Two keys, one of which the engine also sets: the unmentioned
			// default must survive. A build that used the operator's query
			// wholesale instead of merging would drop sslmode here.
			name:    "an unmentioned default survives beside an operator key",
			rawURL:  "docker://postgres/16/dev?search_path=app",
			wantURL: "postgres://postgres:ptah-dev@127.0.0.1:15432/dev?search_path=app&sslmode=disable",
		},
		{
			// MySQL has no default parameters, so its URL carries a query only
			// when the operator wrote one -- and no bare `?`, which the driver
			// would read as an empty parameter list.
			name:    "mysql without parameters carries no query at all",
			rawURL:  "docker://mysql/8/dev",
			wantURL: "mysql://root:ptah-dev@tcp(127.0.0.1:15432)/dev",
		},
		{
			name:    "mysql carries the operator parameters",
			rawURL:  "docker://mysql/8/dev?parseTime=true",
			wantURL: "mysql://root:ptah-dev@tcp(127.0.0.1:15432)/dev?parseTime=true",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec, err := devdocker.Parse(tc.rawURL)
			qt.Assert(t, err, qt.IsNil)
			qt.Check(t, spec.URL("127.0.0.1:15432"), qt.Equals, tc.wantURL)
		})
	}
}

func TestReadyURLProbesTheServerWithoutTheOperatorParameters(t *testing.T) {
	spec, err := devdocker.Parse("docker://postgres/16/dev?search_path=app")
	qt.Assert(t, err, qt.IsNil)

	// The readiness wait asks whether the CONTAINER is up. Probing with
	// `search_path=app` cannot answer that: a fresh container never has the
	// schema, so every attempt fails and the wait burns its whole budget before
	// reporting a deterministic error. Measured before this split, that cost
	// two minutes; after it the same run refuses in about five seconds.
	qt.Check(t, spec.ReadyURL("127.0.0.1:15432"), qt.Equals,
		"postgres://postgres:ptah-dev@127.0.0.1:15432/dev?sslmode=disable")
	// The engine's own defaults are still on the probe URL: dropping sslmode
	// here would make the probe fail against a container that speaks no TLS.
	qt.Check(t, spec.URL("127.0.0.1:15432"), qt.Equals,
		"postgres://postgres:ptah-dev@127.0.0.1:15432/dev?search_path=app&sslmode=disable")
}

func TestCloseStaysRetryableUntilRemovalSucceeds(t *testing.T) {
	removeFailed := errors.New("Error response from daemon: removal already in progress")
	runner := &fakeRunner{hostPort: "127.0.0.1:15432", removeFailures: []error{removeFailed}}

	instance, err := devdocker.Provision(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	qt.Assert(t, err, qt.IsNil)

	// First removal is refused, the way a busy or briefly unreachable daemon
	// refuses one.
	first := instance.Close()
	qt.Assert(t, first, qt.IsNotNil)
	qt.Check(t, first, qt.ErrorIs, removeFailed)
	qt.Check(t, first.Error(), qt.Contains, instance.Container())

	// The second call must RETRY. Marking the instance closed before the
	// removal succeeded is the obvious spelling and it leaks: the container
	// keeps running while every later Close reports success, and the deferred
	// release consumers use is exactly such a later call.
	second := instance.Close()
	qt.Check(t, second, qt.IsNil)
	_, removed := runner.calls()
	qt.Assert(t, removed, qt.HasLen, 2)

	// Only once it has actually succeeded does a further call stop asking.
	qt.Check(t, instance.Close(), qt.IsNil)
	_, removed = runner.calls()
	qt.Check(t, removed, qt.HasLen, 2)
}

func TestProvisionRemovesTheContainerWhenReadinessAndTheFirstRemovalBothFail(t *testing.T) {
	removeFailed := errors.New("Error response from daemon: removal already in progress")
	runner := &fakeRunner{
		hostPort:       "127.0.0.1:15432",
		removeFailures: []error{removeFailed},
	}

	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner:            runner,
		Ready:             neverReady,
		ReadyTimeout:      10 * time.Millisecond,
		ReleaseRetryDelay: time.Millisecond,
	})
	qt.Assert(t, err, qt.IsNotNil)
	t.Cleanup(release)

	// The readiness-failure path is the one branch that hands the caller no
	// instance, so nothing is left holding a handle to the container. A single
	// discarded Close there leaks it permanently however retryable Close has
	// since become -- the retry has to be ON this branch, not merely available
	// to a caller that does not exist.
	started, removed := runner.calls()
	qt.Assert(t, started, qt.HasLen, 1)
	// Asserted as a whole rather than by index: a Check records its verdict and
	// lets the test continue, so indexing after a failed length check panics and
	// takes the whole test binary -- and every later test -- with it. Comparing
	// the slice covers the count and the identity in one verdict that cannot.
	qt.Check(t, removed, qt.DeepEquals, []string{started[0], started[0]})
}

func TestProvisionRemovesTheContainerWhenStartFailsAfterCreatingIt(t *testing.T) {
	removeFailed := errors.New("Error response from daemon: removal already in progress")
	runner := &fakeRunner{
		// `docker run` succeeded and reading the published port failed, which
		// leaves a live container behind and is what Start reports as an error.
		startErr:       errors.New("read published port of container: exit status 1"),
		removeFailures: []error{removeFailed},
	}

	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner:            runner,
		Ready:             alwaysReady,
		ReleaseRetryDelay: time.Millisecond,
	})
	qt.Assert(t, err, qt.IsNotNil)
	t.Cleanup(release)

	// The third branch that hands the caller no instance. Like the
	// readiness-failure one, a removal refused here has no later caller to
	// retry it, so the retry has to be on the branch.
	started, removed := runner.calls()
	qt.Assert(t, started, qt.HasLen, 1)
	qt.Check(t, removed, qt.DeepEquals, []string{started[0], started[0]})
}

// stallingReady blocks until its context ends, which is what a probe caught
// inside a TCP or database handshake does.
func stallingReady(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestWaitReadyBoundsAProbeThatStalls(t *testing.T) {
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}

	// The caller's context is deliberately unbounded, which is what a command
	// context normally is. Before the probe carried the readiness deadline, the
	// loop's own `time.Now().After(deadline)` check could not fire -- control
	// never came back to it -- and this call did not return at all.
	started := time.Now()
	_, release, err := devdocker.Resolve(context.Background(), "docker://postgres/16/dev", devdocker.Options{
		Runner:            runner,
		Ready:             stallingReady,
		ReadyTimeout:      150 * time.Millisecond,
		ReleaseRetryDelay: time.Millisecond,
	})
	elapsed := time.Since(started)
	qt.Assert(t, err, qt.IsNotNil)
	t.Cleanup(release)

	qt.Check(t, err.Error(), qt.Contains, "did not become ready")
	// Generous enough not to flake, tight enough that an unbounded probe cannot
	// pass: without the deadline on the probe context this never returns.
	qt.Check(t, elapsed < 30*time.Second, qt.IsTrue, qt.Commentf("elapsed=%s", elapsed))
	// And the container is still cleaned up on the way out.
	started2, removed := runner.calls()
	qt.Assert(t, started2, qt.HasLen, 1)
	qt.Check(t, removed, qt.DeepEquals, started2)
}

func TestResolveReleaseRetriesARefusedRemoval(t *testing.T) {
	removeFailed := errors.New("Error response from daemon: removal already in progress")
	runner := &fakeRunner{
		hostPort:       "127.0.0.1:15432",
		removeFailures: []error{removeFailed, removeFailed},
	}

	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner:            runner,
		Ready:             alwaysReady,
		ReleaseRetryDelay: time.Millisecond,
	})
	qt.Assert(t, err, qt.IsNil)

	// Consumers call the release exactly once, from a defer. A removal refused
	// there has no later caller to retry it, so the retry has to live inside
	// the release rather than depend on one existing.
	release()

	started, removed := runner.calls()
	qt.Assert(t, started, qt.HasLen, 1)
	qt.Check(t, removed, qt.HasLen, 3)
}

// TestDockerCLIRemoveReportsAnUnreachableDaemon is what makes the retry above
// worth having.
//
// [DockerCLI.Remove] treats "No such container" as success, because a container
// the daemon already reaped is not a failure. That tolerance must not widen
// into swallowing everything: a removal that could not reach the daemon at all
// has to come back as an error, or the retry has nothing to act on and the
// release reports success over a container that is still running.
//
// It needs no daemon and starts nothing — an unreachable DOCKER_HOST is the
// failure being measured. Measured output:
//
//	failed to connect to the docker API at unix:///tmp/…sock; check if the
//	path is correct and if the daemon is running: … : exit status 1
func TestDockerCLIRemoveReportsAnUnreachableDaemon(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///tmp/ptah-devdocker-no-such-socket.sock")

	err := devdocker.DockerCLI{}.Remove(t.Context(), "ptah-dev-doesnotexist")

	qt.Assert(t, err, qt.IsNotNil)
}

func TestIsURLAnswersOnTheSchemeAlone(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   bool
	}{
		{name: "docker", rawURL: "docker://postgres/16/dev", want: true},
		{
			// A malformed docker URL must still be routed to this package, so
			// that it is refused with a sentence about the image rather than
			// reaching a connector that reports an unknown dialect.
			name:   "malformed docker",
			rawURL: "docker:///dev",
			want:   true,
		},
		{name: "leading space", rawURL: "  docker://postgres", want: true},
		{name: "postgres", rawURL: "postgres://localhost/db", want: false},
		{name: "empty", rawURL: "", want: false},
		{name: "docker in the path", rawURL: "sqlite://docker://x", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			qt.Check(t, devdocker.IsURL(tc.rawURL), qt.Equals, tc.want)
		})
	}
}
