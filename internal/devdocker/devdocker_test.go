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
	return nil
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
