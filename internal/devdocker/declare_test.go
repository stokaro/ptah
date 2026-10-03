package devdocker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

// baselineCall is one call a recordingBaseline saw.
type baselineCall struct {
	rawURL   string
	dialect  string
	baseline string
}

// recordingBaseline is a BaselineRunner that records its calls and answers
// err.
type recordingBaseline struct {
	mu    sync.Mutex
	err   error
	calls []baselineCall
}

func (r *recordingBaseline) run(_ context.Context, rawURL, dialect, baseline string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, baselineCall{rawURL: rawURL, dialect: dialect, baseline: baseline})
	return r.err
}

func (r *recordingBaseline) seen() []baselineCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append(make([]baselineCall, 0), r.calls...)
}

// noDatabaseCreated is a DatabaseCreator for URLs whose database the image
// already has.
func noDatabaseCreated(_ context.Context, _, _, _ string) error { return nil }

// TestResolveBuildsTheImageADeclarationNames provisions a URL whose fragment
// names a declaration with a build. The image is built under the name the URL
// gives it, started, and removed with the container.
func TestResolveBuildsTheImageADeclarationNames(t *testing.T) {
	c := qt.New(t)
	build := devdocker.Build{
		Context:    "/project/db",
		Dockerfile: "Dockerfile.dev",
		Target:     "dev",
		Args:       map[string]string{"PG": "17"},
		Platform:   "linux/amd64",
	}
	devdocker.Declare("docker.postgres.build-test", devdocker.Declaration{Build: &build})
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}

	_, release, err := devdocker.Resolve(t.Context(), "docker+postgres://_/ptah-built:1/dev#docker.postgres.build-test",
		devdocker.Options{Runner: runner, Ready: alwaysReady, CreateDatabase: noDatabaseCreated})
	c.Assert(err, qt.IsNil)
	c.Assert(runner.builds, qt.DeepEquals, []recordedBuild{{Image: "ptah-built:1", Build: build}})
	c.Assert(runner.startedImages, qt.DeepEquals, []string{"ptah-built:1"})
	c.Assert(runner.removedImages, qt.HasLen, 0)

	release()
	c.Assert(runner.removedImages, qt.DeepEquals, []string{"ptah-built:1"})
	started, removed := runner.calls()
	c.Assert(removed, qt.DeepEquals, started)
}

// TestResolveRunsTheBaselineOnTheDatabase pins where a declaration's baseline
// runs: on the database the URL names, after it was created, without the
// operator's parameters, whose search_path may name a schema the baseline
// creates.
func TestResolveRunsTheBaselineOnTheDatabase(t *testing.T) {
	c := qt.New(t)
	devdocker.Declare("docker.postgres.baseline-test", devdocker.Declaration{Baseline: "CREATE SCHEMA audit;"})
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	creator := &recordingCreator{}
	baseline := &recordingBaseline{}

	_, release, err := devdocker.Resolve(t.Context(),
		"docker+postgres://_/postgres:17/dev?search_path=audit#docker.postgres.baseline-test",
		devdocker.Options{Runner: runner, Ready: alwaysReady, CreateDatabase: creator.create, RunBaseline: baseline.run})
	c.Assert(err, qt.IsNil)
	t.Cleanup(release)

	c.Assert(creator.seen(), qt.HasLen, 1)
	calls := baseline.seen()
	c.Assert(calls, qt.HasLen, 1)
	c.Check(calls[0].rawURL, qt.Matches, `postgres://postgres:[0-9a-f]{48}@127\.0\.0\.1:15432/dev\?sslmode=disable`)
	c.Check(calls[0].dialect, qt.Equals, "postgres")
	c.Check(calls[0].baseline, qt.Equals, "CREATE SCHEMA audit;")
	c.Assert(runner.builds, qt.HasLen, 0)
}

// TestResolvePutsTheDeclaredEnvironmentFirst pins the order of the container
// environment: a declaration's own variables first, the provisioner's after
// them, so a declared POSTGRES_DB cannot point the run at another database.
func TestResolvePutsTheDeclaredEnvironmentFirst(t *testing.T) {
	c := qt.New(t)
	devdocker.Declare("docker.postgres.env-test", devdocker.Declaration{
		Env: []string{"TZ=UTC", "POSTGRES_DB=other"},
	})
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}

	_, release, err := devdocker.Resolve(t.Context(), "docker+postgres://_/postgres:17/dev#docker.postgres.env-test",
		devdocker.Options{Runner: runner, Ready: alwaysReady, CreateDatabase: noDatabaseCreated})
	c.Assert(err, qt.IsNil)
	t.Cleanup(release)

	c.Assert(runner.startedEnv, qt.HasLen, 1)
	env := runner.startedEnv[0]
	c.Assert(env[:2], qt.DeepEquals, []string{"TZ=UTC", "POSTGRES_DB=other"})
	c.Assert(env[len(env)-1], qt.Equals, "POSTGRES_DB=dev")
}

// TestResolveIgnoresAFragmentNoDeclarationNames is the control: a fragment is
// an ordinary part of an image URL, and the pinned community binary ignores
// it, so one nothing was declared under adds nothing.
func TestResolveIgnoresAFragmentNoDeclarationNames(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	baseline := &recordingBaseline{}

	_, release, err := devdocker.Resolve(t.Context(), "docker+postgres://_/postgres:17/dev#docker.postgres.nobody",
		devdocker.Options{Runner: runner, Ready: alwaysReady, CreateDatabase: noDatabaseCreated, RunBaseline: baseline.run})
	c.Assert(err, qt.IsNil)
	release()

	c.Assert(runner.builds, qt.HasLen, 0)
	c.Assert(baseline.seen(), qt.HasLen, 0)
	c.Assert(runner.removedImages, qt.HasLen, 0)
}

// TestResolveStartsNothingWhenTheBuildFails pins the failure path in front of
// the container: nothing was started, and the tag is not removed, since a
// failed build tags nothing and a tag of the same name is not the run's.
func TestResolveStartsNothingWhenTheBuildFails(t *testing.T) {
	c := qt.New(t)
	devdocker.Declare("docker.postgres.failed-build", devdocker.Declaration{Build: &devdocker.Build{Context: "/project"}})
	errBuild := errors.New("failed to solve")
	runner := &fakeRunner{hostPort: "127.0.0.1:15432", buildErr: errBuild}

	resolved, release, err := devdocker.Resolve(t.Context(), "docker+postgres://_/ptah-built:1/dev#docker.postgres.failed-build",
		devdocker.Options{Runner: runner, Ready: alwaysReady, CreateDatabase: noDatabaseCreated})
	t.Cleanup(release)

	c.Assert(err, qt.ErrorIs, errBuild)
	c.Assert(err, qt.ErrorMatches, `build dev database image ptah-built:1: failed to solve`)
	c.Assert(resolved, qt.Equals, "")
	started, _ := runner.calls()
	c.Assert(started, qt.HasLen, 0)
	c.Assert(runner.removedImages, qt.HasLen, 0)
}

// TestResolveRemovesTheContainerAndImageWhenTheBaselineFails holds the release
// discipline on the step a declaration adds after the container starts.
func TestResolveRemovesTheContainerAndImageWhenTheBaselineFails(t *testing.T) {
	c := qt.New(t)
	devdocker.Declare("docker.postgres.failed-baseline", devdocker.Declaration{
		Build:    &devdocker.Build{Context: "/project"},
		Baseline: "CREATE SCHEMA audit;",
	})
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	errRefused := errors.New("permission denied")
	baseline := &recordingBaseline{err: errRefused}

	_, release, err := devdocker.Resolve(t.Context(), "docker+postgres://_/ptah-built:1/dev#docker.postgres.failed-baseline",
		devdocker.Options{Runner: runner, Ready: alwaysReady, CreateDatabase: noDatabaseCreated, RunBaseline: baseline.run})
	t.Cleanup(release)

	c.Assert(err, qt.ErrorIs, errRefused)
	c.Assert(err, qt.ErrorMatches, `run the baseline on dev database ptah-built:1: permission denied`)
	started, removed := runner.calls()
	c.Assert(started, qt.HasLen, 1)
	c.Assert(removed, qt.DeepEquals, started)
	c.Assert(runner.removedImages, qt.DeepEquals, []string{"ptah-built:1"})
}

// TestResolveWaitsTheTimeoutADeclarationSets pins that a declaration's
// readiness timeout replaces the caller's.
func TestResolveWaitsTheTimeoutADeclarationSets(t *testing.T) {
	c := qt.New(t)
	devdocker.Declare("docker.postgres.timeout-test", devdocker.Declaration{ReadyTimeout: time.Millisecond})
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}

	_, release, err := devdocker.Resolve(t.Context(), "docker+postgres://_/postgres:17/dev#docker.postgres.timeout-test",
		devdocker.Options{Runner: runner, Ready: neverReady, ReadyTimeout: time.Hour})
	t.Cleanup(release)

	c.Assert(err, qt.ErrorMatches, `dev database postgres:17 did not become ready: timed out after 1ms: connection refused`)
}
