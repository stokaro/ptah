package devdocker_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
			// A TRAILING space is not whitespace around the URL, it is an
			// ordinary character in the last path segment. Measured on the
			// pinned binary v1.3.0, exit status read from an unpiped
			// `schema inspect -u file://schema.sql --dev-url` invocation:
			// `docker://postgres/16/dev ` exits 0, and it provisions a database
			// whose name ends in a space. Trimming the value renamed that
			// database to `dev` -- a different database, silently.
			name:         "a trailing space is part of the database name",
			rawURL:       "docker://postgres/16/dev ",
			wantImage:    "postgres:16",
			wantDatabase: "dev ",
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
			c := qt.New(t)
			t.Parallel()
			spec, err := devdocker.Parse(tc.rawURL)
			c.Assert(err, qt.IsNil)
			c.Check(spec.Image, qt.Equals, tc.wantImage)
			c.Check(spec.Database, qt.Equals, tc.wantDatabase)
			c.Check(spec.Dialect, qt.Equals, tc.wantDialect)
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
		{
			// A `?` reaches the database name only as `%3F`, since an
			// unescaped one starts the URL query. It is refused because a MySQL
			// DSN carries the name literally and would read everything after it
			// as connection parameters, and no escaping fixes that without
			// renaming the database.
			name:    "a query separator in the database name",
			rawURL:  "docker://mysql/8/foo%3Fbar",
			wantErr: `docker --dev-url database name "foo?bar" contains a query separator`,
		},
		{
			// The dangerous one. libpq and pgx let a `host` in the query
			// override the URL's own authority, so this would provision a
			// throwaway container, pass readiness against it (the probe URL
			// carries the engine's parameters only), and hand the consumer a
			// URL pointing at a production server -- which then gets every
			// table dropped and a migration directory replayed into it. The
			// alias checks that would normally catch a dev URL aiming at a real
			// database are deliberately skipped for docker:// URLs.
			name:   "a host parameter would redirect the connection",
			rawURL: "docker://postgres/16/dev?host=prod.example",
			wantErr: `docker --dev-url parameter "host" would point the connection away from` +
				` the container this URL provisions; remove it, or pass a directly` +
				` connectable dev database URL instead`,
		},
		{
			// `service` is the quiet one: it names a stanza in a connection
			// service file, host and credentials included, so it reroutes
			// without any of the obvious parameter names appearing in the URL.
			name:   "a service parameter would redirect the connection",
			rawURL: "docker://postgres/16/dev?service=prod",
			wantErr: `docker --dev-url parameter "service" would point the connection away from` +
				` the container this URL provisions; remove it, or pass a directly` +
				` connectable dev database URL instead`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			_, err := devdocker.Parse(tc.rawURL)
			c.Assert(err, qt.IsNotNil)
			c.Check(err.Error(), qt.Equals, tc.wantErr)
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
	return append(make([]string, 0), f.started...), append(make([]string, 0), f.removed...)
}

// testPassword stands in for the generated superuser password wherever a URL is
// asserted byte for byte. The real one is random per instance; see
// TestProvisionGeneratesADistinctPasswordPerInstance.
const testPassword = "testpw"

// alwaysReady and neverReady stand in for the connection probe.
func alwaysReady(context.Context, string) error { return nil }

var errNotListening = errors.New("connection refused")

func neverReady(context.Context, string) error { return errNotListening }

func TestResolveLeavesANonDockerURLAloneAndStartsNothing(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	resolved, release, err := devdocker.Resolve(t.Context(), "postgres://u:p@localhost:5432/db", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	c.Assert(err, qt.IsNil)
	t.Cleanup(release)

	c.Check(resolved, qt.Equals, "postgres://u:p@localhost:5432/db")
	started, removed := runner.calls()
	// Both halves matter. A build that provisioned for every dev URL would pass
	// the URL assertion above by accident whenever the fake handed back the
	// same string, and would still be starting a container per invocation.
	c.Check(started, qt.HasLen, 0)
	c.Check(removed, qt.HasLen, 0)
}

func TestResolveProvisionsADockerURLAndReleaseRemovesTheContainer(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	resolved, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	c.Assert(err, qt.IsNil)

	// The password is generated per instance, so the shape is asserted and the
	// secret is not. Its per-instance-ness has its own test below.
	c.Check(resolved, qt.Matches, `postgres://postgres:[0-9a-f]{48}@127\.0\.0\.1:15432/dev\?sslmode=disable`)
	started, removed := runner.calls()
	c.Assert(started, qt.HasLen, 1)
	// Nothing is removed until the caller releases: a release that removed
	// eagerly would hand back a URL to a container that is already gone.
	c.Check(removed, qt.HasLen, 0)

	release()
	started, removed = runner.calls()
	c.Check(removed, qt.DeepEquals, started)
}

func TestResolveRemovesTheContainerWhenTheServerNeverBecomesReady(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner:       runner,
		Ready:        neverReady,
		ReadyTimeout: 10 * time.Millisecond,
	})
	c.Assert(err, qt.IsNotNil)
	t.Cleanup(release)

	c.Check(err, qt.ErrorIs, errNotListening)
	c.Check(err.Error(), qt.Contains, "did not become ready")
	started, removed := runner.calls()
	// The whole point of this row: a failed wait must not leave the container
	// behind, and the caller is handed an error rather than a releasable
	// instance, so nothing else can remove it.
	c.Assert(started, qt.HasLen, 1)
	c.Check(removed, qt.DeepEquals, started)
}

func TestResolveRemovesTheContainerWhenTheRuntimeFailsToStartIt(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{startErr: errors.New("no such image")}
	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	c.Assert(err, qt.IsNotNil)
	t.Cleanup(release)

	started, removed := runner.calls()
	// A `docker run` that fails after creating the container leaves one behind,
	// so removal is attempted on this path too rather than assumed unnecessary.
	c.Assert(started, qt.HasLen, 1)
	c.Check(removed, qt.DeepEquals, started)
}

func TestResolveReportsAnUnavailableRuntimeWithoutStartingAnything(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{availableErr: errors.New("docker daemon is not running")}
	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	c.Assert(err, qt.IsNotNil)
	t.Cleanup(release)

	c.Check(err.Error(), qt.Equals, "docker daemon is not running")
	started, _ := runner.calls()
	c.Check(started, qt.HasLen, 0)
}

func TestConcurrentProvisioningPicksDistinctContainerNames(t *testing.T) {
	c := qt.New(t)
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
	c.Assert(errors.Join(errs...), qt.IsNil)

	started, _ := runner.calls()
	c.Assert(started, qt.HasLen, parallel)
	unique := make(map[string]struct{}, parallel)
	for _, name := range started {
		unique[name] = struct{}{}
	}
	// A name derived from the URL rather than from randomness would collide on
	// every one of these, and two concurrent runs would fight over one
	// container -- the "parallel invocations do not collide" half of #844.
	c.Check(unique, qt.HasLen, parallel)
}

func TestReleaseIsSafeToCallTwice(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	_, release, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	c.Assert(err, qt.IsNil)

	release()
	release()
	_, removed := runner.calls()
	// Consumers defer the release and some also call it on an early return, so
	// a second call must not issue a second removal against a name the daemon
	// may by then have given to nobody.
	c.Check(removed, qt.HasLen, 1)
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
		{ // #nosec G101 -- this package's own throwaway dev password, asserted verbatim
			name:    "no parameters leaves the engine defaults",
			rawURL:  "docker://postgres/16/dev",
			wantURL: "postgres://postgres:testpw@127.0.0.1:15432/dev?sslmode=disable",
		},
		{ // #nosec G101 -- this package's own throwaway dev password, asserted verbatim
			// The row the pinned binary answers exit 1 on. The parameter has to
			// reach the connection for Ptah to answer it at all.
			name:    "search_path survives alongside the defaults",
			rawURL:  "docker://postgres/16/dev?search_path=app",
			wantURL: "postgres://postgres:testpw@127.0.0.1:15432/dev?search_path=app&sslmode=disable",
		},
		{ // #nosec G101 -- this package's own throwaway dev password, asserted verbatim
			// The operator wins on a key the engine also sets. Defaults exist to
			// make a throwaway container reachable; an operator who writes one
			// has a reason the container cannot know.
			name:    "an operator sslmode replaces the default",
			rawURL:  "docker://postgres/16/dev?sslmode=require",
			wantURL: "postgres://postgres:testpw@127.0.0.1:15432/dev?sslmode=require",
		},
		{ // #nosec G101 -- this package's own throwaway dev password, asserted verbatim
			// Two keys, one of which the engine also sets: the unmentioned
			// default must survive. A build that used the operator's query
			// wholesale instead of merging would drop sslmode here.
			name:    "an unmentioned default survives beside an operator key",
			rawURL:  "docker://postgres/16/dev?search_path=app",
			wantURL: "postgres://postgres:testpw@127.0.0.1:15432/dev?search_path=app&sslmode=disable",
		},
		{
			// MySQL has no default parameters, so its URL carries a query only
			// when the operator wrote one -- and no bare `?`, which the driver
			// would read as an empty parameter list.
			name:    "mysql without parameters carries no query at all",
			rawURL:  "docker://mysql/8/dev",
			wantURL: "mysql://root:testpw@tcp(127.0.0.1:15432)/dev",
		},
		{ // #nosec G101 -- this package's own throwaway dev password, asserted verbatim
			// url.Parse decodes the path before the formatter sees it, so a
			// database written `foo%23bar` arrives as `foo#bar` and must be
			// re-escaped. Measured before this: the URL became
			// `…/foo#bar?sslmode=disable`, whose path is `/foo` and whose
			// FRAGMENT is `bar?sslmode=disable` -- so the run connected to a
			// database called `foo`, lost `sslmode=disable` with it, and spent
			// the full two-minute readiness budget before exiting 1 where the
			// pinned binary exits 0.
			name:    "a delimiter in the database name is escaped for postgres",
			rawURL:  "docker://postgres/16/foo%23bar",
			wantURL: "postgres://postgres:testpw@127.0.0.1:15432/foo%23bar?sslmode=disable",
		},
		{
			// The MySQL family is the other way round: its DSN is not a URL,
			// the driver reads the name literally, and escaping would connect
			// to `foo%23bar` while the container created `foo#bar`.
			name:    "the same delimiter stays literal for mysql",
			rawURL:  "docker://mysql/8/foo%23bar",
			wantURL: "mysql://root:testpw@tcp(127.0.0.1:15432)/foo#bar",
		},
		{
			name:    "mysql carries the operator parameters",
			rawURL:  "docker://mysql/8/dev?parseTime=true",
			wantURL: "mysql://root:testpw@tcp(127.0.0.1:15432)/dev?parseTime=true",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			spec, err := devdocker.Parse(tc.rawURL)
			c.Assert(err, qt.IsNil)
			c.Check(spec.URL("127.0.0.1:15432", testPassword), qt.Equals, tc.wantURL)
		})
	}
}

func TestReadyURLProbesTheServerWithoutTheOperatorParameters(t *testing.T) {
	c := qt.New(t)
	spec, err := devdocker.Parse("docker://postgres/16/dev?search_path=app")
	c.Assert(err, qt.IsNil)

	// The readiness wait asks whether the CONTAINER is up. Probing with
	// `search_path=app` cannot answer that: a fresh container never has the
	// schema, so every attempt fails and the wait burns its whole budget before
	// reporting a deterministic error. Measured before this split, that cost
	// two minutes; after it the same run refuses in about five seconds.
	c.Check(spec.ReadyURL("127.0.0.1:15432", testPassword), qt.Equals,
		"postgres://postgres:testpw@127.0.0.1:15432/dev?sslmode=disable")
	// The engine's own defaults are still on the probe URL: dropping sslmode
	// here would make the probe fail against a container that speaks no TLS.
	c.Check(spec.URL("127.0.0.1:15432", testPassword), qt.Equals,
		"postgres://postgres:testpw@127.0.0.1:15432/dev?search_path=app&sslmode=disable")
}

func TestCloseStaysRetryableUntilRemovalSucceeds(t *testing.T) {
	c := qt.New(t)
	removeFailed := errors.New("Error response from daemon: removal already in progress")
	runner := &fakeRunner{hostPort: "127.0.0.1:15432", removeFailures: []error{removeFailed}}

	instance, err := devdocker.Provision(t.Context(), "docker://postgres/16/dev", devdocker.Options{
		Runner: runner,
		Ready:  alwaysReady,
	})
	c.Assert(err, qt.IsNil)

	// First removal is refused, the way a busy or briefly unreachable daemon
	// refuses one.
	first := instance.Close()
	c.Assert(first, qt.IsNotNil)
	c.Check(first, qt.ErrorIs, removeFailed)
	c.Check(first.Error(), qt.Contains, instance.Container())

	// The second call must RETRY. Marking the instance closed before the
	// removal succeeded is the obvious spelling and it leaks: the container
	// keeps running while every later Close reports success, and the deferred
	// release consumers use is exactly such a later call.
	second := instance.Close()
	c.Check(second, qt.IsNil)
	_, removed := runner.calls()
	c.Assert(removed, qt.HasLen, 2)

	// Only once it has actually succeeded does a further call stop asking.
	c.Check(instance.Close(), qt.IsNil)
	_, removed = runner.calls()
	c.Check(removed, qt.HasLen, 2)
}

func TestProvisionRemovesTheContainerWhenReadinessAndTheFirstRemovalBothFail(t *testing.T) {
	c := qt.New(t)
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
	c.Assert(err, qt.IsNotNil)
	t.Cleanup(release)

	// The readiness-failure path is the one branch that hands the caller no
	// instance, so nothing is left holding a handle to the container. A single
	// discarded Close there leaks it permanently however retryable Close has
	// since become -- the retry has to be ON this branch, not merely available
	// to a caller that does not exist.
	started, removed := runner.calls()
	c.Assert(started, qt.HasLen, 1)
	// Asserted as a whole rather than by index: a Check records its verdict and
	// lets the test continue, so indexing after a failed length check panics and
	// takes the whole test binary -- and every later test -- with it. Comparing
	// the slice covers the count and the identity in one verdict that cannot.
	c.Check(removed, qt.DeepEquals, []string{started[0], started[0]})
}

// TestProvisionGeneratesADistinctPasswordPerInstance pins the credential that a
// remote daemon made load-bearing.
//
// The password used to be the constant `ptah-dev`, justified in a comment by the
// container publishing on loopback only. That premise stopped holding the moment
// a remote daemon began publishing on every interface of its host: a known
// superuser password on a reachable ephemeral port lets any peer that finds it
// read the replayed schema, or write to it and corrupt a lint or diff result.
// The binding cannot be tightened -- a daemon can only publish on interfaces it
// owns, and the one this process must reach is not that host's loopback -- so
// the credential is what has to change.
func TestProvisionGeneratesADistinctPasswordPerInstance(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	options := devdocker.Options{Runner: runner, Ready: alwaysReady}

	first, releaseFirst, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", options)
	c.Assert(err, qt.IsNil)
	t.Cleanup(releaseFirst)
	second, releaseSecond, err := devdocker.Resolve(t.Context(), "docker://postgres/16/dev", options)
	c.Assert(err, qt.IsNil)
	t.Cleanup(releaseSecond)

	c.Check(first, qt.Not(qt.Equals), second)
	// And neither carries the constant this replaced, which is the string an
	// operator's old notes or a copied script would still be holding.
	c.Check(first, qt.Not(qt.Contains), "ptah-dev@")
	c.Check(second, qt.Not(qt.Contains), "ptah-dev@")
}

func TestProvisionRemovesTheContainerWhenStartFailsAfterCreatingIt(t *testing.T) {
	c := qt.New(t)
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
	c.Assert(err, qt.IsNotNil)
	t.Cleanup(release)

	// The third branch that hands the caller no instance. Like the
	// readiness-failure one, a removal refused here has no later caller to
	// retry it, so the retry has to be on the branch.
	started, removed := runner.calls()
	c.Assert(started, qt.HasLen, 1)
	c.Check(removed, qt.DeepEquals, []string{started[0], started[0]})
}

// stallingReady blocks until its context ends, which is what a probe caught
// inside a TCP or database handshake does.
func stallingReady(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestWaitReadyBoundsAProbeThatStalls(t *testing.T) {
	c := qt.New(t)
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
	c.Assert(err, qt.IsNotNil)
	t.Cleanup(release)

	c.Check(err.Error(), qt.Contains, "did not become ready")
	// Generous enough not to flake, tight enough that an unbounded probe cannot
	// pass: without the deadline on the probe context this never returns.
	c.Check(elapsed < 30*time.Second, qt.IsTrue, qt.Commentf("elapsed=%s", elapsed))
	// And the container is still cleaned up on the way out.
	started2, removed := runner.calls()
	c.Assert(started2, qt.HasLen, 1)
	c.Check(removed, qt.DeepEquals, started2)
}

func TestResolveReleaseRetriesARefusedRemoval(t *testing.T) {
	c := qt.New(t)
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
	c.Assert(err, qt.IsNil)

	// Consumers call the release exactly once, from a defer. A removal refused
	// there has no later caller to retry it, so the retry has to live inside
	// the release rather than depend on one existing.
	release()

	started, removed := runner.calls()
	c.Assert(started, qt.HasLen, 1)
	c.Check(removed, qt.HasLen, 3)
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
	c := qt.New(t)
	t.Setenv("DOCKER_HOST", "unix:///tmp/ptah-devdocker-no-such-socket.sock")

	err := devdocker.DockerCLI{}.Remove(t.Context(), "ptah-dev-doesnotexist")

	c.Assert(err, qt.IsNotNil)
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
		{
			// A LEADING space is not the same URL with whitespace on it. The
			// pinned binary v1.3.0 parses ` docker://postgres/16/dev` as a
			// relative path whose first segment is `docker:` and exits 1 with
			// `sql/sqlclient: parse open url: first path segment in URL cannot
			// contain colon`, having started nothing. This row wanted `true`
			// while this package trimmed, and that trim is what made
			// `ptah-compat schema inspect --dev-url " docker://postgres/16/dev"`
			// exit 0 with a container started, where the pinned binary exits 1
			// -- the one direction compatibility rule (a) forbids outright.
			name:   "leading space is not a docker URL",
			rawURL: "  docker://postgres",
			want:   false,
		},
		{
			// url.Parse lowercases a scheme, so this IS a docker URL to
			// Parse, to atlasurl.DialectFromURL, and to the pinned binary,
			// which reaches the Docker daemon for it. A case-sensitive prefix
			// match said otherwise, so Resolve passed the value through
			// unprovisioned and the connector answered `unsupported database
			// dialect: docker` -- a capability the pinned binary has, removed
			// by a spelling.
			name:   "uppercase scheme",
			rawURL: "DOCKER://postgres/16/dev",
			want:   true,
		},
		{name: "postgres", rawURL: "postgres://localhost/db", want: false},
		{name: "empty", rawURL: "", want: false},
		{name: "docker in the path", rawURL: "sqlite://docker://x", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			c.Check(devdocker.IsURL(tc.rawURL), qt.Equals, tc.want)
		})
	}
}

// TestIsURLAndParseAgreeOnTheScheme pins the invariant [devdocker.IsURL]
// documents but only one half of which was covered: the two functions answer
// the same question about the scheme.
//
// They have to. [devdocker.Resolve] asks IsURL whether to provision, and Parse
// is the first thing the provisioner does, so a value the two disagree about is
// routed to a provisioner that then refuses it as not its own -- or passed
// through to a connector that reports an unknown dialect for a URL that names a
// container. Only IsURL was pinned on the case of the scheme; Parse taking a
// case-sensitive prefix, or IsURL taking one, would leave the other's rows
// green.
//
// The assertion is not "Parse succeeds": rows like `docker://sqlite/dev` are
// docker URLs that Parse refuses for the image. What is asserted is the SCHEME
// verdict alone -- whether Parse got past it.
func TestIsURLAndParseAgreeOnTheScheme(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
	}{
		{name: "lowercase scheme", rawURL: "docker://postgres/16/dev"},
		// url.Parse lowercases a scheme, and so does the pinned community
		// binary v1.3.0: measured, `--dev-url DOCKER://postgres/16/dev` makes
		// it answer `failed to connect to the docker API`, byte for byte what
		// the lowercase spelling answers on a host with no daemon, while
		// `notascheme://` answers `unknown driver`.
		{name: "uppercase scheme", rawURL: "DOCKER://postgres/16/dev"},
		{name: "mixed-case scheme", rawURL: "DoCkEr://mariadb/11/dev"},
		{name: "docker URL Parse refuses for the image", rawURL: "docker://sqlite/dev"},
		{name: "docker URL with no engine", rawURL: "docker:///dev"},
		{name: "leading space is not a docker URL", rawURL: " docker://postgres/16/dev"},
		{name: "another scheme entirely", rawURL: "postgres://localhost/db"},
		{name: "a scheme docker is a prefix of", rawURL: "dockerx://postgres/16/dev"},
		{name: "empty", rawURL: ""},
		{name: "docker in the path", rawURL: "sqlite://docker://x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			_, err := devdocker.Parse(tc.rawURL)
			c.Check(devdocker.IsURL(tc.rawURL), qt.Equals,
				!parseRefusedTheValueAsNotADockerURL(err, tc.rawURL))
		})
	}
}

// parseRefusedTheValueAsNotADockerURL reports whether err is one of the two
// refusals [devdocker.Parse] produces for a value that is not a docker URL at
// all: one url.Parse cannot read, and one whose scheme names something else.
//
// Every other refusal Parse can produce -- the image, the path, a connection
// parameter -- is reached only after the value was accepted as a docker URL, so
// folding them in would make `docker://sqlite/dev` look like a non-docker URL
// and the invariant would stop discriminating.
func parseRefusedTheValueAsNotADockerURL(err error, rawURL string) bool {
	return err != nil &&
		(strings.HasPrefix(err.Error(), "parse docker --dev-url: ") ||
			err.Error() == fmt.Sprintf("not a docker --dev-url: %q", rawURL))
}
