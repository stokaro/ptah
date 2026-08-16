//go:build integration

package integration_test

import (
	"context"
	"net"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/devdocker"
)

// These tests prove the two claims of stokaro/ptah#844 that an exit status
// cannot: that a `docker://` dev URL really produces a reachable database
// server, and that the container is really removed again -- on the successful
// path and on the failing one alike.
//
// An exit code of 0 proves nothing about whether a database existed. Every
// assertion below is therefore either a query executed against the provisioned
// server or a census taken from the container runtime itself.
//
// This file deliberately does not skip when Docker is missing. The `integration`
// build tag already means "infrastructure is available", the workflow that runs
// these tests starts containers of its own in earlier steps, and a test that
// skips itself reads as a test that passed.

// devDockerTestURL is the dev URL under test. The alpine tag is used because it
// is the smallest official image that still is PostgreSQL; the tag travels
// through [devdocker.Parse] exactly as `16` would.
const devDockerTestURL = "docker://postgres/16-alpine/ptahdev"

// devDockerCensus lists the containers this package has running, by the label
// it stamps on every one of them.
//
// The census is taken from `docker ps`, not from the package's own bookkeeping,
// because the question being asked is whether the RUNTIME still holds a
// container -- a leak is precisely the case where the package believes it does
// not.
func devDockerCensus(c *qt.C) []string {
	c.Helper()
	out, err := exec.Command(
		"docker", "ps", "--all", "--quiet", "--filter", "label="+devdocker.ContainerLabel,
	).Output()
	c.Assert(err, qt.IsNil)
	trimmed := strings.TrimSpace(string(out))
	return strings.FieldsFunc(trimmed, func(r rune) bool { return r == '\n' })
}

// onlyNewContainer returns the single container the census gained, failing if
// the two lists differ by anything else.
func onlyNewContainer(c *qt.C, before, during []string) string {
	c.Helper()
	added := slices.DeleteFunc(slices.Clone(during), func(id string) bool {
		return slices.Contains(before, id)
	})
	c.Assert(added, qt.HasLen, 1, qt.Commentf("before=%v during=%v", before, during))
	return added[0]
}

// devDockerPortMapping reads the published binding of a container's PostgreSQL
// port, as the runtime reports it.
func devDockerPortMapping(c *qt.C, container string) string {
	c.Helper()
	out, err := exec.Command("docker", "port", container, "5432/tcp").Output()
	c.Assert(err, qt.IsNil)
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	c.Assert(len(lines) > 0, qt.IsTrue)
	return strings.TrimSpace(lines[0])
}

// devDockerDaemonHost returns the endpoint the docker CLI is pointed at, with
// `DOCKER_HOST` taking precedence over the selected context -- the same
// precedence the package under test resolves, and read from the same place, so
// this stays an independent reading rather than an echo of the package's own
// answer.
//
// An empty result means the CLI is using its built-in default, which is a
// socket on this machine.
func devDockerDaemonHost(c *qt.C) string {
	c.Helper()
	if host := os.Getenv("DOCKER_HOST"); host != "" {
		return host
	}
	out, err := exec.Command(
		"docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}",
	).Output()
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(string(out))
}

// devDockerDaemonIsRemote reports whether the daemon runs on another machine.
func devDockerDaemonIsRemote(c *qt.C) bool {
	c.Helper()
	host := devDockerDaemonHost(c)
	remote, err := dockerEndpointIsRemote(host)
	c.Assert(err, qt.IsNil, qt.Commentf("docker endpoint %q", host))
	return remote
}

// expectedDevDockerBinding returns the address the dev database has to be
// published on, which is decided by where the daemon runs.
//
// A daemon on this machine must publish on loopback and nowhere else. A daemon
// on another machine cannot be reached over loopback at all -- loopback there
// names the daemon's own host -- so it publishes on every interface of that
// host and announces it in a warning on the same run.
func expectedDevDockerBinding(c *qt.C) string {
	c.Helper()
	if devDockerDaemonIsRemote(c) {
		return "0.0.0.0:"
	}
	return "127.0.0.1:"
}

// devDockerDaemonLocation names where the daemon runs, for the failure message.
func devDockerDaemonLocation(c *qt.C) string {
	c.Helper()
	if devDockerDaemonIsRemote(c) {
		return "a daemon on another machine"
	}
	return "a daemon on this machine"
}

// dockerEndpointIsRemote classifies a docker endpoint by whether the daemon it
// names runs on another machine.
//
// A unix socket or a named pipe is on this machine by construction, and so is
// the CLI's built-in default, which is what an empty endpoint means. A `tcp://`
// or `ssh://` endpoint is remote unless it names a loopback address.
func dockerEndpointIsRemote(endpoint string) (bool, error) {
	if endpoint == "" ||
		strings.HasPrefix(endpoint, "unix://") ||
		strings.HasPrefix(endpoint, "npipe://") {
		return false, nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return false, err
	}
	hostname := parsed.Hostname()
	if hostname == "" || hostname == "localhost" {
		return false, nil
	}
	// A name that is not an address cannot be loopback here: ParseIP returns
	// nil for it, and a nil IP is not loopback, so the endpoint counts as
	// remote -- which is the reading that matters, since the container's port
	// has to be reachable across the network for the run to work at all.
	return !net.ParseIP(hostname).IsLoopback(), nil
}

// The branch this picks decides which binding the provisioning test demands, so
// a wrong answer in either direction would assert the opposite of the truth:
// calling a local daemon remote would accept a dev database published on every
// interface of this machine, and calling a remote daemon local would demand a
// loopback binding no client could reach.
func TestDockerEndpointIsRemote(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     bool
	}{
		{name: "the CLI default", endpoint: "", want: false},
		{name: "a unix socket", endpoint: "unix:///var/run/docker.sock", want: false},
		{name: "a windows named pipe", endpoint: "npipe:////./pipe/docker_engine", want: false},
		{name: "tcp on the loopback address", endpoint: "tcp://127.0.0.1:2375", want: false},
		{name: "tcp on the loopback name", endpoint: "tcp://localhost:2375", want: false},
		{name: "tcp on the IPv6 loopback", endpoint: "tcp://[::1]:2375", want: false},
		{name: "tcp on another address", endpoint: "tcp://192.0.2.10:2375", want: true},
		{name: "tcp on another name", endpoint: "tcp://docker-host.example:2375", want: true},
		{name: "ssh to another machine", endpoint: "ssh://user@docker-host.example", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			remote, err := dockerEndpointIsRemote(tt.endpoint)
			c.Assert(err, qt.IsNil)
			c.Assert(remote, qt.Equals, tt.want)
		})
	}
}

func TestDevDockerProvisionsAReachableServerAndRemovesIt(t *testing.T) {
	c := qt.New(t)
	c.Assert(devdocker.DockerCLI{}.Available(t.Context()), qt.IsNil)

	before := devDockerCensus(c)

	resolved, release, err := devdocker.Resolve(t.Context(), devDockerTestURL, devdocker.Options{})
	c.Assert(err, qt.IsNil)
	// Registered immediately, so a failure in any assertion below still removes
	// the container. That is the same discipline the production consumers use
	// and it is what the failing-run census further down measures.
	released := false
	c.Cleanup(func() {
		release()
		released = true
	})

	// The URL is a real one for the dialect the docker URL named, not the
	// docker URL passed through.
	c.Assert(strings.HasPrefix(resolved, "postgres://"), qt.IsTrue,
		qt.Commentf("resolved=%q", resolved))
	c.Assert(resolved, qt.Contains, "/ptahdev",
		qt.Commentf("resolved=%q does not name the database from the URL", resolved))

	during := devDockerCensus(c)
	c.Assert(during, qt.HasLen, len(before)+1,
		qt.Commentf("before=%v during=%v", before, during))

	// The binding is asserted from the runtime rather than from the publish
	// flag, because the flag has three fields and getting the middle one wrong
	// is silent: `0.0.0.0:5432` reads the bind address as the host port and is
	// refused outright, while `0.0.0.0::5432` exposes the dev database on every
	// interface of the host. The pinned community binary v1.3.0 does the latter
	// unconditionally -- measured, its container publishes
	// `0.0.0.0:59319->5432/tcp` -- so the loopback row below pins a divergence
	// Ptah keeps on purpose.
	//
	// Which binding is correct depends on where the daemon runs, and the
	// package decides that for itself, so the expectation is chosen the same
	// way -- see expectedDevDockerBinding.
	mapping := devDockerPortMapping(c, onlyNewContainer(c, before, during))
	c.Assert(strings.HasPrefix(mapping, expectedDevDockerBinding(c)), qt.IsTrue,
		qt.Commentf("the daemon at %q published %q, which is not the binding %s calls for",
			devDockerDaemonHost(c), mapping, devDockerDaemonLocation(c)))

	// The proof: a statement executed on the provisioned server, and its effect
	// read back. A port that accepts a TCP connection is not a database.
	conn, err := dbschema.ConnectToDatabase(t.Context(), resolved)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err = conn.ExecContext(ctx, "CREATE TABLE provisioning_proof (id integer NOT NULL)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "INSERT INTO provisioning_proof (id) VALUES (7)")
	c.Assert(err, qt.IsNil)

	var readBack int
	row := conn.QueryRowContext(ctx, "SELECT id FROM provisioning_proof")
	c.Assert(row.Scan(&readBack), qt.IsNil)
	c.Assert(readBack, qt.Equals, 7)

	// And the server really is the engine the URL named, not something that
	// merely speaks the wire protocol.
	var version string
	c.Assert(conn.QueryRowContext(ctx, "SELECT version()").Scan(&version), qt.IsNil)
	c.Assert(version, qt.Contains, "PostgreSQL", qt.Commentf("version=%q", version))

	_ = conn.Close()
	release()
	released = true
	c.Assert(released, qt.IsTrue)

	after := devDockerCensus(c)
	c.Assert(after, qt.DeepEquals, before,
		qt.Commentf("release left containers behind: before=%v after=%v", before, after))
}

func TestDevDockerRemovesTheContainerWhenTheRunFails(t *testing.T) {
	c := qt.New(t)
	c.Assert(devdocker.DockerCLI{}.Available(t.Context()), qt.IsNil)

	before := devDockerCensus(c)

	// A readiness budget a freshly started PostgreSQL cannot meet. The container
	// is started -- this is not a failure in front of the runtime -- and then
	// the wait gives up, which is the shape of every real provisioning failure:
	// an image that boots but never serves.
	_, release, err := devdocker.Resolve(t.Context(), devDockerTestURL, devdocker.Options{
		ReadyTimeout: time.Nanosecond,
	})
	c.Cleanup(release)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "did not become ready",
		qt.Commentf("err=%v", err))

	after := devDockerCensus(c)
	// The census is taken with no release call in between: on this path the
	// caller never receives an instance, so if Provision did not remove the
	// container nothing else ever would.
	c.Assert(after, qt.DeepEquals, before,
		qt.Commentf("a failed provisioning leaked a container: before=%v after=%v", before, after))
}

func TestDevDockerParallelInvocationsDoNotCollide(t *testing.T) {
	c := qt.New(t)
	c.Assert(devdocker.DockerCLI{}.Available(t.Context()), qt.IsNil)

	before := devDockerCensus(c)

	const parallel = 3
	urls := make([]string, parallel)
	releases := make([]func(), parallel)
	errs := make([]error, parallel)
	done := make(chan int, parallel)
	for i := range parallel {
		go func() {
			urls[i], releases[i], errs[i] = devdocker.Resolve(t.Context(), devDockerTestURL, devdocker.Options{})
			done <- i
		}()
	}
	for range parallel {
		<-done
	}
	for _, release := range releases {
		c.Cleanup(release)
	}
	for i := range parallel {
		c.Assert(errs[i], qt.IsNil, qt.Commentf("invocation %d", i))
	}

	// Distinct published ports are the observable form of "did not collide":
	// two runs that shared a container would share its port, and one of them
	// would be reading the other's schema.
	unique := make(map[string]struct{}, parallel)
	for _, resolved := range urls {
		unique[resolved] = struct{}{}
	}
	c.Assert(unique, qt.HasLen, parallel, qt.Commentf("urls=%v", urls))

	during := devDockerCensus(c)
	c.Assert(during, qt.HasLen, len(before)+parallel,
		qt.Commentf("before=%v during=%v", before, during))

	for _, release := range releases {
		release()
	}
	after := devDockerCensus(c)
	c.Assert(after, qt.DeepEquals, before,
		qt.Commentf("before=%v after=%v", before, after))
}
