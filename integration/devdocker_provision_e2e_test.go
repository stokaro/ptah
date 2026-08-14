//go:build integration

package integration_test

import (
	"context"
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
	c.Assert(strings.Contains(resolved, "/ptahdev"), qt.IsTrue,
		qt.Commentf("resolved=%q does not name the database from the URL", resolved))

	during := devDockerCensus(c)
	c.Assert(len(during), qt.Equals, len(before)+1,
		qt.Commentf("before=%v during=%v", before, during))

	// A LOCAL daemon must publish on loopback and nowhere else. This is asserted
	// from the runtime rather than from the publish flag, because the flag has
	// three fields and getting the middle one wrong is silent: `0.0.0.0:5432`
	// reads the bind address as the host port and is refused outright, while
	// `0.0.0.0::5432` exposes the dev database on every interface of the host.
	// The pinned community binary v1.3.0 does the latter unconditionally --
	// measured, its container publishes `0.0.0.0:59319->5432/tcp` -- so this row
	// pins a divergence Ptah keeps on purpose.
	mapping := devDockerPortMapping(c, onlyNewContainer(c, before, during))
	c.Assert(strings.HasPrefix(mapping, "127.0.0.1:"), qt.IsTrue,
		qt.Commentf("a local daemon published %q, not a loopback binding", mapping))

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
	c.Assert(strings.Contains(version, "PostgreSQL"), qt.IsTrue, qt.Commentf("version=%q", version))

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
	c.Assert(strings.Contains(err.Error(), "did not become ready"), qt.IsTrue,
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
	c.Assert(len(during), qt.Equals, len(before)+parallel,
		qt.Commentf("before=%v during=%v", before, during))

	for _, release := range releases {
		release()
	}
	after := devDockerCensus(c)
	c.Assert(after, qt.DeepEquals, before,
		qt.Commentf("before=%v after=%v", before, after))
}
