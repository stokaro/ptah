package migratevalidate_test

import (
	"net"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/migratevalidate"
	"go.5x5.cz/ptah/internal/testutils"
)

// This file holds the two halves of stokaro/ptah#1499 that live in this
// package: the native verb resolves an `oci://` --dir, and the
// Atlas-compatible verb built from the same constructor does not.
//
// The second half is not symmetry for its own sake. `ptah-compat` may never
// exit 0 where the pinned community binary exits 1, and that binary reads
// --dir as a filesystem path; --plain-http would also be a flag the
// conformance cli-surface tier finds on one side only. Both commands come out
// of one builder, so the disagreement is one field, and a field is exactly the
// kind of thing a later edit unifies "for consistency".

// closedRegistryPort returns host:port for a port nothing is listening on.
//
// The port is bound, read back and released rather than guessed, so it cannot
// collide with a registry another test — or another agent on this machine — is
// running, which would turn a refusal into a real pull.
func closedRegistryPort(c *qt.C) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	addr := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return addr
}

func TestValidate_NativeResolvesTheOCISchemeRatherThanStattingIt(t *testing.T) {
	c := qt.New(t)
	reference := "oci://" + closedRegistryPort(c) + "/demo/migrations:v1"

	_, stderr, err := execute("--dir", reference, "--plain-http")

	// The dial happened, which only a value that reached the OCI client can
	// produce.
	c.Assert(stderr, qt.Contains, testutils.RefusedConnection)
	c.Assert(stderr, qt.Contains, "http://")
	c.Assert(stderr, qt.Not(qt.Contains), "https://")
	// The failure the issue reported. It is asserted separately because that
	// failure was also an exit 2, so the status alone cannot tell them apart.
	c.Assert(stderr, qt.Not(qt.Contains), "no such file or directory")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
}

func TestValidate_NativeWithoutPlainHTTPDialsTLS(t *testing.T) {
	c := qt.New(t)
	reference := "oci://" + closedRegistryPort(c) + "/demo/migrations:v1"

	_, stderr, err := execute("--dir", reference)

	c.Assert(stderr, qt.Contains, "https://")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
}

func TestValidate_AtlasSurfaceLeavesTheOCISchemeToTheFilesystem(t *testing.T) {
	c := qt.New(t)
	reference := "oci://" + closedRegistryPort(c) + "/demo/migrations:v1"

	_, stderr, err := executeAtlas("--dir", reference)

	c.Assert(stderr, qt.Contains, "stat "+reference)
	c.Assert(stderr, qt.Not(qt.Contains), testutils.RefusedConnection)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
}

func TestValidate_PlainHTTPIsRegisteredOnTheNativeSurfaceOnly(t *testing.T) {
	c := qt.New(t)

	c.Assert(migratevalidate.NewMigrateValidateCommand().Flags().Lookup("plain-http"), qt.IsNotNil)
	c.Assert(migratevalidate.NewAtlasMigrateValidateCommand().Flags().Lookup("plain-http"), qt.IsNil)
}
