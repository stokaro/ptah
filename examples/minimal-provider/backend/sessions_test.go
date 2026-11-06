package backend_test

import (
	"testing"

	"example.com/ptah-tutorial-provider/backend"
	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Each address is refused before the provider touches a file, and the reason
// never repeats the address, which can carry a credential.
func TestOpen_FailurePath(t *testing.T) {
	const reason = "a tutorial address names an absolute file path: tutorial:///path/to/file.db"
	for _, test := range []struct {
		name, engine, address, reason string
	}{
		{"another engine", "sqlite", "tutorial:///data/app.db", "the provider serves the tutorial engine only"},
		{"another scheme", backend.Engine, "sqlite:///data/app.db", reason},
		{"a relative path", backend.Engine, "tutorial:app.db", reason},
		{"a host", backend.Engine, "tutorial://user:secret@server/app.db", reason},
		{"a query", backend.Engine, "tutorial:///data/app.db?mode=ro", reason},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session, err := (&backend.Provider{}).Open(c.Context(), &protocol.OpenRequest{Engine: test.engine, Address: test.address})
			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(status.Convert(err).Message(), qt.Equals, test.reason)
			c.Assert(session, qt.IsNil)
		})
	}
}

// A request for a session that is not open fails with a reason; closing it
// succeeds, because a close may be repeated.
func TestSession_FailurePath(t *testing.T) {
	c := qt.New(t)
	tutorial := &backend.Provider{}
	snapshot, err := tutorial.Inspect(c.Context(), &protocol.InspectRequest{SessionId: "unknown"})
	c.Assert(status.Code(err), qt.Equals, codes.NotFound)
	c.Assert(snapshot, qt.IsNil)
	c.Assert(tutorial.Close(c.Context(), &protocol.CloseRequest{SessionId: "unknown"}), qt.IsNil)
}
