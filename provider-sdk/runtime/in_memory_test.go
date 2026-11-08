package runtime_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	transport "ptah.run/provider-sdk/runtime"
)

// reasonedDescription offers inspection and error reasons, so that a test
// sees the server's negotiation of a two-way feature.
func reasonedDescription() *protocol.Description {
	return &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1",
		Features: []string{provider.InspectFeature, provider.ErrorReasonsFeature},
		Engines: []*protocol.Engine{{Id: "test", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT,
			ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
}

// refusingInspector refuses every read with a reason.
type refusingInspector struct{}

func (refusingInspector) Inspect(context.Context, *protocol.InspectRequest) (*protocol.Schema, error) {
	return nil, transport.ErrorWithReason(codes.NotFound, "the session is closed or was never opened")
}

// The in-memory server negotiates and checks as Serve does: the description
// narrows to what the host lists, and a reason reaches only a host that
// lists errors.reasons.
func TestServeInMemory_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name         string
		hostFeatures []string
		reasons      int
	}{
		{name: "a host that lists errors.reasons", hostFeatures: []string{provider.ErrorReasonsFeature}, reasons: 1},
		{name: "a host that lists nothing", reasons: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			client, stop, err := transport.ServeInMemory(transport.Server{Description: reasonedDescription(), Sessions: &sessions{}, Inspector: refusingInspector{}})
			c.Assert(err, qt.IsNil)
			c.Cleanup(stop)

			described, err := client.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: provider.Contract(), HostFeatures: test.hostFeatures})
			c.Assert(err, qt.IsNil)
			_, err = client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: "unknown"})

			c.Assert(proto.Equal(described, provider.NegotiatedDescription(reasonedDescription(), test.hostFeatures)), qt.IsTrue)
			c.Assert(status.Code(err), qt.Equals, codes.NotFound)
			c.Assert(status.Convert(err).Details(), qt.HasLen, test.reasons)
		})
	}
}

func TestServeInMemory_FailurePath(t *testing.T) {
	t.Run("a false advertisement", func(t *testing.T) {
		c := qt.New(t)

		client, stop, err := transport.ServeInMemory(transport.Server{Description: reasonedDescription(), Sessions: &sessions{}})

		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(client, qt.IsNil)
		c.Assert(stop, qt.IsNil)
	})
	t.Run("a call after stop", func(t *testing.T) {
		c := qt.New(t)
		client, stop, err := transport.ServeInMemory(transport.Server{Description: reasonedDescription(), Sessions: &sessions{}, Inspector: refusingInspector{}})
		c.Assert(err, qt.IsNil)
		stop()
		stop()

		described, err := client.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision})

		c.Assert(status.Code(err), qt.Equals, codes.Canceled)
		c.Assert(described, qt.IsNil)
	})
}
