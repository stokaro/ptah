package runtime

// White-box testing required: the deadline the client gives a call is
// observable only to the peer it dispatches to.

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/grpc"
)

// boundsPeer records the deadline of the calls it answers.
type boundsPeer struct {
	protocol.ProviderClient
	bounded bool
	budget  time.Duration
}

func (p *boundsPeer) record(ctx context.Context) {
	deadline, bounded := ctx.Deadline()
	p.budget, p.bounded = time.Until(deadline), bounded
}

func (p *boundsPeer) Inspect(ctx context.Context, _ *protocol.InspectRequest, _ ...grpc.CallOption) (*protocol.Schema, error) {
	p.record(ctx)
	return &protocol.Schema{}, nil
}

func (p *boundsPeer) AnalyzeSQL(ctx context.Context, _ *protocol.AnalyzeSQLRequest, _ ...grpc.CallOption) (*protocol.SQLAnalysis, error) {
	p.record(ctx)
	return &protocol.SQLAnalysis{}, nil
}

func (p *boundsPeer) ValidateSchema(ctx context.Context, _ *protocol.ValidateSchemaRequest, _ ...grpc.CallOption) (*protocol.SchemaValidation, error) {
	p.record(ctx)
	return &protocol.SchemaValidation{}, nil
}

// boundsCall is one call whose bound the test reads, made through client.
type boundsCall struct {
	name string
	call func(context.Context, *Client)
}

// boundsCalls are the calls whose work grows with a schema: the read, the
// SQL analysis and one offline service, which shares its bound with every
// other. Their replies are not the subject, so their errors are not read.
func boundsCalls() []boundsCall {
	return []boundsCall{
		{"inspect", func(ctx context.Context, client *Client) {
			_, _ = client.Inspect(ctx, &protocol.InspectRequest{SessionId: "session"})
		}},
		{"analyze SQL", func(ctx context.Context, client *Client) {
			_, _ = client.AnalyzeSQL(ctx, &protocol.AnalyzeSQLRequest{})
		}},
		{"offline service", func(ctx context.Context, client *Client) {
			_, _ = callOffline(ctx, client.description, &protocol.ValidateSchemaRequest{}, "validate schema",
				func(*protocol.Description, *protocol.ValidateSchemaRequest) error { return nil },
				func(*protocol.Description, *protocol.ValidateSchemaRequest, *protocol.SchemaValidation) error {
					return nil
				},
				client.rpc.ValidateSchema)
		}},
	}
}

// A call that reads, sends or plans a schema has no deadline of the client's
// own, and keeps the deadline its caller set. A fixed 30-second bound failed
// the read and the plans of a database of 500 tables on a loaded runner,
// which a database answers in full.
func TestSchemaCallsAreBoundedByTheirCaller(t *testing.T) {
	for _, test := range boundsCalls() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			peer := &boundsPeer{}
			client := &Client{description: &protocol.Description{}, rpc: peer}

			test.call(context.Background(), client)
			unbounded := peer.bounded
			caller, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			test.call(caller, client)

			c.Assert(unbounded, qt.IsFalse)
			c.Assert(peer.bounded, qt.IsTrue)
			c.Assert(peer.budget > time.Hour-time.Minute, qt.IsTrue)
		})
	}
}
