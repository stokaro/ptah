package runtime

// White-box testing required: the loopback sends each client call through the
// server adapter without a process, so a test can drive each end's check of
// the negotiation on its own.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// typedArgumentsDescription serves operator statements with typed arguments.
func typedArgumentsDescription() *protocol.Description {
	description := statementsDescription()
	description.Features = append(description.Features, provider.TypedArgumentsFeature)
	return description
}

// typedArgumentOwner records the typed arguments of each statement it runs.
type typedArgumentOwner struct {
	statementOwner
	received [][]*protocol.StatementArgument
}

func (o *typedArgumentOwner) RunStatement(ctx context.Context, request *protocol.RunStatementRequest) (*protocol.StatementResult, error) {
	o.received = append(o.received, request.GetTypedArguments())
	return o.statementOwner.RunStatement(ctx, request)
}

func newTypedArgumentLoopback(description *protocol.Description, owner *typedArgumentOwner, hostFeatures ...string) (*Client, *statementLoopback) {
	server := negotiatedServer(Server{Description: description, Sessions: &sessionOwner{}, Statements: owner, Executor: executorOwner{},
		Transactions: &idleTransactions{}}, hostFeatures...)
	peer := &statementLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

// typedWrite writes an Int32 and a null of an optional Uint64, and binds a
// list of Utf8.
func typedWrite() *protocol.RunStatementRequest {
	return &protocol.RunStatementRequest{SessionId: "session", RequestId: "typed", Sql: "UPDATE rates SET rate = ? WHERE code IN ?",
		Access: protocol.RunStatementRequest_READ_WRITE, MaxRows: 1, TypedArguments: []*protocol.StatementArgument{
			{Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_INT32}, Value: &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: -5}}},
			{Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_UINT64, Optional: true}, Value: &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}},
			{Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_STRING, List: true}, Elements: []*protocol.DataValue{statementText("CZ"), statementText("SK")}},
		}}
}

// Under the negotiated feature the provider receives each argument with its
// type.
func TestTypedArgumentsConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &typedArgumentOwner{}
	client, peer := newTypedArgumentLoopback(typedArgumentsDescription(), owner, provider.TypedArgumentsFeature)
	c.Assert(peer.server.server.Validate(), qt.IsNil)
	request := typedWrite()
	result, err := client.RunStatement(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetRowsAffected(), qt.Equals, uint64(1))
	c.Assert(owner.received, qt.HasLen, 1)
	c.Assert(owner.received[0], qt.HasLen, 3)
	for i, argument := range owner.received[0] {
		c.Assert(proto.Equal(argument, request.TypedArguments[i]), qt.IsTrue, qt.Commentf("argument %d", i))
	}
}

// Without the feature on both sides, the client sends no typed argument and
// the server refuses one before its owner sees it. Plain arguments still run.
func TestTypedArgumentsConformance_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name         string
		description  *protocol.Description
		hostFeatures []string
	}{
		{"a host that does not list it", typedArgumentsDescription(), nil},
		{"a provider that does not list it", statementsDescription(), []string{provider.TypedArgumentsFeature}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &typedArgumentOwner{}
			client, peer := newTypedArgumentLoopback(test.description, owner, test.hostFeatures...)
			_, err := client.RunStatement(c.Context(), typedWrite())
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*typed statement arguments were not negotiated`)
			c.Assert(peer.calls, qt.Equals, 0)

			_, err = peer.server.RunStatement(c.Context(), typedWrite())
			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(status.Convert(err).Message(), qt.Matches, `.*typed statement arguments were not negotiated`)
			c.Assert(owner.received, qt.HasLen, 0)

			plain := typedWrite()
			plain.TypedArguments, plain.Arguments = nil, []*protocol.DataValue{statementText("1.5")}
			_, err = client.RunStatement(c.Context(), plain)
			c.Assert(err, qt.IsNil)
			c.Assert(owner.received, qt.HasLen, 1)
		})
	}
}

// A typed argument its type does not hold is refused on both ends before it
// reaches the owner.
func TestTypedArgumentsShape_FailurePath(t *testing.T) {
	c := qt.New(t)
	owner := &typedArgumentOwner{}
	client, peer := newTypedArgumentLoopback(typedArgumentsDescription(), owner, provider.TypedArgumentsFeature)
	wide := typedWrite()
	wide.TypedArguments[0].Value = &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: 1 << 31}}
	_, err := client.RunStatement(c.Context(), wide)
	c.Assert(err, qt.ErrorMatches, `.*not one its scalar INT32 holds`)
	c.Assert(peer.calls, qt.Equals, 0)
	_, err = peer.server.RunStatement(c.Context(), wide)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(owner.received, qt.HasLen, 0)
}
