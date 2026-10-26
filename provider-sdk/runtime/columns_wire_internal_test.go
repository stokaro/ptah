package runtime

// White-box testing required: a real gRPC connection must use the private
// validated adapter while malformed owner receipts remain injectable.

import (
	"context"
	"net"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestColumnRPCsPreserveOptionalFalseAndTypedEmptyDefaults(t *testing.T) {
	c := qt.New(t)
	d, request, expected := runtimeColumnsExchange()
	defaults, expectedDefaults := runtimeDefaults(request)
	defaults.Columns[0].Column.DefaultExpression = proto.String("''")
	expectedDefaults.Columns[0].Kind = protocol.ColumnDefaultReceipt_LITERAL
	owner := &columnOwner{comparison: expected, defaults: expectedDefaults}
	adapter := negotiatedServer(Server{Description: d, Columns: owner}, provider.ColumnsFeature)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	protocol.RegisterProviderServer(server, adapter)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		c.Check(listener.Close(), qt.IsNil)
		c.Check(<-done, qt.IsNil)
	})
	connection, err := grpc.NewClient("passthrough:///columns", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { c.Check(connection.Close(), qt.IsNil) })
	client := &Client{description: d, rpc: protocol.NewProviderClient(connection)}
	compared, err := client.CompareColumns(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(compared, expected), qt.IsTrue)
	read, err := client.ReadColumnDefaults(c.Context(), defaults)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(read, expectedDefaults), qt.IsTrue)
	c.Assert(compared.Tables[0].Columns[0].Aspects[0].GetNullability().Nullable, qt.DeepEquals, proto.Bool(false))
	c.Assert(read.Columns[0].Value, qt.Equals, "")
}
