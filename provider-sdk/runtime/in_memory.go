package runtime

import (
	"context"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// inMemoryBuffer is the buffer of the in-memory connection, in bytes. It
// bounds how much the connection holds unread, not the size of a message.
const inMemoryBuffer = 1 << 20

// ServeInMemory serves server in this process, through the checks,
// interceptors and message limits Serve applies, over a connection that never
// leaves the process, and returns the generated client of that connection.
//
// It starts no process and authenticates nothing, so it is for tests that
// drive a provider's services the way a host does without building the
// executable, such as the conformance suite. The client validates nothing: a
// caller that wants the host's checks applies the provider package's
// validators to what it receives. Like Serve, it refuses a server whose
// description and services differ.
//
// Each call serves a new negotiation: Describe on the returned client sets
// what the server reads from the host's features until the next Describe.
// The services themselves are the values server holds, so sessions opened
// through one client are the services' sessions in every other. stop closes
// the connection and stops the server; calls after it fail, and repeated
// calls of stop are harmless.
func ServeInMemory(server Server) (client protocol.ProviderClient, stop func(), err error) {
	if err := server.Validate(); err != nil {
		return nil, nil, err
	}
	server.Description = proto.CloneOf(server.Description)
	adapter := newServerAdapter(server)
	served := grpcServer(adapter)(nil)
	protocol.RegisterProviderServer(served, adapter)
	listener := bufconn.Listen(inMemoryBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Serve returns when stop stops the server; its error then says only
		// that the listener closed.
		_ = served.Serve(listener)
	}()
	connection, err := grpc.NewClient("passthrough:///"+bootstrapName,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(LargeMessageBytes), grpc.MaxCallSendMsgSize(LargeMessageBytes)))
	if err != nil {
		served.Stop()
		<-done
		return nil, nil, err
	}
	stop = sync.OnceFunc(func() {
		_ = connection.Close()
		served.Stop()
		<-done
	})
	return protocol.NewProviderClient(connection), stop, nil
}

// bootstrapName is the authority the in-memory connection dials; nothing
// resolves it.
const bootstrapName = "ptah-provider-in-memory"
