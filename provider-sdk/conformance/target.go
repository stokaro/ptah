package conformance

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/stokaro/ptah/provider-sdk/internal/bootstrap"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// startupTimeout bounds the bootstrap of an executable. A provider built
// with the race detector starts slower than a host's default allows.
const startupTimeout = 30 * time.Second

// Target is the provider under test. Each check that needs a fresh
// negotiation connects again: an executable starts again, an in-process
// server serves a new negotiation.
type Target struct {
	kind    string
	connect func(ctx context.Context) (*link, error)
}

// link is one connection to the provider: a process for an executable, a
// server for the others.
type link struct {
	client protocol.ProviderClient
	close  func()
	// exited reports whether the process ended. It is nil for a target
	// that has no process.
	exited func() bool
}

// Executable is the provider executable at path, which must be absolute,
// started the way a host starts it: by the bootstrap, with the checksum
// pinned and nothing in its environment but environment, entries of the
// form NAME=value. sha256 is the executable's lowercase hexadecimal SHA-256
// digest.
func Executable(path, sha256 string, environment ...string) Target {
	return Target{kind: "executable", connect: func(ctx context.Context) (*link, error) {
		checksum, err := hex.DecodeString(sha256)
		if err != nil || len(checksum) != 32 {
			return nil, fmt.Errorf("the executable's checksum is not a SHA-256 digest")
		}
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("the executable's path is not absolute")
		}
		if err := bootstrap.ValidateEnvironment(environment); err != nil {
			return nil, err
		}
		started, err := bootstrap.Start(ctx, bootstrap.Options{
			Executable: path, Checksum: checksum, Environment: environment, Timeout: startupTimeout,
			DialOptions: []grpc.DialOption{grpc.WithDisableRetry(),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(runtime.LargeMessageBytes), grpc.MaxCallSendMsgSize(runtime.LargeMessageBytes))},
		})
		if err != nil {
			return nil, fmt.Errorf("the executable did not start through the bootstrap: %w", err)
		}
		if !started.Keep() {
			started.Kill()
			return nil, ctx.Err()
		}
		return &link{client: started.Provider(), close: started.Kill, exited: started.Exited}, nil
	}}
}

// InProcess is server served in the test process by runtime.ServeInMemory,
// through the checks Serve applies. The suite reaches the provider's
// services as a host reaches them, without building the executable; the
// bootstrap itself is outside what it checks.
func InProcess(server runtime.Server) Target {
	return Target{kind: "in-process", connect: func(context.Context) (*link, error) {
		client, stop, err := runtime.ServeInMemory(server)
		if err != nil {
			return nil, err
		}
		return &link{client: client, close: stop}, nil
	}}
}

// Wire serves a provider that implements the wire service itself, such as
// a test double of one written in another language, with no SDK server in
// front of it: what the server sends is what the suite receives. newServer
// returns the server of one connection; the suite calls it for each
// connection it makes, as each start of an executable is a new process with
// a negotiation of its own, and may call it from several goroutines at once.
func Wire(newServer func() protocol.ProviderServer) Target {
	return Target{kind: "wire", connect: func(context.Context) (*link, error) {
		served := grpc.NewServer(grpc.MaxRecvMsgSize(runtime.LargeMessageBytes), grpc.MaxSendMsgSize(runtime.LargeMessageBytes))
		protocol.RegisterProviderServer(served, newServer())
		listener := bufconn.Listen(1 << 20)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = served.Serve(listener)
		}()
		connection, err := grpc.NewClient("passthrough:///ptah-conformance-wire",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
			grpc.WithDisableRetry(),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(runtime.LargeMessageBytes), grpc.MaxCallSendMsgSize(runtime.LargeMessageBytes)))
		if err != nil {
			served.Stop()
			<-done
			return nil, err
		}
		stop := sync.OnceFunc(func() {
			_ = connection.Close()
			served.Stop()
			<-done
		})
		return &link{client: protocol.NewProviderClient(connection), close: stop}, nil
	}}
}
