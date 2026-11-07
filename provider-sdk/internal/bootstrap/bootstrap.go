// Package bootstrap holds the experimental provider bootstrap: the go-plugin
// handshake, the binding of the provider service, and the host side of a
// launch, which starts an executable pinned by its checksum with an explicit
// environment and dispenses the raw gRPC client of its provider service.
//
// The runtime's Start and Serve use it, and so does the conformance suite, so
// the suite reaches a provider executable through the same launch a host
// makes. It negotiates nothing: what a caller sends in Describe and how it
// validates the replies belong to the caller.
package bootstrap

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/grpc"
)

const (
	// ServiceName is the go-plugin name of the provider service.
	ServiceName = "ptah-provider"
	// ParentEnvironment names the variable that carries the host's process
	// ID, which the provider watches so that it exits when its host dies.
	ParentEnvironment = "PTAH_PROVIDER_PARENT_PID"
)

// Handshake is the go-plugin handshake of the experimental bootstrap. The
// cookie identifies the launch protocol; it authenticates nothing.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "PTAH_PROVIDER_BOOTSTRAP",
	MagicCookieValue: "ptah-provider-experimental-1",
}

var (
	// ErrBootstrap reports a process that did not start or a transport that
	// was not established.
	ErrBootstrap = errors.New("provider bootstrap failed")
	// ErrService reports a process that started without the provider
	// service.
	ErrService = errors.New("provider service was not available")
	// ErrServiceType reports a provider service that is not the generated
	// client of the protocol.
	ErrServiceType = errors.New("invalid provider service")
)

// Binding is the go-plugin binding of the provider service. A provider's
// binding names the service it registers; a host's binding has none and only
// dials.
type Binding struct {
	plugin.NetRPCUnsupportedPlugin
	Service protocol.ProviderServer
}

// GRPCServer registers the provider service.
func (b *Binding) GRPCServer(_ *plugin.GRPCBroker, server *grpc.Server) error {
	protocol.RegisterProviderServer(server, b.Service)
	return nil
}

// GRPCClient returns the generated client of the provider service.
func (*Binding) GRPCClient(_ context.Context, _ *plugin.GRPCBroker, conn *grpc.ClientConn) (any, error) {
	return protocol.NewProviderClient(conn), nil
}

// ErrEnvironment reports an environment entry that is malformed, repeated
// or reserved for the bootstrap.
var ErrEnvironment = errors.New("invalid or reserved provider environment entry")

// ValidateEnvironment refuses an entry that is not NAME=value, that contains
// NUL or a line break, that repeats a name in any letter case, or whose name
// the bootstrap reserves: PLUGIN_ and PTAH_PROVIDER_ in front.
func ValidateEnvironment(entries []string) error {
	names := make(map[string]bool, len(entries))
	for _, entry := range entries {
		name, _, ok := strings.Cut(entry, "=")
		name = strings.ToUpper(name)
		if !ok || name == "" || strings.ContainsAny(entry, "\x00\r\n") || strings.HasPrefix(name, "PLUGIN_") || strings.HasPrefix(name, "PTAH_PROVIDER_") || names[name] {
			return ErrEnvironment
		}
		names[name] = true
	}
	return nil
}

// Options select the executable a host launches. The caller has validated
// them: the path is absolute, the checksum is a SHA-256 digest, and the
// environment holds no reserved name.
type Options struct {
	Executable  string
	Checksum    []byte
	Environment []string
	// Timeout bounds the bootstrap; it must be positive.
	Timeout time.Duration
	// DialOptions are the gRPC options the host dials the provider with.
	DialOptions []grpc.DialOption
}

// Process is one launched provider executable. Kill ends it; repeated kills
// are harmless.
type Process struct {
	client     *plugin.Client
	provider   protocol.ProviderClient
	stopCancel func() bool
	cancel     context.CancelFunc
	killOnce   sync.Once
}

// Start launches the executable and dispenses its provider service. ctx
// bounds the launch: until Keep is called, the process ends when ctx ends.
// The provider's standard output and standard error are discarded, because
// they may contain credentials.
func Start(ctx context.Context, options Options) (*Process, error) {
	life, cancelLife := context.WithCancel(context.Background())
	stopCancel := context.AfterFunc(ctx, cancelLife)
	command := exec.CommandContext(life, options.Executable) // #nosec G204 -- the caller explicitly authorizes an absolute, checksum-pinned executable
	command.Env = append(slices.Clone(options.Environment), ParentEnvironment+"="+strconv.Itoa(os.Getpid()))
	launched := &Process{stopCancel: stopCancel, cancel: cancelLife}
	launched.client = plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins:         map[string]plugin.Plugin{ServiceName: &Binding{}},
		Cmd:             command, AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		SecureConfig: &plugin.SecureConfig{Checksum: options.Checksum, Hash: sha256.New()},
		AutoMTLS:     true, SkipHostEnv: true, StartTimeout: options.Timeout,
		Logger: hclog.NewNullLogger(), SyncStdout: io.Discard, SyncStderr: io.Discard,
		GRPCDialOptions: options.DialOptions,
	})
	connection, err := launched.client.Client()
	if err != nil {
		launched.Kill()
		return nil, ErrBootstrap
	}
	dispensed, err := connection.Dispense(ServiceName)
	if err != nil {
		launched.Kill()
		return nil, ErrService
	}
	service, ok := dispensed.(protocol.ProviderClient)
	if !ok {
		launched.Kill()
		return nil, ErrServiceType
	}
	launched.provider = service
	return launched, nil
}

// Provider returns the raw gRPC client of the provider service. It validates
// nothing.
func (p *Process) Provider() protocol.ProviderClient { return p.provider }

// Keep ends the bound of the context Start was given: from now on the process
// ends only with Kill. It reports false when that context had already ended
// the process.
func (p *Process) Keep() bool { return p.stopCancel() }

// Exited reports whether the process has exited.
func (p *Process) Exited() bool { return p.client.Exited() }

// Kill ends the process. Ending it never proves that a database operation
// rolled back.
func (p *Process) Kill() {
	p.killOnce.Do(func() {
		defer p.cancel()
		p.client.Kill()
	})
}
