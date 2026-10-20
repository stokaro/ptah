package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Options selects an explicitly trusted executable. A checksum detects changes
// to those trusted bytes; it does not establish publisher authenticity. Relative
// executable paths and inherited host environments are refused by construction.
type Options struct {
	Executable       string
	SHA256           string
	ProviderID       string
	RequiredFeatures []string
	// Environment is an explicit allowlist of name=value entries. It must not
	// include credentials; connection credentials belong to Open requests.
	Environment []string
	// StartupTimeout defaults to ten seconds. Negative values are invalid.
	StartupTimeout time.Duration
}

// Client owns one subprocess and its negotiated services. Calls can be
// concurrent. Close ends the subprocess and invalidates every session handle.
// A client is never reconnected or retried automatically.
type Client struct {
	process     *plugin.Client
	rpc         protocol.ProviderClient
	description *protocol.Description
	cancel      context.CancelFunc
	closeOnce   sync.Once
}

// Start validates the executable and checksum, authenticates the local
// transport, and negotiates the contract before any database call. ctx bounds
// startup only; the returned client must be closed explicitly.
func Start(ctx context.Context, options Options) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	checksum, err := validateOptions(options)
	if err != nil {
		return nil, err
	}
	timeout := options.StartupTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	startup, cancelStartup := context.WithTimeout(ctx, timeout)
	defer cancelStartup()
	life, cancelLife := context.WithCancel(context.Background())
	stopCancel := context.AfterFunc(startup, cancelLife)
	defer stopCancel()
	command := exec.CommandContext(life, options.Executable) // #nosec G204 -- the caller explicitly authorizes an absolute, checksum-pinned executable
	command.Env = append(slices.Clone(options.Environment), parentEnvironment+"="+strconv.Itoa(os.Getpid()))
	process := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: handshake,
		Plugins:         map[string]plugin.Plugin{transportName: &transport{}},
		Cmd:             command, AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		SecureConfig: &plugin.SecureConfig{Checksum: checksum, Hash: sha256.New()},
		AutoMTLS:     true, SkipHostEnv: true, StartTimeout: timeout,
		Logger: hclog.NewNullLogger(), SyncStdout: io.Discard, SyncStderr: io.Discard,
		GRPCDialOptions: []grpc.DialOption{grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageBytes), grpc.MaxCallSendMsgSize(MaxMessageBytes))},
	})
	client := &Client{process: process, cancel: cancelLife}
	connection, err := process.Client()
	if err != nil {
		client.Close()
		return nil, startupError(startup)
	}
	dispensed, err := connection.Dispense(transportName)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("%w: provider service was not available", provider.ErrProtocol)
	}
	rpc, ok := dispensed.(protocol.ProviderClient)
	if !ok {
		client.Close()
		return nil, fmt.Errorf("%w: invalid provider service", provider.ErrProtocol)
	}
	client.rpc = rpc
	description, err := rpc.Describe(startup, &protocol.DescribeRequest{Revision: provider.Revision, RequiredFeatures: options.RequiredFeatures})
	if err != nil {
		client.Close()
		return nil, remoteError(startup, "negotiate", err)
	}
	if err := provider.ValidateDescription(description, options.RequiredFeatures); err != nil {
		client.Close()
		return nil, err
	}
	if description.GetProviderId() != options.ProviderID {
		client.Close()
		return nil, fmt.Errorf("%w: executable provider identity differs from the trusted manifest", provider.ErrProtocol)
	}
	client.description = proto.CloneOf(description)
	if !stopCancel() || startup.Err() != nil {
		client.Close()
		return nil, startupError(startup)
	}
	return client, nil
}

func startupError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: provider startup failed; verify the executable, checksum, and bootstrap contract", provider.ErrProtocol)
}

// Description returns a private copy of the negotiated identity and services.
func (c *Client) Description() *protocol.Description {
	return proto.CloneOf(c.description)
}

// Close terminates the subprocess. Callers close their sessions first when
// possible. Repeated closes are harmless; termination never proves rollback.
func (c *Client) Close() {
	c.closeOnce.Do(func() { defer c.cancel(); c.process.Kill() })
}

// Open starts a session after negotiation. It returns no usable handle on an
// error, an empty handle, or an engine mismatch.
func (c *Client) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.Open(call, request)
	if err != nil {
		return nil, remoteError(call, "open session", err)
	}
	if result.GetId() == "" || result.GetEngine() != request.GetEngine() {
		return nil, fmt.Errorf("%w: invalid session identity", provider.ErrProtocol)
	}
	if err := provider.ValidateIdentifierRules(result.GetIdentifiers()); err != nil {
		return nil, err
	}
	return result, nil
}

// CloseSession releases the named session. It never opens a replacement.
func (c *Client) CloseSession(ctx context.Context, id string) error {
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	_, err := c.rpc.Close(call, &protocol.CloseRequest{SessionId: id})
	if err != nil {
		return remoteError(call, "close session", err)
	}
	return nil
}

// Inspect returns only a complete, validated snapshot. A transport failure
// yields an error and no schema, even if the database could be empty.
func (c *Client) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.Inspect(call, request)
	if err != nil {
		return nil, remoteError(call, "inspect", err)
	}
	if err := provider.ValidateSchema(result); err != nil {
		return nil, err
	}
	return result, nil
}

// Render validates the complete batch and exposes no prefix on failure.
func (c *Client) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.Render(call, request)
	if err != nil {
		return nil, remoteError(call, "render", err)
	}
	if err := provider.ValidateRender(request, result); err != nil {
		return nil, err
	}
	return result, nil
}

// Execute sends a validated batch once. Any lost or invalid reply is an unknown
// outcome. A request canceled before dispatch sends nothing. Request IDs do not
// enable replay, and this method performs no application-level retries.
func (c *Client) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.GetSessionId() == "" || request.GetRequestId() == "" || len(request.GetUnits()) == 0 {
		return nil, fmt.Errorf("%w: session ID, request ID, and units are required", provider.ErrProtocol)
	}
	for _, unit := range request.GetUnits() {
		if err := provider.ValidateUnit(unit); err != nil {
			return nil, err
		}
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.Execute(call, request)
	if err != nil {
		return nil, errors.Join(&provider.ExecutionError{Outcome: protocol.Execution_UNKNOWN}, remoteError(call, "execute", err))
	}
	if err := provider.CheckExecution(len(request.GetUnits()), result); err != nil {
		return result, err
	}
	return result, nil
}

func remoteError(ctx context.Context, operation string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	code := status.Code(err)
	if code == codes.Canceled {
		return context.Canceled
	}
	if code == codes.DeadlineExceeded {
		return context.DeadlineExceeded
	}
	// Raw gRPC messages are provider-authored and can contain connection secrets.
	return fmt.Errorf("provider %s failed (%s)", operation, code)
}

func validateOptions(options Options) ([]byte, error) {
	if !filepath.IsAbs(options.Executable) || options.ProviderID == "" || options.StartupTimeout < 0 {
		return nil, fmt.Errorf("%w: absolute executable, provider identity, and nonnegative startup timeout are required", provider.ErrProtocol)
	}
	names := make(map[string]bool)
	for _, entry := range options.Environment {
		name, _, ok := strings.Cut(entry, "=")
		name = strings.ToUpper(name)
		if !ok || name == "" || strings.ContainsAny(entry, "\x00\r\n") || strings.HasPrefix(name, "PLUGIN_") || strings.HasPrefix(name, "PTAH_PROVIDER_") || names[name] {
			return nil, fmt.Errorf("%w: invalid or reserved provider environment entry", provider.ErrProtocol)
		}
		names[name] = true
	}
	checksum, err := hex.DecodeString(options.SHA256)
	if err != nil || len(checksum) != sha256.Size {
		return nil, fmt.Errorf("%w: a SHA-256 checksum is required", provider.ErrProtocol)
	}

	return checksum, nil
}

// AnalyzeSQL accepts only complete owner-defined boundaries and safety facts.
func (c *Client) AnalyzeSQL(ctx context.Context, request *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error) {
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.AnalyzeSQL(call, request)
	if err != nil {
		return nil, remoteError(call, "SQL analysis", err)
	}
	if err := provider.ValidateSQLAnalysis(request, result); err != nil {
		return nil, err
	}
	return result, nil
}
