package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
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
	"google.golang.org/protobuf/proto"
)

// ErrStartup identifies failure to start the process or establish transport.
// Protocol negotiation failures instead wrap provider.ErrProtocol.
var ErrStartup = errors.New("provider startup failed")

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
	// HostFeatures are the two-way features the host acts on, from
	// provider.TwoWayFeatures. The provider uses an addition to an existing
	// message only when its feature is listed here and in its description.
	// Empty asks for the messages of contract 25.0.
	HostFeatures []string
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
	exchange := &negotiatedTransport{}
	process := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: handshake,
		Plugins:         map[string]plugin.Plugin{transportName: &transport{}},
		Cmd:             command, AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		SecureConfig: &plugin.SecureConfig{Checksum: checksum, Hash: sha256.New()},
		AutoMTLS:     true, SkipHostEnv: true, StartTimeout: timeout,
		Logger: hclog.NewNullLogger(), SyncStdout: io.Discard, SyncStderr: io.Discard,
		GRPCDialOptions: dialOptions(exchange),
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
	description, err := rpc.Describe(startup, &protocol.DescribeRequest{Revision: provider.Revision, RequiredFeatures: options.RequiredFeatures,
		HostContract: provider.Contract(), HostFeatures: options.HostFeatures})
	if err != nil {
		client.Close()
		return nil, remoteError(startup, nil, "negotiate", err)
	}
	description, err = negotiate(description, options)
	if err != nil {
		client.Close()
		return nil, err
	}
	if description.GetProviderId() != options.ProviderID {
		client.Close()
		return nil, fmt.Errorf("%w: executable provider identity differs from the trusted manifest", provider.ErrProtocol)
	}
	client.description = proto.CloneOf(description)
	exchange.negotiated(client.description)
	if !stopCancel() || startup.Err() != nil {
		client.Close()
		return nil, startupError(startup)
	}
	return client, nil
}

// dialOptions are the options a host dials its provider with: no retries,
// replies up to LargeMessageBytes, and requests up to MaxMessageBytes until
// exchange negotiates more.
func dialOptions(exchange *negotiatedTransport) []grpc.DialOption {
	return []grpc.DialOption{grpc.WithDisableRetry(), grpc.WithChainUnaryInterceptor(exchange.intercept),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(LargeMessageBytes), grpc.MaxCallSendMsgSize(MaxMessageBytes))}
}

// negotiate checks a provider's description against this host: the contract
// version first, so an incompatible provider is named by version, then the
// description itself. It returns the description without the two-way
// features the host did not list.
func negotiate(description *protocol.Description, options Options) (*protocol.Description, error) {
	if err := provider.CheckContract(provider.Contract(), provider.MinimumContract(description)); err != nil {
		return nil, err
	}
	negotiated := provider.NegotiatedDescription(description, options.HostFeatures)
	if err := provider.ValidateDescription(negotiated, options.RequiredFeatures); err != nil {
		return nil, err
	}
	return negotiated, nil
}

func startupError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: verify the executable, checksum, and bootstrap contract", ErrStartup)
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
	request = proto.CloneOf(request)
	expected, err := c.addressContext(ctx, request)
	if err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.Open(call, request)
	if err != nil {
		return nil, remoteError(call, c.description, "open session", err)
	}
	if err := provider.ValidateSessionContext(c.description, request.GetEngine(), expected, result); err != nil {
		if result.GetId() != "" {
			return nil, errors.Join(err, c.CloseSession(context.WithoutCancel(ctx), result.GetId()))
		}
		return nil, err
	}
	return proto.CloneOf(result), nil
}

// CloseSession releases the named session. It never opens a replacement.
func (c *Client) CloseSession(ctx context.Context, id string) error {
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	_, err := c.rpc.Close(call, &protocol.CloseRequest{SessionId: id})
	if err != nil {
		return remoteError(call, c.description, "close session", err)
	}
	return nil
}

// Inspect returns only a complete, validated snapshot. A transport failure
// yields an error and no schema, even if the database could be empty. The
// caller's context alone bounds the read, which grows with the database.
func (c *Client) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	request = proto.CloneOf(request)
	if request.GetSessionId() == "" {
		return nil, fmt.Errorf("%w: inspection session is missing", provider.ErrProtocol)
	}
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	result, err := c.rpc.Inspect(call, proto.CloneOf(request))
	if err != nil {
		return nil, remoteError(call, c.description, "inspect", err)
	}
	if err := call.Err(); err != nil {
		return nil, err
	}
	if _, err := provider.InspectionValues(c.description, request, result); err != nil {
		return nil, err
	}
	return result, nil
}

// Render validates the complete batch and exposes no prefix on failure.
func (c *Client) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	return callOffline(ctx, c.description, provider.CloneSharingModels(request), "render", provider.ValidateRenderRequest, provider.ValidateRenderedBatch, c.rpc.Render)
}

// Execute sends a validated batch once. Any lost or invalid reply is an unknown
// outcome. A request canceled before dispatch sends nothing. Request IDs do not
// enable replay, and this method performs no application-level retries.
//
// A request with a statement timeout is bounded by CallTimeout and that
// timeout for each unit, so the provider's stop at the timeout reaches the
// caller as a confirmed outcome rather than a lost reply. A request without
// one is bounded by the caller's context alone: a batch may create hundreds
// of tables, and a unit may build an index over a large table. A caller's
// earlier deadline applies either way. A stop at the timeout returns an
// error matching provider.ErrStatementTimeout.
//
// Under provider.MigrationSplitFeature, a NOT_APPLIED reply in a caller
// transaction may set transaction_aborted: the engine aborted the
// transaction for a conflict and applied nothing. Any other use of the flag
// is an unknown outcome and returns no result.
//
// Under provider.ErrorReasonsFeature, a result other than APPLIED may carry
// the provider's reason in its diagnostic, and the returned
// provider.ExecutionError carries it in Reason and in its text. A malformed
// reason reads as a sentence of the SDK's own, and a reason the description
// does not list is removed from the returned result.
func (c *Client) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	request = proto.CloneOf(request)
	if request.GetTransactionId() != "" {
		if err := c.requireTransactions(); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := provider.ValidateExecuteRequest(c.description, request); err != nil {
		return nil, err
	}
	call, cancel := withinBound(ctx, executeTimeout(request))
	defer cancel()
	result, err := c.rpc.Execute(call, request)
	if err != nil {
		return nil, errors.Join(&provider.ExecutionError{Outcome: protocol.Execution_UNKNOWN}, remoteError(call, c.description, "execute", err))
	}
	result = negotiatedExecution(c.description, result)
	if err := provider.ValidateExecutionAbort(c.description, request, result); err != nil {
		// The flag invites a retry, so a reply that misuses it is exposed to
		// no caller.
		return nil, errors.Join(err, &provider.ExecutionError{Outcome: protocol.Execution_UNKNOWN})
	}
	if err := provider.CheckExecuted(request, result); err != nil {
		return result, err
	}
	return result, nil
}

// executeTimeout is CallTimeout plus the statement timeout of every unit of a
// request that sets one, saturating rather than wrapping, and zero, no bound,
// for a request that sets none.
func executeTimeout(request *protocol.ExecuteRequest) time.Duration {
	limit := time.Duration(request.GetStatementTimeoutNanoseconds())
	units := time.Duration(len(request.GetUnits()))
	if limit <= 0 {
		return 0
	}
	if limit > (math.MaxInt64-CallTimeout)/units {
		return math.MaxInt64
	}
	return CallTimeout + limit*units
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
	if err := provider.ValidateHostFeatures(options.HostFeatures); err != nil {
		return nil, err
	}
	checksum, err := hex.DecodeString(options.SHA256)
	if err != nil || len(checksum) != sha256.Size {
		return nil, fmt.Errorf("%w: a SHA-256 checksum is required", provider.ErrProtocol)
	}

	return checksum, nil
}

// AnalyzeSQL accepts only complete owner-defined boundaries and safety facts.
// The caller's context alone bounds the analysis, which grows with the SQL.
func (c *Client) AnalyzeSQL(ctx context.Context, request *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error) {
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	result, err := c.rpc.AnalyzeSQL(call, request)
	if err != nil {
		return nil, remoteError(call, c.description, "SQL analysis", err)
	}
	if err := provider.ValidateSQLAnalysis(request, result); err != nil {
		return nil, err
	}
	return result, nil
}
