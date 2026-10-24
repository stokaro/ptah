// Package runtime implements Ptah's local authenticated subprocess bootstrap.
// The public protocol belongs to Ptah; go-plugin is an internal transport
// choice. Native providers run with their caller's OS privileges, not in a
// security sandbox.
package runtime

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"slices"
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

const (
	// MaxMessageBytes bounds each request and response in both directions.
	MaxMessageBytes = 4 << 20
	// CallTimeout bounds calls whose caller supplied no earlier deadline.
	CallTimeout = 30 * time.Second
	// ProvisionTimeout bounds a dev database call that has no earlier deadline:
	// starting a container may pull an image, and a cleanup drops every object
	// a run left.
	ProvisionTimeout  = 10 * time.Minute
	transportName     = "ptah-provider"
	parentEnvironment = "PTAH_PROVIDER_PARENT_PID"
)

var handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "PTAH_PROVIDER_BOOTSTRAP",
	MagicCookieValue: "ptah-provider-experimental-1",
}

// Server supplies the services exposed by one provider process. Description is
// copied before serving. Advertised services must have implementations. Service
// implementations must honor cancellation and isolate sessions and credentials.
type Server struct {
	Description     *protocol.Description
	Sessions        provider.Sessions
	Inspector       provider.Inspector
	Renderer        provider.Renderer
	Executor        provider.Executor
	Transactions    provider.Transactions
	History         provider.History
	Metadata        provider.Metadata
	Locks           provider.Locks
	SQLAnalyzer     provider.SQLAnalyzer
	ValueNormalizer provider.ValueNormalizer
	SourceDecoder   provider.SourceDecoder
	ValueConverter  provider.ValueConverter
	ModelComparer   provider.ModelComparer
	FeaturePlanner  provider.FeaturePlanner
	TablePreparer   provider.TablePreparer
	TableCreations  provider.TableCreationProjector
	Linter          provider.Linter
	DevProvisioner  provider.DevProvisioner
	DevCleaner      provider.DevCleaner
	ReplayGuard     provider.ReplayGuard
	SchemaValidator provider.SchemaValidator
	SQLSchemaReader provider.SQLSchemaReader
	RowTyper        provider.RowTyper
	RowReader       provider.RowReader
	RowWriter       provider.RowWriteRenderer
	Seeds           provider.Seeds
}

// Validate refuses a malformed description or a false service advertisement.
func (s Server) Validate() error {
	if err := provider.ValidateDescription(s.Description, nil); err != nil {
		return err
	}
	if nilService(s.Sessions) {
		return fmt.Errorf("%w: session service is required", provider.ErrProtocol)
	}
	for _, service := range []struct {
		feature string
		present bool
	}{
		{provider.InspectFeature, !nilService(s.Inspector)},
		{provider.RenderFeature, !nilService(s.Renderer)},
		{provider.ExecuteFeature, !nilService(s.Executor)},
		{provider.TransactionsFeature, !nilService(s.Transactions)},
		{provider.HistoryFeature, !nilService(s.History)},
		{provider.MetadataFeature, !nilService(s.Metadata)},
		{provider.LocksFeature, !nilService(s.Locks)},
		{provider.AnalyzeSQLFeature, !nilService(s.SQLAnalyzer)},
		{provider.NormalizeValuesFeature, !nilService(s.ValueNormalizer)},
		{provider.DecodeSourceFeature, !nilService(s.SourceDecoder)},
		{provider.ConvertValuesFeature, !nilService(s.ValueConverter)},
		{provider.CompareModelsFeature, !nilService(s.ModelComparer)},
		{provider.PlanFeaturesFeature, !nilService(s.FeaturePlanner)},
		{provider.PrepareTablesFeature, !nilService(s.TablePreparer)},
		{provider.ProjectTableCreationsFeature, !nilService(s.TableCreations)},
		{provider.ValidateSchemaFeature, !nilService(s.SchemaValidator)},
		{provider.LintFeature, !nilService(s.Linter)},
		{provider.DevDatabaseFeature, !nilService(s.DevProvisioner)},
		{provider.DevCleanupFeature, !nilService(s.DevCleaner)},
		{provider.ReplayGuardFeature, !nilService(s.ReplayGuard)},
		{provider.ReadSQLSchemaFeature, !nilService(s.SQLSchemaReader)},
		{provider.TypeRowsFeature, !nilService(s.RowTyper)},
		{provider.ReadRowsFeature, !nilService(s.RowReader)},
		{provider.RenderRowWritesFeature, !nilService(s.RowWriter)},
		{provider.SeedsFeature, !nilService(s.Seeds)},
	} {
		if slices.Contains(s.Description.GetFeatures(), service.feature) != service.present {
			return fmt.Errorf("%w: %s advertisement differs from implementation", provider.ErrProtocol, service.feature)
		}
	}
	return nil
}

// Serve runs the provider process until its host shuts it down. It requires the
// SDK bootstrap environment, uses go-plugin's authenticated local gRPC endpoint,
// and reserves stdout for bootstrap framing. Call it from the executable's main
// function. The process exits if its parent dies; that does not prove rollback.
func Serve(server Server) error {
	if err := server.Validate(); err != nil {
		return err
	}
	if os.Getenv("PLUGIN_CLIENT_CERT") == "" {
		return fmt.Errorf("provider bootstrap requires authenticated transport")
	}
	stop, err := watchParent()
	if err != nil {
		return err
	}
	defer stop()
	server.Description = proto.CloneOf(server.Description)
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshake,
		Plugins:         map[string]plugin.Plugin{transportName: &transport{server: server}},
		Logger:          hclog.NewNullLogger(),
		GRPCServer: func(options []grpc.ServerOption) *grpc.Server {
			options = append(options, grpc.MaxRecvMsgSize(MaxMessageBytes), grpc.MaxSendMsgSize(MaxMessageBytes))
			return grpc.NewServer(options...)
		},
	})
	return nil
}

type transport struct {
	plugin.NetRPCUnsupportedPlugin
	server Server
}

func (t *transport) GRPCServer(_ *plugin.GRPCBroker, server *grpc.Server) error {
	protocol.RegisterProviderServer(server, newServerAdapter(t.server))
	return nil
}

func (*transport) GRPCClient(_ context.Context, _ *plugin.GRPCBroker, conn *grpc.ClientConn) (any, error) {
	return protocol.NewProviderClient(conn), nil
}

type serverAdapter struct {
	protocol.UnimplementedProviderServer
	server Server
	// negotiation holds what the host announced in Describe. Without one, or
	// before Describe, the host is taken to list no feature.
	negotiation *negotiation
}

// negotiation is the outcome of the host's Describe: the features it listed
// and the description negotiated with them.
type negotiation struct {
	mu          sync.RWMutex
	features    []string
	description *protocol.Description
}

func newServerAdapter(server Server) *serverAdapter {
	return &serverAdapter{server: server, negotiation: &negotiation{}}
}

// negotiated returns the description both sides validate with: the
// provider's own, without the two-way features the host did not list.
func (s *serverAdapter) negotiated() *protocol.Description {
	if s.negotiation != nil {
		s.negotiation.mu.RLock()
		description := s.negotiation.description
		s.negotiation.mu.RUnlock()
		if description != nil {
			return description
		}
	}
	return provider.NegotiatedDescription(s.server.Description, nil)
}

// owner returns the context a service is called with, which carries the
// features the host listed.
func (s *serverAdapter) owner(ctx context.Context) context.Context {
	var features []string
	if s.negotiation != nil {
		s.negotiation.mu.RLock()
		features = s.negotiation.features
		s.negotiation.mu.RUnlock()
	}
	return provider.ContextWithHostFeatures(ctx, features)
}

// Describe negotiates with the host. A host of another major is refused. A
// host that announces no contract version speaks minor 0 and cannot compare
// versions, so the provider refuses it here when its own minimum is higher; a
// host that announces one receives the description and decides itself.
func (s *serverAdapter) Describe(ctx context.Context, request *protocol.DescribeRequest) (*protocol.Description, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request.GetRevision() != provider.Revision {
		return nil, status.Error(codes.FailedPrecondition, "incompatible provider protocol revision")
	}
	host := provider.HostContract(request)
	if host.GetMajor() != request.GetRevision() {
		return nil, status.Error(codes.InvalidArgument, "host contract major differs from its revision")
	}
	if err := provider.ValidateHostFeatures(request.GetHostFeatures()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	description := provider.NegotiatedDescription(s.server.Description, request.GetHostFeatures())
	if err := provider.ValidateDescription(description, request.GetRequiredFeatures()); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if request.GetHostContract() == nil {
		if err := provider.CheckContract(host, provider.MinimumContract(description)); err != nil {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
	}
	if s.negotiation != nil {
		s.negotiation.mu.Lock()
		s.negotiation.features = slices.Clone(request.GetHostFeatures())
		s.negotiation.description = description
		s.negotiation.mu.Unlock()
	}
	return proto.CloneOf(description), nil
}

func (s *serverAdapter) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	if request.GetEngine() == "" || request.GetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "engine and connection address are required")
	}
	if !s.hasEngine(request.GetEngine()) {
		return nil, status.Error(codes.InvalidArgument, "engine is not owned by this provider")
	}
	result, err := s.server.Sessions.Open(s.owner(ctx), proto.CloneOf(request))
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateSession(request.GetEngine(), result); err != nil {
		if result.GetId() != "" {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), CallTimeout)
			defer cancel()
			_ = s.server.Sessions.Close(cleanup, &protocol.CloseRequest{SessionId: result.GetId()})
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return proto.CloneOf(result), nil
}

func (s *serverAdapter) hasEngine(name string) bool {
	for _, engine := range s.server.Description.GetEngines() {
		if engine.GetId() == name {
			return true
		}
	}
	return false
}

func (s *serverAdapter) Close(ctx context.Context, request *protocol.CloseRequest) (*protocol.Empty, error) {
	if request.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session ID is required")
	}
	if err := s.server.Sessions.Close(s.owner(ctx), request); err != nil {
		return nil, err
	}
	return &protocol.Empty{}, nil
}

func (s *serverAdapter) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	if s.server.Inspector == nil {
		return nil, status.Error(codes.Unimplemented, "inspection is unavailable")
	}
	if request.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session ID is required")
	}
	result, err := s.server.Inspector.Inspect(s.owner(ctx), proto.CloneOf(request))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if _, err := provider.InspectionValues(s.negotiated(), request, result); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return result, nil
}

func (s *serverAdapter) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	if s.server.Renderer == nil {
		return nil, status.Error(codes.Unimplemented, "rendering is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateRenderRequest, provider.ValidateRenderedBatch, s.server.Renderer.Render)
}

func (s *serverAdapter) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	request = proto.CloneOf(request)
	if request.GetTransactionId() != "" && nilService(s.server.Transactions) {
		return nil, status.Error(codes.Unimplemented, "transactions are unavailable")
	}
	if s.server.Executor == nil {
		return nil, status.Error(codes.Unimplemented, "execution is unavailable")
	}
	if err := provider.ValidateExecuteRequest(s.negotiated(), request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.server.Executor.Execute(s.owner(ctx), request)
	if err == nil && result.GetStatementTimedOut() && request.GetStatementTimeoutNanoseconds() <= 0 {
		// The flag answers a timeout only a host that negotiated it sends.
		return nil, status.Error(codes.Internal, "provider reported a statement timeout the request did not set")
	}
	return result, err
}

// nilService refuses typed-nil implementations before the server starts.
func nilService(service any) bool {
	if service == nil {
		return true
	}
	value := reflect.ValueOf(service)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (s *serverAdapter) AnalyzeSQL(ctx context.Context, request *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error) {
	if s.server.SQLAnalyzer == nil {
		return nil, status.Error(codes.Unimplemented, "SQL analysis is unavailable")
	}
	if !s.hasEngine(request.GetEngine()) {
		return nil, status.Error(codes.InvalidArgument, "engine is not owned by this provider")
	}
	result, err := s.server.SQLAnalyzer.AnalyzeSQL(s.owner(ctx), request)
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateSQLAnalysis(request, result); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return result, nil
}
