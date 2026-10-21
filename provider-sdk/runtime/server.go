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
	CallTimeout       = 30 * time.Second
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
	SQLAnalyzer     provider.SQLAnalyzer
	ValueNormalizer provider.ValueNormalizer
	SourceDecoder   provider.SourceDecoder
	ValueConverter  provider.ValueConverter
	ModelComparer   provider.ModelComparer
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
		{provider.AnalyzeSQLFeature, !nilService(s.SQLAnalyzer)},
		{provider.NormalizeValuesFeature, !nilService(s.ValueNormalizer)},
		{provider.DecodeSourceFeature, !nilService(s.SourceDecoder)},
		{provider.ConvertValuesFeature, !nilService(s.ValueConverter)},
		{provider.CompareModelsFeature, !nilService(s.ModelComparer)},
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
	protocol.RegisterProviderServer(server, &serverAdapter{server: t.server})
	return nil
}

func (*transport) GRPCClient(_ context.Context, _ *plugin.GRPCBroker, conn *grpc.ClientConn) (any, error) {
	return protocol.NewProviderClient(conn), nil
}

type serverAdapter struct {
	protocol.UnimplementedProviderServer
	server Server
}

func (s *serverAdapter) Describe(ctx context.Context, request *protocol.DescribeRequest) (*protocol.Description, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request.GetRevision() != provider.Revision {
		return nil, status.Error(codes.FailedPrecondition, "incompatible provider protocol revision")
	}
	if err := provider.ValidateDescription(s.server.Description, request.GetRequiredFeatures()); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return proto.CloneOf(s.server.Description), nil
}

func (s *serverAdapter) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	if request.GetEngine() == "" || request.GetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "engine and connection address are required")
	}
	if !s.hasEngine(request.GetEngine()) {
		return nil, status.Error(codes.InvalidArgument, "engine is not owned by this provider")
	}
	return s.server.Sessions.Open(ctx, request)
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
	if err := s.server.Sessions.Close(ctx, request); err != nil {
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
	result, err := s.server.Inspector.Inspect(ctx, proto.CloneOf(request))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if _, err := provider.InspectionValues(s.server.Description, request, result); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return result, nil
}

func (s *serverAdapter) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	if s.server.Renderer == nil {
		return nil, status.Error(codes.Unimplemented, "rendering is unavailable")
	}
	if !s.hasEngine(request.GetEngine()) {
		return nil, status.Error(codes.InvalidArgument, "engine is not owned by this provider")
	}
	result, err := s.server.Renderer.Render(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateRender(request, result); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return result, nil
}

func (s *serverAdapter) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	if s.server.Executor == nil {
		return nil, status.Error(codes.Unimplemented, "execution is unavailable")
	}
	if request.GetSessionId() == "" || request.GetRequestId() == "" || len(request.GetUnits()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "session ID, request ID, and units are required")
	}
	for _, unit := range request.GetUnits() {
		if err := provider.ValidateUnit(unit); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	return s.server.Executor.Execute(ctx, request)
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
	result, err := s.server.SQLAnalyzer.AnalyzeSQL(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateSQLAnalysis(request, result); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return result, nil
}
