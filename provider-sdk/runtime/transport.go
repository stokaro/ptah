package runtime

import (
	"context"
	"slices"
	"sync/atomic"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// negotiatedTransport is what a client's calls take from the negotiated
// description: model references where it lists
// provider.ModelReferencesFeature, and messages up to LargeMessageBytes
// where it lists provider.LargeMessagesFeature. Until negotiation, and for
// Describe, whose reply is the description a reference names, it passes
// every message as it is.
type negotiatedTransport struct {
	description atomic.Pointer[protocol.Description]
}

// negotiated records the description a client negotiated.
func (t *negotiatedTransport) negotiated(description *protocol.Description) {
	t.description.Store(description)
}

// intercept is the client's interceptor. The request it sends is a copy when
// a model in it is replaced, so the caller's request keeps every definition.
func (t *negotiatedTransport) intercept(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoker grpc.UnaryInvoker,
	options ...grpc.CallOption,
) error {
	description := t.description.Load()
	if description == nil || method == protocol.Provider_Describe_FullMethodName {
		return invoker(ctx, method, request, reply, connection, options...)
	}
	if slices.Contains(description.GetFeatures(), provider.LargeMessagesFeature) {
		options = append(slices.Clip(options), grpc.MaxCallSendMsgSize(LargeMessageBytes))
	}
	references := slices.Contains(description.GetFeatures(), provider.ModelReferencesFeature)
	if message, isMessage := request.(proto.Message); isMessage && references {
		request = provider.ReferenceModels(description, message)
	}
	if err := invoker(ctx, method, request, reply, connection, options...); err != nil {
		return err
	}
	if message, isMessage := reply.(proto.Message); isMessage && references {
		if err := provider.ResolveModelReferences(description, message); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
	return nil
}

// references is the server's interceptor: it resolves the model references
// of a request before the service or a validator reads it, and sends the
// described models of the reply by reference, when the description
// negotiated with the host lists provider.ModelReferencesFeature.
func (s *serverAdapter) references(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	description := s.negotiated()
	if info.FullMethod == protocol.Provider_Describe_FullMethodName || !slices.Contains(description.GetFeatures(), provider.ModelReferencesFeature) {
		return handler(ctx, request)
	}
	if message, isMessage := request.(proto.Message); isMessage {
		if err := provider.ResolveModelReferences(description, message); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	reply, err := handler(ctx, request)
	if err != nil {
		return reply, err
	}
	if message, isMessage := reply.(proto.Message); isMessage {
		return provider.ReferenceModels(description, message), nil
	}
	return reply, nil
}
