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
// provider.ModelReferencesFeature, coverage kinds sent once per message
// where it lists provider.SharedCoverageFeature, and messages up to
// LargeMessageBytes where it lists provider.LargeMessagesFeature. Until
// negotiation, and for Describe, whose reply is the description a reference
// names, it passes every message as it is.
type negotiatedTransport struct {
	description atomic.Pointer[protocol.Description]
}

// negotiated records the description a client negotiated.
func (t *negotiatedTransport) negotiated(description *protocol.Description) {
	t.description.Store(description)
}

// intercept is the client's interceptor. The request it sends is a copy when
// a model or a list of coverage kinds in it is replaced, so the caller's
// request keeps every definition and every list. A list is shared before
// models are referenced, since sharing recognizes one list by the model
// messages it names, and a reply's references are resolved before its
// lists, so a list's models are resolved once.
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
	if message, isMessage := request.(proto.Message); isMessage {
		request = sendable(description, message)
	}
	if err := invoker(ctx, method, request, reply, connection, options...); err != nil {
		return err
	}
	if message, isMessage := reply.(proto.Message); isMessage {
		if err := received(description, message); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
	return nil
}

// sendable is message as it is sent under the negotiated description: its
// repeated lists of coverage kinds shared where it lists
// provider.SharedCoverageFeature, then its described models by reference
// where it lists provider.ModelReferencesFeature. It is message itself when
// neither changes anything.
func sendable(description *protocol.Description, message proto.Message) proto.Message {
	if slices.Contains(description.GetFeatures(), provider.SharedCoverageFeature) {
		message = provider.ShareCoverage(message)
	}
	if slices.Contains(description.GetFeatures(), provider.ModelReferencesFeature) {
		message = provider.ReferenceModels(description, message)
	}
	return message
}

// received resolves, in a message that arrived under the negotiated
// description, its model references and then its shared lists of coverage
// kinds, as far as the description lists their features.
func received(description *protocol.Description, message proto.Message) error {
	if slices.Contains(description.GetFeatures(), provider.ModelReferencesFeature) {
		if err := provider.ResolveModelReferences(description, message); err != nil {
			return err
		}
	}
	if slices.Contains(description.GetFeatures(), provider.SharedCoverageFeature) {
		return provider.ResolveSharedCoverage(message)
	}
	return nil
}

// references is the server's interceptor: it resolves the model references
// and the shared lists of coverage kinds of a request before the service or a
// validator reads it, and sends the reply's repeated lists once and its
// described models by reference, as far as the description negotiated with
// the host lists provider.ModelReferencesFeature and
// provider.SharedCoverageFeature.
func (s *serverAdapter) references(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	description := s.negotiated()
	if info.FullMethod == protocol.Provider_Describe_FullMethodName {
		return handler(ctx, request)
	}
	if message, isMessage := request.(proto.Message); isMessage {
		if err := received(description, message); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	reply, err := handler(ctx, request)
	if err != nil {
		return reply, err
	}
	if message, isMessage := reply.(proto.Message); isMessage {
		return sendable(description, message), nil
	}
	return reply, nil
}
