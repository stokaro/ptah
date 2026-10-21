package runtime

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func metadataClientCall[Q proto.Message, R proto.Message](ctx context.Context, c *Client, request Q, scope *protocol.MetadataScope,
	validate func(Q) error, invoke func(context.Context, Q, ...grpc.CallOption) (R, error), check func(Q, R) error, failureOutcome error) (R, error) {
	var zero R
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if !slices.Contains(c.description.GetFeatures(), provider.MetadataFeature) {
		return zero, fmt.Errorf("%w: metadata service was not negotiated", provider.ErrProtocol)
	}
	if scope.GetTransactionId() != "" {
		if err := c.requireTransactions(); err != nil {
			return zero, err
		}
	}
	if err := validate(request); err != nil {
		return zero, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := invoke(call, proto.CloneOf(request))
	if err != nil {
		err = remoteError(call, "migration metadata", err)
	} else {
		err = check(request, result)
	}
	if err != nil {
		return zero, errors.Join(failureOutcome, err)
	}
	return proto.CloneOf(result), nil
}

func metadataServerCall[Q proto.Message, R proto.Message](ctx context.Context, s *serverAdapter, request Q, scope *protocol.MetadataScope,
	validate func(Q) error, invoke func(context.Context, Q) (R, error), check func(Q, R) error, cleanup func(context.Context) error) (R, error) {
	var zero R
	if err := ctx.Err(); err != nil {
		return zero, status.FromContextError(err).Err()
	}
	if nilService(s.server.Metadata) {
		return zero, status.Error(codes.Unimplemented, "migration metadata is unavailable")
	}
	if scope.GetTransactionId() != "" && nilService(s.server.Transactions) {
		return zero, status.Error(codes.Unimplemented, "transactions are unavailable")
	}
	request = proto.CloneOf(request)
	if err := validate(request); err != nil {
		return zero, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := invoke(ctx, proto.CloneOf(request))
	if err == nil {
		if failure := check(request, result); failure != nil {
			err = status.Error(codes.Internal, "provider returned an invalid metadata receipt")
		}
	}
	if err != nil {
		if cleanup != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CallTimeout)
			defer cancel()
			if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
				return zero, status.Error(codes.Internal, "metadata operation and snapshot cleanup could not be confirmed")
			}
		}
		return zero, err
	}
	return proto.CloneOf(result), nil
}

func metadataReadCleanup(request *protocol.MetadataReadRequest) *protocol.MetadataReadRequest {
	return &protocol.MetadataReadRequest{Scope: proto.CloneOf(request.GetScope()), ReaderId: request.GetReaderId(), RequestId: rand.Text()}
}
func (s *serverAdapter) closeMetadataReader(ctx context.Context, request *protocol.MetadataReadRequest) error {
	fence := metadataReadCleanup(request)
	result, err := s.server.Metadata.CloseMetadataRead(ctx, proto.CloneOf(fence))
	if err != nil {
		return err
	}
	return provider.ValidateMetadataReadResult(fence, result)
}

// PrepareMetadata validates or creates the selected store. A lost receipt is unknown and is never replayed.
func (c *Client) PrepareMetadata(ctx context.Context, request *protocol.PrepareMetadataRequest) (*protocol.MetadataResult, error) {
	request = proto.CloneOf(request)
	result, err := metadataClientCall(ctx, c, request, request.GetScope(), provider.ValidatePrepareMetadataRequest, c.rpc.PrepareMetadata, provider.ValidatePrepareMetadataResult, provider.ErrOutcomeUnknown)
	if err != nil {
		return nil, err
	}
	return result, provider.CheckMetadataResult(result)
}
func (s *serverAdapter) PrepareMetadata(ctx context.Context, request *protocol.PrepareMetadataRequest) (*protocol.MetadataResult, error) {
	request = proto.CloneOf(request)
	return metadataServerCall(ctx, s, request, request.GetScope(), provider.ValidatePrepareMetadataRequest,
		func(call context.Context, message *protocol.PrepareMetadataRequest) (*protocol.MetadataResult, error) {
			return s.server.Metadata.PrepareMetadata(call, message)
		},
		provider.ValidatePrepareMetadataResult, nil)
}

// MutateMetadata performs one typed mutation. Confirmed rejection differs from an unknown outcome; neither permits transport replay.
func (c *Client) MutateMetadata(ctx context.Context, request *protocol.MutateMetadataRequest) (*protocol.MetadataResult, error) {
	request = proto.CloneOf(request)
	result, err := metadataClientCall(ctx, c, request, request.GetScope(), provider.ValidateMutateMetadataRequest, c.rpc.MutateMetadata, provider.ValidateMutateMetadataResult, provider.ErrOutcomeUnknown)
	if err != nil {
		return nil, err
	}
	return result, provider.CheckMetadataResult(result)
}
func (s *serverAdapter) MutateMetadata(ctx context.Context, request *protocol.MutateMetadataRequest) (*protocol.MetadataResult, error) {
	request = proto.CloneOf(request)
	return metadataServerCall(ctx, s, request, request.GetScope(), provider.ValidateMutateMetadataRequest,
		func(call context.Context, message *protocol.MutateMetadataRequest) (*protocol.MetadataResult, error) {
			return s.server.Metadata.MutateMetadata(call, message)
		},
		provider.ValidateMutateMetadataResult, nil)
}

// LookupMetadataTag reads one exact label without creating storage. Missing labels remain distinct from version zero.
func (c *Client) LookupMetadataTag(ctx context.Context, request *protocol.LookupMetadataTagRequest) (*protocol.MetadataTagResult, error) {
	request = proto.CloneOf(request)
	result, err := metadataClientCall(ctx, c, request, request.GetScope(), provider.ValidateLookupMetadataTagRequest, c.rpc.LookupMetadataTag, provider.ValidateMetadataTagResult, nil)
	return result, err
}
func (s *serverAdapter) LookupMetadataTag(ctx context.Context, request *protocol.LookupMetadataTagRequest) (*protocol.MetadataTagResult, error) {
	request = proto.CloneOf(request)
	return metadataServerCall(ctx, s, request, request.GetScope(), provider.ValidateLookupMetadataTagRequest,
		func(call context.Context, message *protocol.LookupMetadataTagRequest) (*protocol.MetadataTagResult, error) {
			return s.server.Metadata.LookupMetadataTag(call, message)
		},
		provider.ValidateMetadataTagResult, nil)
}

// OpenMetadataRead opens a single-use snapshot. Uncertain opening triggers bounded close of the same handle, fencing a late open.
func (c *Client) OpenMetadataRead(ctx context.Context, request *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
	request = proto.CloneOf(request)
	result, err := metadataClientCall(ctx, c, request, request.GetScope(), provider.ValidateMetadataReadRequest, c.rpc.OpenMetadataRead, provider.ValidateMetadataReadResult, provider.ErrOutcomeUnknown)
	if errors.Is(err, provider.ErrOutcomeUnknown) {
		_, cleanupErr := c.CloseMetadataRead(context.WithoutCancel(ctx), metadataReadCleanup(request))
		return nil, errors.Join(err, cleanupErr)
	}
	return result, err
}
func (s *serverAdapter) OpenMetadataRead(ctx context.Context, request *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
	request = proto.CloneOf(request)
	return metadataServerCall(ctx, s, request, request.GetScope(), provider.ValidateMetadataReadRequest,
		func(call context.Context, message *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
			return s.server.Metadata.OpenMetadataRead(call, message)
		},
		provider.ValidateMetadataReadResult, func(cleanup context.Context) error { return s.closeMetadataReader(cleanup, request) })
}

// ReadMetadataPage advances one bounded page without retry. A lost or malformed reply closes the snapshot and exposes no usable prefix.
func (c *Client) ReadMetadataPage(ctx context.Context, request *protocol.ReadMetadataPageRequest) (*protocol.MetadataPage, error) {
	request = proto.CloneOf(request)
	result, err := metadataClientCall(ctx, c, request, request.GetReader().GetScope(), provider.ValidateReadMetadataPageRequest, c.rpc.ReadMetadataPage, provider.ValidateMetadataPage, provider.ErrOutcomeUnknown)
	if errors.Is(err, provider.ErrOutcomeUnknown) {
		_, cleanupErr := c.CloseMetadataRead(context.WithoutCancel(ctx), metadataReadCleanup(request.GetReader()))
		return nil, errors.Join(err, cleanupErr)
	}
	return result, err
}
func (s *serverAdapter) ReadMetadataPage(ctx context.Context, request *protocol.ReadMetadataPageRequest) (*protocol.MetadataPage, error) {
	request = proto.CloneOf(request)
	return metadataServerCall(ctx, s, request, request.GetReader().GetScope(), provider.ValidateReadMetadataPageRequest,
		func(call context.Context, message *protocol.ReadMetadataPageRequest) (*protocol.MetadataPage, error) {
			return s.server.Metadata.ReadMetadataPage(call, message)
		},
		provider.ValidateMetadataPage, func(cleanup context.Context) error { return s.closeMetadataReader(cleanup, request.GetReader()) })
}

// CloseMetadataRead releases or fences the exact snapshot handle. Missing acknowledgment remains an unknown cleanup outcome.
func (c *Client) CloseMetadataRead(ctx context.Context, request *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
	request = proto.CloneOf(request)
	result, err := metadataClientCall(ctx, c, request, request.GetScope(), provider.ValidateMetadataReadRequest, c.rpc.CloseMetadataRead, provider.ValidateMetadataReadResult, provider.ErrOutcomeUnknown)
	return result, err
}
func (s *serverAdapter) CloseMetadataRead(ctx context.Context, request *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
	request = proto.CloneOf(request)
	return metadataServerCall(ctx, s, request, request.GetScope(), provider.ValidateMetadataReadRequest,
		func(call context.Context, message *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
			return s.server.Metadata.CloseMetadataRead(call, message)
		},
		provider.ValidateMetadataReadResult, nil)
}
