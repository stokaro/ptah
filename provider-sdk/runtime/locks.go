package runtime

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TryLock makes one nonblocking attempt. A missing or malformed receipt exposes
// no usable lock and triggers bounded release of the exact requested handle.
// Release fences a late acquisition; even successful cleanup retains the
// original uncertainty. Cancellation before dispatch sends nothing.
func (c *Client) TryLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	request = proto.CloneOf(request)
	result, err := c.lockCall(ctx, request, provider.LockTry)
	if errors.Is(err, provider.ErrOutcomeUnknown) {
		_, cleanupErr := c.ReleaseLock(context.WithoutCancel(ctx), lockCleanupRequest(request))
		return nil, errors.Join(err, cleanupErr)
	}
	return result, err
}

// CheckLock asks the provider to verify current ownership. An earlier receipt
// is never substituted for a failed check. Callers must stop guarded work when
// ownership is lost or uncertain; the transport does not retry the check.
func (c *Client) CheckLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	return c.lockCall(ctx, request, provider.LockCheck)
}

// ReleaseLock releases or fences the exact handle once. An uncertain reply is
// not retried or reported as successful release.
func (c *Client) ReleaseLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	return c.lockCall(ctx, request, provider.LockRelease)
}

func lockCleanupRequest(request *protocol.LockRequest) *protocol.LockRequest {
	return &protocol.LockRequest{Scope: proto.CloneOf(request.GetScope()), RequestId: rand.Text()}
}

func (c *Client) lockCall(ctx context.Context, request *protocol.LockRequest, operation provider.LockOperation) (*protocol.LockResult, error) {
	request = proto.CloneOf(request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !slices.Contains(c.description.GetFeatures(), provider.LocksFeature) {
		return nil, fmt.Errorf("%w: lock service was not negotiated", provider.ErrProtocol)
	}
	if err := provider.ValidateLockRequest(request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	var result *protocol.LockResult
	var err error
	switch operation {
	case provider.LockTry:
		result, err = c.rpc.TryLock(call, proto.CloneOf(request))
	case provider.LockCheck:
		result, err = c.rpc.CheckLock(call, proto.CloneOf(request))
	case provider.LockRelease:
		result, err = c.rpc.ReleaseLock(call, proto.CloneOf(request))
	default:
		return nil, fmt.Errorf("%w: unknown lock operation", provider.ErrProtocol)
	}
	if err != nil {
		return nil, errors.Join(&provider.LockError{}, remoteError(call, "lock operation", err))
	}
	if err := provider.ValidateLockResult(operation, request, result); err != nil {
		return nil, errors.Join(err, &provider.LockError{})
	}
	result = proto.CloneOf(result)
	return result, provider.CheckLockResult(operation, request, result)
}

func (s *serverAdapter) TryLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	return s.lockCall(ctx, request, provider.LockTry)
}

func (s *serverAdapter) CheckLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	return s.lockCall(ctx, request, provider.LockCheck)
}

func (s *serverAdapter) ReleaseLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	return s.lockCall(ctx, request, provider.LockRelease)
}

func (s *serverAdapter) lockCall(ctx context.Context, request *protocol.LockRequest, operation provider.LockOperation) (*protocol.LockResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if nilService(s.server.Locks) {
		return nil, status.Error(codes.Unimplemented, "locks are unavailable")
	}
	request = proto.CloneOf(request)
	if err := provider.ValidateLockRequest(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var result *protocol.LockResult
	var err error
	switch operation {
	case provider.LockTry:
		result, err = s.server.Locks.TryLock(s.owner(ctx), proto.CloneOf(request))
	case provider.LockCheck:
		result, err = s.server.Locks.CheckLock(s.owner(ctx), proto.CloneOf(request))
	case provider.LockRelease:
		result, err = s.server.Locks.ReleaseLock(s.owner(ctx), proto.CloneOf(request))
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown lock operation")
	}
	if err == nil {
		if failure := provider.ValidateLockResult(operation, request, result); failure != nil {
			err = status.Error(codes.Internal, "provider returned an invalid lock receipt")
		}
	}
	if operation == provider.LockTry && (err != nil || result.GetOutcome() == protocol.LockResult_UNKNOWN) {
		if cleanupErr := s.cleanupLock(ctx, request); cleanupErr != nil {
			return nil, status.Error(codes.Internal, "provider lock acquisition and cleanup could not be confirmed")
		}
	}
	if err != nil {
		return nil, err
	}
	return proto.CloneOf(result), nil
}

func (s *serverAdapter) cleanupLock(ctx context.Context, request *protocol.LockRequest) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), CallTimeout)
	defer cancel()
	fence := lockCleanupRequest(request)
	result, err := s.server.Locks.ReleaseLock(cleanup, proto.CloneOf(fence))
	if err != nil {
		return err
	}
	return provider.CheckLockResult(provider.LockRelease, fence, result)
}
