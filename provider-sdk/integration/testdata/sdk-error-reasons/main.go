// Package main gives the reason for a failure over RPC. It serves the
// fixture with locks and lists errors.reasons. An inspection of a session
// opened with the fault "refuse" fails with a reason the program wrote,
// while the status message quotes the session's address, and a lock that
// another session holds is BUSY with a reason. A process test then sees which
// of the two texts crossed the boundary.
package main

import (
	"context"
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-sdk-error-reasons"
	description.Features = append(description.Features, provider.TransactionsFeature, provider.LocksFeature, provider.ErrorReasonsFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.TransactionsFeature, provider.LocksFeature)
	owner := &reasonsOwner{TransactionalProvider: &backend.TransactionalProvider{}}
	if err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		Transactions: owner, Locks: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type reasonsOwner struct {
	*backend.TransactionalProvider
}

// Inspect refuses a session opened with the fault "refuse". The reason is the
// program's own sentence; the status message quotes the session's database,
// which the host must not show.
func (o *reasonsOwner) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	var refused, path string
	err := o.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		refused, path = access.Fault, access.Path
		return nil
	})
	if err != nil {
		return nil, err
	}
	if refused != "refuse" {
		return o.TransactionalProvider.Inspect(ctx, request)
	}
	reported, err := status.New(codes.FailedPrecondition, "cannot read "+path).
		WithDetails(&protocol.ProviderDiagnostic{Reason: "the fixture refuses to read this database"})
	if err != nil {
		return nil, err
	}
	return nil, reported.Err()
}

// TryLock says why a lock is busy.
func (o *reasonsOwner) TryLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	result, err := o.TransactionalProvider.TryLock(ctx, request)
	if err != nil || result.GetOutcome() != protocol.LockResult_BUSY {
		return result, err
	}
	result = proto.CloneOf(result)
	result.Diagnostic = &protocol.ProviderDiagnostic{Reason: "another session holds the lock"}
	return result, nil
}
