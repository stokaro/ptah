package conformance_test

import (
	"context"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wireDefects are the ways a provider that implements the wire service
// itself can break the contract where the SDK's server would not let it.
type wireDefects struct {
	// describeAll answers every Describe as a host that lists every two-way
	// feature asked.
	describeAll bool
	// reasonAlways gives every refusal a reason, negotiated or not.
	reasonAlways bool
	// renderUnimplemented answers Render with Unimplemented although the
	// description offers it.
	renderUnimplemented bool
}

// wireProvider forwards each call to the memory provider behind the SDK's
// server, and its defects change what it sends back.
type wireProvider struct {
	protocol.UnimplementedProviderServer
	inner   protocol.ProviderClient
	defects wireDefects
}

// wireTarget is the memory provider behind a wire provider with defects. Each
// connection gets a server of its own, with a negotiation of its own, over
// the same databases.
func wireTarget(c *qt.C, memory *memoryProvider, defects wireDefects) func() protocol.ProviderServer {
	return func() protocol.ProviderServer {
		client, stop, err := runtime.ServeInMemory(memory.server())
		c.Assert(err, qt.IsNil)
		c.Cleanup(stop)
		return &wireProvider{inner: client, defects: defects}
	}
}

// refusal is the error the wire provider sends for err.
func (w *wireProvider) refusal(err error) error {
	if err == nil || !w.defects.reasonAlways {
		return err
	}
	reported := status.Convert(err)
	if len(reported.Details()) != 0 {
		return err
	}
	reasoned, detailErr := reported.WithDetails(&protocol.ProviderDiagnostic{Reason: "the wire provider refused the call"})
	if detailErr != nil {
		return err
	}
	return reasoned.Err()
}

func (w *wireProvider) Describe(ctx context.Context, request *protocol.DescribeRequest) (*protocol.Description, error) {
	if w.defects.describeAll {
		request.HostFeatures = provider.TwoWayFeatures()
	}
	reply, err := w.inner.Describe(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	reply, err := w.inner.Open(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) Close(ctx context.Context, request *protocol.CloseRequest) (*protocol.Empty, error) {
	reply, err := w.inner.Close(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	reply, err := w.inner.Inspect(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	if w.defects.renderUnimplemented {
		return nil, status.Error(codes.Unimplemented, "rendering is not implemented yet")
	}
	reply, err := w.inner.Render(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	reply, err := w.inner.Execute(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) TryLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	reply, err := w.inner.TryLock(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) CheckLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	reply, err := w.inner.CheckLock(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) ReleaseLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	reply, err := w.inner.ReleaseLock(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) BeginTransaction(ctx context.Context, request *protocol.BeginTransactionRequest) (*protocol.Transaction, error) {
	reply, err := w.inner.BeginTransaction(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	reply, err := w.inner.FinishTransaction(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	reply, err := w.inner.PrepareCommonObjects(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) ListSeedTables(ctx context.Context, request *protocol.ListSeedTablesRequest) (*protocol.SeedTables, error) {
	reply, err := w.inner.ListSeedTables(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) ApplySeed(ctx context.Context, request *protocol.ApplySeedRequest) (*protocol.SeedResult, error) {
	reply, err := w.inner.ApplySeed(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) RecordSeed(ctx context.Context, request *protocol.RecordSeedRequest) (*protocol.SeedResult, error) {
	reply, err := w.inner.RecordSeed(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) ReadSeedLedger(ctx context.Context, request *protocol.ReadSeedLedgerRequest) (*protocol.SeedLedger, error) {
	reply, err := w.inner.ReadSeedLedger(ctx, request)
	return reply, w.refusal(err)
}
