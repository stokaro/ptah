package conformance_test

import (
	"context"
	"slices"
	"sync"

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
	// imageAndLinked describes the certified release line with an image
	// beside the linked engine it names.
	imageAndLinked bool
	// enumColumnsRegardless describes the memory engine's enum columns to a
	// host that did not list schema.enum-columns.
	enumColumnsRegardless bool
	// ownProtocolRefusal refuses PrepareCommonObjects for a host that did not
	// list schema.common-objects itself, as InvalidArgument with a protocol
	// error of its own words, before the SDK's server sees the request.
	ownProtocolRefusal bool
}

// wireProvider forwards each call to the memory provider behind the SDK's
// server, and its defects change what it sends back.
type wireProvider struct {
	protocol.UnimplementedProviderServer
	inner   protocol.ProviderClient
	defects wireDefects
	// exclusive, when set, keeps the databases a session of this connection
	// opened from every other connection, which is process, as an engine
	// linked into its provider keeps other processes out.
	exclusive *exclusiveDatabases
	process   int

	mu sync.Mutex
	// hostFeatures are what the host listed in the last Describe of this
	// connection.
	hostFeatures []string
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

// exclusiveDatabases is what an engine linked into its provider keeps: one
// process holds a database from the Open of its first session to the Close
// of its last, and the other processes cannot open it meanwhile. Each
// connection of a wire target is a process.
type exclusiveDatabases struct {
	mu        sync.Mutex
	processes int
	holders   map[string]int
	sessions  map[string]string
	open      map[string]int
}

func newExclusiveDatabases() *exclusiveDatabases {
	return &exclusiveDatabases{holders: make(map[string]int), sessions: make(map[string]string), open: make(map[string]int)}
}

// process returns the number of a new process.
func (e *exclusiveDatabases) process() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.processes++
	return e.processes
}

// enter opens a session of address for process with open, unless another
// process holds the database.
func (e *exclusiveDatabases) enter(process int, address string, open func() (*protocol.Session, error)) (*protocol.Session, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if holder, held := e.holders[address]; held && holder != process {
		return nil, status.Error(codes.FailedPrecondition, "another process holds the database")
	}
	session, err := open()
	if err != nil {
		return nil, err
	}
	e.holders[address] = process
	e.sessions[session.GetId()] = address
	e.open[address]++
	return session, nil
}

// leave releases a closed session, and the database with its process's last
// session.
func (e *exclusiveDatabases) leave(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	address, found := e.sessions[id]
	if !found {
		return
	}
	delete(e.sessions, id)
	if e.open[address]--; e.open[address] == 0 {
		delete(e.open, address)
		delete(e.holders, address)
	}
}

// exclusiveWireTarget is the memory provider behind a wire provider whose
// engine one process holds at a time, as an engine linked into its provider
// is: each connection is a process, and a database one process has a session
// of refuses every other.
func exclusiveWireTarget(c *qt.C, memory *memoryProvider) func() protocol.ProviderServer {
	exclusive := newExclusiveDatabases()
	return func() protocol.ProviderServer {
		client, stop, err := runtime.ServeInMemory(memory.server())
		c.Assert(err, qt.IsNil)
		c.Cleanup(stop)
		return &wireProvider{inner: client, exclusive: exclusive, process: exclusive.process()}
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
	w.mu.Lock()
	w.hostFeatures = slices.Clone(request.GetHostFeatures())
	w.mu.Unlock()
	if w.defects.describeAll {
		request.HostFeatures = provider.TwoWayFeatures()
	}
	reply, err := w.inner.Describe(ctx, request)
	if err == nil && w.defects.enumColumnsRegardless && !slices.Contains(reply.GetFeatures(), provider.EnumColumnsFeature) {
		engine := reply.GetEngines()[0]
		reply.Features = append(reply.Features, provider.EnumColumnsFeature)
		engine.Features = append(engine.Features, provider.EnumColumnsFeature)
		engine.EnumColumns = &protocol.EnumColumns{Form: protocol.EnumColumns_CHECK, CheckType: "TEXT"}
	}
	if err == nil && w.defects.imageAndLinked {
		for _, line := range reply.GetEngines()[0].GetReleaseLines() {
			line.Image = "example.com/memory:1"
		}
	}
	return reply, w.refusal(err)
}

func (w *wireProvider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	if w.exclusive != nil {
		return w.exclusive.enter(w.process, request.GetAddress(), func() (*protocol.Session, error) { return w.inner.Open(ctx, request) })
	}
	reply, err := w.inner.Open(ctx, request)
	return reply, w.refusal(err)
}

func (w *wireProvider) Close(ctx context.Context, request *protocol.CloseRequest) (*protocol.Empty, error) {
	reply, err := w.inner.Close(ctx, request)
	if w.exclusive != nil && err == nil {
		w.exclusive.leave(request.GetSessionId())
	}
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
	w.mu.Lock()
	listed := slices.Contains(w.hostFeatures, provider.CommonObjectsFeature)
	w.mu.Unlock()
	if w.defects.ownProtocolRefusal && !listed {
		return nil, status.Error(codes.InvalidArgument, provider.ErrProtocol.Error()+": the wire provider does not prepare common objects for this host")
	}
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

func (w *wireProvider) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	reply, err := w.inner.NormalizeValues(ctx, request)
	return reply, w.refusal(err)
}
