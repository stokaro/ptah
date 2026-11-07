package conformance

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// probe calls one RPC with an empty request. An empty request is invalid for
// every RPC, so a provider refuses it; what the suite reads is how: a
// service the description offers must not answer Unimplemented, and one it
// does not offer must not answer at all.
type probe struct {
	method string
	// feature is the provider feature that offers the RPC, empty for the
	// session RPCs every provider serves.
	feature string
	call    func(context.Context, protocol.ProviderClient) error
}

// empty is the probe of one RPC. Q is the request message, so new(Q) is the
// empty request.
func empty[Q any, R any](method, feature string, rpc func(protocol.ProviderClient, context.Context, *Q, ...grpc.CallOption) (R, error)) probe {
	return probe{method: method, feature: feature, call: func(ctx context.Context, client protocol.ProviderClient) error {
		_, err := rpc(client, ctx, new(Q))
		return err
	}}
}

// probes returns a probe of every RPC of the protocol but Describe, which
// every check sends. TestProbesCoverTheService keeps the list equal to the
// generated service.
func probes() []probe {
	return []probe{
		empty("Open", "", protocol.ProviderClient.Open),
		empty("Close", "", protocol.ProviderClient.Close),
		empty("ResolveDatabaseContext", provider.DatabaseContextFeature, protocol.ProviderClient.ResolveDatabaseContext),
		empty("Inspect", provider.InspectFeature, protocol.ProviderClient.Inspect),
		empty("Render", provider.RenderFeature, protocol.ProviderClient.Render),
		empty("AnalyzeSQL", provider.AnalyzeSQLFeature, protocol.ProviderClient.AnalyzeSQL),
		empty("DecodeSource", provider.DecodeSourceFeature, protocol.ProviderClient.DecodeSource),
		empty("ReadSQLSchema", provider.ReadSQLSchemaFeature, protocol.ProviderClient.ReadSQLSchema),
		empty("NormalizeValues", provider.NormalizeValuesFeature, protocol.ProviderClient.NormalizeValues),
		empty("ConvertValues", provider.ConvertValuesFeature, protocol.ProviderClient.ConvertValues),
		empty("CompareModels", provider.CompareModelsFeature, protocol.ProviderClient.CompareModels),
		empty("PlanFeatures", provider.PlanFeaturesFeature, protocol.ProviderClient.PlanFeatures),
		empty("PlanDeclarations", provider.SchemaDeclarationsFeature, protocol.ProviderClient.PlanDeclarations),
		empty("CompareColumns", provider.ColumnsFeature, protocol.ProviderClient.CompareColumns),
		empty("ReadColumnDefaults", provider.ColumnsFeature, protocol.ProviderClient.ReadColumnDefaults),
		empty("DescribeExecutionInputs", provider.SensitiveInputsFeature, protocol.ProviderClient.DescribeExecutionInputs),
		empty("PrepareTables", provider.PrepareTablesFeature, protocol.ProviderClient.PrepareTables),
		empty("ProjectTableCreations", provider.ProjectTableCreationsFeature, protocol.ProviderClient.ProjectTableCreations),
		empty("ValidateSchema", provider.ValidateSchemaFeature, protocol.ProviderClient.ValidateSchema),
		empty("SplitLintStatements", provider.LintFeature, protocol.ProviderClient.SplitLintStatements),
		empty("AnnotateLintStatements", provider.LintFeature, protocol.ProviderClient.AnnotateLintStatements),
		empty("CheckLintMigrations", provider.LintFeature, protocol.ProviderClient.CheckLintMigrations),
		empty("ProvisionDevDatabase", provider.DevDatabaseFeature, protocol.ProviderClient.ProvisionDevDatabase),
		empty("ReleaseDevDatabase", provider.DevDatabaseFeature, protocol.ProviderClient.ReleaseDevDatabase),
		empty("ListDevObjects", provider.DevCleanupFeature, protocol.ProviderClient.ListDevObjects),
		empty("CleanDevDatabase", provider.DevCleanupFeature, protocol.ProviderClient.CleanDevDatabase),
		empty("GuardReplay", provider.ReplayGuardFeature, protocol.ProviderClient.GuardReplay),
		empty("TypeRows", provider.TypeRowsFeature, protocol.ProviderClient.TypeRows),
		empty("ReadRows", provider.ReadRowsFeature, protocol.ProviderClient.ReadRows),
		empty("RenderRowWrites", provider.RenderRowWritesFeature, protocol.ProviderClient.RenderRowWrites),
		empty("ListSeedTables", provider.SeedsFeature, protocol.ProviderClient.ListSeedTables),
		empty("ApplySeed", provider.SeedsFeature, protocol.ProviderClient.ApplySeed),
		empty("RecordSeed", provider.SeedsFeature, protocol.ProviderClient.RecordSeed),
		empty("ReadSeedLedger", provider.SeedsFeature, protocol.ProviderClient.ReadSeedLedger),
		empty("Execute", provider.ExecuteFeature, protocol.ProviderClient.Execute),
		empty("BeginTransaction", provider.TransactionsFeature, protocol.ProviderClient.BeginTransaction),
		empty("FinishTransaction", provider.TransactionsFeature, protocol.ProviderClient.FinishTransaction),
		empty("ReadHistory", provider.HistoryFeature, protocol.ProviderClient.ReadHistory),
		empty("PrepareHistory", provider.HistoryFeature, protocol.ProviderClient.PrepareHistory),
		empty("MutateHistory", provider.HistoryFeature, protocol.ProviderClient.MutateHistory),
		empty("PrepareMetadata", provider.MetadataFeature, protocol.ProviderClient.PrepareMetadata),
		empty("MutateMetadata", provider.MetadataFeature, protocol.ProviderClient.MutateMetadata),
		empty("LookupMetadataTag", provider.MetadataFeature, protocol.ProviderClient.LookupMetadataTag),
		empty("OpenMetadataRead", provider.MetadataFeature, protocol.ProviderClient.OpenMetadataRead),
		empty("ReadMetadataPage", provider.MetadataFeature, protocol.ProviderClient.ReadMetadataPage),
		empty("CloseMetadataRead", provider.MetadataFeature, protocol.ProviderClient.CloseMetadataRead),
		empty("TryLock", provider.LocksFeature, protocol.ProviderClient.TryLock),
		empty("CheckLock", provider.LocksFeature, protocol.ProviderClient.CheckLock),
		empty("ReleaseLock", provider.LocksFeature, protocol.ProviderClient.ReleaseLock),
		empty("PrepareEmbeddingStore", provider.EmbeddingStoreFeature, protocol.ProviderClient.PrepareEmbeddingStore),
		empty("ReadEmbeddingState", provider.EmbeddingStoreFeature, protocol.ProviderClient.ReadEmbeddingState),
		empty("MutateEmbeddingState", provider.EmbeddingStoreFeature, protocol.ProviderClient.MutateEmbeddingState),
		empty("AppendEmbeddingEvent", provider.EmbeddingStoreFeature, protocol.ProviderClient.AppendEmbeddingEvent),
		empty("ReadEmbeddingEvents", provider.EmbeddingStoreFeature, protocol.ProviderClient.ReadEmbeddingEvents),
		empty("DescribeEmbeddingTarget", provider.EmbeddingStoreFeature, protocol.ProviderClient.DescribeEmbeddingTarget),
		empty("ResolveEmbeddingRelations", provider.EmbeddingStoreFeature, protocol.ProviderClient.ResolveEmbeddingRelations),
		empty("MeasureEmbedding", provider.EmbeddingStoreFeature, protocol.ProviderClient.MeasureEmbedding),
		empty("PrepareEmbeddingTarget", provider.EmbeddingStoreFeature, protocol.ProviderClient.PrepareEmbeddingTarget),
		empty("BuildEmbeddingIndex", provider.EmbeddingStoreFeature, protocol.ProviderClient.BuildEmbeddingIndex),
		empty("ReadEmbeddingStructure", provider.EmbeddingStoreFeature, protocol.ProviderClient.ReadEmbeddingStructure),
		empty("DropEmbeddingGeneration", provider.EmbeddingStoreFeature, protocol.ProviderClient.DropEmbeddingGeneration),
		empty("ScanEmbeddingSource", provider.EmbeddingStoreFeature, protocol.ProviderClient.ScanEmbeddingSource),
		empty("ReadEmbeddingSourceRows", provider.EmbeddingStoreFeature, protocol.ProviderClient.ReadEmbeddingSourceRows),
		empty("ReadEmbeddingBatchState", provider.EmbeddingStoreFeature, protocol.ProviderClient.ReadEmbeddingBatchState),
		empty("CommitEmbeddingBatch", provider.EmbeddingStoreFeature, protocol.ProviderClient.CommitEmbeddingBatch),
		empty("ReadEmbeddingVerification", provider.EmbeddingStoreFeature, protocol.ProviderClient.ReadEmbeddingVerification),
		empty("SearchEmbeddings", provider.EmbeddingStoreFeature, protocol.ProviderClient.SearchEmbeddings),
		empty("PrepareCommonObjects", provider.CommonObjectsFeature, protocol.ProviderClient.PrepareCommonObjects),
		empty("EncodeSource", provider.EncodeSourceFeature, protocol.ProviderClient.EncodeSource),
		empty("RunStatement", provider.StatementsFeature, protocol.ProviderClient.RunStatement),
		empty("ReadStatementRows", provider.StatementsFeature, protocol.ProviderClient.ReadStatementRows),
		empty("CloseStatement", provider.StatementsFeature, protocol.ProviderClient.CloseStatement),
		empty("SplitMigration", provider.MigrationSplitFeature, protocol.ProviderClient.SplitMigration),
		empty("ReverseChanges", provider.ReverseChangesFeature, protocol.ProviderClient.ReverseChanges),
		empty("ReportValues", provider.ReportCountsFeature, protocol.ProviderClient.ReportValues),
		empty("ScanSQL", provider.ScanSQLFeature, protocol.ProviderClient.ScanSQL),
		empty("CompareChecks", provider.CheckEquivalenceFeature, protocol.ProviderClient.CompareChecks),
		empty("CompareViewQueries", provider.ViewQueryEquivalenceFeature, protocol.ProviderClient.CompareViewQueries),
	}
}

// services probes every RPC under a host that lists every two-way feature,
// so that every service the provider offers is negotiated.
func (r *run) services(ctx context.Context) {
	var unanswered, answered []string
	for _, entry := range probes() {
		offered := entry.feature == "" || slices.Contains(r.all.description.GetFeatures(), entry.feature)
		bounded, cancel := context.WithTimeout(ctx, r.timeout)
		err := entry.call(bounded, r.all.client)
		cancel()
		if err != nil {
			r.observeError(r.all, entry.method, err)
		}
		switch {
		case offered && status.Code(err) == codes.Unimplemented:
			unanswered = append(unanswered, fmt.Sprintf("%s answers Unimplemented although the description offers %s", entry.method, featureName(entry.feature)))
		case !offered && err == nil:
			answered = append(answered, fmt.Sprintf("%s succeeds although the description does not offer %s", entry.method, entry.feature))
		}
	}
	r.result(checkAdvertised, unanswered)
	r.result(checkUnadvertised, answered)
}

func featureName(feature string) string {
	if feature == "" {
		return "sessions"
	}
	return feature
}

// unknownSession is a session handle no provider opened.
const unknownSession = "ptah-conformance-unknown-session"

// unknownSessions sends each session-bound call the provider offers with a
// handle it never opened, under both profiles. A handle is scoped to the
// process that opened it; accepting one from nowhere is accepting a stale
// one after a restart.
func (r *run) unknownSessions(ctx context.Context) {
	var failures []string
	for _, c := range []*conn{r.all, r.none} {
		if c == nil {
			continue
		}
		failures = append(failures, r.unknownSessionCalls(ctx, c)...)
	}
	if len(failures) == 0 && !slices.ContainsFunc([]string{provider.InspectFeature, provider.ExecuteFeature, provider.LocksFeature, provider.TransactionsFeature},
		func(feature string) bool { return slices.Contains(r.full.GetFeatures(), feature) }) {
		r.add(checkUnknownSession, NotApplicable, "the provider offers no session-bound service the suite probes")
		return
	}
	r.result(checkUnknownSession, failures)
}

func (r *run) unknownSessionCalls(ctx context.Context, c *conn) []string {
	var failures []string
	offers := func(feature string) bool { return slices.Contains(c.description.GetFeatures(), feature) }
	under := " under " + c.profile.name
	if offers(provider.InspectFeature) {
		if _, err := invoke(ctx, r, c, "Inspect", c.client.Inspect, &protocol.InspectRequest{SessionId: unknownSession}); err == nil {
			failures = append(failures, "Inspect read a session the provider never opened"+under)
		}
	}
	if offers(provider.ExecuteFeature) {
		request := r.executeRequest(c, unknownSession, "ptah-conformance-unknown-session", []*protocol.Unit{{ChangeId: "ptah-conformance", Body: &protocol.Unit_Sql{Sql: "SELECT 1"}}})
		result, err := invoke(ctx, r, c, "Execute", c.client.Execute, request)
		if err == nil {
			r.observeResultReason(c, "Execute", result.GetDiagnostic())
			if result.GetOutcome() == protocol.Execution_APPLIED || result.GetAppliedCount() != 0 {
				failures = append(failures, "Execute applied units in a session the provider never opened"+under)
			}
		}
	}
	if offers(provider.LocksFeature) {
		for _, method := range []string{"TryLock", "CheckLock"} {
			request := &protocol.LockRequest{Scope: &protocol.LockScope{SessionId: unknownSession, LockId: "ptah-conformance-" + method, Name: defaultLockName}, RequestId: "ptah-conformance-" + method}
			result, err := invoke(ctx, r, c, method, lockCall(c, method), request)
			if err == nil {
				r.observeResultReason(c, method, result.GetDiagnostic())
				if result.GetOutcome() == protocol.LockResult_ACQUIRED {
					failures = append(failures, method+" reported a lock held in a session the provider never opened"+under)
				}
			}
		}
	}
	if offers(provider.TransactionsFeature) {
		request := &protocol.BeginTransactionRequest{SessionId: unknownSession, TransactionId: "ptah-conformance-unknown-session", Isolation: protocol.TransactionIsolation_DEFAULT}
		if _, err := invoke(ctx, r, c, "BeginTransaction", c.client.BeginTransaction, request); err == nil {
			failures = append(failures, "BeginTransaction began a transaction in a session the provider never opened"+under)
		}
	}
	return failures
}

// lockCall returns the client method of a lock RPC.
func lockCall(c *conn, method string) func(context.Context, *protocol.LockRequest, ...grpc.CallOption) (*protocol.LockResult, error) {
	switch method {
	case "TryLock":
		return c.client.TryLock
	case "CheckLock":
		return c.client.CheckLock
	default:
		return c.client.ReleaseLock
	}
}

// executeRequest is an execution request the negotiated description
// accepts: it names the engine when the host and the provider negotiated
// sensitive inputs, which makes the engine required.
func (r *run) executeRequest(c *conn, session, requestID string, units []*protocol.Unit) *protocol.ExecuteRequest {
	request := &protocol.ExecuteRequest{SessionId: session, RequestId: requestID, Units: cloneUnits(units)}
	if slices.Contains(c.description.GetFeatures(), provider.SensitiveInputsFeature) {
		request.Engine = r.engine
		if request.Engine == "" && len(c.description.GetEngines()) > 0 {
			request.Engine = c.description.GetEngines()[0].GetId()
		}
	}
	return request
}

func cloneUnits(units []*protocol.Unit) []*protocol.Unit {
	cloned := make([]*protocol.Unit, 0, len(units))
	for _, unit := range units {
		cloned = append(cloned, proto.CloneOf(unit))
	}
	return cloned
}
