package conformance

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// rpc is one RPC the suite sends a caller's request to, with the validators
// the SDK's client runs on its request and its reply.
type rpc struct {
	// request is the full name of the request message.
	request protoreflect.FullName
	method  string
	feature string
	call    func(ctx context.Context, r *run, c *conn, request proto.Message) (proto.Message, error)
	// validRequest and validReply validate with the description both sides
	// negotiated.
	validRequest func(*protocol.Description, proto.Message) error
	validReply   func(*protocol.Description, proto.Message, proto.Message) error
}

// described is the rpc of a client method and its validators.
func described[Q, R proto.Message](method, feature string, client func(protocol.ProviderClient, context.Context, Q, ...grpc.CallOption) (R, error),
	request func(*protocol.Description, Q) error, reply func(*protocol.Description, Q, R) error,
) rpc {
	var zero Q
	return rpc{
		request: zero.ProtoReflect().Descriptor().FullName(), method: method, feature: feature,
		call: func(ctx context.Context, r *run, c *conn, message proto.Message) (proto.Message, error) {
			typed, _ := message.(Q)
			bound := func(ctx context.Context, q Q, options ...grpc.CallOption) (R, error) {
				return client(c.client, ctx, q, options...)
			}
			result, err := invoke(ctx, r, c, method, bound, typed)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		validRequest: func(description *protocol.Description, message proto.Message) error {
			typed, _ := message.(Q)
			return request(description, typed)
		},
		validReply: func(description *protocol.Description, message, result proto.Message) error {
			typed, _ := message.(Q)
			answer, _ := result.(R)
			return reply(description, typed, answer)
		},
	}
}

// alone adapts a request validator that needs no description.
func alone[Q any](validate func(Q) error) func(*protocol.Description, Q) error {
	return func(_ *protocol.Description, request Q) error { return validate(request) }
}

// aloneReply adapts a reply validator that needs no description.
func aloneReply[Q, R any](validate func(Q, R) error) func(*protocol.Description, Q, R) error {
	return func(_ *protocol.Description, request Q, result R) error { return validate(request, result) }
}

// engineOwned is the request check of AnalyzeSQL, which has no validator of
// its own: the engine must be one the description names.
func engineOwned(description *protocol.Description, request *protocol.AnalyzeSQLRequest) error {
	for _, engine := range description.GetEngines() {
		if engine.GetId() == request.GetEngine() {
			return nil
		}
	}
	return fmt.Errorf("%w: engine is not owned by this provider", provider.ErrProtocol)
}

// offlineRPCs are the offline services a Call may address.
func offlineRPCs() []rpc {
	return []rpc{
		described("Render", provider.RenderFeature, protocol.ProviderClient.Render, provider.ValidateRenderRequest, provider.ValidateRenderedBatchReply),
		described("AnalyzeSQL", provider.AnalyzeSQLFeature, protocol.ProviderClient.AnalyzeSQL, engineOwned, aloneReply(provider.ValidateSQLAnalysis)),
		described("DecodeSource", provider.DecodeSourceFeature, protocol.ProviderClient.DecodeSource, provider.ValidateSourceRequest, provider.ValidateDecodedSourceReply),
		described("ReadSQLSchema", provider.ReadSQLSchemaFeature, protocol.ProviderClient.ReadSQLSchema, provider.ValidateReadSQLSchemaRequest, provider.ValidateSQLSchemaReply),
		described("NormalizeValues", provider.NormalizeValuesFeature, protocol.ProviderClient.NormalizeValues, provider.ValidateValueInputs, provider.ValidateNormalizedValues),
		described("ConvertValues", provider.ConvertValuesFeature, protocol.ProviderClient.ConvertValues, provider.ValidateConversionRequest, provider.ValidateConvertedValuesReply),
		described("CompareModels", provider.CompareModelsFeature, protocol.ProviderClient.CompareModels, provider.ValidateComparisonRequest, provider.ValidateComparedModelsReply),
		described("PlanFeatures", provider.PlanFeaturesFeature, protocol.ProviderClient.PlanFeatures, provider.ValidatePlanningRequest, provider.ValidatePlannedFeaturesReply),
		described("PlanDeclarations", provider.SchemaDeclarationsFeature, protocol.ProviderClient.PlanDeclarations, provider.ValidateDeclarationsRequest, provider.ValidatePlannedDeclarationsReply),
		described("CompareColumns", provider.ColumnsFeature, protocol.ProviderClient.CompareColumns, provider.ValidateColumnsRequest, provider.ValidateComparedColumnsReply),
		described("ReadColumnDefaults", provider.ColumnsFeature, protocol.ProviderClient.ReadColumnDefaults, provider.ValidateColumnDefaultsRequest, provider.ValidateColumnDefaultsReply),
		described("DescribeExecutionInputs", provider.SensitiveInputsFeature, protocol.ProviderClient.DescribeExecutionInputs, provider.ValidateExecutionInputsRequest,
			provider.ValidateExecutionInputsReply),
		described("PrepareTables", provider.PrepareTablesFeature, protocol.ProviderClient.PrepareTables, provider.ValidatePreparationRequest, provider.ValidatePreparedTablesReply),
		described("ProjectTableCreations", provider.ProjectTableCreationsFeature, protocol.ProviderClient.ProjectTableCreations, provider.ValidateCreationRequest,
			provider.ValidateProjectedCreationsReply),
		described("ValidateSchema", provider.ValidateSchemaFeature, protocol.ProviderClient.ValidateSchema, provider.ValidateSchemaRequest, provider.ValidateSchemaResultReply),
		described("SplitLintStatements", provider.LintFeature, protocol.ProviderClient.SplitLintStatements, provider.ValidateSplitLintRequest, provider.ValidateSplitLintReply),
		described("AnnotateLintStatements", provider.LintFeature, protocol.ProviderClient.AnnotateLintStatements, provider.ValidateAnnotateLintRequest, provider.ValidateAnnotatedLintReply),
		described("CheckLintMigrations", provider.LintFeature, protocol.ProviderClient.CheckLintMigrations, provider.ValidateCheckLintRequest, provider.ValidateCheckedLintReply),
		described("GuardReplay", provider.ReplayGuardFeature, protocol.ProviderClient.GuardReplay, provider.ValidateGuardReplayRequest, provider.ValidateGuardedReplayReply),
		described("TypeRows", provider.TypeRowsFeature, protocol.ProviderClient.TypeRows, provider.ValidateTypeRowsRequest, provider.ValidateTypedRowsReply),
		described("RenderRowWrites", provider.RenderRowWritesFeature, protocol.ProviderClient.RenderRowWrites, provider.ValidateRenderRowWritesRequest, provider.ValidateRenderedRowWritesReply),
		described("DescribeEmbeddingTarget", provider.EmbeddingStoreFeature, protocol.ProviderClient.DescribeEmbeddingTarget, provider.ValidateDescribeEmbeddingTargetRequest,
			provider.ValidateEmbeddingTargetObjectsReply),
		described("PrepareCommonObjects", provider.CommonObjectsFeature, protocol.ProviderClient.PrepareCommonObjects, provider.ValidatePrepareCommonObjectsRequest,
			provider.ValidatePreparedCommonObjectsReply),
		described("EncodeSource", provider.EncodeSourceFeature, protocol.ProviderClient.EncodeSource, provider.ValidateEncodeSourceRequest, provider.ValidateEncodedSourceReply),
		described("SplitMigration", provider.MigrationSplitFeature, protocol.ProviderClient.SplitMigration, provider.ValidateSplitMigrationRequest, provider.ValidateMigrationSplitReply),
		described("ReverseChanges", provider.ReverseChangesFeature, protocol.ProviderClient.ReverseChanges, provider.ValidateReverseChangesRequest, provider.ValidateReversedChangesReply),
		described("ReportValues", provider.ReportCountsFeature, protocol.ProviderClient.ReportValues, provider.ValidateReportValuesRequest, provider.ValidateValueReportsReply),
		described("ScanSQL", provider.ScanSQLFeature, protocol.ProviderClient.ScanSQL, provider.ValidateScanSQLRequest, provider.ValidateSQLScanReply),
		described("ResolveDatabaseContext", provider.DatabaseContextFeature, protocol.ProviderClient.ResolveDatabaseContext, provider.ValidateDatabaseContextRequest,
			provider.ValidateResolvedDatabaseContextReply),
		described("CompareChecks", provider.CheckEquivalenceFeature, protocol.ProviderClient.CompareChecks, provider.ValidateCompareChecksRequest,
			provider.ValidateComparedChecksReply),
	}
}

// liveRPCs are the session-bound services a LiveCall may address: the reads,
// which the suite can send without leaving state behind.
func liveRPCs() []rpc {
	return []rpc{
		described("ReadRows", provider.ReadRowsFeature, protocol.ProviderClient.ReadRows, alone(provider.ValidateReadRowsRequest), aloneReply(provider.ValidateRowPageReply)),
		described("ListSeedTables", provider.SeedsFeature, protocol.ProviderClient.ListSeedTables, alone(provider.ValidateListSeedTablesRequest), aloneReply(provider.ValidateSeedTablesReply)),
		described("ReadSeedLedger", provider.SeedsFeature, protocol.ProviderClient.ReadSeedLedger, alone(provider.ValidateReadSeedLedgerRequest), aloneReply(provider.ValidateSeedLedgerReply)),
		described("ReadHistory", provider.HistoryFeature, protocol.ProviderClient.ReadHistory, alone(provider.ValidateReadHistoryRequest), aloneReply(provider.ValidateHistorySnapshotReply)),
		described("ListDevObjects", provider.DevCleanupFeature, protocol.ProviderClient.ListDevObjects, alone(provider.ValidateListDevObjectsRequest), aloneReply(provider.ValidateDevObjectsReply)),
		described("RunStatement", provider.StatementsFeature, protocol.ProviderClient.RunStatement, statementRequest, aloneReply(provider.ValidateStatementResultReply)),
	}
}

// statementRequest is the request check of RunStatement: the request alone,
// then its typed arguments against the description.
func statementRequest(description *protocol.Description, request *protocol.RunStatementRequest) error {
	if err := provider.ValidateRunStatementRequest(request); err != nil {
		return err
	}
	return provider.ValidateTypedArguments(description, request)
}

func rpcFor(table []rpc, request proto.Message) *rpc {
	if request == nil {
		return nil
	}
	name := request.ProtoReflect().Descriptor().FullName()
	for i := range table {
		if table[i].request == name {
			return &table[i]
		}
	}
	return nil
}

func offlineRPCFor(request proto.Message) *rpc { return rpcFor(offlineRPCs(), request) }

// calls sends each offline call under both profiles.
func (r *run) calls(ctx context.Context) {
	if len(r.config.Calls) == 0 {
		r.add(checkCalls, NotApplicable, "the configuration has no offline calls")
		return
	}
	for _, call := range r.config.Calls {
		r.result(checkCalls+"."+call.Name, r.offlineCall(ctx, call))
	}
}

// offlineCall checks one call. Under a host that lists every two-way
// feature it requires the expected outcome, a reply the SDK's validator
// accepts, and the same reply from a second call and from a second process.
// Under a host that lists none it requires a refusal of a request that uses
// an addition, and otherwise a reply that uses none.
func (r *run) offlineCall(ctx context.Context, call Call) []string {
	entry := offlineRPCFor(call.Request)
	if !slices.Contains(r.all.description.GetFeatures(), entry.feature) {
		return []string{fmt.Sprintf("the call needs %s, which the description does not offer", entry.feature)}
	}
	if err := entry.validRequest(r.all.description, call.Request); err != nil {
		return []string{"the request is not one the SDK's client sends: " + err.Error()}
	}
	failures := r.answered(ctx, entry, call.Request, call.Refused, r.all, r.second)
	if r.none != nil {
		failures = append(failures, r.narrowed(ctx, entry, call.Request, call.Refused, r.none)...)
	}
	return append(failures, r.notedFor(checkCalls+"."+call.Name)...)
}

// answered sends request to c twice and to other once, and checks the
// expected outcome, the reply and that the replies are equal.
func (r *run) answered(ctx context.Context, entry *rpc, request proto.Message, refused bool, c, other *conn) []string {
	first, err := entry.call(ctx, r, c, proto.CloneOf(request))
	switch {
	case refused && err == nil:
		return []string{entry.method + " answered a request it must refuse"}
	case refused && !deliberate(err):
		return []string{entry.method + " failed where it must refuse: " + errorText(err)}
	case refused:
		return nil
	case err != nil:
		return []string{entry.method + " refused the request: " + errorText(err)}
	}
	if err := entry.validReply(c.description, request, first); err != nil {
		return []string{"the reply fails the SDK's validator: " + err.Error()}
	}
	var failures []string
	targets := []*conn{c}
	if other != nil {
		targets = append(targets, other)
	}
	for i, target := range targets {
		again, err := entry.call(ctx, r, target, proto.CloneOf(request))
		where := "a second call"
		if i > 0 {
			where = "a call to a second process"
		}
		switch {
		case err != nil:
			failures = append(failures, fmt.Sprintf("%s refused the request: %s", where, errorText(err)))
		case !proto.Equal(again, first):
			failures = append(failures, where+" returned another reply to the same request")
		}
	}
	return failures
}

// narrowed sends request under a host that lists no two-way feature.
func (r *run) narrowed(ctx context.Context, entry *rpc, request proto.Message, refused bool, c *conn) []string {
	offered := slices.Contains(c.description.GetFeatures(), entry.feature)
	valid := offered && entry.validRequest(c.description, request) == nil
	reply, err := entry.call(ctx, r, c, proto.CloneOf(request))
	under := " under " + c.profile.name
	switch {
	case err != nil && !deliberate(err):
		return []string{fmt.Sprintf("%s failed%s: %s", entry.method, under, errorText(err))}
	case err != nil:
		return nil
	case !offered:
		return []string{fmt.Sprintf("%s answered%s, which did not negotiate %s", entry.method, under, entry.feature)}
	case !valid:
		return []string{fmt.Sprintf("%s answered%s a request that uses an addition the host did not list", entry.method, under)}
	case refused:
		return []string{entry.method + " answered" + under + " a request it must refuse"}
	}
	if err := entry.validReply(c.description, request, reply); err != nil {
		return []string{fmt.Sprintf("the reply%s uses what the host did not negotiate: %v", under, err)}
	}
	return nil
}
