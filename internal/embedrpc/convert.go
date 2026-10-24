package embedrpc

import (
	"fmt"
	"maps"
	"strings"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// The conversions between the host's run state and the records the service
// carries. Each is lossless both ways, so a record read, converted, decided
// on and converted back is equal to the one read when the decision did not
// change it, and only the records a decision changed are written.

// timestamp is a time as the service carries it: absent for the zero time,
// and UTC seconds and nanoseconds otherwise.
func timestamp(at time.Time) *protocol.TimestampValue {
	if at.IsZero() {
		return nil
	}
	return &protocol.TimestampValue{Seconds: at.Unix(), Nanos: int32(at.Nanosecond())} // #nosec G115 -- a nanosecond of a second
}

// instant reads a time back, the zero time for an absent one.
func instant(at *protocol.TimestampValue) time.Time {
	if at == nil {
		return time.Time{}
	}
	return time.Unix(at.GetSeconds(), int64(at.GetNanos())).UTC()
}

// dimension is a vector width as the service carries it. A width that does
// not fit is refused by the SDK's bound long before it would wrap.
func dimension(width int) uint32 {
	return uint32(max(width, 0)) // #nosec G115 -- bounded by provider.MaxEmbeddingDimension
}

func generationRecord(generation embedstore.Generation) *protocol.EmbeddingGeneration {
	return &protocol.EmbeddingGeneration{
		Identity: generation.Identity, SpecDigest: generation.SpecDigest, SpecDocument: generation.SpecDocument,
		Name: generation.Name, Reproducibility: generation.Reproducibility, ReproducibilityReason: generation.ReproducibilityReason,
		ResolvedModel: generation.ResolvedModel, Dimension: dimension(generation.Dimension),
		TargetSchema: generation.TargetSchema, TargetTable: generation.TargetTable, TargetColumn: generation.TargetColumn,
		SourceSchema: generation.SourceSchema, SourceTable: generation.SourceTable, ConsistencyMode: generation.ConsistencyMode,
		CreatedAt: timestamp(generation.CreatedAt), RetiredAt: timestamp(generation.RetiredAt),
		VerifiedAt: timestamp(generation.VerifiedAt), MaintainedUntil: timestamp(generation.MaintainedUntil),
	}
}

func generationOf(record *protocol.EmbeddingGeneration) embedstore.Generation {
	return embedstore.Generation{
		Identity: record.GetIdentity(), SpecDigest: record.GetSpecDigest(), SpecDocument: record.GetSpecDocument(),
		Name: record.GetName(), Reproducibility: record.GetReproducibility(), ReproducibilityReason: record.GetReproducibilityReason(),
		ResolvedModel: record.GetResolvedModel(), Dimension: int(record.GetDimension()),
		TargetSchema: record.GetTargetSchema(), TargetTable: record.GetTargetTable(), TargetColumn: record.GetTargetColumn(),
		SourceSchema: record.GetSourceSchema(), SourceTable: record.GetSourceTable(), ConsistencyMode: record.GetConsistencyMode(),
		CreatedAt: instant(record.GetCreatedAt()), RetiredAt: instant(record.GetRetiredAt()),
		VerifiedAt: instant(record.GetVerifiedAt()), MaintainedUntil: instant(record.GetMaintainedUntil()),
	}
}

func runRecord(run embedrun.Run) *protocol.EmbeddingRun {
	progress := run.Progress
	return &protocol.EmbeddingRun{
		Id: run.ID, SpecDigest: run.SpecDigest, GenerationIdentity: run.GenerationIdentity, Environment: run.Environment,
		Source: run.Source, Target: run.Target, ProviderProfile: run.ProviderProfile, ResolvedModel: run.ResolvedModel,
		PtahVersion: run.PtahVersion, PolicyDigest: run.PolicyDigest, Phase: string(run.Phase), Status: string(run.Status),
		LeaseOwner: run.LeaseOwner, LeaseExpires: timestamp(run.LeaseExpires), FencingToken: run.FencingToken,
		SnapshotDone: run.SnapshotDone, SnapshotWatermark: run.SnapshotWatermark, CatchUpWatermark: run.CatchUpWatermark,
		Cursor: run.Cursor,
		Progress: &protocol.EmbeddingProgress{
			RowsScanned: progress.RowsScanned, RowsEmbedded: progress.RowsEmbedded, RowsSkipped: progress.RowsSkipped,
			RowsDeleted: progress.RowsDeleted, BatchesCommitted: progress.BatchesCommitted,
			ProviderPromptTokens: progress.ProviderPromptTokens, ProviderTotalTokens: progress.ProviderTotalTokens,
			ProviderUsageBatches: progress.ProviderUsageBatches, RetryCount: uint32(max(progress.RetryCount, 0)), // #nosec G115 -- retries of one batch
		},
		VerificationRef: run.VerificationRef, CutoverPlanRef: run.CutoverPlanRef, ApprovalRef: run.ApprovalRef,
		ActivePointer: run.ActivePointer, RollbackEligible: run.RollbackEligible,
		FailureClass: run.FailureClass, FailureDetail: run.FailureDetail,
		CreatedAt: timestamp(run.CreatedAt), UpdatedAt: timestamp(run.UpdatedAt),
	}
}

func runOf(record *protocol.EmbeddingRun) embedrun.Run {
	progress := record.GetProgress()
	return embedrun.Run{
		ID: record.GetId(), SpecDigest: record.GetSpecDigest(), GenerationIdentity: record.GetGenerationIdentity(),
		Environment: record.GetEnvironment(), Source: record.GetSource(), Target: record.GetTarget(),
		ProviderProfile: record.GetProviderProfile(), ResolvedModel: record.GetResolvedModel(),
		PtahVersion: record.GetPtahVersion(), PolicyDigest: record.GetPolicyDigest(),
		Phase: embedrun.Phase(record.GetPhase()), Status: embedrun.Status(record.GetStatus()),
		LeaseOwner: record.GetLeaseOwner(), LeaseExpires: instant(record.GetLeaseExpires()), FencingToken: record.GetFencingToken(),
		SnapshotDone: record.GetSnapshotDone(), SnapshotWatermark: record.GetSnapshotWatermark(),
		CatchUpWatermark: record.GetCatchUpWatermark(), Cursor: record.GetCursor(),
		Progress: embedrun.Progress{
			RowsScanned: progress.GetRowsScanned(), RowsEmbedded: progress.GetRowsEmbedded(), RowsSkipped: progress.GetRowsSkipped(),
			RowsDeleted: progress.GetRowsDeleted(), BatchesCommitted: progress.GetBatchesCommitted(),
			ProviderPromptTokens: progress.GetProviderPromptTokens(), ProviderTotalTokens: progress.GetProviderTotalTokens(),
			ProviderUsageBatches: progress.GetProviderUsageBatches(), RetryCount: int(progress.GetRetryCount()),
		},
		VerificationRef: record.GetVerificationRef(), CutoverPlanRef: record.GetCutoverPlanRef(), ApprovalRef: record.GetApprovalRef(),
		ActivePointer: record.GetActivePointer(), RollbackEligible: record.GetRollbackEligible(),
		FailureClass: record.GetFailureClass(), FailureDetail: record.GetFailureDetail(),
		CreatedAt: instant(record.GetCreatedAt()), UpdatedAt: instant(record.GetUpdatedAt()),
	}
}

func pointerRecord(pointer embedstore.Pointer) *protocol.EmbeddingPointer {
	return &protocol.EmbeddingPointer{
		TargetSchema: pointer.TargetSchema, TargetTable: pointer.TargetTable, ActiveGeneration: pointer.Active,
		PreviousGeneration: pointer.Previous, CutOverAt: timestamp(pointer.CutOverAt), CutOverBy: pointer.CutOverBy,
		PlanDigest: pointer.PlanDigest,
	}
}

func pointerOf(record *protocol.EmbeddingPointer) embedstore.Pointer {
	return embedstore.Pointer{
		TargetSchema: record.GetTargetSchema(), TargetTable: record.GetTargetTable(), Active: record.GetActiveGeneration(),
		Previous: record.GetPreviousGeneration(), CutOverAt: instant(record.GetCutOverAt()), CutOverBy: record.GetCutOverBy(),
		PlanDigest: record.GetPlanDigest(),
	}
}

// eventRecord is an event as the service carries it. An event keeps the five
// counts of the audit trail, which are what embedstore.EventFields records on
// every engine; the token counts belong to the run.
func eventRecord(event embedrun.Event) *protocol.EmbeddingEvent {
	return &protocol.EmbeddingEvent{
		RunId: event.RunID, Kind: string(event.Kind), At: timestamp(event.At), Actor: event.Actor,
		FencingToken: event.FencingToken, FromPhase: string(event.FromPhase), ToPhase: string(event.ToPhase), Detail: event.Detail,
		Counts: &protocol.EmbeddingEventCounts{
			RowsScanned: event.Counts.RowsScanned, RowsEmbedded: event.Counts.RowsEmbedded, RowsSkipped: event.Counts.RowsSkipped,
			RowsDeleted: event.Counts.RowsDeleted, BatchesCommitted: event.Counts.BatchesCommitted,
		},
	}
}

func eventOf(record *protocol.EmbeddingEvent) embedrun.Event {
	counts := record.GetCounts()
	return embedrun.Event{
		RunID: record.GetRunId(), Kind: embedrun.EventKind(record.GetKind()), At: instant(record.GetAt()), Actor: record.GetActor(),
		FencingToken: record.GetFencingToken(), FromPhase: embedrun.Phase(record.GetFromPhase()), ToPhase: embedrun.Phase(record.GetToPhase()),
		Detail: record.GetDetail(),
		Counts: embedrun.Progress{
			RowsScanned: counts.GetRowsScanned(), RowsEmbedded: counts.GetRowsEmbedded(), RowsSkipped: counts.GetRowsSkipped(),
			RowsDeleted: counts.GetRowsDeleted(), BatchesCommitted: counts.GetBatchesCommitted(),
		},
	}
}

// snapshotOf is the host's view of a set of records.
func snapshotOf(records *protocol.EmbeddingRecords) embedstore.Snapshot {
	var snapshot embedstore.Snapshot
	for _, record := range records.GetGenerations() {
		snapshot.Generations = append(snapshot.Generations, generationOf(record))
	}
	for _, record := range records.GetRuns() {
		snapshot.Runs = append(snapshot.Runs, runOf(record))
	}
	for _, record := range records.GetPointers() {
		snapshot.Pointers = append(snapshot.Pointers, pointerOf(record))
	}
	return snapshot
}

// table is a relation as the service names it. A relation travels as its
// components, never as SQL text.
func table(schema, name string) *protocol.Reference {
	return &protocol.Reference{Kind: "table", Schema: strings.TrimSpace(schema), Name: strings.TrimSpace(name)}
}

var metrics = map[embedgen.DistanceMetric]protocol.EmbeddingTarget_Metric{
	embedgen.MetricCosine:       protocol.EmbeddingTarget_COSINE,
	embedgen.MetricL2:           protocol.EmbeddingTarget_L2,
	embedgen.MetricInnerProduct: protocol.EmbeddingTarget_INNER_PRODUCT,
}

// layoutOf is a specification's layout: a table of the generation's own, or
// columns on the target, which is the default.
func layoutOf(spec embedgen.Spec) protocol.EmbeddingTarget_Layout {
	if spec.Target.Layout.OwnsTable() {
		return protocol.EmbeddingTarget_OWN_TABLE
	}
	return protocol.EmbeddingTarget_SOURCE_COLUMNS
}

// names are the names of a generation's objects, from the one derivation the
// host uses on every engine.
func names(spec embedgen.Spec) *protocol.EmbeddingNames {
	identity := spec.Identity()
	return &protocol.EmbeddingNames{
		Index:         embedgen.IndexName(spec.Target.Table, spec.Target.Column, identity.Digest),
		OwnershipMark: embedgen.TableComment(identity.Digest),
		ColumnComment: spec.ColumnComment(),
	}
}

// specRecord is what the engine needs of a specification. It refuses a
// layout or a metric the service has no word for, before anything is sent.
func specRecord(spec embedgen.Spec) (*protocol.EmbeddingSpec, error) {
	if !embedgen.KnownLayout(spec.Target.Layout) {
		return nil, fmt.Errorf("target objects: %q is not a target layout", string(spec.Target.Layout))
	}
	metric, known := metrics[spec.Target.Metric]
	if !known {
		return nil, fmt.Errorf("target objects: %q is not a distance metric", string(spec.Target.Metric))
	}
	if spec.Model.ReportedDimension <= 0 {
		return nil, fmt.Errorf("target objects: the model reports no output dimension yet; run the provider test first")
	}
	return &protocol.EmbeddingSpec{
		Generation: spec.Identity().Digest,
		Source: &protocol.EmbeddingSource{
			Table: table(spec.Source.Schema, spec.Source.Table), KeyFields: spec.Source.KeyFields,
			InputFields: spec.Source.InputFields, VersionField: strings.TrimSpace(spec.Source.VersionField),
			Filter: strings.TrimSpace(spec.Source.Filter),
		},
		Target: &protocol.EmbeddingTarget{
			Table: table(spec.Target.Schema, spec.Target.Table), Column: spec.Target.Column, Layout: layoutOf(spec),
			Representation: strings.TrimSpace(spec.Target.Representation), Metric: metric,
			Dimension: dimension(spec.Model.ReportedDimension), IndexMethod: strings.TrimSpace(spec.Target.IndexMethod),
			IndexOptions: maps.Clone(spec.Target.IndexOptions),
		},
		Chunked: spec.Chunks(),
		Names:   names(spec),
	}, nil
}

// locationOf is where a registered generation's vectors are, from its record
// alone, because a retirement has no specification for the generation it
// destroys.
func locationOf(generation embedstore.Generation) *protocol.EmbeddingLocation {
	return &protocol.EmbeddingLocation{
		Generation: generation.Identity, Table: table(generation.TargetSchema, generation.TargetTable),
		Column: generation.TargetColumn, Dimension: dimension(generation.Dimension),
		Names: &protocol.EmbeddingNames{
			Index:         embedgen.IndexName(generation.TargetTable, generation.TargetColumn, generation.Identity),
			OwnershipMark: embedgen.TableComment(generation.Identity),
		},
	}
}

// specLocation is where a specification's generation is.
func specLocation(spec embedgen.Spec, record *protocol.EmbeddingSpec) *protocol.EmbeddingLocation {
	return &protocol.EmbeddingLocation{
		Generation: record.GetGeneration(), Table: record.GetTarget().GetTable(), Column: spec.Target.Column,
		Dimension: record.GetTarget().GetDimension(), Names: record.GetNames(),
	}
}

var writeKinds = map[embedrun.WriteKind]protocol.EmbeddingWriteKind{
	embedrun.WriteUpsert:    protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT,
	embedrun.WriteSkip:      protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_SKIP,
	embedrun.WriteTombstone: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_TOMBSTONE,
}

// writeKindOf reads a kind back; one the service does not name is no kind.
func writeKindOf(kind protocol.EmbeddingWriteKind) embedrun.WriteKind {
	for host, wire := range writeKinds {
		if wire == kind {
			return host
		}
	}
	return ""
}

func key(components []string) *protocol.EmbeddingKey {
	return &protocol.EmbeddingKey{Components: components}
}
