package provider

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// EmbeddingStoreFeature advertises the embedding store of contract 25.2. The
// host keeps the lifecycle of an embedding generation: the loops, the batch
// bounds, leases and fencing, every lifecycle rule and verification. The
// provider keeps the run state the host decides, in a layout of its own, and
// reads and writes a generation's storage, rows and vectors.
//
// The service adds Engine.embeddings to an existing message, so it is a
// two-way feature: the provider lists it in its features and in each
// supporting engine's, the host lists it in its host features, and the SDK
// drops the declaration and refuses every call unless both do.
const EmbeddingStoreFeature = "embeddings.store"

const (
	// MaxEmbeddingRecords bounds the records one run-state read returns. A
	// larger scope is an error, never a partial snapshot.
	MaxEmbeddingRecords = 10000
	// MaxEmbeddingRows bounds the rows of one source, verification or
	// relation page and the keys of one batch.
	MaxEmbeddingRows = 10000
	// MaxEmbeddingEvents bounds the events of one page.
	MaxEmbeddingEvents = 10000
	// MaxEmbeddingSearchDepth bounds the keys one search returns per list.
	MaxEmbeddingSearchDepth = 10000
	// MaxEmbeddingDimension bounds the elements of one vector.
	MaxEmbeddingDimension = 65536
)

// ErrEmbeddingConflict identifies a compare-and-write whose state changed
// after it was read. Nothing was written; the host reads again and decides
// again.
var ErrEmbeddingConflict = errors.New("embedding state changed after it was read")

// EmbeddingStore is the embedding store of one provider.
//
// Every session call acts on a session the host opened. MutateEmbeddingState,
// CommitEmbeddingBatch and AppendEmbeddingEvent are each one atomic
// operation, and APPLIED means committed: no call joins a transaction of the
// host's. The owner may run such an operation again after its engine aborted
// it in a way that proves nothing committed, a bounded number of times, and
// rereads what it compares on each attempt. A CONFLICT goes back to the host.
// A case in which the owner cannot prove whether it committed is UNKNOWN.
//
// The owner stores records whole and compares them whole. It refuses nothing
// for a lifecycle reason; phase, status and kind are text it stores without
// reading. It creates, alters and drops only objects whose names a request
// carries, and drops storage only when it can prove the storage belongs to the
// generation the request names. Diagnostics name objects and a reason, and
// carry no row content, vector, filter or credential.
type EmbeddingStore interface {
	PrepareEmbeddingStore(context.Context, *protocol.PrepareEmbeddingStoreRequest) (*protocol.EmbeddingWriteResult, error)
	ReadEmbeddingState(context.Context, *protocol.ReadEmbeddingStateRequest) (*protocol.EmbeddingState, error)
	MutateEmbeddingState(context.Context, *protocol.MutateEmbeddingStateRequest) (*protocol.EmbeddingWriteResult, error)
	AppendEmbeddingEvent(context.Context, *protocol.AppendEmbeddingEventRequest) (*protocol.EmbeddingWriteResult, error)
	ReadEmbeddingEvents(context.Context, *protocol.ReadEmbeddingEventsRequest) (*protocol.EmbeddingEventPage, error)
	DescribeEmbeddingTarget(context.Context, *protocol.DescribeEmbeddingTargetRequest) (*protocol.EmbeddingTargetObjects, error)
	ResolveEmbeddingRelations(context.Context, *protocol.ResolveEmbeddingRelationsRequest) (*protocol.EmbeddingRelations, error)
	MeasureEmbedding(context.Context, *protocol.MeasureEmbeddingRequest) (*protocol.EmbeddingMeasurement, error)
	PrepareEmbeddingTarget(context.Context, *protocol.PrepareEmbeddingTargetRequest) (*protocol.EmbeddingWriteResult, error)
	BuildEmbeddingIndex(context.Context, *protocol.BuildEmbeddingIndexRequest) (*protocol.EmbeddingWriteResult, error)
	ReadEmbeddingStructure(context.Context, *protocol.ReadEmbeddingStructureRequest) (*protocol.EmbeddingStructure, error)
	DropEmbeddingGeneration(context.Context, *protocol.DropEmbeddingGenerationRequest) (*protocol.EmbeddingWriteResult, error)
	ScanEmbeddingSource(context.Context, *protocol.ScanEmbeddingSourceRequest) (*protocol.EmbeddingSourcePage, error)
	ReadEmbeddingSourceRows(context.Context, *protocol.ReadEmbeddingSourceRowsRequest) (*protocol.EmbeddingSourceRows, error)
	ReadEmbeddingBatchState(context.Context, *protocol.ReadEmbeddingBatchStateRequest) (*protocol.EmbeddingBatchState, error)
	CommitEmbeddingBatch(context.Context, *protocol.CommitEmbeddingBatchRequest) (*protocol.EmbeddingWriteResult, error)
	ReadEmbeddingVerification(context.Context, *protocol.ReadEmbeddingVerificationRequest) (*protocol.EmbeddingVerificationPage, error)
	SearchEmbeddings(context.Context, *protocol.SearchEmbeddingsRequest) (*protocol.EmbeddingSearchResult, error)
}

// validateEmbeddingEngine checks that an engine lists embeddings.store only
// when the provider serves it, and declares an embedding store exactly when
// it lists the feature: one or more consistency modes, each a name, none
// repeated.
func validateEmbeddingEngine(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), EmbeddingStoreFeature)
	if listed && !slices.Contains(description.GetFeatures(), EmbeddingStoreFeature) {
		return fmt.Errorf("%w: engine %s requires the service", ErrProtocol, EmbeddingStoreFeature)
	}
	declaration := engine.GetEmbeddings()
	if listed != (declaration != nil) {
		return fmt.Errorf("%w: an engine declares an embedding store exactly when it lists %s", ErrProtocol, EmbeddingStoreFeature)
	}
	if declaration == nil {
		return nil
	}
	modes := declaration.GetConsistencyModes()
	if len(modes) == 0 {
		return fmt.Errorf("%w: an embedding store declares no consistency mode", ErrProtocol)
	}
	for i, mode := range modes {
		if !validName(mode) || slices.Contains(modes[:i], mode) {
			return fmt.Errorf("%w: an invalid or repeated consistency mode", ErrProtocol)
		}
	}
	return nil
}

// embeddingEngine checks the negotiated service and the engine an offline
// request names.
func embeddingEngine(description *protocol.Description, name string) error {
	if !slices.Contains(description.GetFeatures(), EmbeddingStoreFeature) {
		return fmt.Errorf("%w: %s was not negotiated", ErrProtocol, EmbeddingStoreFeature)
	}
	for _, engine := range description.GetEngines() {
		if engine.GetId() == name && slices.Contains(engine.GetFeatures(), EmbeddingStoreFeature) {
			return nil
		}
	}
	return fmt.Errorf("%w: engine %q keeps no embedding store", ErrProtocol, name)
}

// embeddingText reports text a field may hold: UTF-8 without NUL.
func embeddingText(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

// embeddingName reports a name a request may carry: non-empty text.
func embeddingName(value string) bool {
	return value != "" && embeddingText(value)
}

// embeddingTexts reports whether every value is text a field may hold.
func embeddingTexts(values ...string) bool {
	return !slices.ContainsFunc(values, func(value string) bool { return !embeddingText(value) })
}

// validateEmbeddingSession refuses a request without a session and, for a
// write, without a request ID.
func validateEmbeddingSession(sessionID string, requestID *string) error {
	if sessionID == "" {
		return fmt.Errorf("%w: an embedding call requires a session", ErrProtocol)
	}
	if requestID != nil && *requestID == "" {
		return fmt.Errorf("%w: an embedding write requires a request ID", ErrProtocol)
	}
	return nil
}

// validateEmbeddingToken refuses an empty token and one too long or not text.
func validateEmbeddingToken(token string) error {
	if token == "" || len(token) > maxPageToken || !embeddingText(token) {
		return fmt.Errorf("%w: invalid embedding state token", ErrProtocol)
	}
	return nil
}

// validateEmbeddingTable requires a table reference with a name and text
// components. A relation travels as a reference, never as SQL text.
func validateEmbeddingTable(table *protocol.Reference) error {
	if err := validateRowTable(table); err != nil {
		return err
	}
	if !embeddingName(table.GetName()) || !embeddingTexts(table.GetSchema(), table.GetDatabase(), table.GetCatalog()) {
		return fmt.Errorf("%w: an embedding table reference is not text", ErrProtocol)
	}
	return nil
}

// validateEmbeddingKey requires one text component per key field.
func validateEmbeddingKey(key *protocol.EmbeddingKey, width int) error {
	if len(key.GetComponents()) != width || !embeddingTexts(key.GetComponents()...) {
		return fmt.Errorf("%w: an embedding key does not have one text component per key field", ErrProtocol)
	}
	return nil
}

// embeddingKeyID renders a key for a set lookup. The NUL separator cannot
// occur in a component, which validateEmbeddingKey refuses.
func embeddingKeyID(key *protocol.EmbeddingKey) string {
	return strings.Join(key.GetComponents(), "\x00")
}

// validateEmbeddingKeys requires keys of one width, none repeated, at most
// MaxEmbeddingRows of them, and returns them as a set.
func validateEmbeddingKeys(keys []*protocol.EmbeddingKey, width int) (map[string]bool, error) {
	if len(keys) > MaxEmbeddingRows {
		return nil, fmt.Errorf("%w: more than %d embedding keys", ErrProtocol, MaxEmbeddingRows)
	}
	set := make(map[string]bool, len(keys))
	for _, key := range keys {
		if err := validateEmbeddingKey(key, width); err != nil {
			return nil, err
		}
		if set[embeddingKeyID(key)] {
			return nil, fmt.Errorf("%w: an embedding key is repeated", ErrProtocol)
		}
		set[embeddingKeyID(key)] = true
	}
	return set, nil
}

// validateEmbeddingVector requires the dimension's number of finite values. A
// NaN or an infinity is refused, because verification relies on the writer
// to keep them out of a corpus.
func validateEmbeddingVector(vector []float32, dimension uint32) error {
	if len(vector) != int(dimension) {
		return fmt.Errorf("%w: a vector of %d values for dimension %d", ErrProtocol, len(vector), dimension)
	}
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("%w: a vector holds a value that is not finite", ErrProtocol)
		}
	}
	return nil
}

// validateEmbeddingFields requires one value per input field, each null or
// text.
func validateEmbeddingFields(fields []*protocol.DataValue, width int) error {
	if len(fields) != width {
		return fmt.Errorf("%w: a row does not carry one value per input field", ErrProtocol)
	}
	for _, field := range fields {
		switch value := field.GetValue().(type) {
		case *protocol.DataValue_Null:
		case *protocol.DataValue_StringValue:
			if !utf8.ValidString(value.StringValue) {
				return fmt.Errorf("%w: an input field is not UTF-8", ErrProtocol)
			}
		default:
			return fmt.Errorf("%w: an input field is neither null nor text", ErrProtocol)
		}
	}
	return nil
}

// writePayload is what an APPLIED write result carries besides its outcome.
type writePayload int

const (
	payloadNone writePayload = iota
	payloadRecords
	payloadSequence
	payloadIndex
)

// writeShape is what one write call can answer: whether it compares state,
// and so can conflict, and the payload it returns with APPLIED.
type writeShape struct {
	compares bool
	payload  writePayload
}

// validateEmbeddingWriteResult checks the request binding and the shape of an
// outcome: a known outcome the call can answer, a refusal exactly with
// REFUSED, one line of message where one is set or required, and a payload
// only with APPLIED and only of the kind the call returns.
func validateEmbeddingWriteResult(requestID string, shape writeShape, result *protocol.EmbeddingWriteResult) error {
	// A missing result has no request ID, and requestID is never empty.
	if result.GetRequestId() != requestID {
		return fmt.Errorf("%w: embedding write result changed its request binding", ErrProtocol)
	}
	outcome, payload := result.GetOutcome(), shape.payload
	if _, known := protocol.EmbeddingWriteResult_Outcome_name[int32(outcome)]; !known ||
		outcome == protocol.EmbeddingWriteResult_CONFLICT && !shape.compares {
		return fmt.Errorf("%w: an embedding write outcome this call cannot answer", ErrProtocol)
	}
	refused := outcome == protocol.EmbeddingWriteResult_REFUSED
	if refused != (result.GetRefusal() != nil) {
		return fmt.Errorf("%w: an embedding refusal comes with REFUSED and only then", ErrProtocol)
	}
	if refused {
		if err := validateEmbeddingRefusal(result.GetRefusal()); err != nil {
			return err
		}
	}
	message := result.GetMessage()
	if (message != "" || outcome == protocol.EmbeddingWriteResult_NOT_APPLIED) && !diagnosticLine(message) {
		return fmt.Errorf("%w: an embedding write message is one line of text", ErrProtocol)
	}
	applied := outcome == protocol.EmbeddingWriteResult_APPLIED
	if (result.GetWritten() != nil) != (applied && payload == payloadRecords) ||
		(result.GetSequence() != 0) != (applied && payload == payloadSequence) ||
		(result.GetIndexOutcome() != protocol.EmbeddingWriteResult_INDEX_OUTCOME_UNSPECIFIED) != (applied && payload == payloadIndex) {
		return fmt.Errorf("%w: an embedding write result carries a payload its outcome cannot carry", ErrProtocol)
	}
	return nil
}

// validateEmbeddingRefusal requires a known reason and one line of text.
func validateEmbeddingRefusal(refusal *protocol.EmbeddingRefusal) error {
	reason := refusal.GetReason()
	if _, known := protocol.EmbeddingRefusal_Reason_name[int32(reason)]; !known ||
		reason == protocol.EmbeddingRefusal_REASON_UNSPECIFIED || !diagnosticLine(refusal.GetMessage()) {
		return fmt.Errorf("%w: an embedding refusal needs a reason and one line of text", ErrProtocol)
	}
	return nil
}

// EmbeddingWriteError records an embedding write that did not apply. UNKNOWN
// wraps ErrOutcomeUnknown; CONFLICT, REFUSED and NOT_APPLIED wrap
// ErrExecution and confirm that nothing was written, and a CONFLICT also
// matches ErrEmbeddingConflict. Reason is the refusal's, for REFUSED.
// Provider-authored text is not included.
type EmbeddingWriteError struct {
	Outcome protocol.EmbeddingWriteResult_Outcome
	Reason  protocol.EmbeddingRefusal_Reason
}

// Error describes the outcome without provider diagnostics.
func (e *EmbeddingWriteError) Error() string {
	if e.Outcome == protocol.EmbeddingWriteResult_REFUSED {
		return fmt.Sprintf("provider embedding write was refused: %s", e.Reason)
	}
	return fmt.Sprintf("provider embedding write outcome is %s", e.Outcome)
}

// Is reports a CONFLICT as ErrEmbeddingConflict.
func (e *EmbeddingWriteError) Is(target error) bool {
	return e.Outcome == protocol.EmbeddingWriteResult_CONFLICT && target == ErrEmbeddingConflict
}

// Unwrap distinguishes an uncertain write from a confirmed one.
func (e *EmbeddingWriteError) Unwrap() error {
	if e.Outcome == protocol.EmbeddingWriteResult_UNKNOWN {
		return ErrOutcomeUnknown
	}
	return ErrExecution
}

// CheckEmbeddingWrite accepts only APPLIED. It is called with a result that
// passed its validator.
func CheckEmbeddingWrite(result *protocol.EmbeddingWriteResult) error {
	if result.GetOutcome() == protocol.EmbeddingWriteResult_APPLIED {
		return nil
	}
	return &EmbeddingWriteError{Outcome: result.GetOutcome(), Reason: result.GetRefusal().GetReason()}
}
