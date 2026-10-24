package provider

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// The range of seconds a run-state time may take: years 1 through 9999,
// which every host and every engine can hold.
const (
	minEmbeddingSeconds = -62135596800
	maxEmbeddingSeconds = 253402300799
)

// validateEmbeddingTime accepts an absent time, which is the zero time, and a
// time inside the range a host can hold.
func validateEmbeddingTime(at *protocol.TimestampValue) error {
	if at.GetNanos() < 0 || at.GetNanos() >= 1e9 || at.GetSeconds() < minEmbeddingSeconds || at.GetSeconds() > maxEmbeddingSeconds {
		return fmt.Errorf("%w: invalid embedding state time", ErrProtocol)
	}
	return nil
}

func validateEmbeddingTimes(times ...*protocol.TimestampValue) error {
	for _, at := range times {
		if err := validateEmbeddingTime(at); err != nil {
			return err
		}
	}
	return nil
}

func validateEmbeddingGeneration(generation *protocol.EmbeddingGeneration) error {
	if !embeddingName(generation.GetIdentity()) {
		return fmt.Errorf("%w: a generation record needs an identity", ErrProtocol)
	}
	if !embeddingTexts(generation.GetSpecDigest(), generation.GetSpecDocument(), generation.GetName(), generation.GetReproducibility(),
		generation.GetReproducibilityReason(), generation.GetResolvedModel(), generation.GetTargetSchema(), generation.GetTargetTable(),
		generation.GetTargetColumn(), generation.GetSourceSchema(), generation.GetSourceTable(), generation.GetConsistencyMode()) {
		return fmt.Errorf("%w: a generation record holds a field that is not text", ErrProtocol)
	}
	return validateEmbeddingTimes(generation.GetCreatedAt(), generation.GetRetiredAt(), generation.GetVerifiedAt(), generation.GetMaintainedUntil())
}

func validateEmbeddingProgress(progress *protocol.EmbeddingProgress) error {
	for _, count := range []int64{progress.GetRowsScanned(), progress.GetRowsEmbedded(), progress.GetRowsSkipped(), progress.GetRowsDeleted(),
		progress.GetBatchesCommitted(), progress.GetProviderPromptTokens(), progress.GetProviderTotalTokens(), progress.GetProviderUsageBatches()} {
		if count < 0 {
			return fmt.Errorf("%w: a run record holds a negative count", ErrProtocol)
		}
	}
	return nil
}

func validateEmbeddingRun(run *protocol.EmbeddingRun) error {
	if !embeddingName(run.GetId()) || !embeddingName(run.GetGenerationIdentity()) {
		return fmt.Errorf("%w: a run record needs an ID and a generation", ErrProtocol)
	}
	if !embeddingTexts(run.GetSpecDigest(), run.GetEnvironment(), run.GetSource(), run.GetTarget(), run.GetProviderProfile(),
		run.GetResolvedModel(), run.GetPtahVersion(), run.GetPolicyDigest(), run.GetPhase(), run.GetStatus(), run.GetLeaseOwner(),
		run.GetSnapshotWatermark(), run.GetCatchUpWatermark(), run.GetVerificationRef(), run.GetCutoverPlanRef(), run.GetApprovalRef(),
		run.GetActivePointer(), run.GetFailureClass(), run.GetFailureDetail()) || !embeddingTexts(run.GetCursor()...) {
		return fmt.Errorf("%w: a run record holds a field that is not text", ErrProtocol)
	}
	if run.GetFencingToken() < 0 {
		return fmt.Errorf("%w: a run record holds a negative fencing token", ErrProtocol)
	}
	if err := validateEmbeddingProgress(run.GetProgress()); err != nil {
		return err
	}
	return validateEmbeddingTimes(run.GetLeaseExpires(), run.GetCreatedAt(), run.GetUpdatedAt())
}

func validateEmbeddingPointer(pointer *protocol.EmbeddingPointer) error {
	if !embeddingName(pointer.GetTargetTable()) || !embeddingName(pointer.GetActiveGeneration()) {
		return fmt.Errorf("%w: a pointer record needs a target table and an active generation", ErrProtocol)
	}
	if !embeddingTexts(pointer.GetTargetSchema(), pointer.GetPreviousGeneration(), pointer.GetCutOverBy(), pointer.GetPlanDigest()) {
		return fmt.Errorf("%w: a pointer record holds a field that is not text", ErrProtocol)
	}
	return validateEmbeddingTime(pointer.GetCutOverAt())
}

func comparePointers(a, b *protocol.EmbeddingPointer) int {
	return cmp.Or(cmp.Compare(a.GetTargetSchema(), b.GetTargetSchema()), cmp.Compare(a.GetTargetTable(), b.GetTargetTable()))
}

// validateEmbeddingRecords checks every record and requires each list in
// strict key order: generations by identity, runs by ID, pointers by schema
// and then table. Strict order also refuses a record listed twice.
func validateEmbeddingRecords(records *protocol.EmbeddingRecords) error {
	for i, generation := range records.GetGenerations() {
		if err := validateEmbeddingGeneration(generation); err != nil {
			return err
		}
		if i > 0 && records.GetGenerations()[i-1].GetIdentity() >= generation.GetIdentity() {
			return fmt.Errorf("%w: generation records are not in key order", ErrProtocol)
		}
	}
	for i, run := range records.GetRuns() {
		if err := validateEmbeddingRun(run); err != nil {
			return err
		}
		if i > 0 && records.GetRuns()[i-1].GetId() >= run.GetId() {
			return fmt.Errorf("%w: run records are not in key order", ErrProtocol)
		}
	}
	for i, pointer := range records.GetPointers() {
		if err := validateEmbeddingPointer(pointer); err != nil {
			return err
		}
		if i > 0 && comparePointers(records.GetPointers()[i-1], pointer) >= 0 {
			return fmt.Errorf("%w: pointer records are not in key order", ErrProtocol)
		}
	}
	return nil
}

// recordCount is the number of records a list of records holds.
func recordCount(records *protocol.EmbeddingRecords) int {
	return len(records.GetGenerations()) + len(records.GetRuns()) + len(records.GetPointers())
}

// validateEmbeddingScope requires exactly one selection, each naming what it
// selects.
func validateEmbeddingScope(scope *protocol.EmbeddingStateScope) error {
	switch selected := scope.GetScope().(type) {
	case *protocol.EmbeddingStateScope_Everything:
		if selected.Everything != nil {
			return nil
		}
	case *protocol.EmbeddingStateScope_Run:
		if embeddingName(selected.Run) {
			return nil
		}
	case *protocol.EmbeddingStateScope_Generation:
		if embeddingName(selected.Generation) {
			return nil
		}
	case *protocol.EmbeddingStateScope_RunsOfGeneration:
		if embeddingName(selected.RunsOfGeneration) {
			return nil
		}
	case *protocol.EmbeddingStateScope_Pointer:
		key := selected.Pointer
		if embeddingName(key.GetTargetTable()) && embeddingText(key.GetTargetSchema()) {
			return nil
		}
	}
	return fmt.Errorf("%w: an embedding state scope selects nothing it can name", ErrProtocol)
}

// validateWithinScope refuses a record the scope does not cover. A scope of
// one record admits at most that record.
func validateWithinScope(scope *protocol.EmbeddingStateScope, records *protocol.EmbeddingRecords) error {
	generations, runs, pointers := records.GetGenerations(), records.GetRuns(), records.GetPointers()
	covered := true
	switch selected := scope.GetScope().(type) {
	case *protocol.EmbeddingStateScope_Everything:
	case *protocol.EmbeddingStateScope_Run:
		covered = len(generations) == 0 && len(pointers) == 0 && len(runs) <= 1 &&
			!slices.ContainsFunc(runs, func(run *protocol.EmbeddingRun) bool { return run.GetId() != selected.Run })
	case *protocol.EmbeddingStateScope_Generation:
		covered = len(runs) == 0 && len(pointers) == 0 && len(generations) <= 1 &&
			!slices.ContainsFunc(generations, func(generation *protocol.EmbeddingGeneration) bool {
				return generation.GetIdentity() != selected.Generation
			})
	case *protocol.EmbeddingStateScope_RunsOfGeneration:
		covered = len(generations) == 0 && len(pointers) == 0 &&
			!slices.ContainsFunc(runs, func(run *protocol.EmbeddingRun) bool { return run.GetGenerationIdentity() != selected.RunsOfGeneration })
	case *protocol.EmbeddingStateScope_Pointer:
		key := selected.Pointer
		covered = len(generations) == 0 && len(runs) == 0 && len(pointers) <= 1 &&
			!slices.ContainsFunc(pointers, func(pointer *protocol.EmbeddingPointer) bool {
				return pointer.GetTargetSchema() != key.GetTargetSchema() || pointer.GetTargetTable() != key.GetTargetTable()
			})
	}
	if !covered {
		return fmt.Errorf("%w: an embedding record lies outside its scope", ErrProtocol)
	}
	return nil
}

// ValidateReadEmbeddingStateRequest checks the session, the scope and the
// bound on the records a read may return.
func ValidateReadEmbeddingStateRequest(request *protocol.ReadEmbeddingStateRequest) error {
	if err := validateEmbeddingSession(request.GetSessionId(), nil); err != nil {
		return err
	}
	if err := validateEmbeddingScope(request.GetScope()); err != nil {
		return err
	}
	if request.GetMaxRecords() == 0 || request.GetMaxRecords() > MaxEmbeddingRecords {
		return fmt.Errorf("%w: a state read requires a bound of at most %d records", ErrProtocol, MaxEmbeddingRecords)
	}
	return nil
}

// ValidateEmbeddingState binds a snapshot to its scope. It refuses a missing
// token, a record out of key order, repeated or outside the scope, more
// records than the bound, and records from a store that does not exist.
func ValidateEmbeddingState(request *protocol.ReadEmbeddingStateRequest, result *protocol.EmbeddingState) error {
	if err := ValidateReadEmbeddingStateRequest(request); err != nil {
		return err
	}
	if !proto.Equal(result.GetScope(), request.GetScope()) {
		return fmt.Errorf("%w: embedding state answers another scope", ErrProtocol)
	}
	if err := validateEmbeddingToken(result.GetToken()); err != nil {
		return err
	}
	records := result.GetRecords()
	if count := recordCount(records); count > int(request.GetMaxRecords()) || !result.GetExists() && count != 0 {
		return fmt.Errorf("%w: embedding state holds more records than its bound, or records of a missing store", ErrProtocol)
	}
	if err := validateEmbeddingRecords(records); err != nil {
		return err
	}
	return validateWithinScope(request.GetScope(), records)
}

// ValidateMutateEmbeddingStateRequest checks the session, the request ID, the
// scope, the expected token, and the records: at least one, each valid, in
// key order, and within the scope.
func ValidateMutateEmbeddingStateRequest(request *protocol.MutateEmbeddingStateRequest) error {
	requestID := request.GetRequestId()
	if err := validateEmbeddingSession(request.GetSessionId(), &requestID); err != nil {
		return err
	}
	if err := validateEmbeddingScope(request.GetScope()); err != nil {
		return err
	}
	if err := validateEmbeddingToken(request.GetExpectedToken()); err != nil {
		return err
	}
	if recordCount(request.GetRecords()) == 0 {
		return fmt.Errorf("%w: a state mutation writes no record", ErrProtocol)
	}
	if err := validateEmbeddingRecords(request.GetRecords()); err != nil {
		return err
	}
	return validateWithinScope(request.GetScope(), request.GetRecords())
}

// sameRecordKeys reports whether two lists hold records with the same keys in
// the same order.
func sameRecordKeys(a, b *protocol.EmbeddingRecords) bool {
	return slices.EqualFunc(a.GetGenerations(), b.GetGenerations(), func(x, y *protocol.EmbeddingGeneration) bool {
		return x.GetIdentity() == y.GetIdentity()
	}) && slices.EqualFunc(a.GetRuns(), b.GetRuns(), func(x, y *protocol.EmbeddingRun) bool {
		return x.GetId() == y.GetId()
	}) && slices.EqualFunc(a.GetPointers(), b.GetPointers(), func(x, y *protocol.EmbeddingPointer) bool {
		return comparePointers(x, y) == 0
	})
}

// validateWrittenRecords requires an APPLIED result's records to be valid and
// to answer the records the request wrote, key for key.
func validateWrittenRecords(result *protocol.EmbeddingWriteResult, wrote *protocol.EmbeddingRecords) error {
	if result.GetOutcome() != protocol.EmbeddingWriteResult_APPLIED {
		return nil
	}
	if err := validateEmbeddingRecords(result.GetWritten()); err != nil {
		return err
	}
	if !sameRecordKeys(result.GetWritten(), wrote) {
		return fmt.Errorf("%w: written records do not answer the records the request wrote", ErrProtocol)
	}
	return nil
}

// ValidateMutatedEmbeddingState checks the reply to a state mutation. An
// APPLIED reply carries each written record as a later read returns it, key
// for key with the request.
func ValidateMutatedEmbeddingState(request *protocol.MutateEmbeddingStateRequest, result *protocol.EmbeddingWriteResult) error {
	if err := ValidateMutateEmbeddingStateRequest(request); err != nil {
		return err
	}
	if err := validateEmbeddingWriteResult(request.GetRequestId(), writeShape{compares: true, payload: payloadRecords}, result); err != nil {
		return err
	}
	return validateWrittenRecords(result, request.GetRecords())
}

// ValidatePrepareEmbeddingStoreRequest checks the session and the request ID.
func ValidatePrepareEmbeddingStoreRequest(request *protocol.PrepareEmbeddingStoreRequest) error {
	requestID := request.GetRequestId()
	return validateEmbeddingSession(request.GetSessionId(), &requestID)
}

// ValidatePreparedEmbeddingStore checks the reply to a store preparation,
// which compares nothing and so cannot conflict.
func ValidatePreparedEmbeddingStore(request *protocol.PrepareEmbeddingStoreRequest, result *protocol.EmbeddingWriteResult) error {
	if err := ValidatePrepareEmbeddingStoreRequest(request); err != nil {
		return err
	}
	return validateEmbeddingWriteResult(request.GetRequestId(), writeShape{}, result)
}

// validateNewEmbeddingEvent checks an event the provider has not given a
// sequence yet.
func validateNewEmbeddingEvent(event *protocol.EmbeddingEvent) error {
	if event.GetSequence() != 0 {
		return fmt.Errorf("%w: the provider assigns an event's sequence", ErrProtocol)
	}
	return validateEmbeddingEvent(event)
}

// validateEmbeddingEvent checks an event's run, its text, its counts and its
// time.
func validateEmbeddingEvent(event *protocol.EmbeddingEvent) error {
	if !embeddingName(event.GetRunId()) {
		return fmt.Errorf("%w: an event needs a run", ErrProtocol)
	}
	if !embeddingTexts(event.GetKind(), event.GetActor(), event.GetFromPhase(), event.GetToPhase(), event.GetDetail()) {
		return fmt.Errorf("%w: an event holds a field that is not text", ErrProtocol)
	}
	counts := event.GetCounts()
	if event.GetFencingToken() < 0 || slices.ContainsFunc([]int64{counts.GetRowsScanned(), counts.GetRowsEmbedded(),
		counts.GetRowsSkipped(), counts.GetRowsDeleted(), counts.GetBatchesCommitted()}, func(count int64) bool { return count < 0 }) {
		return fmt.Errorf("%w: an event holds a negative count", ErrProtocol)
	}
	return validateEmbeddingTime(event.GetAt())
}

// ValidateAppendEmbeddingEventRequest checks the session, the request ID and
// an event that has no sequence yet.
func ValidateAppendEmbeddingEventRequest(request *protocol.AppendEmbeddingEventRequest) error {
	requestID := request.GetRequestId()
	if err := validateEmbeddingSession(request.GetSessionId(), &requestID); err != nil {
		return err
	}
	return validateNewEmbeddingEvent(request.GetEvent())
}

// ValidateAppendedEmbeddingEvent checks the reply to an append. An append
// compares nothing, so it cannot conflict; an APPLIED reply carries the
// sequence the event took, and a run that is not there is REFUSED as ABSENT.
func ValidateAppendedEmbeddingEvent(request *protocol.AppendEmbeddingEventRequest, result *protocol.EmbeddingWriteResult) error {
	if err := ValidateAppendEmbeddingEventRequest(request); err != nil {
		return err
	}
	if err := validateEmbeddingWriteResult(request.GetRequestId(), writeShape{payload: payloadSequence}, result); err != nil {
		return err
	}
	if result.GetSequence() < 0 {
		return fmt.Errorf("%w: an event sequence is negative", ErrProtocol)
	}
	return nil
}

// ValidateReadEmbeddingEventsRequest checks the session, the run, the
// position and the page bound.
func ValidateReadEmbeddingEventsRequest(request *protocol.ReadEmbeddingEventsRequest) error {
	if err := validateEmbeddingSession(request.GetSessionId(), nil); err != nil {
		return err
	}
	if !embeddingName(request.GetRunId()) || request.GetAfterSequence() < 0 {
		return fmt.Errorf("%w: an event read needs a run and a position", ErrProtocol)
	}
	if request.GetMaxEvents() == 0 || request.GetMaxEvents() > MaxEmbeddingEvents {
		return fmt.Errorf("%w: an event read requires a bound of at most %d events", ErrProtocol, MaxEmbeddingEvents)
	}
	return nil
}

// ValidateEmbeddingEventPage binds a page to its run. It refuses events of a
// run that is not there, more events than the bound, an event of another run,
// and sequences that do not rise past the requested position.
func ValidateEmbeddingEventPage(request *protocol.ReadEmbeddingEventsRequest, result *protocol.EmbeddingEventPage) error {
	if err := ValidateReadEmbeddingEventsRequest(request); err != nil {
		return err
	}
	if result.GetRunId() != request.GetRunId() {
		return fmt.Errorf("%w: an event page answers another run", ErrProtocol)
	}
	events := result.GetEvents()
	if !result.GetExists() && (len(events) != 0 || !result.GetComplete()) || len(events) > int(request.GetMaxEvents()) {
		return fmt.Errorf("%w: an event page holds more events than its bound, or events of a missing run", ErrProtocol)
	}
	// Each sequence rises past the one before it, from a position that is not
	// negative, so a stored event's sequence starts at 1.
	last := request.GetAfterSequence()
	for _, event := range events {
		if err := validateEmbeddingEvent(event); err != nil {
			return err
		}
		if event.GetRunId() != request.GetRunId() || event.GetSequence() <= last {
			return fmt.Errorf("%w: an event page holds another run's event or a sequence out of order", ErrProtocol)
		}
		last = event.GetSequence()
	}
	return nil
}

// EmbeddingStateToken returns a token over messages: the hexadecimal SHA-256
// of their deterministic encodings, each with its presence and its length. A
// provider may use it for EmbeddingState.token and EmbeddingBatchState.token,
// computing it over the same records in the same order when it reads and when
// it compares. An absent message differs from an empty one.
func EmbeddingStateToken(messages ...proto.Message) (string, error) {
	digest := sha256.New()
	encoder := proto.MarshalOptions{Deterministic: true}
	for _, message := range messages {
		if message == nil || !message.ProtoReflect().IsValid() {
			digest.Write([]byte{0})
			continue
		}
		encoded, err := encoder.Marshal(message)
		if err != nil {
			return "", fmt.Errorf("encode embedding state: %w", err)
		}
		digest.Write([]byte{1})
		digest.Write(binary.BigEndian.AppendUint64(nil, uint64(len(encoded))))
		digest.Write(encoded)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
