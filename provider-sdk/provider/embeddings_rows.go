package provider

import (
	"fmt"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// validateEmbeddingSourceRow checks a row against its source: one key
// component per key field, one null or text value per input field, and a
// version only when the source names a version field.
func validateEmbeddingSourceRow(row *protocol.EmbeddingSourceRow, source *protocol.EmbeddingSource) error {
	if err := validateEmbeddingKey(row.GetKey(), len(source.GetKeyFields())); err != nil {
		return err
	}
	if err := validateEmbeddingFields(row.GetFields(), len(source.GetInputFields())); err != nil {
		return err
	}
	return validateEmbeddingVersion(row.GetVersion(), source)
}

// validateEmbeddingVersion refuses a version that is not text, and one from
// a source that names no version field.
func validateEmbeddingVersion(version string, source *protocol.EmbeddingSource) error {
	if !embeddingText(version) || version != "" && source.GetVersionField() == "" {
		return fmt.Errorf("%w: a version from a source without a version field", ErrProtocol)
	}
	return nil
}

// validateEmbeddingRowBound requires a page bound of at most
// MaxEmbeddingRows.
func validateEmbeddingRowBound(bound uint32) error {
	if bound == 0 || bound > MaxEmbeddingRows {
		return fmt.Errorf("%w: an embedding page requires a bound of at most %d rows", ErrProtocol, MaxEmbeddingRows)
	}
	return nil
}

// ValidateScanEmbeddingSourceRequest checks the session, the source, the key
// to resume after, and the page bound.
func ValidateScanEmbeddingSourceRequest(request *protocol.ScanEmbeddingSourceRequest) error {
	if err := validateEmbeddingSession(request.GetSessionId(), nil); err != nil {
		return err
	}
	if err := validateEmbeddingSource(request.GetSource()); err != nil {
		return err
	}
	if after := request.GetAfter(); after != nil {
		if err := validateEmbeddingKey(after, len(request.GetSource().GetKeyFields())); err != nil {
			return err
		}
	}
	return validateEmbeddingRowBound(request.GetMaxRows())
}

// ValidateEmbeddingSourcePage refuses more rows than the bound, a row that
// does not fit its source, a key listed twice, and a page that is not done
// and does not say where the next one starts.
func ValidateEmbeddingSourcePage(request *protocol.ScanEmbeddingSourceRequest, result *protocol.EmbeddingSourcePage) error {
	if err := ValidateScanEmbeddingSourceRequest(request); err != nil {
		return err
	}
	if len(result.GetRows()) > int(request.GetMaxRows()) {
		return fmt.Errorf("%w: a source page holds more rows than its bound", ErrProtocol)
	}
	source := request.GetSource()
	seen := make(map[string]bool, len(result.GetRows()))
	for _, row := range result.GetRows() {
		if err := validateEmbeddingSourceRow(row, source); err != nil {
			return err
		}
		if seen[embeddingKeyID(row.GetKey())] {
			return fmt.Errorf("%w: a source page lists a key twice", ErrProtocol)
		}
		seen[embeddingKeyID(row.GetKey())] = true
	}
	cursor := result.GetCursor()
	if cursor == nil && !result.GetDone() {
		return fmt.Errorf("%w: a source page that is not done names no cursor", ErrProtocol)
	}
	if cursor != nil {
		return validateEmbeddingKey(cursor, len(source.GetKeyFields()))
	}
	return nil
}

// ValidateReadEmbeddingSourceRowsRequest checks the session, the source and
// between one and MaxEmbeddingRows keys, none repeated.
func ValidateReadEmbeddingSourceRowsRequest(request *protocol.ReadEmbeddingSourceRowsRequest) error {
	if err := validateEmbeddingSession(request.GetSessionId(), nil); err != nil {
		return err
	}
	if err := validateEmbeddingSource(request.GetSource()); err != nil {
		return err
	}
	if len(request.GetKeys()) == 0 {
		return fmt.Errorf("%w: a source reread names no key", ErrProtocol)
	}
	_, err := validateEmbeddingKeys(request.GetKeys(), len(request.GetSource().GetKeyFields()))
	return err
}

// ValidateEmbeddingSourceRows refuses a row whose key the request did not
// name, a key answered twice, and a row that does not fit its source.
func ValidateEmbeddingSourceRows(request *protocol.ReadEmbeddingSourceRowsRequest, result *protocol.EmbeddingSourceRows) error {
	if err := ValidateReadEmbeddingSourceRowsRequest(request); err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("%w: reread source rows are missing", ErrProtocol)
	}
	requested, _ := validateEmbeddingKeys(request.GetKeys(), len(request.GetSource().GetKeyFields()))
	for _, row := range result.GetRows() {
		if err := validateEmbeddingSourceRow(row, request.GetSource()); err != nil {
			return err
		}
		if !requested[embeddingKeyID(row.GetKey())] {
			return fmt.Errorf("%w: a reread row is not one the request named, or is answered twice", ErrProtocol)
		}
		delete(requested, embeddingKeyID(row.GetKey()))
	}
	return nil
}

// validateEmbeddingWriteKind requires a kind that names a write.
func validateEmbeddingWriteKind(kind protocol.EmbeddingWriteKind) error {
	if _, known := protocol.EmbeddingWriteKind_name[int32(kind)]; !known || kind == protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UNSPECIFIED {
		return fmt.Errorf("%w: an embedding write needs a known kind", ErrProtocol)
	}
	return nil
}

// validateBatchScope checks what both halves of a batch commit carry: the
// session, the specification, the run and the keys.
func validateBatchScope(sessionID string, spec *protocol.EmbeddingSpec, runID string, keys []*protocol.EmbeddingKey) (map[string]bool, error) {
	if err := validateEmbeddingSession(sessionID, nil); err != nil {
		return nil, err
	}
	if err := validateEmbeddingSpec(spec); err != nil {
		return nil, err
	}
	if !embeddingName(runID) {
		return nil, fmt.Errorf("%w: a batch names no run", ErrProtocol)
	}
	return validateEmbeddingKeys(keys, len(spec.GetSource().GetKeyFields()))
}

// ValidateReadEmbeddingBatchStateRequest checks the session, the
// specification, the run and the keys the batch writes.
func ValidateReadEmbeddingBatchStateRequest(request *protocol.ReadEmbeddingBatchStateRequest) error {
	_, err := validateBatchScope(request.GetSessionId(), request.GetSpec(), request.GetRunId(), request.GetKeys())
	return err
}

// ValidateEmbeddingBatchState binds a batch state to its run and keys. It
// refuses another run, a held key the request did not name or one held
// twice, a held state without a generation or a kind, and a missing token.
func ValidateEmbeddingBatchState(request *protocol.ReadEmbeddingBatchStateRequest, result *protocol.EmbeddingBatchState) error {
	requested, err := validateBatchScope(request.GetSessionId(), request.GetSpec(), request.GetRunId(), request.GetKeys())
	if err != nil {
		return err
	}
	if result.GetRunId() != request.GetRunId() {
		return fmt.Errorf("%w: a batch state answers another run", ErrProtocol)
	}
	if run := result.GetRun(); run != nil {
		if err := validateEmbeddingRun(run); err != nil {
			return err
		}
		if run.GetId() != request.GetRunId() {
			return fmt.Errorf("%w: a batch state holds another run", ErrProtocol)
		}
	}
	for _, held := range result.GetHeld() {
		if err := validateEmbeddingKey(held.GetKey(), len(request.GetSpec().GetSource().GetKeyFields())); err != nil {
			return err
		}
		if !requested[embeddingKeyID(held.GetKey())] {
			return fmt.Errorf("%w: a held state is not one the request named, or is answered twice", ErrProtocol)
		}
		delete(requested, embeddingKeyID(held.GetKey()))
		if !embeddingName(held.GetGeneration()) || !embeddingTexts(held.GetInputHash(), held.GetVersion()) {
			return fmt.Errorf("%w: a held state needs a generation", ErrProtocol)
		}
		if err := validateEmbeddingWriteKind(held.GetKind()); err != nil {
			return err
		}
	}
	return validateEmbeddingToken(result.GetToken())
}

// validateEmbeddingSet checks the writes of one key: adjacent, in ordinal
// order from 0, sharing one input hash, version and kind; a skip or a
// tombstone alone at ordinal 0; and one member when the specification does
// not chunk. Each write already carries the request's generation.
func validateEmbeddingSet(set []*protocol.EmbeddingTargetWrite, spec *protocol.EmbeddingSpec) error {
	first := set[0]
	for ordinal, write := range set {
		if write.GetOrdinal() != uint32(ordinal) { // #nosec G115 -- a set is bounded by MaxEmbeddingRows
			return fmt.Errorf("%w: a key's writes are not adjacent and in ordinal order from 0", ErrProtocol)
		}
		if write.GetInputHash() != first.GetInputHash() || write.GetVersion() != first.GetVersion() || write.GetKind() != first.GetKind() {
			return fmt.Errorf("%w: the members of a set differ in input hash, version or kind", ErrProtocol)
		}
	}
	if len(set) > 1 && (!spec.GetChunked() || first.GetKind() != protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT) {
		return fmt.Errorf("%w: a set of more than one is a chunked set of vectors", ErrProtocol)
	}
	return nil
}

// validateEmbeddingTargetWrite checks one write against the specification:
// a key the batch read, the request's generation, a known kind, text, and a
// vector of the target's dimension for an UPSERT and none otherwise.
func validateEmbeddingTargetWrite(write *protocol.EmbeddingTargetWrite, spec *protocol.EmbeddingSpec, keys map[string]bool) error {
	if err := validateEmbeddingKey(write.GetKey(), len(spec.GetSource().GetKeyFields())); err != nil {
		return err
	}
	if !keys[embeddingKeyID(write.GetKey())] {
		return fmt.Errorf("%w: a target write names a key the batch state was not read over", ErrProtocol)
	}
	if write.GetGeneration() != spec.GetGeneration() {
		return fmt.Errorf("%w: a target write belongs to another generation", ErrProtocol)
	}
	if err := validateEmbeddingWriteKind(write.GetKind()); err != nil {
		return err
	}
	if !embeddingTexts(write.GetInputHash(), write.GetVersion()) {
		return fmt.Errorf("%w: a target write holds a field that is not text", ErrProtocol)
	}
	if write.GetKind() != protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT {
		if len(write.GetVector()) != 0 {
			return fmt.Errorf("%w: only an upsert carries a vector", ErrProtocol)
		}
		return nil
	}
	return validateEmbeddingVector(write.GetVector(), spec.GetTarget().GetDimension())
}

// ValidateCommitEmbeddingBatchRequest checks the session, the request ID, the
// specification, the keys and token the batch state was read over, a run of
// the request's run ID and generation, and each key's set of writes.
func ValidateCommitEmbeddingBatchRequest(request *protocol.CommitEmbeddingBatchRequest) error {
	keys, err := validateBatchScope(request.GetSessionId(), request.GetSpec(), request.GetRunId(), request.GetKeys())
	if err != nil {
		return err
	}
	if request.GetRequestId() == "" {
		return fmt.Errorf("%w: an embedding write requires a request ID", ErrProtocol)
	}
	if err := validateEmbeddingToken(request.GetExpectedToken()); err != nil {
		return err
	}
	run := request.GetRun()
	if err := validateEmbeddingRun(run); err != nil {
		return err
	}
	if run.GetId() != request.GetRunId() || run.GetGenerationIdentity() != request.GetSpec().GetGeneration() {
		return fmt.Errorf("%w: a batch commit carries another run or another generation's run", ErrProtocol)
	}
	writes := request.GetWrites()
	finished := make(map[string]bool, len(keys))
	for start := 0; start < len(writes); {
		if err := validateEmbeddingTargetWrite(writes[start], request.GetSpec(), keys); err != nil {
			return err
		}
		key := embeddingKeyID(writes[start].GetKey())
		if finished[key] {
			return fmt.Errorf("%w: a key's writes are not adjacent", ErrProtocol)
		}
		end := start + 1
		for end < len(writes) && writes[end] != nil && embeddingKeyID(writes[end].GetKey()) == key {
			if err := validateEmbeddingTargetWrite(writes[end], request.GetSpec(), keys); err != nil {
				return err
			}
			end++
		}
		if err := validateEmbeddingSet(writes[start:end], request.GetSpec()); err != nil {
			return err
		}
		finished[key] = true
		start = end
	}
	return nil
}

// ValidateCommittedEmbeddingBatch checks the reply to a batch commit. An
// APPLIED reply carries the run as a later read returns it, and nothing
// else.
func ValidateCommittedEmbeddingBatch(request *protocol.CommitEmbeddingBatchRequest, result *protocol.EmbeddingWriteResult) error {
	if err := ValidateCommitEmbeddingBatchRequest(request); err != nil {
		return err
	}
	if err := validateEmbeddingWriteResult(request.GetRequestId(), writeShape{compares: true, payload: payloadRecords}, result); err != nil {
		return err
	}
	return validateWrittenRecords(result, &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{request.GetRun()}})
}

// ValidateReadEmbeddingVerificationRequest checks the session, the
// specification, the page token and the page bound.
func ValidateReadEmbeddingVerificationRequest(request *protocol.ReadEmbeddingVerificationRequest) error {
	if err := validateEmbeddingSession(request.GetSessionId(), nil); err != nil {
		return err
	}
	if err := validateEmbeddingSpec(request.GetSpec()); err != nil {
		return err
	}
	if len(request.GetPageToken()) > maxPageToken || !utf8.ValidString(request.GetPageToken()) {
		return fmt.Errorf("%w: invalid page token", ErrProtocol)
	}
	return validateEmbeddingRowBound(request.GetMaxRows())
}

// validateEmbeddingStoredRow checks a stored row's text and kind. It refuses
// no state of the data itself: what a row holds is what verification exists
// to report.
func validateEmbeddingStoredRow(stored *protocol.EmbeddingStoredRow) error {
	if !embeddingTexts(stored.GetGeneration(), stored.GetInputHash(), stored.GetVersion()) {
		return fmt.Errorf("%w: a stored row holds a field that is not text", ErrProtocol)
	}
	if _, known := protocol.EmbeddingWriteKind_name[int32(stored.GetKind())]; !known {
		return fmt.Errorf("%w: a stored row holds an unknown kind", ErrProtocol)
	}
	return nil
}

// validateEmbeddingVerificationRow checks one position of the walk: its key,
// at least one side, and each side against the specification.
func validateEmbeddingVerificationRow(row *protocol.EmbeddingVerificationRow, spec *protocol.EmbeddingSpec) error {
	if row.GetSource() == nil && row.GetStored() == nil {
		return fmt.Errorf("%w: a verification row has neither a source nor a stored side", ErrProtocol)
	}
	source := spec.GetSource()
	if err := validateEmbeddingKey(row.GetKey(), len(source.GetKeyFields())); err != nil {
		return err
	}
	if side := row.GetSource(); side != nil {
		if err := validateEmbeddingFields(side.GetFields(), len(source.GetInputFields())); err != nil {
			return err
		}
		if err := validateEmbeddingVersion(side.GetVersion(), source); err != nil {
			return err
		}
	}
	if stored := row.GetStored(); stored != nil {
		return validateEmbeddingStoredRow(stored)
	}
	return nil
}

// ValidateEmbeddingVerificationPage refuses more rows than the bound, a
// completion that disagrees with the next page token, a row that does not
// fit the specification, and a key whose rows are not adjacent.
func ValidateEmbeddingVerificationPage(request *protocol.ReadEmbeddingVerificationRequest, result *protocol.EmbeddingVerificationPage) error {
	if err := ValidateReadEmbeddingVerificationRequest(request); err != nil {
		return err
	}
	if len(result.GetRows()) > int(request.GetMaxRows()) {
		return fmt.Errorf("%w: a verification page holds more rows than its bound", ErrProtocol)
	}
	if result.GetComplete() != (result.GetNextPageToken() == "") ||
		len(result.GetNextPageToken()) > maxPageToken || !utf8.ValidString(result.GetNextPageToken()) {
		return fmt.Errorf("%w: verification page completion disagrees with its next page token", ErrProtocol)
	}
	finished := make(map[string]bool)
	previous := ""
	for i, row := range result.GetRows() {
		if err := validateEmbeddingVerificationRow(row, request.GetSpec()); err != nil {
			return err
		}
		key := embeddingKeyID(row.GetKey())
		if i > 0 && key != previous {
			finished[previous] = true
		}
		if finished[key] {
			return fmt.Errorf("%w: the rows of a key are not adjacent", ErrProtocol)
		}
		previous = key
	}
	return nil
}

// ValidateSearchEmbeddingsRequest checks the session, the specification, a
// vector of the target's dimension with finite values, and the depth.
func ValidateSearchEmbeddingsRequest(request *protocol.SearchEmbeddingsRequest) error {
	if err := validateEmbeddingSession(request.GetSessionId(), nil); err != nil {
		return err
	}
	if err := validateEmbeddingSpec(request.GetSpec()); err != nil {
		return err
	}
	if err := validateEmbeddingVector(request.GetVector(), request.GetSpec().GetTarget().GetDimension()); err != nil {
		return err
	}
	if request.GetDepth() == 0 || request.GetDepth() > MaxEmbeddingSearchDepth {
		return fmt.Errorf("%w: a search depth is between 1 and %d", ErrProtocol, MaxEmbeddingSearchDepth)
	}
	return nil
}

// validateSearchKeys requires at most depth keys of the source's width, none
// repeated.
func validateSearchKeys(keys []*protocol.EmbeddingKey, request *protocol.SearchEmbeddingsRequest) error {
	if len(keys) > int(request.GetDepth()) {
		return fmt.Errorf("%w: a search returned more keys than its depth", ErrProtocol)
	}
	_, err := validateEmbeddingKeys(keys, len(request.GetSpec().GetSource().GetKeyFields()))
	return err
}

// ValidateEmbeddingSearchResult accepts a completed search, whose lists hold
// at most depth keys each and whose first list an index served only when the
// specification declares one, or a failed search with a reason and no key.
func ValidateEmbeddingSearchResult(request *protocol.SearchEmbeddingsRequest, result *protocol.EmbeddingSearchResult) error {
	if err := ValidateSearchEmbeddingsRequest(request); err != nil {
		return err
	}
	switch result.GetOutcome() {
	case protocol.EmbeddingSearchResult_COMPLETED:
		if result.GetMessage() != "" || result.GetIndexUsed() && request.GetSpec().GetTarget().GetIndexMethod() == "" {
			return fmt.Errorf("%w: a completed search carries a reason, or an index the specification does not declare", ErrProtocol)
		}
		if err := validateSearchKeys(result.GetIndexed(), request); err != nil {
			return err
		}
		return validateSearchKeys(result.GetExact(), request)
	case protocol.EmbeddingSearchResult_FAILED:
		if !diagnosticLine(result.GetMessage()) || len(result.GetIndexed()) != 0 || len(result.GetExact()) != 0 || result.GetIndexUsed() {
			return fmt.Errorf("%w: a failed search needs one line of reason and carries no key", ErrProtocol)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown search outcome", ErrProtocol)
	}
}
