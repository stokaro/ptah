package provider

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// HistoryFeature advertises typed revision storage. Each supporting engine
// also declares it; storage support does not claim distributed locking.
const HistoryFeature = "sessions.history"

// MaxHistoryRevisions bounds a complete read. Larger stores are refused,
// never truncated into a snapshot that looks like full history.
const MaxHistoryRevisions = 10000

// History owns storage on live sessions, not ordering, checksum, retry, or
// repair policy. Reads are consistent snapshots. Mutations atomically compare
// the complete expected row; a conflict changes nothing. Cancellation and lost
// acknowledgments never permit replay. The transaction is the same handle
// used by execution, including visibility of its uncommitted changes.
//
// Operations serialize with finish and close. Closed or foreign handles are
// refused. Preparation creates missing storage outside transactions and checks
// ownership and layout before adopting it. It never upgrades an older layout.
// Implementations refuse triggers or other behavior that could change the
// requested write, and must not silently select a different store.
type History interface {
	ReadHistory(context.Context, *protocol.ReadHistoryRequest) (*protocol.HistorySnapshot, error)
	PrepareHistory(context.Context, *protocol.PrepareHistoryRequest) (*protocol.HistoryResult, error)
	MutateHistory(context.Context, *protocol.MutateHistoryRequest) (*protocol.HistoryResult, error)
}

// ValidateHistoryScope checks the explicit session, table, and representation.
// Identifier spelling is preserved; the owner controls quoting and resolution.
func ValidateHistoryScope(scope *protocol.HistoryScope) error {
	if scope.GetSessionId() == "" || strings.TrimSpace(scope.GetTable()) == "" || strings.ContainsRune(scope.GetTable(), 0) || strings.ContainsRune(scope.GetSchema(), 0) {
		return fmt.Errorf("%w: history requires a session and valid store identifiers", ErrProtocol)
	}
	if scope.GetFormat() != protocol.HistoryScope_NATIVE && scope.GetFormat() != protocol.HistoryScope_ATLAS {
		return fmt.Errorf("%w: unknown history format", ErrProtocol)
	}
	return nil
}

// ValidateReadHistoryRequest refuses unbounded reads before dispatch.
func ValidateReadHistoryRequest(request *protocol.ReadHistoryRequest) error {
	if err := ValidateHistoryScope(request.GetScope()); err != nil {
		return err
	}
	if request.GetMaxRevisions() == 0 || request.GetMaxRevisions() > MaxHistoryRevisions {
		return fmt.Errorf("%w: history read requires a bounded revision limit", ErrProtocol)
	}
	return nil
}

// ValidatePrepareHistoryRequest refuses preparation within a transaction.
func ValidatePrepareHistoryRequest(request *protocol.PrepareHistoryRequest) error {
	if err := ValidateHistoryScope(request.GetScope()); err != nil {
		return err
	}
	if request.GetRequestId() == "" || request.GetScope().GetTransactionId() != "" {
		return fmt.Errorf("%w: history preparation requires a request ID and no transaction", ErrProtocol)
	}
	return nil
}

// ValidateHistoryRevision checks representable facts without deciding whether
// the host may resume, repair, or remove this revision.
func ValidateHistoryRevision(format protocol.HistoryScope_Format, row *protocol.HistoryRevision) error {
	if row == nil {
		return fmt.Errorf("%w: missing history revision", ErrProtocol)
	}
	if err := validateHistoryRepresentation(format, row); err != nil {
		return err
	}
	if row.GetApplied() > row.GetTotal() || row.GetTotal() > uint64(^uint32(0)>>1) || row.GetExecutionNanoseconds() < 0 {
		return fmt.Errorf("%w: invalid history progress or duration", ErrProtocol)
	}
	if row.GetAppliedAtNanos() < 0 || row.GetAppliedAtNanos() >= 1e9 || row.GetAppliedAtSeconds() < -62135596800 || row.GetAppliedAtSeconds() > 253402300799 {
		return fmt.Errorf("%w: invalid history timestamp", ErrProtocol)
	}
	return nil
}

// Atlas stores progress, errors, and operator identity, not derived state or
// direction. Only the host interprets those facts as migration policy.
func validateHistoryRepresentation(format protocol.HistoryScope_Format, row *protocol.HistoryRevision) error {
	if format == protocol.HistoryScope_ATLAS {
		if row.GetState() != protocol.HistoryRevision_STATE_UNSPECIFIED || row.GetDirection() != protocol.HistoryRevision_DIRECTION_UNSPECIFIED {
			return fmt.Errorf("%w: Atlas history must not supply derived state or direction", ErrProtocol)
		}
		return nil
	}
	if format != protocol.HistoryScope_NATIVE {
		return fmt.Errorf("%w: unknown history format", ErrProtocol)
	}
	version, err := strconv.ParseInt(row.GetVersion(), 10, 64)
	if err != nil || strconv.FormatInt(version, 10) != row.GetVersion() {
		return fmt.Errorf("%w: native history version is not canonical", ErrProtocol)
	}
	if row.GetAtlasType() != 0 || len(row.GetPartialHashes()) != 0 {
		return fmt.Errorf("%w: native history contains Atlas fields", ErrProtocol)
	}
	switch row.GetState() {
	case protocol.HistoryRevision_PENDING, protocol.HistoryRevision_APPLIED, protocol.HistoryRevision_FAILED:
	default:
		return fmt.Errorf("%w: unknown history state", ErrProtocol)
	}
	if row.GetDirection() != protocol.HistoryRevision_UP && row.GetDirection() != protocol.HistoryRevision_DOWN {
		return fmt.Errorf("%w: unknown history direction", ErrProtocol)
	}
	return nil
}

// ValidateHistorySnapshot binds a complete result to the exact requested store
// and transaction. Invalid results expose no usable prefix of revision rows.
func ValidateHistorySnapshot(request *protocol.ReadHistoryRequest, result *protocol.HistorySnapshot) error {
	if err := ValidateReadHistoryRequest(request); err != nil {
		return err
	}
	if result == nil || !proto.Equal(result.GetScope(), request.GetScope()) {
		return fmt.Errorf("%w: history snapshot changed its scope", ErrProtocol)
	}
	if !result.GetComplete() || len(result.GetRevisions()) > int(request.GetMaxRevisions()) {
		return fmt.Errorf("%w: history snapshot is incomplete", ErrIncomplete)
	}
	if !result.GetExists() && len(result.GetRevisions()) != 0 {
		return fmt.Errorf("%w: missing history store returned revisions", ErrProtocol)
	}
	seen := make(map[string]bool, len(result.GetRevisions()))
	for _, row := range result.GetRevisions() {
		if err := ValidateHistoryRevision(request.GetScope().GetFormat(), row); err != nil {
			return err
		}
		if seen[row.GetVersion()] {
			return fmt.Errorf("%w: duplicate history identity", ErrProtocol)
		}
		seen[row.GetVersion()] = true
	}
	return nil
}

// ValidateMutateHistoryRequest checks an exact insert, replacement, or deletion.
// A replacement cannot rename a revision or omit the expected row.
func ValidateMutateHistoryRequest(request *protocol.MutateHistoryRequest) error {
	if err := ValidateHistoryScope(request.GetScope()); err != nil {
		return err
	}
	if request.GetRequestId() == "" || request.GetExpected() == nil && request.GetReplacement() == nil {
		return fmt.Errorf("%w: history mutation requires a request ID and a row", ErrProtocol)
	}
	for _, row := range []*protocol.HistoryRevision{request.GetExpected(), request.GetReplacement()} {
		if row != nil {
			if err := ValidateHistoryRevision(request.GetScope().GetFormat(), row); err != nil {
				return err
			}
		}
	}
	if request.GetExpected() != nil && request.GetReplacement() != nil && request.GetExpected().GetVersion() != request.GetReplacement().GetVersion() {
		return fmt.Errorf("%w: history mutation changed revision identity", ErrProtocol)
	}
	return nil
}

// HistoryError records whether a mutation took effect. UNKNOWN wraps
// ErrOutcomeUnknown; confirmed conflicts and refusals wrap ErrExecution.
// Provider-authored diagnostics are not included in its text.
type HistoryError struct {
	Outcome protocol.HistoryResult_Outcome
}

// Error describes the outcome without exposing provider diagnostics.
func (e *HistoryError) Error() string {
	return fmt.Sprintf("provider history outcome is %s", e.Outcome)
}

// Unwrap distinguishes uncertain writes from confirmed refusals.
func (e *HistoryError) Unwrap() error {
	if e.Outcome == protocol.HistoryResult_UNKNOWN {
		return ErrOutcomeUnknown
	}
	return ErrExecution
}

// ValidateHistoryResult checks the exact request binding and outcome shape.
func ValidateHistoryResult(scope *protocol.HistoryScope, requestID string, result *protocol.HistoryResult) error {
	if err := ValidateHistoryScope(scope); err != nil {
		return err
	}
	if result == nil || requestID == "" || result.GetRequestId() != requestID || !proto.Equal(scope, result.GetScope()) {
		return fmt.Errorf("%w: history result changed its request binding", ErrProtocol)
	}
	switch result.GetOutcome() {
	case protocol.HistoryResult_UNKNOWN, protocol.HistoryResult_APPLIED, protocol.HistoryResult_CONFLICT, protocol.HistoryResult_REFUSED:
		return nil
	default:
		return fmt.Errorf("%w: unknown history outcome", ErrProtocol)
	}
}

// CheckHistoryResult accepts only confirmed application. Missing, malformed,
// or mismatched receipts leave the outcome unknown and never permit retry.
func CheckHistoryResult(scope *protocol.HistoryScope, requestID string, result *protocol.HistoryResult) error {
	if err := ValidateHistoryResult(scope, requestID, result); err != nil {
		return errors.Join(err, &HistoryError{})
	}
	if result.GetOutcome() == protocol.HistoryResult_APPLIED {
		return nil
	}
	return &HistoryError{Outcome: result.GetOutcome()}
}
