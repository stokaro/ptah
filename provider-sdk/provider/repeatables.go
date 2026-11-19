package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// RepeatableRecordFeature advertises the record of repeatable migrations: the
// table beside the revision table that keeps, per repeatable migration, the
// content that last applied and the last attempt. It enables ReadRepeatables,
// PrepareRepeatables, ReadRepeatableBody and MutateRepeatable, and
// MigrationLogRecord.repeatable, the identity a log entry about a repeatable
// migration carries.
//
// It is a two-way feature, because the log field is an addition to an
// existing message: the provider and each engine that keeps the record list it
// beside HistoryFeature, and the host lists it to call the services and send
// the field. A host that runs a directory holding repeatable migrations on an
// engine that does not list it refuses before anything changes. Contract
// 25.44.
const RepeatableRecordFeature = "sessions.repeatable-record"

// MaxRepeatableRows bounds a complete read of the record. A larger record is
// refused, never truncated into a snapshot that looks complete.
const MaxRepeatableRows = 10000

// MaxRepeatableIdentityBytes bounds an identity, the length of the record's
// key column on every built-in engine.
const MaxRepeatableIdentityBytes = 255

// MaxRepeatableTokenBytes bounds a digest and a run ID in the record. Ptah
// writes digests of 47 bytes and run IDs of 36.
const MaxRepeatableTokenBytes = 255

// MaxRepeatableErrorBytes bounds the error a failure records and a row
// returns. With every other field of a row bounded too, a complete snapshot of
// MaxRepeatableRows rows stays under the 64 MiB a host receives in one
// message, about 52 MB at worst, so one long error cannot make the record
// unreadable and with it impossible to run or repair. The host truncates a
// longer error, at a UTF-8 boundary, before it sends a failure.
const MaxRepeatableErrorBytes = 4096

// MaxRepeatableBodyBytes bounds the content a completion stores and a body
// read returns, so either message fits the transport with room to spare. A
// host refuses, before it runs anything, a repeatable migration whose content
// is longer.
const MaxRepeatableBodyBytes = 2 << 20

// maxRepeatableMessageBytes bounds every request and reply of the record.
const maxRepeatableMessageBytes = 3 << 20

// Repeatables owns the storage of the record of repeatable migrations, not its
// meaning: the host decides which file runs, retries and repairs. Reads are
// consistent snapshots and create nothing. Preparation creates a missing
// record outside transactions, checks the ownership and layout of an existing
// one, and never upgrades another layout. Each mutation is atomic and changes
// one row; a conflict changes nothing and names the run the row holds.
// Cancellation and lost acknowledgments never permit a replay.
//
// A completion may carry the transaction its migration body ran in, which is
// the same handle execution uses, so the record and the work commit together.
// Every other operation runs on the session outside any transaction.
// Operations serialize with finish and close, and refuse closed or foreign
// handles and a session whose lock is lost.
type Repeatables interface {
	ReadRepeatables(context.Context, *protocol.ReadRepeatablesRequest) (*protocol.RepeatableSnapshot, error)
	PrepareRepeatables(context.Context, *protocol.PrepareRepeatablesRequest) (*protocol.RepeatableResult, error)
	ReadRepeatableBody(context.Context, *protocol.ReadRepeatableBodyRequest) (*protocol.RepeatableBody, error)
	MutateRepeatable(context.Context, *protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error)
}

// validateRepeatableRecordFeature refuses a provider that lists
// RepeatableRecordFeature without HistoryFeature, an engine that lists it
// without the provider's, and an engine that lists it without its own
// HistoryFeature: the record sits beside the revision table the history
// service keeps.
func validateRepeatableRecordFeature(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(description.GetFeatures(), RepeatableRecordFeature) && !slices.Contains(description.GetFeatures(), HistoryFeature) {
		return fmt.Errorf("%w: %s requires the provider's history service", ErrProtocol, RepeatableRecordFeature)
	}
	if !slices.Contains(engine.GetFeatures(), RepeatableRecordFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), RepeatableRecordFeature) {
		return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, RepeatableRecordFeature)
	}
	if !slices.Contains(engine.GetFeatures(), HistoryFeature) {
		return fmt.Errorf("%w: engine %s requires the engine's history", ErrProtocol, RepeatableRecordFeature)
	}
	return nil
}

// ValidateRepeatableScope checks the session and the record's identifiers.
// Identifier spelling is preserved; the owner controls quoting and
// resolution. A transaction is checked by the operation that may carry one.
func ValidateRepeatableScope(scope *protocol.RepeatableScope) error {
	if scope.GetSessionId() == "" || strings.TrimSpace(scope.GetTable()) == "" ||
		!metadataText(scope.GetSessionId(), scope.GetTransactionId(), scope.GetSchema(), scope.GetTable()) {
		return fmt.Errorf("%w: the repeatable migration record requires a session and valid store identifiers", ErrProtocol)
	}
	return nil
}

// validateRepeatableSessionScope is ValidateRepeatableScope for an operation
// that runs on the session alone.
func validateRepeatableSessionScope(scope *protocol.RepeatableScope) error {
	if err := ValidateRepeatableScope(scope); err != nil {
		return err
	}
	if scope.GetTransactionId() != "" {
		return fmt.Errorf("%w: only a completion writes the repeatable migration record inside a transaction", ErrProtocol)
	}
	return nil
}

// ValidateRepeatableIdentity checks that identity can be a key of the record:
// not empty, valid UTF-8 with no NUL byte, and at most
// MaxRepeatableIdentityBytes long. Which identities a file name can carry is
// the host's rule.
func ValidateRepeatableIdentity(identity string) error {
	if identity == "" || len(identity) > MaxRepeatableIdentityBytes || !metadataText(identity) {
		return fmt.Errorf("%w: invalid repeatable migration identity", ErrProtocol)
	}
	return nil
}

// repeatableText reports whether each value is non-empty valid text of at
// most MaxRepeatableTokenBytes.
func repeatableText(values ...string) bool {
	for _, value := range values {
		if value == "" || len(value) > MaxRepeatableTokenBytes || !metadataText(value) {
			return false
		}
	}
	return true
}

// repeatableError reports whether text can be the error of a row: valid
// UTF-8 of at most MaxRepeatableErrorBytes.
func repeatableError(text string) bool {
	return len(text) <= MaxRepeatableErrorBytes && utf8.ValidString(text)
}

// ValidateRepeatableRow checks the representable facts of one row without
// deciding what the host does with it. A row whose content applied keeps its
// digest and time; a row that never applied keeps neither, and no body.
func ValidateRepeatableRow(row *protocol.RepeatableRow) error {
	if row == nil {
		return fmt.Errorf("%w: missing repeatable migration row", ErrProtocol)
	}
	if err := ValidateRepeatableIdentity(row.GetIdentity()); err != nil {
		return err
	}
	switch row.GetState() {
	case protocol.RepeatableRow_RUNNING, protocol.RepeatableRow_APPLIED, protocol.RepeatableRow_FAILED:
	default:
		return fmt.Errorf("%w: unknown repeatable migration state", ErrProtocol)
	}
	if !repeatableText(row.GetAttemptChecksum(), row.GetRunId()) || row.GetAttemptOrder() == 0 ||
		!metadataTimestamp(row.GetAttemptedAtSeconds(), row.GetAttemptedAtNanos()) || !repeatableError(row.GetError()) {
		return fmt.Errorf("%w: invalid repeatable migration attempt", ErrProtocol)
	}
	if row.GetApplied() {
		if !repeatableText(row.GetChecksum()) || !metadataTimestamp(row.GetAppliedAtSeconds(), row.GetAppliedAtNanos()) {
			return fmt.Errorf("%w: an applied repeatable migration needs its digest and time", ErrProtocol)
		}
		return nil
	}
	if row.GetChecksum() != "" || row.GetAppliedAtSeconds() != 0 || row.GetAppliedAtNanos() != 0 || row.GetBodyKept() ||
		row.GetState() == protocol.RepeatableRow_APPLIED {
		return fmt.Errorf("%w: a repeatable migration that never applied keeps no digest, time or body", ErrProtocol)
	}
	return nil
}

// ValidateReadRepeatablesRequest refuses an unbounded read before dispatch.
func ValidateReadRepeatablesRequest(request *protocol.ReadRepeatablesRequest) error {
	if err := validateRepeatableSessionScope(request.GetScope()); err != nil {
		return err
	}
	if request.GetMaxRows() == 0 || request.GetMaxRows() > MaxRepeatableRows {
		return fmt.Errorf("%w: a repeatable migration record read requires a bounded row limit", ErrProtocol)
	}
	return nil
}

// ValidateRepeatableSnapshotReply checks a snapshot against a request that
// [ValidateReadRepeatablesRequest] accepted. An invalid reply exposes no
// usable prefix of rows.
func ValidateRepeatableSnapshotReply(request *protocol.ReadRepeatablesRequest, result *protocol.RepeatableSnapshot) error {
	if result == nil || !proto.Equal(result.GetScope(), request.GetScope()) {
		return fmt.Errorf("%w: repeatable migration snapshot changed its scope", ErrProtocol)
	}
	if !result.GetComplete() || len(result.GetRows()) > int(request.GetMaxRows()) {
		return fmt.Errorf("%w: repeatable migration snapshot is incomplete", ErrIncomplete)
	}
	if !result.GetExists() && len(result.GetRows()) != 0 {
		return fmt.Errorf("%w: a missing repeatable migration record returned rows", ErrProtocol)
	}
	seen := make(map[string]bool, len(result.GetRows()))
	for _, row := range result.GetRows() {
		if err := ValidateRepeatableRow(row); err != nil {
			return err
		}
		if seen[row.GetIdentity()] {
			return fmt.Errorf("%w: duplicate repeatable migration identity", ErrProtocol)
		}
		seen[row.GetIdentity()] = true
	}
	return nil
}

// ValidatePrepareRepeatablesRequest refuses preparation within a transaction.
func ValidatePrepareRepeatablesRequest(request *protocol.PrepareRepeatablesRequest) error {
	if err := validateRepeatableSessionScope(request.GetScope()); err != nil {
		return err
	}
	if request.GetRequestId() == "" || !metadataText(request.GetRequestId()) {
		return fmt.Errorf("%w: repeatable migration record preparation requires a request ID", ErrProtocol)
	}
	return nil
}

// ValidateReadRepeatableBodyRequest checks a body read before dispatch.
func ValidateReadRepeatableBodyRequest(request *protocol.ReadRepeatableBodyRequest) error {
	if err := validateRepeatableSessionScope(request.GetScope()); err != nil {
		return err
	}
	return ValidateRepeatableIdentity(request.GetIdentity())
}

// ValidateRepeatableBodyReply checks a body against a request that
// [ValidateReadRepeatableBodyRequest] accepted. A row the record does not hold
// keeps no body, and a row that keeps none returns no bytes.
func ValidateRepeatableBodyReply(request *protocol.ReadRepeatableBodyRequest, result *protocol.RepeatableBody) error {
	if result == nil || !proto.Equal(result.GetScope(), request.GetScope()) || result.GetIdentity() != request.GetIdentity() {
		return fmt.Errorf("%w: repeatable migration body changed its request binding", ErrProtocol)
	}
	if !result.GetFound() && result.GetKept() || !result.GetKept() && len(result.GetBody()) != 0 {
		return fmt.Errorf("%w: a repeatable migration body that is not kept has no bytes", ErrProtocol)
	}
	if len(result.GetBody()) > MaxRepeatableBodyBytes {
		return fmt.Errorf("%w: repeatable migration body exceeds %d bytes", ErrProtocol, MaxRepeatableBodyBytes)
	}
	return nil
}

// ValidateMutateRepeatableRequest checks one claim, completion, failure or
// removal. Only a completion may carry a transaction.
func ValidateMutateRepeatableRequest(request *protocol.MutateRepeatableRequest) error {
	scope := request.GetScope()
	if err := ValidateRepeatableScope(scope); err != nil {
		return err
	}
	if request.GetRequestId() == "" || !metadataText(request.GetRequestId()) || proto.Size(request) > maxRepeatableMessageBytes {
		return fmt.Errorf("%w: invalid repeatable migration write identity or size", ErrProtocol)
	}
	if _, completes := request.GetMutation().(*protocol.MutateRepeatableRequest_Complete); !completes && scope.GetTransactionId() != "" {
		return fmt.Errorf("%w: only a completion writes the repeatable migration record inside a transaction", ErrProtocol)
	}
	switch mutation := request.GetMutation().(type) {
	case *protocol.MutateRepeatableRequest_Claim:
		return validateRepeatableClaim(mutation.Claim)
	case *protocol.MutateRepeatableRequest_Complete:
		return validateRepeatableCompletion(mutation.Complete)
	case *protocol.MutateRepeatableRequest_Fail:
		return validateRepeatableFailure(mutation.Fail)
	case *protocol.MutateRepeatableRequest_Remove:
		return ValidateRepeatableIdentity(mutation.Remove.GetIdentity())
	default:
		return fmt.Errorf("%w: missing repeatable migration write", ErrProtocol)
	}
}

func validateRepeatableClaim(claim *protocol.RepeatableClaim) error {
	if err := ValidateRepeatableIdentity(claim.GetIdentity()); err != nil {
		return err
	}
	if !repeatableText(claim.GetRunId(), claim.GetAttemptChecksum()) || claim.GetAttemptOrder() == 0 ||
		!metadataTimestamp(claim.GetAttemptedAtSeconds(), claim.GetAttemptedAtNanos()) {
		return fmt.Errorf("%w: a repeatable migration claim needs its run, digest, order and time", ErrProtocol)
	}
	if claim.GetInsert() != (claim.GetExpectedRunId() == "") || !claim.GetInsert() && !repeatableText(claim.GetExpectedRunId()) {
		return fmt.Errorf("%w: a repeatable migration claim either inserts a row or expects the run it read", ErrProtocol)
	}
	return nil
}

func validateRepeatableFailure(failure *protocol.RepeatableFailure) error {
	if err := ValidateRepeatableIdentity(failure.GetIdentity()); err != nil {
		return err
	}
	if !repeatableText(failure.GetRunId()) || failure.GetError() == "" || !metadataText(failure.GetError()) {
		return fmt.Errorf("%w: a repeatable migration failure needs its run and error", ErrProtocol)
	}
	if len(failure.GetError()) > MaxRepeatableErrorBytes {
		return fmt.Errorf("%w: repeatable migration error exceeds %d bytes", ErrProtocol, MaxRepeatableErrorBytes)
	}
	return nil
}

func validateRepeatableCompletion(completion *protocol.RepeatableCompletion) error {
	if err := ValidateRepeatableIdentity(completion.GetIdentity()); err != nil {
		return err
	}
	if !repeatableText(completion.GetRunId(), completion.GetChecksum()) ||
		!metadataTimestamp(completion.GetAppliedAtSeconds(), completion.GetAppliedAtNanos()) {
		return fmt.Errorf("%w: a repeatable migration completion needs its run, digest and time", ErrProtocol)
	}
	if len(completion.GetBody()) > MaxRepeatableBodyBytes {
		return fmt.Errorf("%w: repeatable migration body exceeds %d bytes", ErrProtocol, MaxRepeatableBodyBytes)
	}
	return nil
}

// ValidateRepeatableResult checks that a receipt answers its exact request and
// that its outcome has a valid shape: a holder only with CONFLICT, always with
// the CONFLICT of an inserting claim, whose row exists, and removed only from
// a removal that APPLIED.
func ValidateRepeatableResult(request *protocol.MutateRepeatableRequest, result *protocol.RepeatableResult) error {
	if result == nil || request.GetRequestId() == "" || result.GetRequestId() != request.GetRequestId() ||
		!proto.Equal(request.GetScope(), result.GetScope()) {
		return fmt.Errorf("%w: repeatable migration result changed its request binding", ErrProtocol)
	}
	return validateRepeatableOutcome(result, request.GetMutation())
}

// ValidatePrepareRepeatablesResult is [ValidateRepeatableResult] for a
// preparation, which neither names a holder nor removes a row.
func ValidatePrepareRepeatablesResult(request *protocol.PrepareRepeatablesRequest, result *protocol.RepeatableResult) error {
	if result == nil || request.GetRequestId() == "" || result.GetRequestId() != request.GetRequestId() ||
		!proto.Equal(request.GetScope(), result.GetScope()) {
		return fmt.Errorf("%w: repeatable migration result changed its request binding", ErrProtocol)
	}
	return validateRepeatableOutcome(result, nil)
}

func validateRepeatableOutcome(result *protocol.RepeatableResult, mutation any) error {
	switch result.GetOutcome() {
	case protocol.RepeatableResult_UNKNOWN, protocol.RepeatableResult_APPLIED, protocol.RepeatableResult_CONFLICT, protocol.RepeatableResult_REFUSED:
	default:
		return fmt.Errorf("%w: unknown repeatable migration outcome", ErrProtocol)
	}
	if result.GetHolderRunId() != "" {
		_, removes := mutation.(*protocol.MutateRepeatableRequest_Remove)
		if result.GetOutcome() != protocol.RepeatableResult_CONFLICT || mutation == nil || removes || !repeatableText(result.GetHolderRunId()) {
			return fmt.Errorf("%w: only a conflicting write names the run that holds the row", ErrProtocol)
		}
	}
	if inserting, ok := mutation.(*protocol.MutateRepeatableRequest_Claim); ok && inserting.Claim.GetInsert() &&
		result.GetOutcome() == protocol.RepeatableResult_CONFLICT && result.GetHolderRunId() == "" {
		return fmt.Errorf("%w: an inserting claim conflicts only with a row, which names its run", ErrProtocol)
	}
	if result.GetRemoved() {
		if _, removes := mutation.(*protocol.MutateRepeatableRequest_Remove); !removes || result.GetOutcome() != protocol.RepeatableResult_APPLIED {
			return fmt.Errorf("%w: only a removal that applied removes a row", ErrProtocol)
		}
	}
	return nil
}

// RepeatableError records a write to the record that did not apply. An
// UNKNOWN outcome wraps ErrOutcomeUnknown; a confirmed conflict or refusal
// wraps ErrExecution. Holder is the run a conflicting row names, empty when
// the record holds no row for the identity. Provider diagnostics are not part
// of its text.
type RepeatableError struct {
	Outcome protocol.RepeatableResult_Outcome
	Holder  string
}

// Error describes the outcome without exposing provider diagnostics.
func (e *RepeatableError) Error() string {
	if e.Outcome == protocol.RepeatableResult_CONFLICT && e.Holder != "" {
		return fmt.Sprintf("provider repeatable migration record outcome is CONFLICT: run %q holds the row", e.Holder)
	}
	return fmt.Sprintf("provider repeatable migration record outcome is %s", e.Outcome)
}

// Unwrap distinguishes an uncertain write from a confirmed one that did not
// apply.
func (e *RepeatableError) Unwrap() error {
	if e.Outcome == protocol.RepeatableResult_UNKNOWN {
		return ErrOutcomeUnknown
	}
	return ErrExecution
}

// CheckRepeatableResult accepts only a confirmed application. Any other
// outcome returns a [*RepeatableError] that carries it. It expects a receipt
// [ValidateRepeatableResult] or [ValidatePrepareRepeatablesResult] accepted.
func CheckRepeatableResult(result *protocol.RepeatableResult) error {
	if result.GetOutcome() == protocol.RepeatableResult_APPLIED {
		return nil
	}
	return &RepeatableError{Outcome: result.GetOutcome(), Holder: result.GetHolderRunId()}
}

// ValidateMigrationLogNegotiation refuses a log record that names a repeatable
// migration when the negotiated description does not list
// RepeatableRecordFeature: a peer that did not negotiate the field would store
// or read the entry as one about version 0.
func ValidateMigrationLogNegotiation(description *protocol.Description, rows ...*protocol.MigrationLogRecord) error {
	if slices.Contains(description.GetFeatures(), RepeatableRecordFeature) {
		return nil
	}
	for _, row := range rows {
		if row.GetRepeatable() != "" {
			return fmt.Errorf("%w: a migration log entry about a repeatable migration requires %s", ErrProtocol, RepeatableRecordFeature)
		}
	}
	return nil
}
