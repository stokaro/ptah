package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// LocksFeature advertises session-owned database exclusion. Each supporting
// engine must declare it too. It promises no particular SQL lock mechanism.
const LocksFeature = "sessions.locks"

var (
	// ErrLockBusy reports a confirmed acquisition refusal due to another owner.
	ErrLockBusy = errors.New("provider lock is held by another session")
	// ErrLockLost reports that an acquired handle no longer establishes ownership.
	ErrLockLost = errors.New("provider lock ownership was lost")
)

// LockOperation identifies the semantics against which a receipt is validated.
// The RPC method selects it; it is not an engine-specific policy selector.
type LockOperation uint8

const (
	// LockTry requests one nonblocking acquisition attempt.
	LockTry LockOperation = iota + 1
	// LockCheck asks the owner to verify its current exclusion guarantee.
	LockCheck
	// LockRelease releases or fences the exact acquisition handle.
	LockRelease
)

// Locks owns database exclusion within live sessions. TryLock reserves the
// caller's lock ID before contacting the database. Every ID is single-use:
// repeated calls cannot acquire again, including after BUSY or RELEASED.
// A confirmed BUSY permits the host to try a new ID under its wait policy.
// An unknown outcome never authorizes a retry. ReleaseLock of an unused ID
// fences it against a delayed TryLock and returns NOT_ACQUIRED.
//
// Scope names and handles remain bound to their original session and target.
// Closing a session releases its locks and fences late requests. Process
// restart invalidates every handle. Release is idempotent and cannot release
// another session's lock. Implementations must honor cancellation.
//
// CheckLock verifies the engine's current ownership guarantee, including the
// freshness of any lease. A stale local flag cannot establish ACQUIRED. Before
// execution, history writes, and transaction commits, the provider must verify
// every lock guarding that session. Lost or uncertain ownership permanently
// fences the session against further work; release, rollback, and close remain
// available for cleanup. A lost lock during execution does not prove rollback.
// The host also observes loss and cancels guarded work. Neither an earlier
// ACQUIRED receipt nor a successful release proves that the whole run held a lock.
type Locks interface {
	TryLock(context.Context, *protocol.LockRequest) (*protocol.LockResult, error)
	CheckLock(context.Context, *protocol.LockRequest) (*protocol.LockResult, error)
	ReleaseLock(context.Context, *protocol.LockRequest) (*protocol.LockResult, error)
}

// ValidateLockRequest checks complete acquisition and request identities.
// It does not establish the existence or ownership of any live handle.
func ValidateLockRequest(request *protocol.LockRequest) error {
	scope := request.GetScope()
	if scope.GetSessionId() == "" || scope.GetLockId() == "" || request.GetRequestId() == "" {
		return fmt.Errorf("%w: lock session, handle, and request IDs are required", ErrProtocol)
	}
	name := scope.GetName()
	if name == "" || strings.TrimSpace(name) != name || !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return fmt.Errorf("%w: lock name must be nonempty exact text without surrounding whitespace or NUL", ErrProtocol)
	}
	return nil
}

// ValidateLockResult checks exact binding and operation-specific outcomes.
// UNKNOWN is a valid uncertainty report. An invalid receipt proves no ownership.
// A diagnostic is valid only with an outcome its operation does not succeed
// with, and only in the shape ValidateDiagnostic accepts. Whether
// ErrorReasonsFeature was negotiated is not checked here: the SDK removes a
// diagnostic the description does not list on both sides of the transport.
func ValidateLockResult(operation LockOperation, request *protocol.LockRequest, result *protocol.LockResult) error {
	if err := ValidateLockRequest(request); err != nil {
		return err
	}
	return ValidateLockResultReply(operation, request, result)
}

// ValidateLockResultReply is [ValidateLockResult] for a request that
// [ValidateLockRequest] accepted and that has not changed since: it checks the
// reply and not the request again. The SDK's client and server validate each
// request once and call it for the reply.
func ValidateLockResultReply(operation LockOperation, request *protocol.LockRequest, result *protocol.LockResult) error {
	if operation != LockTry && operation != LockCheck && operation != LockRelease {
		return fmt.Errorf("%w: unknown lock operation", ErrProtocol)
	}
	if result == nil || !proto.Equal(request.GetScope(), result.GetScope()) || request.GetRequestId() != result.GetRequestId() {
		return fmt.Errorf("%w: lock receipt changed its scope or request identity", ErrProtocol)
	}
	if err := validLockOutcome(operation, result.GetOutcome()); err != nil {
		return err
	}
	if result.GetDiagnostic() == nil {
		return nil
	}
	if lockSucceeded(operation, result.GetOutcome()) {
		return fmt.Errorf("%w: a lock receipt gives a reason only for an outcome its operation does not succeed with", ErrProtocol)
	}
	return ValidateDiagnostic(result.GetDiagnostic())
}

func validLockOutcome(operation LockOperation, outcome protocol.LockResult_Outcome) error {
	switch outcome {
	case protocol.LockResult_UNKNOWN, protocol.LockResult_RELEASED, protocol.LockResult_NOT_ACQUIRED, protocol.LockResult_LOST:
		return nil
	case protocol.LockResult_ACQUIRED:
		if operation != LockRelease {
			return nil
		}
	case protocol.LockResult_BUSY:
		if operation == LockTry {
			return nil
		}
	}
	return fmt.Errorf("%w: lock receipt has an invalid outcome for its operation", ErrProtocol)
}

// lockSucceeded reports the outcomes an operation succeeds with: ownership
// for an acquisition or a check, and a confirmed release or a fenced handle
// for a release. CheckLockResult accepts exactly these, and a diagnostic
// never accompanies them, so the two cannot disagree about which outcome is
// a failure.
func lockSucceeded(operation LockOperation, outcome protocol.LockResult_Outcome) bool {
	if operation == LockRelease {
		return outcome == protocol.LockResult_RELEASED || outcome == protocol.LockResult_NOT_ACQUIRED
	}
	return outcome == protocol.LockResult_ACQUIRED
}

// LockError preserves a confirmed refusal or the absence of confirmation.
// Its zero value records an unknown outcome. Reason is the provider's reason
// for the outcome, from the receipt's diagnostic, and is empty when the
// receipt gave none or ErrorReasonsFeature was not negotiated.
type LockError struct {
	Outcome protocol.LockResult_Outcome
	Reason  string
}

// Error reports the outcome, and the provider's reason when there is one,
// without including connection or lock identifiers.
func (e *LockError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("provider lock outcome is %s: %s", e.Outcome, e.Reason)
	}
	return fmt.Sprintf("provider lock outcome is %s", e.Outcome)
}

// Unwrap distinguishes contention, lost ownership, refusal, and uncertainty.
func (e *LockError) Unwrap() error {
	switch e.Outcome {
	case protocol.LockResult_BUSY:
		return ErrLockBusy
	case protocol.LockResult_LOST:
		return ErrLockLost
	case protocol.LockResult_RELEASED, protocol.LockResult_NOT_ACQUIRED:
		return ErrExecution
	default:
		return ErrOutcomeUnknown
	}
}

// CheckLockResult accepts only ownership for try/check, or confirmed release
// and fenced absence for release. A malformed result retains uncertainty. A
// failure carries the receipt's reason in LockError.Reason.
func CheckLockResult(operation LockOperation, request *protocol.LockRequest, result *protocol.LockResult) error {
	if err := ValidateLockResult(operation, request, result); err != nil {
		return errors.Join(err, &LockError{})
	}
	outcome := result.GetOutcome()
	if lockSucceeded(operation, outcome) {
		return nil
	}
	failure := &LockError{Outcome: outcome, Reason: result.GetDiagnostic().GetReason()}
	if operation == LockCheck && (outcome == protocol.LockResult_RELEASED || outcome == protocol.LockResult_NOT_ACQUIRED) {
		return errors.Join(ErrLockLost, failure)
	}
	return failure
}
