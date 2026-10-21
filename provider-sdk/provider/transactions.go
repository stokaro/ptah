package provider

import (
	"context"
	"errors"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// TransactionsFeature advertises session-bound transactions. A supporting
// engine also includes this feature in its engine declaration.
const TransactionsFeature = "sessions.transactions"

// Transactions owns transaction handles within one provider process. Begin
// reserves the caller's handle before opening a transaction. Duplicate begins
// must not create another transaction, even after the handle becomes terminal.
// Finish serializes with begin and execution on that handle. A confirmed
// terminal result remains available until its session closes; another finish
// reports that result without repeating a commit or rollback. A rollback of an
// unused handle fences it against a late begin and reports NOT_STARTED.
//
// Handles are bound to the named session. Closing a session rolls back every
// unfinished transaction and prevents later begin or execution. A process
// restart invalidates all handles. Implementations honor cancellation, preserve
// uncertain outcomes, and never retry a write merely because its handle exists.
// Begin must establish the requested isolation or refuse it. Advertising this
// service does not promise support for every isolation level.
type Transactions interface {
	BeginTransaction(context.Context, *protocol.BeginTransactionRequest) (*protocol.Transaction, error)
	FinishTransaction(context.Context, *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error)
}

// ValidateTransactionRequest checks the identity of a transaction request. It
// does not establish that the session or transaction exists or is still active.
func ValidateTransactionRequest(sessionID, transactionID string) error {
	if sessionID == "" || transactionID == "" {
		return fmt.Errorf("%w: session ID and transaction ID are required", ErrProtocol)
	}
	return nil
}

// ValidateBeginTransactionRequest checks the handle and explicit isolation.
// Missing and unknown levels are refused before the provider is called.
func ValidateBeginTransactionRequest(request *protocol.BeginTransactionRequest) error {
	if err := ValidateTransactionRequest(request.GetSessionId(), request.GetTransactionId()); err != nil {
		return err
	}
	switch request.GetIsolation() {
	case protocol.TransactionIsolation_DEFAULT, protocol.TransactionIsolation_SERIALIZABLE:
		return nil
	default:
		return fmt.Errorf("%w: explicit supported transaction isolation is required", ErrProtocol)
	}
}

// ValidateTransaction checks that a begin reply binds the exact requested
// handle to its session and isolation. A malformed reply exposes no usable transaction.
func ValidateTransaction(request *protocol.BeginTransactionRequest, result *protocol.Transaction) error {
	if err := ValidateBeginTransactionRequest(request); err != nil {
		return err
	}
	if result == nil || result.GetId() != request.GetTransactionId() || result.GetSessionId() != request.GetSessionId() {
		return fmt.Errorf("%w: transaction reply changed the requested handle or session", ErrProtocol)
	}
	if result.GetIsolation() != request.GetIsolation() {
		return fmt.Errorf("%w: transaction reply changed the requested isolation", ErrProtocol)
	}
	return nil
}

// ValidateTransactionAction refuses missing and unrecognized finish actions.
func ValidateTransactionAction(action protocol.FinishTransactionRequest_Action) error {
	if action != protocol.FinishTransactionRequest_COMMIT && action != protocol.FinishTransactionRequest_ROLLBACK {
		return fmt.Errorf("%w: unknown transaction action", ErrProtocol)
	}
	return nil
}

// TransactionError carries the confirmed terminal state, or UNKNOWN when the
// result does not establish the final effect. It wraps ErrOutcomeUnknown for an
// unknown outcome and ErrExecution for a confirmed state that contradicts the
// requested action. The zero value records an unknown outcome.
type TransactionError struct {
	Outcome protocol.TransactionResult_Outcome
}

// Error names the confirmed state or the absence of confirmation.
func (e *TransactionError) Error() string {
	return fmt.Sprintf("provider transaction outcome is %s", e.Outcome)
}

// Unwrap identifies whether the transaction's final effect is uncertain.
func (e *TransactionError) Unwrap() error {
	if e.Outcome == protocol.TransactionResult_UNKNOWN {
		return ErrOutcomeUnknown
	}
	return ErrExecution
}

// ValidateTransactionResult checks a terminal result's shape. UNKNOWN is a
// valid report of uncertainty, not evidence that the transaction had no effect.
func ValidateTransactionResult(result *protocol.TransactionResult) error {
	if result == nil {
		return fmt.Errorf("%w: missing transaction result", ErrProtocol)
	}
	switch result.GetOutcome() {
	case protocol.TransactionResult_UNKNOWN, protocol.TransactionResult_COMMITTED, protocol.TransactionResult_ROLLED_BACK, protocol.TransactionResult_NOT_STARTED:
		return nil
	default:
		return fmt.Errorf("%w: unrecognized transaction outcome", ErrProtocol)
	}
}

// CheckTransactionResult validates a finish reply against the requested action.
// Rollback accepts NOT_STARTED only because that result fences a late begin.
// Missing or malformed replies leave the outcome unknown. No result permits an
// automatic retry of the operation that produced it.
func CheckTransactionResult(action protocol.FinishTransactionRequest_Action, result *protocol.TransactionResult) error {
	if err := ValidateTransactionAction(action); err != nil {
		return err
	}
	if err := ValidateTransactionResult(result); err != nil {
		return errors.Join(err, &TransactionError{})
	}
	switch result.GetOutcome() {
	case protocol.TransactionResult_COMMITTED:
		if action == protocol.FinishTransactionRequest_COMMIT {
			return nil
		}
	case protocol.TransactionResult_ROLLED_BACK, protocol.TransactionResult_NOT_STARTED:
		if action == protocol.FinishTransactionRequest_ROLLBACK {
			return nil
		}
	case protocol.TransactionResult_UNKNOWN:
	default:
		return errors.Join(ErrProtocol, &TransactionError{})
	}
	return &TransactionError{Outcome: result.GetOutcome()}
}
