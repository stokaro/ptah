package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// MigrationSplitFeature enables SplitMigration, by which the owner splits one
// migration body into the units its engine runs, and the abort flags
// Execution.transaction_aborted and TransactionResult.aborted. Both sides
// list it. It requires TransactionsFeature, and so ExecuteFeature, and
// HistoryFeature; a supporting engine declares it beside its own transactions
// and history.
const MigrationSplitFeature = "units.migration-split"

// MigrationSplitter splits migration bodies offline.
type MigrationSplitter interface {
	// SplitMigration reads the request's text alone. It returns the units in
	// run order, each with the text Execute receives, the source of its own
	// statements and a placement, or a refusal of the whole body; and a
	// verdict for each check assertion. It runs nothing.
	SplitMigration(context.Context, *protocol.SplitMigrationRequest) (*protocol.MigrationSplit, error)
}

func migrationSplitError(reason string) error {
	return fmt.Errorf("%w: migration split: %s", ErrProtocol, reason)
}

func transactionAbortError(reason string) error {
	return fmt.Errorf("%w: transaction abort: %s", ErrProtocol, reason)
}

// ValidateSplitMigrationRequest refuses a request without an engine or a
// body, one the negotiated description does not list MigrationSplitFeature
// for, and one for an engine that does not declare it.
func ValidateSplitMigrationRequest(description *protocol.Description, request *protocol.SplitMigrationRequest) error {
	if request.GetEngine() == "" {
		return migrationSplitError("the request names no engine")
	}
	if request.GetBody() == "" {
		return migrationSplitError("the request carries no body")
	}
	if !slices.Contains(description.GetFeatures(), MigrationSplitFeature) {
		return migrationSplitError("the feature was not negotiated")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), nil)
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), MigrationSplitFeature) {
		return migrationSplitError("the engine does not split migrations")
	}
	return nil
}

// ValidateMigrationSplit checks a reply against its request. Each unit
// carries the text to execute, its own source and a known placement. A
// refusal comes with no unit, names a byte range inside the body and gives
// its reason on one line. There is one verdict per assertion of the request;
// a verdict that is not read-only gives its reason on one line, and a
// read-only one gives none.
func ValidateMigrationSplit(description *protocol.Description, request *protocol.SplitMigrationRequest, result *protocol.MigrationSplit) error {
	if err := ValidateSplitMigrationRequest(description, request); err != nil {
		return err
	}
	return ValidateMigrationSplitReply(description, request, result)
}

// ValidateMigrationSplitReply is [ValidateMigrationSplit] for a request that [ValidateSplitMigrationRequest] accepted
// with description and that has not changed since: it checks the reply and not the
// request again. The SDK's client and server validate each request once
// and call it for the reply.
func ValidateMigrationSplitReply(description *protocol.Description, request *protocol.SplitMigrationRequest, result *protocol.MigrationSplit) error {
	if result == nil {
		return migrationSplitError("the reply is missing")
	}
	for _, unit := range result.GetUnits() {
		if err := validateMigrationUnit(unit); err != nil {
			return err
		}
	}
	if err := validateSplitRefusal(request, result); err != nil {
		return err
	}
	if len(result.GetAssertions()) != len(request.GetAssertions()) {
		return migrationSplitError("the reply does not give one verdict for each assertion")
	}
	for _, verdict := range result.GetAssertions() {
		if verdict.GetReadOnly() && verdict.GetReason() != "" {
			return migrationSplitError("a read-only assertion carries a reason")
		}
		if !verdict.GetReadOnly() && !diagnosticLine(verdict.GetReason()) {
			return migrationSplitError("a refused assertion needs its reason on one line")
		}
	}
	return nil
}

func validateMigrationUnit(unit *protocol.MigrationUnit) error {
	if strings.TrimSpace(unit.GetSql()) == "" {
		return migrationSplitError("a unit carries no text to execute")
	}
	if strings.TrimSpace(unit.GetSource()) == "" {
		return migrationSplitError("a unit carries no source")
	}
	switch unit.GetPlacement() {
	case protocol.MigrationUnit_WITH_CHECKPOINT, protocol.MigrationUnit_OUTSIDE:
		return nil
	default:
		return migrationSplitError("a unit needs a known placement")
	}
}

func validateSplitRefusal(request *protocol.SplitMigrationRequest, result *protocol.MigrationSplit) error {
	refusal := result.GetRefusal()
	if refusal == nil {
		return nil
	}
	if len(result.GetUnits()) != 0 {
		return migrationSplitError("a refused body has no units")
	}
	if refusal.GetStart() > refusal.GetEnd() || uint64(refusal.GetEnd()) > uint64(len(request.GetBody())) {
		return migrationSplitError("a refusal names a range outside the body")
	}
	if !diagnosticLine(refusal.GetReason()) {
		return migrationSplitError("a refusal needs its reason on one line")
	}
	return nil
}

// ValidateExecutionAbort refuses Execution.transaction_aborted where the
// reply cannot set it: without MigrationSplitFeature in the negotiated
// description, for a request outside a caller transaction, on an outcome
// other than NOT_APPLIED, and beside statement_timed_out. A reply that does
// not set it is valid here.
func ValidateExecutionAbort(description *protocol.Description, request *protocol.ExecuteRequest, result *protocol.Execution) error {
	if !result.GetTransactionAborted() {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), MigrationSplitFeature) {
		return transactionAbortError("aborts were not negotiated")
	}
	if request.GetTransactionId() == "" {
		return transactionAbortError("only a unit in a caller transaction is aborted")
	}
	if result.GetOutcome() != protocol.Execution_NOT_APPLIED {
		return transactionAbortError("an aborted unit is NOT_APPLIED")
	}
	if result.GetStatementTimedOut() {
		return transactionAbortError("an aborted unit did not stop at the statement timeout")
	}
	return nil
}

// ValidateTransactionAbort refuses TransactionResult.aborted where the reply
// cannot set it: without MigrationSplitFeature in the negotiated description,
// in answer to anything but COMMIT, and on an outcome other than ROLLED_BACK.
// A reply that does not set it is valid here.
func ValidateTransactionAbort(description *protocol.Description, request *protocol.FinishTransactionRequest, result *protocol.TransactionResult) error {
	if !result.GetAborted() {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), MigrationSplitFeature) {
		return transactionAbortError("aborts were not negotiated")
	}
	if request.GetAction() != protocol.FinishTransactionRequest_COMMIT {
		return transactionAbortError("only a commit is aborted")
	}
	if result.GetOutcome() != protocol.TransactionResult_ROLLED_BACK {
		return transactionAbortError("an aborted commit is ROLLED_BACK")
	}
	return nil
}

// validateMigrationSplitFeatures checks what the feature requires of the
// provider: transactions, which require execution, and history.
func validateMigrationSplitFeatures(features []string) error {
	if !slices.Contains(features, MigrationSplitFeature) {
		return nil
	}
	for _, required := range []string{TransactionsFeature, HistoryFeature} {
		if !slices.Contains(features, required) {
			return fmt.Errorf("%w: %s requires %s", ErrProtocol, MigrationSplitFeature, required)
		}
	}
	return nil
}

// validateMigrationSplitEngine checks what the feature requires of an engine
// that declares it: the provider's feature, and the engine's own transactions
// and history, which the host runs its units and checkpoints with.
func validateMigrationSplitEngine(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), MigrationSplitFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), MigrationSplitFeature) {
		return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, MigrationSplitFeature)
	}
	for _, required := range []string{TransactionsFeature, HistoryFeature} {
		if !slices.Contains(engine.GetFeatures(), required) {
			return fmt.Errorf("%w: engine %s requires engine %s", ErrProtocol, MigrationSplitFeature, required)
		}
	}
	return nil
}
