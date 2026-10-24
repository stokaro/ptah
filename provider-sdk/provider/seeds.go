package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// SeedsFeature advertises the seed service on live sessions. The host
// discovers seed files, computes their checksums, selects them by environment
// and refuses protected tables; the provider runs a file and keeps the ledger
// of applied files, whose layout is its own. Each supporting engine also
// declares it.
const SeedsFeature = "data.seeds"

// MaxSeedLedgerEntries bounds a complete ledger read. A larger ledger is
// refused, never truncated into a reply that looks complete.
const MaxSeedLedgerEntries = 10000

// Seeds runs seed files and keeps their ledger on live sessions.
//
// ApplySeed runs a file and records it atomically: the file and its entry
// commit together or not at all. The owner may run the file again after its
// engine aborts a transaction in a way that proves nothing committed, a
// bounded number of times, and answers NOT_APPLIED when it stops. Any case in
// which it cannot prove that nothing committed is UNKNOWN, and the host does
// not retry it. A statement the engine runs only outside a transaction is
// refused before anything runs. RecordSeed writes an entry without running
// the file. Both create a missing ledger, refuse one whose layout the owner
// does not recognize as its own, and replace an entry for the same path.
// ReadSeedLedger never creates the ledger.
type Seeds interface {
	ListSeedTables(context.Context, *protocol.ListSeedTablesRequest) (*protocol.SeedTables, error)
	ApplySeed(context.Context, *protocol.ApplySeedRequest) (*protocol.SeedResult, error)
	RecordSeed(context.Context, *protocol.RecordSeedRequest) (*protocol.SeedResult, error)
	ReadSeedLedger(context.Context, *protocol.ReadSeedLedgerRequest) (*protocol.SeedLedger, error)
}

// ValidateListSeedTablesRequest checks the session and the schema.
func ValidateListSeedTablesRequest(request *protocol.ListSeedTablesRequest) error {
	if request.GetSessionId() == "" || strings.ContainsRune(request.GetSchema(), 0) || !utf8.ValidString(request.GetSchema()) {
		return fmt.Errorf("%w: a seed table list requires a session and a valid schema", ErrProtocol)
	}
	return nil
}

// ValidateSeedTables refuses a table without a name, a table outside the
// requested schema when one was named, and a table listed twice.
func ValidateSeedTables(request *protocol.ListSeedTablesRequest, result *protocol.SeedTables) error {
	if err := ValidateListSeedTablesRequest(request); err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("%w: seed table list is missing", ErrProtocol)
	}
	tables := result.GetTables()
	for i, table := range tables {
		if validateRowTable(table) != nil || request.GetSchema() != "" && table.GetSchema() != request.GetSchema() {
			return fmt.Errorf("%w: a seed table needs a name in the requested schema", ErrProtocol)
		}
		if slices.ContainsFunc(tables[:i], func(seen *protocol.Reference) bool { return proto.Equal(seen, table) }) {
			return fmt.Errorf("%w: a seed table is listed twice", ErrProtocol)
		}
	}
	return nil
}

// validSeedChecksum reports lowercase hexadecimal SHA-256 text.
func validSeedChecksum(checksum string) bool {
	return len(checksum) == 64 && strings.Trim(checksum, "0123456789abcdef") == ""
}

func validateSeed(seed *protocol.Seed) error {
	if seed == nil || seed.GetPath() == "" || !utf8.ValidString(seed.GetPath()) || strings.ContainsRune(seed.GetPath(), 0) ||
		seed.GetFilename() == "" || !utf8.ValidString(seed.GetFilename()) {
		return fmt.Errorf("%w: a seed needs a path and a file name", ErrProtocol)
	}
	if !validSeedChecksum(seed.GetChecksum()) {
		return fmt.Errorf("%w: a seed checksum is lowercase hex SHA-256", ErrProtocol)
	}
	if seed.GetEnv() != "" && !comparisonReason(seed.GetEnv()) {
		return fmt.Errorf("%w: a seed environment is one line of text", ErrProtocol)
	}
	if !utf8.ValidString(seed.GetText()) {
		return fmt.Errorf("%w: seed text is not UTF-8", ErrProtocol)
	}
	return nil
}

func validateSeedSession(sessionID, requestID string) error {
	if sessionID == "" || requestID == "" {
		return fmt.Errorf("%w: a seed request requires a session and a request ID", ErrProtocol)
	}
	return nil
}

// ValidateApplySeedRequest checks the session, the request ID and the seed.
func ValidateApplySeedRequest(request *protocol.ApplySeedRequest) error {
	if err := validateSeedSession(request.GetSessionId(), request.GetRequestId()); err != nil {
		return err
	}
	return validateSeed(request.GetSeed())
}

// ValidateRecordSeedRequest checks the session, the request ID and the seed,
// which carries no text: recording a file does not run it.
func ValidateRecordSeedRequest(request *protocol.RecordSeedRequest) error {
	if err := validateSeedSession(request.GetSessionId(), request.GetRequestId()); err != nil {
		return err
	}
	if request.GetSeed().GetText() != "" {
		return fmt.Errorf("%w: a recorded seed carries no text", ErrProtocol)
	}
	return validateSeed(request.GetSeed())
}

// validateSeedResult checks the request binding and the outcome's shape: a
// known outcome, a reason where one is required, and a position only on a
// refusal, inside the text.
func validateSeedResult(requestID string, seed *protocol.Seed, result *protocol.SeedResult) error {
	if result == nil || result.GetRequestId() != requestID || result.GetPath() != seed.GetPath() {
		return fmt.Errorf("%w: seed result changed its request binding", ErrProtocol)
	}
	outcome := result.GetOutcome()
	if _, known := protocol.SeedResult_Outcome_name[int32(outcome)]; !known {
		return fmt.Errorf("%w: unknown seed outcome", ErrProtocol)
	}
	reason := result.GetReason()
	needsReason := outcome == protocol.SeedResult_REFUSED || outcome == protocol.SeedResult_NOT_APPLIED
	if (reason != "" || needsReason) && !diagnosticLine(reason) {
		return fmt.Errorf("%w: a seed outcome needs one line of reason", ErrProtocol)
	}
	position := result.GetPosition()
	if position == nil {
		return nil
	}
	if outcome != protocol.SeedResult_REFUSED {
		return fmt.Errorf("%w: only a refused seed carries a position", ErrProtocol)
	}
	if position.GetByteOffset() > uint64(len(seed.GetText())) || position.GetLine() < 1 || position.GetColumn() < 1 {
		return fmt.Errorf("%w: a seed position lies outside its text", ErrProtocol)
	}
	return nil
}

// ValidateAppliedSeed checks the reply to an ApplySeed request.
func ValidateAppliedSeed(request *protocol.ApplySeedRequest, result *protocol.SeedResult) error {
	if err := ValidateApplySeedRequest(request); err != nil {
		return err
	}
	return validateSeedResult(request.GetRequestId(), request.GetSeed(), result)
}

// ValidateRecordedSeed checks the reply to a RecordSeed request. A record
// runs no statement, so it cannot conflict on a key or name a position.
func ValidateRecordedSeed(request *protocol.RecordSeedRequest, result *protocol.SeedResult) error {
	if err := ValidateRecordSeedRequest(request); err != nil {
		return err
	}
	if err := validateSeedResult(request.GetRequestId(), request.GetSeed(), result); err != nil {
		return err
	}
	if result.GetOutcome() == protocol.SeedResult_KEY_CONFLICT || result.GetPosition() != nil {
		return fmt.Errorf("%w: a recorded seed reports a key conflict or a position", ErrProtocol)
	}
	return nil
}

// SeedError records a seed request that did not apply. UNKNOWN wraps
// ErrOutcomeUnknown; KEY_CONFLICT, REFUSED and NOT_APPLIED wrap ErrExecution,
// and confirm that nothing of the file stayed. Provider-authored text is not
// included.
type SeedError struct {
	Outcome protocol.SeedResult_Outcome
}

// Error describes the outcome without provider diagnostics.
func (e *SeedError) Error() string {
	return fmt.Sprintf("provider seed outcome is %s", e.Outcome)
}

// Unwrap distinguishes an uncertain seed from a confirmed one.
func (e *SeedError) Unwrap() error {
	if e.Outcome == protocol.SeedResult_UNKNOWN {
		return ErrOutcomeUnknown
	}
	return ErrExecution
}

// CheckSeedResult accepts only APPLIED. It is called with a result that
// passed its validator; a host decides what a confirmed KEY_CONFLICT means.
func CheckSeedResult(result *protocol.SeedResult) error {
	if result.GetOutcome() == protocol.SeedResult_APPLIED {
		return nil
	}
	return &SeedError{Outcome: result.GetOutcome()}
}

// ValidateReadSeedLedgerRequest refuses an unbounded read.
func ValidateReadSeedLedgerRequest(request *protocol.ReadSeedLedgerRequest) error {
	if request.GetSessionId() == "" || request.GetMaxEntries() == 0 || request.GetMaxEntries() > MaxSeedLedgerEntries {
		return fmt.Errorf("%w: a seed ledger read requires a session and a bound of at most %d entries", ErrProtocol, MaxSeedLedgerEntries)
	}
	return nil
}

// ValidateSeedLedger refuses an incomplete or oversized ledger, entries from a
// missing ledger, an entry without a path or a checksum, a path listed twice,
// and a time outside the range a host can hold.
func ValidateSeedLedger(request *protocol.ReadSeedLedgerRequest, result *protocol.SeedLedger) error {
	if err := ValidateReadSeedLedgerRequest(request); err != nil {
		return err
	}
	if result == nil || !result.GetComplete() || len(result.GetEntries()) > int(request.GetMaxEntries()) {
		return fmt.Errorf("%w: seed ledger is incomplete", ErrIncomplete)
	}
	if !result.GetExists() && len(result.GetEntries()) != 0 {
		return fmt.Errorf("%w: a missing seed ledger returned entries", ErrProtocol)
	}
	paths := make(map[string]bool, len(result.GetEntries()))
	for _, entry := range result.GetEntries() {
		if entry.GetPath() == "" || !utf8.ValidString(entry.GetPath()) || paths[entry.GetPath()] || !validSeedChecksum(entry.GetChecksum()) {
			return fmt.Errorf("%w: a seed ledger entry needs a unique path and a checksum", ErrProtocol)
		}
		paths[entry.GetPath()] = true
		if entry.GetEnv() != "" && !comparisonReason(entry.GetEnv()) {
			return fmt.Errorf("%w: a seed ledger environment is one line of text", ErrProtocol)
		}
		if entry.GetAppliedAtNanos() < 0 || entry.GetAppliedAtNanos() >= 1e9 ||
			entry.GetAppliedAtSeconds() < -62135596800 || entry.GetAppliedAtSeconds() > 253402300799 {
			return fmt.Errorf("%w: invalid seed ledger time", ErrProtocol)
		}
	}
	return nil
}
