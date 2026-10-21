package provider

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// MetadataFeature advertises migration event and tag storage. Supporting
// engines declare it too; it does not establish locking or transactional DDL.
const MetadataFeature = "sessions.migration-metadata"

// MaxMetadataRecords bounds each immutable snapshot page, not the whole store.
const MaxMetadataRecords = 256

// MaxMetadataBytes leaves transport framing space below the RPC message limit.
// An oversized record or page is refused, never truncated.
const MaxMetadataBytes = 3 << 20

// Metadata owns storage facts. The host owns event meaning, attempt pairing,
// ordering, tag normalization, and migration recovery. Log writes use durable
// session storage outside execution transactions; tags use the selected handle.
// Prepare validates ownership and layout. It may create absent storage outside
// transactions; inside a transaction it only validates an existing store.
// Mutations serialize with finish and close, guard current lock ownership, and
// never replay after uncertainty. Duplicate log identities are refused.
//
// Read handles own immutable snapshots and advance once per page. Opening an
// absent store creates nothing. Each caller-selected reader ID is single-use,
// scoped to its session, transaction, store, and process. Close is idempotent
// and fences a late open even if no reader exists yet. Sessions release all
// readers on close; transaction finish invalidates its readers. A failed page
// must be closed rather than retried. Providers reject foreign handles and
// serialize open, page advancement, finish, and close.
type Metadata interface {
	PrepareMetadata(context.Context, *protocol.PrepareMetadataRequest) (*protocol.MetadataResult, error)
	MutateMetadata(context.Context, *protocol.MutateMetadataRequest) (*protocol.MetadataResult, error)
	LookupMetadataTag(context.Context, *protocol.LookupMetadataTagRequest) (*protocol.MetadataTagResult, error)
	OpenMetadataRead(context.Context, *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error)
	ReadMetadataPage(context.Context, *protocol.ReadMetadataPageRequest) (*protocol.MetadataPage, error)
	CloseMetadataRead(context.Context, *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error)
}

// ValidateMetadataScope refuses invalid store identities and transactional logs.
func ValidateMetadataScope(scope *protocol.MetadataScope) error {
	if scope.GetSessionId() == "" || strings.TrimSpace(scope.GetTable()) == "" ||
		!metadataText(scope.GetSessionId(), scope.GetTransactionId(), scope.GetSchema(), scope.GetTable()) {
		return fmt.Errorf("%w: metadata requires a session and valid store identifiers", ErrProtocol)
	}
	switch scope.GetKind() {
	case protocol.MetadataScope_LOG:
		if scope.GetTransactionId() != "" {
			return fmt.Errorf("%w: migration logs require a durable session store", ErrProtocol)
		}
	case protocol.MetadataScope_TAGS:
	default:
		return fmt.Errorf("%w: unknown metadata kind", ErrProtocol)
	}
	return nil
}

func metadataText(values ...string) bool {
	for _, value := range values {
		if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return false
		}
	}
	return true
}
func metadataTimestamp(seconds int64, nanos int32) bool {
	return seconds >= -62135596800 && seconds <= 253402300799 && nanos >= 0 && nanos < 1e9 && (seconds != -62135596800 || nanos != 0)
}
func metadataRequest(scope *protocol.MetadataScope, id string, message proto.Message) error {
	if err := ValidateMetadataScope(scope); err != nil {
		return err
	}
	if id == "" || !metadataText(id) || proto.Size(message) > MaxMetadataBytes {
		return fmt.Errorf("%w: invalid metadata request identity or size", ErrProtocol)
	}
	return nil
}

// ValidateMigrationLogRecord checks stored event identity and representation.
func ValidateMigrationLogRecord(row *protocol.MigrationLogRecord) error {
	if row == nil || row.GetRunId() == "" || row.GetSequence() <= 0 || !metadataTimestamp(row.GetAtSeconds(), row.GetAtNanos()) ||
		!metadataText(row.GetRunId()) || !utf8.ValidString(row.GetActor()) || !utf8.ValidString(row.GetChecksum()) || !utf8.ValidString(row.GetError()) {
		return fmt.Errorf("%w: invalid migration log record", ErrProtocol)
	}
	if row.GetOperation() != "up" && row.GetOperation() != "down" {
		return fmt.Errorf("%w: invalid migration log operation", ErrProtocol)
	}
	switch row.GetState() {
	case "started", "applied", "failed", "rolled_back":
	default:
		return fmt.Errorf("%w: invalid migration log state", ErrProtocol)
	}
	switch row.GetActorSource() {
	case "unknown", "provided", "process-user":
	default:
		return fmt.Errorf("%w: invalid migration log actor source", ErrProtocol)
	}
	return nil
}

// ValidateMigrationTagRecord checks representable facts without interpreting
// the selected version; zero and negative versions are stored exactly.
func ValidateMigrationTagRecord(row *protocol.MigrationTagRecord) error {
	if row == nil || row.GetName() == "" || !metadataText(row.GetName()) || !metadataTimestamp(row.GetRecordedAtSeconds(), row.GetRecordedAtNanos()) {
		return fmt.Errorf("%w: invalid migration tag record", ErrProtocol)
	}
	return nil
}

// ValidatePrepareMetadataRequest checks storage preparation before dispatch.
func ValidatePrepareMetadataRequest(request *protocol.PrepareMetadataRequest) error {
	return metadataRequest(request.GetScope(), request.GetRequestId(), request)
}

// ValidateMutateMetadataRequest refuses mismatched or missing typed operations.
func ValidateMutateMetadataRequest(request *protocol.MutateMetadataRequest) error {
	if err := metadataRequest(request.GetScope(), request.GetRequestId(), request); err != nil {
		return err
	}
	switch mutation := request.GetMutation().(type) {
	case *protocol.MutateMetadataRequest_AppendLog:
		if mutation == nil {
			break
		}
		if request.GetScope().GetKind() != protocol.MetadataScope_LOG {
			break
		}
		return ValidateMigrationLogRecord(mutation.AppendLog)
	case *protocol.MutateMetadataRequest_PutTag:
		if mutation == nil {
			break
		}
		if request.GetScope().GetKind() != protocol.MetadataScope_TAGS {
			break
		}
		return ValidateMigrationTagRecord(mutation.PutTag)
	case *protocol.MutateMetadataRequest_DeleteTag:
		if mutation == nil {
			break
		}
		if request.GetScope().GetKind() != protocol.MetadataScope_TAGS || mutation.DeleteTag == "" || !metadataText(mutation.DeleteTag) {
			break
		}
		return nil
	}
	return fmt.Errorf("%w: metadata mutation does not match its store", ErrProtocol)
}

// validateMetadataResult binds every receipt to its exact request.
func validateMetadataResult(scope *protocol.MetadataScope, requestID string, result *protocol.MetadataResult) error {
	if result == nil || !proto.Equal(scope, result.GetScope()) || requestID != result.GetRequestId() || proto.Size(result) > MaxMetadataBytes {
		return fmt.Errorf("%w: metadata receipt changed request identity", ErrProtocol)
	}
	switch result.GetOutcome() {
	case protocol.MetadataResult_UNKNOWN, protocol.MetadataResult_APPLIED, protocol.MetadataResult_REJECTED:
	default:
		return fmt.Errorf("%w: unknown metadata outcome", ErrProtocol)
	}
	if result.GetDeleted() && result.GetOutcome() != protocol.MetadataResult_APPLIED {
		return fmt.Errorf("%w: inconsistent metadata deletion receipt", ErrProtocol)
	}
	return nil
}

// ValidatePrepareMetadataResult refuses preparation receipts claiming deletion.
func ValidatePrepareMetadataResult(request *protocol.PrepareMetadataRequest, result *protocol.MetadataResult) error {
	if err := validateMetadataResult(request.GetScope(), request.GetRequestId(), result); err != nil {
		return err
	}
	if result.GetDeleted() {
		return fmt.Errorf("%w: preparation cannot delete a migration tag", ErrProtocol)
	}
	return nil
}

// ValidateMutateMetadataResult checks request identity and atomic deletion facts.
func ValidateMutateMetadataResult(request *protocol.MutateMetadataRequest, result *protocol.MetadataResult) error {
	if err := validateMetadataResult(request.GetScope(), request.GetRequestId(), result); err != nil {
		return err
	}
	if result.GetDeleted() && request.GetDeleteTag() == "" {
		return fmt.Errorf("%w: only tag deletion can report a removed tag", ErrProtocol)
	}
	return nil
}

// CheckMetadataResult reports the confirmed outcome without retrying a write.
func CheckMetadataResult(result *protocol.MetadataResult) error {
	switch result.GetOutcome() {
	case protocol.MetadataResult_APPLIED:
		return nil
	case protocol.MetadataResult_REJECTED:
		return ErrExecution
	default:
		return ErrOutcomeUnknown
	}
}

// ValidateLookupMetadataTagRequest refuses unbound or malformed tag reads.
func ValidateLookupMetadataTagRequest(request *protocol.LookupMetadataTagRequest) error {
	if err := metadataRequest(request.GetScope(), request.GetRequestId(), request); err != nil {
		return err
	}
	if request.GetScope().GetKind() != protocol.MetadataScope_TAGS || request.GetName() == "" || !metadataText(request.GetName()) {
		return fmt.Errorf("%w: invalid migration tag lookup", ErrProtocol)
	}
	return nil
}

// ValidateMetadataTagResult distinguishes a missing label from version zero.
func ValidateMetadataTagResult(request *protocol.LookupMetadataTagRequest, result *protocol.MetadataTagResult) error {
	if result == nil || !proto.Equal(request.GetScope(), result.GetScope()) || request.GetRequestId() != result.GetRequestId() || proto.Size(result) > MaxMetadataBytes {
		return fmt.Errorf("%w: migration tag lookup changed request identity", ErrProtocol)
	}
	if !result.GetFound() {
		if result.GetRecord() != nil {
			return fmt.Errorf("%w: missing migration tag has a record", ErrProtocol)
		}
		return nil
	}
	if err := ValidateMigrationTagRecord(result.GetRecord()); err != nil {
		return err
	}
	if result.GetRecord().GetName() != request.GetName() {
		return fmt.Errorf("%w: migration tag lookup changed name", ErrProtocol)
	}
	return nil
}

// ValidateMetadataReadRequest checks a caller-selected snapshot handle.
func ValidateMetadataReadRequest(request *protocol.MetadataReadRequest) error {
	if err := metadataRequest(request.GetScope(), request.GetRequestId(), request); err != nil {
		return err
	}
	if request.GetReaderId() == "" || !metadataText(request.GetReaderId()) {
		return fmt.Errorf("%w: missing metadata reader identity", ErrProtocol)
	}
	return nil
}

// ValidateMetadataReadResult binds open or close acknowledgment to its handle.
func ValidateMetadataReadResult(request *protocol.MetadataReadRequest, result *protocol.MetadataReadResult) error {
	if result == nil || !proto.Equal(request.GetScope(), result.GetScope()) || request.GetReaderId() != result.GetReaderId() || request.GetRequestId() != result.GetRequestId() || proto.Size(result) > MaxMetadataBytes {
		return fmt.Errorf("%w: metadata reader receipt changed request identity", ErrProtocol)
	}
	return nil
}

// ValidateReadMetadataPageRequest checks the bound on a single page.
func ValidateReadMetadataPageRequest(request *protocol.ReadMetadataPageRequest) error {
	if err := ValidateMetadataReadRequest(request.GetReader()); err != nil {
		return err
	}
	if request.GetMaxRecords() == 0 || request.GetMaxRecords() > MaxMetadataRecords || request.GetPage() == ^uint64(0) {
		return fmt.Errorf("%w: invalid metadata page bound or index", ErrProtocol)
	}
	return nil
}

// ValidateMetadataPage refuses inconsistent, duplicate, or oversized records.
// The snapshot owner and host also enforce existence and uniqueness across pages.
func ValidateMetadataPage(request *protocol.ReadMetadataPageRequest, result *protocol.MetadataPage) error {
	if result == nil {
		return fmt.Errorf("%w: missing metadata page", ErrProtocol)
	}
	if err := ValidateMetadataReadResult(request.GetReader(), result.GetReader()); err != nil {
		return err
	}
	count := len(result.GetLogs()) + len(result.GetTags())
	if result.GetPage() != request.GetPage() || count > int(request.GetMaxRecords()) || proto.Size(result) > MaxMetadataBytes ||
		(!result.GetExists() && (count != 0 || !result.GetComplete())) || (!result.GetComplete() && count == 0) {
		return fmt.Errorf("%w: inconsistent metadata page", ErrProtocol)
	}
	kind := request.GetReader().GetScope().GetKind()
	if kind == protocol.MetadataScope_LOG && len(result.GetTags()) != 0 || kind == protocol.MetadataScope_TAGS && len(result.GetLogs()) != 0 {
		return fmt.Errorf("%w: metadata page changed record kind", ErrProtocol)
	}
	return validateMetadataPageRecords(result)
}

func validateMetadataPageRecords(result *protocol.MetadataPage) error {
	seen := make(map[string]bool, len(result.GetLogs())+len(result.GetTags()))
	for _, row := range result.GetLogs() {
		if err := ValidateMigrationLogRecord(row); err != nil {
			return err
		}
		key := row.GetRunId() + "\x00" + strconv.FormatInt(row.GetSequence(), 10)
		if seen[key] {
			return fmt.Errorf("%w: metadata page repeats a log identity", ErrProtocol)
		}
		seen[key] = true
	}
	for _, row := range result.GetTags() {
		if err := ValidateMigrationTagRecord(row); err != nil {
			return err
		}
		if seen[row.GetName()] {
			return fmt.Errorf("%w: metadata page repeats a tag identity", ErrProtocol)
		}
		seen[row.GetName()] = true
	}
	return nil
}
