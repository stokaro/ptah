package provider

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// DevCleanupFeature advertises the dev database cleanup: listing what a
// cleanup would remove from a session's dev database and removing it. The
// host uses it to judge a dev database clean before a replay and to empty it
// between runs. Each supporting engine also declares it.
const DevCleanupFeature = "database.dev-cleanup"

// maxDiagnosticBytes bounds a provider-authored line a host may show: a
// refusal, a reason or a message. It names a subject and never quotes
// statement text or a value.
const maxDiagnosticBytes = 512

// DevCleaner lists and removes the objects of a dev database on a live
// session. The list and the cleanup read the same set, so an empty list means
// a cleanup has nothing to do. The server's own objects and storage the
// provider keeps for the session, such as a lock it holds, are left out of
// both. The host decides that the session's database is a dev database; the
// owner cannot, and must not be asked to clean anything else.
type DevCleaner interface {
	// ListDevObjects returns every object in scope, or an error. A partial list
	// is never a reply.
	ListDevObjects(context.Context, *protocol.ListDevObjectsRequest) (*protocol.DevObjects, error)
	// CleanDevDatabase removes every object in scope. An object it cannot
	// remove stops it before it removes anything, and the reply names it.
	CleanDevDatabase(context.Context, *protocol.CleanDevRequest) (*protocol.DevCleanup, error)
}

// diagnosticLine reports provider-authored text a host may show on one line.
func diagnosticLine(text string) bool {
	return comparisonReason(text) && len(text) <= maxDiagnosticBytes
}

func validateDevScope(sessionID string, schemas []string) error {
	if sessionID == "" {
		return fmt.Errorf("%w: dev cleanup requires a session", ErrProtocol)
	}
	for i, schema := range schemas {
		if strings.TrimSpace(schema) == "" || strings.ContainsRune(schema, 0) || slices.Contains(schemas[:i], schema) {
			return fmt.Errorf("%w: dev cleanup names an empty, invalid, or repeated schema", ErrProtocol)
		}
	}
	return nil
}

// ValidateListDevObjectsRequest checks the session and the scope.
func ValidateListDevObjectsRequest(request *protocol.ListDevObjectsRequest) error {
	if request == nil {
		return fmt.Errorf("%w: dev object list request is missing", ErrProtocol)
	}
	return validateDevScope(request.GetSessionId(), request.GetSchemas())
}

// ValidateDevObjects refuses an incomplete list, an object without a kind or
// a name, an object listed twice, and a refusal that is not one line.
func ValidateDevObjects(request *protocol.ListDevObjectsRequest, result *protocol.DevObjects) error {
	if err := ValidateListDevObjectsRequest(request); err != nil {
		return err
	}
	if result == nil || !result.GetComplete() {
		return fmt.Errorf("%w: dev object list is incomplete", ErrIncomplete)
	}
	return validateDevObjectList(result.GetObjects())
}

// validateDevObjectList checks each object's identity and refusal text, and
// that no object is listed twice.
func validateDevObjectList(objects []*protocol.DevObject) error {
	for i, object := range objects {
		ref := object.GetObject()
		if ref == nil || ref.GetName() == "" || ref.GetKind() == "" {
			return fmt.Errorf("%w: dev object needs a kind and a name", ErrProtocol)
		}
		if slices.ContainsFunc(objects[:i], func(seen *protocol.DevObject) bool { return proto.Equal(seen.GetObject(), ref) }) {
			return fmt.Errorf("%w: dev object is listed twice", ErrProtocol)
		}
		if object.GetRefusal() != "" && !diagnosticLine(object.GetRefusal()) {
			return fmt.Errorf("%w: dev object refusal is not one line of text", ErrProtocol)
		}
	}
	return nil
}

// ValidateCleanDevRequest checks the session, the request ID and the scope.
func ValidateCleanDevRequest(request *protocol.CleanDevRequest) error {
	if request == nil || request.GetRequestId() == "" {
		return fmt.Errorf("%w: dev cleanup requires a request ID", ErrProtocol)
	}
	return validateDevScope(request.GetSessionId(), request.GetSchemas())
}

// ValidateDevCleanup checks the request binding and the outcome's shape: a
// refusal names at least one object and why it stopped, and no other outcome
// names one.
func ValidateDevCleanup(request *protocol.CleanDevRequest, result *protocol.DevCleanup) error {
	if err := ValidateCleanDevRequest(request); err != nil {
		return err
	}
	if result == nil || result.GetRequestId() != request.GetRequestId() {
		return fmt.Errorf("%w: dev cleanup result changed its request binding", ErrProtocol)
	}
	if result.GetMessage() != "" && !diagnosticLine(result.GetMessage()) {
		return fmt.Errorf("%w: dev cleanup message is not one line of text", ErrProtocol)
	}
	switch result.GetOutcome() {
	case protocol.DevCleanup_REFUSED:
		if len(result.GetBlocking()) == 0 {
			return fmt.Errorf("%w: a refused dev cleanup names no object", ErrProtocol)
		}
		for _, object := range result.GetBlocking() {
			if object.GetRefusal() == "" {
				return fmt.Errorf("%w: a blocking dev object gives no reason", ErrProtocol)
			}
		}
		return validateDevObjectList(result.GetBlocking())
	case protocol.DevCleanup_CLEANED, protocol.DevCleanup_UNKNOWN:
		if len(result.GetBlocking()) != 0 {
			return fmt.Errorf("%w: only a refused dev cleanup names objects", ErrProtocol)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown dev cleanup outcome", ErrProtocol)
	}
}

// DevCleanupError records a cleanup that did not leave its scope empty.
// UNKNOWN wraps ErrOutcomeUnknown; a confirmed refusal wraps ErrExecution.
// Provider-authored text is not included.
type DevCleanupError struct {
	Outcome protocol.DevCleanup_Outcome
}

// Error describes the outcome without provider diagnostics.
func (e *DevCleanupError) Error() string {
	return fmt.Sprintf("provider dev cleanup outcome is %s", e.Outcome)
}

// Unwrap distinguishes an uncertain cleanup from a confirmed refusal.
func (e *DevCleanupError) Unwrap() error {
	if e.Outcome == protocol.DevCleanup_UNKNOWN {
		return ErrOutcomeUnknown
	}
	return ErrExecution
}

// CheckDevCleanup accepts only CLEANED. A malformed reply leaves the outcome
// unknown, and the caller must treat the database as not clean.
func CheckDevCleanup(request *protocol.CleanDevRequest, result *protocol.DevCleanup) error {
	if err := ValidateDevCleanup(request, result); err != nil {
		return errors.Join(err, &DevCleanupError{})
	}
	if result.GetOutcome() == protocol.DevCleanup_CLEANED {
		return nil
	}
	return &DevCleanupError{Outcome: result.GetOutcome()}
}
