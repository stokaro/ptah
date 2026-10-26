package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// DatabaseContextFeature enables pure address resolution and context-bearing
// session receipts. Both peers negotiate it before using either addition.
const DatabaseContextFeature = "database.context"

// ValueContextFeature enables durable interpretation context separate from
// model identity, and explicit unresolved dependency facts.
const ValueContextFeature = "values.context"

// DatabaseContextResolver interprets an address without connecting, resolving
// credentials, reading the environment, or provisioning resources. Its result
// and errors must contain no credentials or connection address.
type DatabaseContextResolver interface {
	ResolveDatabaseContext(context.Context, *protocol.DatabaseContextRequest) (*protocol.DatabaseContext, error)
}

func validateContextFeatures(description *protocol.Description, engine *protocol.Engine) error {
	for _, feature := range []string{DatabaseContextFeature, ValueContextFeature} {
		if slices.Contains(engine.GetFeatures(), feature) && !slices.Contains(description.GetFeatures(), feature) {
			return fmt.Errorf("%w: engine context feature requires the provider's feature", ErrProtocol)
		}
	}
	if slices.Contains(engine.GetFeatures(), ValueContextFeature) && !slices.Contains(description.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: value context requires normalization", ErrProtocol)
	}
	return nil
}

// ValidateDatabaseContext checks a non-secret owner receipt without interpreting
// its paths. A missing context is unknown and is not a valid resolved receipt.
func ValidateDatabaseContext(value *protocol.DatabaseContext) error {
	for _, root := range []string{value.GetDatabase(), value.GetSourceDatabase()} {
		if root == "" || strings.TrimSpace(root) != root || !utf8.ValidString(root) || strings.ContainsAny(root, "\u2028\u2029") || strings.ContainsFunc(root, unicode.IsControl) {
			return fmt.Errorf("%w: database context requires exact nonempty roots", ErrProtocol)
		}
	}
	return nil
}

// ValidateDatabaseContextRequest requires an explicitly owned engine and an
// address. Refusals never repeat address text.
func ValidateDatabaseContextRequest(description *protocol.Description, request *protocol.DatabaseContextRequest) error {
	if !EngineOffers(description, request.GetEngine(), DatabaseContextFeature) || strings.TrimSpace(request.GetAddress()) == "" {
		return fmt.Errorf("%w: database context requires its negotiated engine and an address", ErrProtocol)
	}
	return nil
}

// ValidateResolvedDatabaseContext validates the complete pure resolution result.
func ValidateResolvedDatabaseContext(description *protocol.Description, request *protocol.DatabaseContextRequest, result *protocol.DatabaseContext) error {
	if err := ValidateDatabaseContextRequest(description, request); err != nil {
		return err
	}
	return ValidateDatabaseContext(result)
}

// EngineOffers reports whether the negotiated provider and exact engine both
// advertise feature. Aliases must be resolved before this boundary.
func EngineOffers(description *protocol.Description, engine, feature string) bool {
	if !slices.Contains(description.GetFeatures(), feature) {
		return false
	}
	for _, candidate := range description.GetEngines() {
		if candidate.GetId() == engine {
			return slices.Contains(candidate.GetFeatures(), feature)
		}
	}
	return false
}

// ValidateSessionContext binds an opened session to the pure resolution made
// for its address. An unnegotiated peer may carry neither context operand.
func ValidateSessionContext(description *protocol.Description, engine string, expected *protocol.DatabaseContext, session *protocol.Session) error {
	if err := ValidateSession(engine, session); err != nil {
		return err
	}
	if !EngineOffers(description, engine, DatabaseContextFeature) {
		if expected != nil || session.GetContext() != nil {
			return fmt.Errorf("%w: session database context was not negotiated", ErrProtocol)
		}
		return nil
	}
	if err := ValidateDatabaseContext(expected); err != nil {
		return err
	}
	if !proto.Equal(expected, session.GetContext()) || session.GetDatabase() != expected.GetDatabase() {
		return fmt.Errorf("%w: session differs from the resolved database context", ErrProtocol)
	}
	return nil
}
