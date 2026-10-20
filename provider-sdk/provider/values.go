package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// NormalizeValuesFeature advertises offline owner validation and canonical
// projections. It does not promise desired/observed comparison or planning.
const NormalizeValuesFeature = "values.normalize"

// ValueNormalizer validates and normalizes one complete batch without opening a
// session. The owner interprets every payload field or refuses the batch. It
// preserves absent, null, exact numbers, and all identity components. Canonical
// projections describe equality within the requested representation only;
// desired/observed equivalence requires a separate contextual comparison.
type ValueNormalizer interface {
	NormalizeValues(context.Context, *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error)
}

// ValidateValueInputs checks transport ownership before dispatch. It does not
// interpret payloads; the selected owner must validate their complete meaning.
func ValidateValueInputs(description *protocol.Description, request *protocol.NormalizeValuesRequest) error {
	if request == nil || description == nil || !slices.Contains(description.GetFeatures(), NormalizeValuesFeature) {
		return fmt.Errorf("%w: value normalization is unavailable", ErrProtocol)
	}
	var engine *protocol.Engine
	for _, candidate := range description.GetEngines() {
		if candidate.GetId() == request.GetEngine() {
			engine = candidate
			break
		}
	}
	if engine == nil {
		return fmt.Errorf("%w: value engine is not owned by this provider", ErrProtocol)
	}
	features := make(map[string]bool)
	for _, feature := range request.GetTargetFeatures() {
		if feature == "" || features[feature] || !slices.Contains(engine.GetFeatures(), feature) {
			return fmt.Errorf("%w: invalid or unsupported target feature", ErrProtocol)
		}
		features[feature] = true
	}
	ids := make(map[string]bool)
	for _, input := range request.GetValues() {
		if strings.TrimSpace(input.GetId()) == "" || ids[input.GetId()] {
			return fmt.Errorf("%w: missing or duplicate value ID", ErrProtocol)
		}
		ids[input.GetId()] = true
		switch input.GetRepresentation() {
		case protocol.ValueInput_DESIRED, protocol.ValueInput_OBSERVED, protocol.ValueInput_OPERATION, protocol.ValueInput_CHANGE:
		default:
			return fmt.Errorf("%w: unknown value representation", ErrProtocol)
		}
		if err := validateValueEnvelope(description.GetProviderId(), request.GetEngine(), features, input.GetValue()); err != nil {
			return err
		}
	}
	return nil
}

func validateValueEnvelope(owner, engine string, features map[string]bool, value *protocol.Envelope) error {
	if value == nil || owner == "" || value.GetProviderId() != owner || strings.TrimSpace(value.GetKind()) == "" ||
		value.GetSchemaVersion() == 0 || strings.TrimSpace(value.GetEncoding()) == "" {
		return fmt.Errorf("%w: invalid value envelope or foreign owner", ErrProtocol)
	}
	if !validValueReference(value.GetOwner(), engine) {
		return fmt.Errorf("%w: invalid value subject", ErrProtocol)
	}
	seen := make(map[string]bool)
	for _, feature := range value.GetRequiredFeatures() {
		if !features[feature] || seen[feature] {
			return fmt.Errorf("%w: unavailable or repeated value feature", ErrProtocol)
		}
		seen[feature] = true
	}
	return nil
}

func validValueReference(ref *protocol.Reference, engine string) bool {
	return ref != nil && engine != "" && ref.GetEngine() == engine && ref.GetKind() != "" && ref.GetName() != ""
}

// ValidateNormalizedValues requires complete, ordered, owner-bound results. It
// compares only envelope metadata; raw payload bytes never establish equality.
// The owner remains responsible for normalization and canonical semantics.
func ValidateNormalizedValues(description *protocol.Description, request *protocol.NormalizeValuesRequest, result *protocol.NormalizedValues) error {
	if err := ValidateValueInputs(description, request); err != nil {
		return err
	}
	if result == nil || !result.GetComplete() || len(request.GetValues()) != len(result.GetValues()) {
		return fmt.Errorf("%w: incomplete value normalization", ErrProtocol)
	}
	for i, input := range request.GetValues() {
		output := result.GetValues()[i]
		before, after := input.GetValue(), output.GetValue()
		if output.GetId() != input.GetId() || after == nil || before.GetProviderId() != after.GetProviderId() ||
			before.GetKind() != after.GetKind() || before.GetSchemaVersion() != after.GetSchemaVersion() ||
			before.GetEncoding() != after.GetEncoding() || !slices.Equal(before.GetRequiredFeatures(), after.GetRequiredFeatures()) ||
			!proto.Equal(before.GetOwner(), after.GetOwner()) {
			return fmt.Errorf("%w: normalized value changed input ownership or metadata", ErrProtocol)
		}
		if !validValueReference(output.GetNormalizedOwner(), request.GetEngine()) ||
			output.GetNormalizedOwner().GetKind() != before.GetOwner().GetKind() ||
			strings.TrimSpace(output.GetCanonicalFormat()) == "" || len(output.GetCanonicalProjection()) == 0 {
			return fmt.Errorf("%w: normalized identity or canonical projection is missing", ErrProtocol)
		}
	}
	return nil
}
