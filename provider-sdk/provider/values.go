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
	_, features, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
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
		if err := validateValueContext(description, request.GetEngine(), features, input.GetValue()); err != nil {
			return err
		}
		if err := validateValueEnvelope(description.GetProviderId(), request.GetEngine(), features, input.GetValue()); err != nil {
			return err
		}
		model := findValueModel(description, request.GetEngine(), input.GetValue().GetKind(), input.GetRepresentation())
		if !envelopeMatchesModel(input.GetValue(), model) {
			return fmt.Errorf("%w: value does not match the negotiated model definition", ErrProtocol)
		}
	}
	return nil
}

func validateValueEnvelope(owner, engine string, features map[string]bool, value *protocol.Envelope) error {
	if value.GetContext() != nil {
		if !features[ValueContextFeature] {
			return fmt.Errorf("%w: value context was not negotiated", ErrProtocol)
		}
		if err := ValidateDatabaseContext(value.GetContext()); err != nil {
			return err
		}
	}
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
		if err := validateContextReadiness(description, request, output); err != nil {
			return err
		}
		if err := validateNormalizationFacts(input.GetRepresentation(), output); err != nil {
			return err
		}
		before := input.GetValue()
		if err := validateNormalizedMetadata(input, output); err != nil {
			return err
		}
		model := findValueModel(description, request.GetEngine(), before.GetKind(), input.GetRepresentation())
		if !validValueReference(output.GetNormalizedOwner(), request.GetEngine()) ||
			output.GetNormalizedOwner().GetKind() != before.GetOwner().GetKind() ||
			output.GetCanonicalFormat() != model.GetCanonicalFormat() || len(output.GetCanonicalProjection()) == 0 {
			return fmt.Errorf("%w: normalized identity or canonical projection is missing", ErrProtocol)
		}
	}
	return nil
}

// Normalization and inspection bind values to the same negotiated target.
func valueTarget(description *protocol.Description, name string, targetFeatures []string) (*protocol.Engine, map[string]bool, error) {
	if !validName(name) {
		return nil, nil, fmt.Errorf("%w: value engine is missing", ErrProtocol)
	}
	var engine *protocol.Engine
	for _, candidate := range description.GetEngines() {
		if candidate.GetId() == name {
			engine = candidate
			break
		}
	}
	if engine == nil {
		return nil, nil, fmt.Errorf("%w: value engine is not owned by this provider", ErrProtocol)
	}
	if err := validateValueModels(engine.GetValueModels()); err != nil {
		return nil, nil, err
	}
	features := make(map[string]bool)
	for _, feature := range targetFeatures {
		if feature == "" || features[feature] || !slices.Contains(engine.GetFeatures(), feature) {
			return nil, nil, fmt.Errorf("%w: invalid or unsupported target feature", ErrProtocol)
		}
		features[feature] = true
	}
	return engine, features, nil
}

func validateNormalizedMetadata(input *protocol.ValueInput, output *protocol.NormalizedValue) error {
	before, after := input.GetValue(), output.GetValue()
	if output.GetId() != input.GetId() || after == nil || before.GetProviderId() != after.GetProviderId() ||
		before.GetKind() != after.GetKind() || before.GetSchemaVersion() != after.GetSchemaVersion() ||
		before.GetEncoding() != after.GetEncoding() || before.GetDefinitionSha256() != after.GetDefinitionSha256() || !slices.Equal(before.GetRequiredFeatures(), after.GetRequiredFeatures()) ||
		!proto.Equal(before.GetOwner(), after.GetOwner()) || !proto.Equal(before.GetContext(), after.GetContext()) {
		return fmt.Errorf("%w: normalized value changed input ownership or metadata", ErrProtocol)
	}
	return nil
}
