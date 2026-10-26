package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// DecodeSourceFeature advertises offline interpretation of captured extension
// fragments. It does not promise common schema parsing or migration planning.
const DecodeSourceFeature = "source.decode"

// SourceDecoder interprets whole captured documents without a database session.
// Every input field must be understood or the complete batch refused. The owner
// preserves provenance and explicit model scope; unsupported syntax must never
// become an empty authoritative result. HCL evaluation belongs to the host.
type SourceDecoder interface {
	DecodeSource(context.Context, *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error)
}

// ValidateSourceRequest binds captured documents to negotiated desired models.
// Models are explicitly enrolled per document before dispatch; registry growth
// cannot enlarge their scope. No input is mutated and no source file is opened.
func ValidateSourceRequest(description *protocol.Description, request *protocol.DecodeSourceRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), DecodeSourceFeature) {
		return sourceError("source decoding is unavailable")
	}
	// Normalized output is part of this contract, using the same target-feature
	// and envelope validation as standalone normalization.
	if err := ValidateValueInputs(description, &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures()}); err != nil {
		return err
	}
	if err := ValidateSourceDocuments(request.GetEngine(), request.GetDocuments()); err != nil {
		return err
	}
	for _, document := range request.GetDocuments() {
		kinds := make(map[string]bool)
		for _, model := range document.GetModels() {
			declared := findValueModel(description, request.GetEngine(), model.GetKind(), protocol.ValueInput_DESIRED)
			if model == nil || declared == nil || !proto.Equal(model, declared) || kinds[model.GetKind()] || !slices.Contains(model.GetSourceFormats(), document.GetFormat()) {
				return sourceError("unknown, changed, duplicate, or unsupported source model")
			}
			kinds[model.GetKind()] = true
		}
		if err := validateYAMLSourceFragments(description, request.GetEngine(), document, kinds); err != nil {
			return err
		}
		if err := validateGoSourceFragments(description, request.GetEngine(), document, kinds); err != nil {
			return err
		}
		if err := validateHCLSourceFragments(description, request.GetEngine(), document, kinds); err != nil {
			return err
		}
		if err := validateGoSourceLimits(description, request.GetEngine(), document); err != nil {
			return err
		}
		if len(document.GetFragments()) != 0 && len(kinds) == 0 {
			return sourceError("fragments require explicit model scope")
		}
	}
	return nil
}

// ValidateDecodedSource requires a complete ordered result with preserved scope,
// normalized owner values, and resolvable original provenance. It validates the
// wire contract, not whether the owner correctly interpreted engine syntax.
func ValidateDecodedSource(description *protocol.Description, request *protocol.DecodeSourceRequest, result *protocol.DecodedSource) error {
	if err := ValidateSourceRequest(description, request); err != nil {
		return err
	}
	if result == nil || !result.GetComplete() || len(result.GetDocuments()) != len(request.GetDocuments()) {
		return sourceError("incomplete decoded batch")
	}
	for i, document := range request.GetDocuments() {
		output := result.GetDocuments()[i]
		if output.GetId() != document.GetId() || len(output.GetFragments()) != len(document.GetFragments()) {
			return sourceError("decoded document changed identity or fragment count")
		}
		if err := validateSourceCoverage(description, request.GetEngine(), document, output.GetCoverage()); err != nil {
			return err
		}
		if err := validateDistinctSourceValues(output); err != nil {
			return err
		}
		ids := make(map[string]bool)
		for j, fragment := range document.GetFragments() {
			decoded := output.GetFragments()[j]
			if decoded.GetId() != fragment.GetId() {
				return sourceError("decoded fragment changed identity or order")
			}
			for _, value := range decoded.GetValues() {
				id := value.GetValue().GetId()
				if id == "" || ids[id] {
					return sourceError("missing or duplicate decoded value ID")
				}
				ids[id] = true
				if err := validateSourceModelValue(description, request, document, fragment, output.GetCoverage(), value); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateSourceCoverage(description *protocol.Description, engine string, document *protocol.SourceDocument, coverage *protocol.ModelCoverage) error {
	if coverage.GetRepresentation() != protocol.ValueInput_DESIRED || len(coverage.GetKinds()) != len(document.GetModels()) {
		return sourceError("decoded coverage changed input scope")
	}
	if err := ValidateModelCoverage(description, engine, coverage); err != nil {
		return err
	}
	for i, model := range document.GetModels() {
		record := coverage.GetKinds()[i]
		if !proto.Equal(record.GetModel(), model) || record.GetKnowledge().GetState() != protocol.ModelKnowledge_COMPLETE || len(record.GetRegions()) != 0 {
			return sourceError("decoded model scope is changed or incomplete")
		}
	}
	for _, record := range coverage.GetSubjects() {
		switch record.GetKnowledge().GetState() {
		case protocol.ModelKnowledge_COMPLETE, protocol.ModelKnowledge_ABSENT, protocol.ModelKnowledge_DEFAULTED:
		default:
			return sourceError("decoded subject knowledge is incomplete")
		}
	}
	return nil
}

func validateSourceModelValue(
	description *protocol.Description, request *protocol.DecodeSourceRequest, document *protocol.SourceDocument,
	fragment *protocol.SourceFragment, coverage *protocol.ModelCoverage, value *protocol.SourceModelValue,
) error {
	normalized := value.GetValue()
	envelope := normalized.GetValue()
	if document.GetFormat() == protocol.SourceDocument_YAML && !slices.Contains(fragment.GetYamlKey().GetModels(), envelope.GetKind()) {
		return sourceError("decoded YAML value is outside its key claim")
	}
	if fragment.GetGoDirective() != nil && !slices.Contains(fragment.GetGoDirective().GetModels(), envelope.GetKind()) {
		return sourceError("decoded Go value is outside its directive claim")
	}
	if fragment.GetHclBlock() != nil && !slices.Contains(fragment.GetHclBlock().GetModels(), envelope.GetKind()) {
		return sourceError("decoded HCL value is outside its block claim")
	}
	switch value.GetPlacement() {
	case protocol.SourceModelValue_NATIVE_OBJECT:
		if envelope.GetKind() != envelope.GetOwner().GetKind() {
			return sourceError("native object identity differs from its model kind")
		}
	case protocol.SourceModelValue_FACET:
	default:
		return sourceError("unknown source value placement")
	}
	normalization := &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(),
		Values: []*protocol.ValueInput{{Id: normalized.GetId(), Representation: protocol.ValueInput_DESIRED, Value: envelope}}}
	if err := ValidateNormalizedValues(description, normalization, &protocol.NormalizedValues{Complete: true, Values: []*protocol.NormalizedValue{normalized}}); err != nil {
		return err
	}
	if !sourceSubjectCovered(coverage, normalized) {
		return sourceError("decoded value lacks matching subject coverage")
	}
	trace := value.GetSource()
	if trace.GetDocumentId() != document.GetId() || trace.GetFragmentId() != fragment.GetId() {
		return sourceError("decoded provenance changed document or fragment")
	}
	if _, err := resolveFragmentTrace(fragment, trace); err != nil {
		return fmt.Errorf("%w: decoded provenance does not resolve", err)
	}
	return nil
}

func sourceSubjectCovered(coverage *protocol.ModelCoverage, value *protocol.NormalizedValue) bool {
	for _, subject := range coverage.GetSubjects() {
		if subject.GetKind() == value.GetValue().GetKind() && proto.Equal(subject.GetSubject(), value.GetValue().GetOwner()) &&
			proto.Equal(subject.GetNormalizedSubject(), value.GetNormalizedOwner()) {
			return subject.GetKnowledge().GetState() == protocol.ModelKnowledge_COMPLETE || subject.GetKnowledge().GetState() == protocol.ModelKnowledge_DEFAULTED
		}
	}
	return false
}

func validateDistinctSourceValues(document *protocol.DecodedSourceDocument) error {
	var subjects []*protocol.ModelSubjectCoverage
	for _, fragment := range document.GetFragments() {
		for _, value := range fragment.GetValues() {
			subjects = append(subjects, &protocol.ModelSubjectCoverage{Kind: value.GetValue().GetValue().GetKind(), NormalizedSubject: value.GetValue().GetNormalizedOwner()})
		}
	}
	slices.SortFunc(subjects, compareCoverageSubjects)
	for i := 1; i < len(subjects); i++ {
		if compareCoverageSubjects(subjects[i-1], subjects[i]) == 0 {
			return sourceError("duplicate normalized source value")
		}
	}
	return nil
}
