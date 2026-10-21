package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ValidateModelCoverage binds captured knowledge to exact negotiated models.
// Empty explicit coverage enrolls no models. It never means all models known
// by the current provider, and a kind's absence does not establish an empty
// namespace. Defaults are author intent and are refused for observations.
func ValidateModelCoverage(description *protocol.Description, engine string, coverage *protocol.ModelCoverage) error {
	if !validName(engine) || coverage == nil || (coverage.GetRepresentation() != protocol.ValueInput_DESIRED && coverage.GetRepresentation() != protocol.ValueInput_OBSERVED) {
		return fmt.Errorf("%w: coverage requires an explicit source representation", ErrProtocol)
	}
	var selected bool
	for _, target := range description.GetEngines() {
		if target.GetId() == engine {
			if err := validateValueModels(target.GetValueModels()); err != nil {
				return err
			}
			selected = true
		}
	}
	if !selected {
		return fmt.Errorf("%w: coverage engine is not owned by the provider", ErrProtocol)
	}
	kinds := make(map[string]bool)
	for _, record := range coverage.GetKinds() {
		model := record.GetModel()
		declared := findValueModel(description, engine, model.GetKind(), coverage.GetRepresentation())
		if model == nil || declared == nil || !proto.Equal(model, declared) || kinds[model.GetKind()] {
			return fmt.Errorf("%w: unknown, changed, or duplicate coverage model", ErrProtocol)
		}
		if err := validateNamespaceKnowledge(coverage.GetRepresentation(), record.GetKnowledge()); err != nil {
			return err
		}
		if err := validateCoverageRegions(coverage.GetRepresentation(), record.GetRegions()); err != nil {
			return err
		}
		kinds[model.GetKind()] = true
	}
	if err := validateCoverageSubjects(engine, coverage, kinds); err != nil {
		return err
	}

	subjects := slices.Clone(coverage.GetSubjects())
	slices.SortFunc(subjects, compareCoverageSubjects)
	for i := 1; i < len(subjects); i++ {
		if compareCoverageSubjects(subjects[i-1], subjects[i]) == 0 {
			return fmt.Errorf("%w: duplicate normalized coverage subject", ErrProtocol)
		}
	}
	return nil
}

func validateKnowledge(representation protocol.ValueInput_Representation, knowledge *protocol.ModelKnowledge) error {
	switch knowledge.GetState() {
	case protocol.ModelKnowledge_UNINSPECTED, protocol.ModelKnowledge_UNREPRESENTABLE:
		if strings.TrimSpace(knowledge.GetReason()) == "" {
			return fmt.Errorf("%w: unknown model knowledge requires a reason", ErrProtocol)
		}
	case protocol.ModelKnowledge_COMPLETE, protocol.ModelKnowledge_ABSENT:
	case protocol.ModelKnowledge_DEFAULTED:
		if representation != protocol.ValueInput_DESIRED {
			return fmt.Errorf("%w: observed coverage cannot request defaults", ErrProtocol)
		}
	default:
		return fmt.Errorf("%w: model knowledge is unspecified or unknown", ErrProtocol)
	}
	return nil
}

func compareCoverageSubjects(left, right *protocol.ModelSubjectCoverage) int {
	if compared := strings.Compare(left.GetKind(), right.GetKind()); compared != 0 {
		return compared
	}
	return compareReferences(left.GetNormalizedSubject(), right.GetNormalizedSubject())
}

// compareReferences compares already-normalized identity components. Display
// spelling is excluded. It neither folds names nor flattens paths into strings.
func compareReferences(left, right *protocol.Reference) int {
	for _, compared := range []int{
		strings.Compare(left.GetEngine(), right.GetEngine()), strings.Compare(left.GetDatabase(), right.GetDatabase()),
		strings.Compare(left.GetCatalog(), right.GetCatalog()), strings.Compare(left.GetSchema(), right.GetSchema()),
		slices.Compare(left.GetPath(), right.GetPath()), strings.Compare(left.GetKind(), right.GetKind()),
		strings.Compare(left.GetName(), right.GetName()), slices.Compare(left.GetOverload(), right.GetOverload()),
	} {
		if compared != 0 {
			return compared
		}
	}
	return 0
}

func validateCoverageSubjects(engine string, coverage *protocol.ModelCoverage, kinds map[string]bool) error {
	for _, record := range coverage.GetSubjects() {
		if !kinds[record.GetKind()] || !validValueReference(record.GetSubject(), engine) ||
			!validValueReference(record.GetNormalizedSubject(), engine) || record.GetSubject().GetKind() != record.GetNormalizedSubject().GetKind() {
			return fmt.Errorf("%w: subject coverage requires an enrolled model and structured identity", ErrProtocol)
		}
		if err := validateKnowledge(coverage.GetRepresentation(), record.GetKnowledge()); err != nil {
			return err
		}
	}
	return nil
}
