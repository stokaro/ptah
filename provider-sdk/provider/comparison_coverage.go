package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

const maxComparisonKnowledgeSources = 256

func validateComparisonCoverage(checks *targetChecks, description *protocol.Description, request *protocol.CompareModelsRequest, representation protocol.ValueInput_Representation, coverage *protocol.ComparisonCoverage) error {
	if coverage == nil {
		return fmt.Errorf("%w: comparison coverage must be explicit", ErrProtocol)
	}
	if err := validateUnsharedCoverage(coverage); err != nil {
		return err
	}
	kinds := make(map[string]bool)
	for _, record := range coverage.GetKinds() {
		if kinds[record.GetModel().GetKind()] {
			return fmt.Errorf("%w: unknown, changed, or duplicate comparison coverage model", ErrProtocol)
		}
		if err := checks.coverageKind(description, request, representation, record); err != nil {
			return err
		}
		kinds[record.GetModel().GetKind()] = true
	}
	for _, record := range coverage.GetSubjects() {
		if !kinds[record.GetKind()] {
			return fmt.Errorf("%w: comparison subject coverage has no enrolled model", ErrProtocol)
		}
		if err := ValidateComparisonIdentity(record.GetSubject()); err != nil {
			return err
		}
		if err := validateKnowledge(representation, record.GetKnowledge()); err != nil {
			return err
		}
		if err := validateComparisonNames(record.GetTargets()); err != nil {
			return err
		}
	}
	ordered := slices.Clone(coverage.GetSubjects())
	slices.SortFunc(ordered, compareComparisonCoverageSubjects)
	for i := 1; i < len(ordered); i++ {
		if compareComparisonCoverageSubjects(ordered[i-1], ordered[i]) == 0 {
			return fmt.Errorf("%w: duplicate comparison coverage subject", ErrProtocol)
		}
	}
	return nil
}

// validateCoverageKind checks one kind of a coverage: a model the engine
// declares for the representation, unchanged, among the request's kinds, with
// valid knowledge.
func validateCoverageKind(checks *targetChecks, description *protocol.Description, request *protocol.CompareModelsRequest, representation protocol.ValueInput_Representation,
	record *protocol.ComparisonKindCoverage,
) error {
	model := record.GetModel()
	declared := findValueModel(description, request.GetEngine(), model.GetKind(), representation)
	if model == nil || declared == nil || !slices.Contains(request.GetKinds(), model.GetKind()) || !checks.sameModel(model, declared) {
		return fmt.Errorf("%w: unknown, changed, or duplicate comparison coverage model", ErrProtocol)
	}
	return validateComparisonKnowledge(representation, record.GetSources(), comparisonSubtreeRefusal(description, request))
}

func compareComparisonCoverageSubjects(a, b *protocol.ComparisonSubjectCoverage) int {
	if compared := strings.Compare(a.GetKind(), b.GetKind()); compared != 0 {
		return compared
	}
	return CompareIdentities(a.GetSubject(), b.GetSubject())
}

// validateComparisonNamespace checks one region's scope; subtrees is the
// error a subtree region meets, nil where subtrees were negotiated.
func validateComparisonNamespace(scope *protocol.ComparisonNamespace, subtrees error) error {
	if scope == nil || (len(scope.GetTargets()) == 0 && len(scope.GetSubjectKinds()) == 0 && scope.GetDatabase() == nil && scope.GetCatalog() == nil &&
		scope.GetSchema() == nil && scope.GetParent() == nil && len(scope.GetPathPrefix()) == 0 && !scope.GetPathExact()) {
		return fmt.Errorf("%w: comparison namespace has no boundary", ErrProtocol)
	}
	if err := comparisonSubtreeError(scope, subtrees); err != nil {
		return err
	}
	for _, names := range [][]string{scope.GetTargets(), scope.GetSubjectKinds()} {
		if err := validateComparisonNames(names); err != nil {
			return err
		}
	}
	for _, part := range []*protocol.ComparisonPart{scope.GetDatabase(), scope.GetCatalog(), scope.GetSchema(), scope.GetParent()} {
		if err := validateComparisonPart(part); err != nil {
			return err
		}
	}
	for _, part := range scope.GetPathPrefix() {
		if part == nil || part.GetSource() == "" {
			return fmt.Errorf("%w: comparison namespace has an empty path component", ErrProtocol)
		}
		if err := validateComparisonPart(part); err != nil {
			return err
		}
	}
	return nil
}

func validateComparisonKnowledge(representation protocol.ValueInput_Representation, sources []*protocol.ComparisonKnowledgeSource, subtrees error) error {
	if len(sources) > maxComparisonKnowledgeSources {
		return fmt.Errorf("%w: too many comparison knowledge sources", ErrProtocol)
	}
	for _, source := range sources {
		if err := validateNamespaceKnowledge(representation, source.GetOutside()); err != nil {
			return err
		}
		if len(source.GetRegions()) > maxCoverageRegions {
			return fmt.Errorf("%w: too many comparison knowledge regions", ErrProtocol)
		}
		for _, region := range source.GetRegions() {
			if err := validateComparisonNamespace(region.GetScope(), subtrees); err != nil {
				return err
			}
			if err := validateNamespaceKnowledge(representation, region.GetKnowledge()); err != nil {
				return err
			}
		}
	}
	return nil
}
