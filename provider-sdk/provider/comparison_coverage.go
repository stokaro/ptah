package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

const maxComparisonKnowledgeSources = 256

func validateComparisonCoverage(description *protocol.Description, request *protocol.CompareModelsRequest, representation protocol.ValueInput_Representation, coverage *protocol.ComparisonCoverage) error {
	if coverage == nil {
		return fmt.Errorf("%w: comparison coverage must be explicit", ErrProtocol)
	}
	kinds := make(map[string]bool)
	for _, record := range coverage.GetKinds() {
		model := record.GetModel()
		declared := findValueModel(description, request.GetEngine(), model.GetKind(), representation)
		if model == nil || declared == nil || !slices.Contains(request.GetKinds(), model.GetKind()) || !proto.Equal(model, declared) || kinds[model.GetKind()] {
			return fmt.Errorf("%w: unknown, changed, or duplicate comparison coverage model", ErrProtocol)
		}
		kinds[model.GetKind()] = true
		if err := validateComparisonKnowledge(representation, record.GetSources()); err != nil {
			return err
		}
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

func compareComparisonCoverageSubjects(a, b *protocol.ComparisonSubjectCoverage) int {
	if compared := strings.Compare(a.GetKind(), b.GetKind()); compared != 0 {
		return compared
	}
	return CompareIdentities(a.GetSubject(), b.GetSubject())
}

func validateComparisonNamespace(scope *protocol.ComparisonNamespace) error {
	if scope == nil || (len(scope.GetTargets()) == 0 && len(scope.GetSubjectKinds()) == 0 && scope.GetDatabase() == nil && scope.GetCatalog() == nil &&
		scope.GetSchema() == nil && scope.GetParent() == nil && len(scope.GetPathPrefix()) == 0 && !scope.GetPathExact()) {
		return fmt.Errorf("%w: comparison namespace has no boundary", ErrProtocol)
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

func validateComparisonKnowledge(representation protocol.ValueInput_Representation, sources []*protocol.ComparisonKnowledgeSource) error {
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
			if err := validateComparisonNamespace(region.GetScope()); err != nil {
				return err
			}
			if err := validateNamespaceKnowledge(representation, region.GetKnowledge()); err != nil {
				return err
			}
		}
	}
	return nil
}
