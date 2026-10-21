package provider

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

const maxCoverageRegions = 4096

func validateNamespaceKnowledge(representation protocol.ValueInput_Representation, knowledge *protocol.ModelKnowledge) error {
	if knowledge.GetState() == protocol.ModelKnowledge_ABSENT || knowledge.GetState() == protocol.ModelKnowledge_DEFAULTED {
		return fmt.Errorf("%w: absence and defaults require a specific subject", ErrProtocol)
	}
	return validateKnowledge(representation, knowledge)
}

func validateCoverageRegions(representation protocol.ValueInput_Representation, regions []*protocol.ModelNamespaceKnowledge) error {
	if len(regions) > maxCoverageRegions {
		return fmt.Errorf("%w: too many model namespace regions", ErrProtocol)
	}
	for _, region := range regions {
		if err := validateModelNamespace(region.GetScope()); err != nil {
			return err
		}
		if err := validateNamespaceKnowledge(representation, region.GetKnowledge()); err != nil {
			return err
		}
	}
	ordered := slices.Clone(regions)
	slices.SortFunc(ordered, func(a, b *protocol.ModelNamespaceKnowledge) int {
		return compareModelNamespaces(a.GetScope(), b.GetScope())
	})
	for i := 1; i < len(ordered); i++ {
		if compareModelNamespaces(ordered[i-1].GetScope(), ordered[i].GetScope()) == 0 {
			return fmt.Errorf("%w: duplicate normalized model namespace", ErrProtocol)
		}
	}
	return nil
}

func validateModelNamespace(scope *protocol.ModelNamespace) error {
	if scope.GetDatabase() == nil && scope.GetCatalog() == nil && scope.GetSchema() == nil && len(scope.GetPathPrefix()) == 0 {
		return fmt.Errorf("%w: model namespace has no boundary", ErrProtocol)
	}
	for _, component := range []*protocol.NamespaceComponent{scope.GetDatabase(), scope.GetCatalog(), scope.GetSchema()} {
		if component != nil && !validNamespaceComponent(component, true) {
			return fmt.Errorf("%w: model namespace qualifier lacks source or normalized spelling", ErrProtocol)
		}
	}
	for _, component := range scope.GetPathPrefix() {
		if !validNamespaceComponent(component, false) {
			return fmt.Errorf("%w: model namespace path has an invalid component", ErrProtocol)
		}
	}
	return nil
}

func validNamespaceComponent(component *protocol.NamespaceComponent, allowEmpty bool) bool {
	return component != nil && utf8.ValidString(component.GetSource()) && utf8.ValidString(component.GetNormalized()) &&
		(component.GetSource() == "") == (component.GetNormalized() == "") && (allowEmpty || component.GetSource() != "")
}

// Presence is part of a namespace: an empty qualifier cannot equal a wildcard.
// Comparison keys omit source spelling but retain each path component boundary.
func compareModelNamespaces(a, b *protocol.ModelNamespace) int {
	for _, compared := range []int{
		compareNamespaceComponents(a.GetDatabase(), b.GetDatabase()),
		compareNamespaceComponents(a.GetCatalog(), b.GetCatalog()),
		compareNamespaceComponents(a.GetSchema(), b.GetSchema()),
		slices.CompareFunc(a.GetPathPrefix(), b.GetPathPrefix(), compareNamespaceComponents),
	} {
		if compared != 0 {
			return compared
		}
	}
	return 0
}

func compareNamespaceComponents(a, b *protocol.NamespaceComponent) int {
	if a == nil && b != nil {
		return -1
	}
	if a != nil && b == nil {
		return 1
	}
	return strings.Compare(a.GetNormalized(), b.GetNormalized())
}
