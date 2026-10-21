package provider

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// Bindings interpret selectors; coverage records state what was learned within
// them. Keeping these separate prevents an empty selection from claiming global
// absence or a partial read from broadening its own request.
func validateInspectionScope(request *protocol.InspectRequest, snapshot *protocol.Schema) error {
	if len(request.GetSchemas()) != len(snapshot.GetScopeBindings()) || len(request.GetSchemas()) > maxCoverageRegions {
		return fmt.Errorf("%w: inspection selectors lack ordered scope bindings", ErrProtocol)
	}
	var scopes []*protocol.ModelNamespace
	seen := make(map[string]bool)
	for i, selector := range request.GetSchemas() {
		binding := snapshot.GetScopeBindings()[i]
		if strings.TrimSpace(selector) == "" || !utf8.ValidString(selector) || seen[selector] || binding.GetSelector() != selector {
			return fmt.Errorf("%w: inspection selector binding changed or repeated", ErrProtocol)
		}
		seen[selector] = true
		for _, scope := range binding.GetNamespaces() {
			if err := validateModelNamespace(scope); err != nil {
				return err
			}
			scopes = append(scopes, scope)
			if len(scopes) > maxCoverageRegions {
				return fmt.Errorf("%w: too many inspection namespaces", ErrProtocol)
			}
		}
	}
	if len(request.GetSchemas()) == 0 {
		return nil
	}
	if len(snapshot.GetDispositions()) != 0 {
		return fmt.Errorf("%w: filtered dispositions require normalized scope identity", ErrProtocol)
	}
	for _, table := range snapshot.GetTables() {
		if !withinInspectionScopes(scopes, table.GetNormalizedIdentity()) {
			return fmt.Errorf("%w: inspected table is outside the requested scope", ErrProtocol)
		}
	}
	for _, record := range snapshot.GetCoverage().GetSubjects() {
		if !withinInspectionScopes(scopes, record.GetNormalizedSubject()) {
			return fmt.Errorf("%w: inspected subject is outside the requested scope", ErrProtocol)
		}
	}
	return validateInspectionRegions(scopes, snapshot.GetCoverage())
}

func validateInspectionRegions(scopes []*protocol.ModelNamespace, coverage *protocol.ModelCoverage) error {
	for _, record := range coverage.GetKinds() {
		if record.GetKnowledge().GetState() == protocol.ModelKnowledge_COMPLETE {
			return fmt.Errorf("%w: filtered inspection cannot claim global completeness", ErrProtocol)
		}
		for _, region := range record.GetRegions() {
			if !slices.ContainsFunc(scopes, func(bound *protocol.ModelNamespace) bool { return namespaceContains(bound, region.GetScope()) }) {
				return fmt.Errorf("%w: inspected region exceeds the requested scope", ErrProtocol)
			}
		}
	}
	return nil
}

func withinInspectionScopes(scopes []*protocol.ModelNamespace, ref *protocol.Reference) bool {
	if ref == nil {
		return false
	}
	point := &protocol.ModelNamespace{Database: &protocol.NamespaceComponent{Normalized: ref.GetDatabase()},
		Catalog: &protocol.NamespaceComponent{Normalized: ref.GetCatalog()}, Schema: &protocol.NamespaceComponent{Normalized: ref.GetSchema()}}
	for _, part := range ref.GetPath() {
		point.PathPrefix = append(point.PathPrefix, &protocol.NamespaceComponent{Normalized: part})
	}
	return slices.ContainsFunc(scopes, func(scope *protocol.ModelNamespace) bool { return namespaceContains(scope, point) })
}

// Containment compares structured normalized components. A wildcard in the
// child cannot fit under an exact parent, and prefix components never flatten.
func namespaceContains(parent, child *protocol.ModelNamespace) bool {
	for _, pair := range [][2]*protocol.NamespaceComponent{{parent.GetDatabase(), child.GetDatabase()}, {parent.GetCatalog(), child.GetCatalog()}, {parent.GetSchema(), child.GetSchema()}} {
		if pair[0] != nil && (pair[1] == nil || pair[0].GetNormalized() != pair[1].GetNormalized()) {
			return false
		}
	}
	if len(parent.GetPathPrefix()) > len(child.GetPathPrefix()) {
		return false
	}
	for i, part := range parent.GetPathPrefix() {
		if part.GetNormalized() != child.GetPathPrefix()[i].GetNormalized() {
			return false
		}
	}
	return true
}
