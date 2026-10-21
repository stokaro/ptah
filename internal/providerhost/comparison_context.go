package providerhost

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

type modelComparer struct {
	target      *target
	declaration *protocol.ModelComparison
}

func (s modelComparer) request(target string, rules identifier.Semantics, capabilities capability.Capabilities, kinds []schemaext.Kind) (*protocol.CompareModelsRequest, error) {
	if target != s.target.engine.GetId() {
		return nil, fmt.Errorf("%w: comparison target changed", schemaext.ErrInvalidValue)
	}
	identifiers, err := comparisonIdentifiers(rules)
	if err != nil {
		return nil, err
	}
	if err := capabilities.Validate(); err != nil {
		return nil, err
	}
	result := &protocol.CompareModelsRequest{Engine: target, ComparisonId: s.declaration.GetId(), Identifiers: identifiers, Capabilities: make(map[string]bool), Kinds: comparisonKinds(kinds)}
	for name, value := range capabilities.All() {
		result.Capabilities[string(name)] = value
	}
	return result, nil
}

func comparisonIdentifiers(rules identifier.Semantics) (*protocol.ComparisonIdentifiers, error) {
	comparisons := map[identifier.Comparison]protocol.IdentifierRules_Comparison{identifier.ComparisonExact: protocol.IdentifierRules_EXACT, identifier.ComparisonASCIIInsensitive: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE}
	namespaces := map[identifier.IndexNamespace]protocol.IdentifierRules_IndexNamespace{identifier.IndexNamespaceTable: protocol.IdentifierRules_TABLE, identifier.IndexNamespaceSchema: protocol.IdentifierRules_SCHEMA}
	result := &protocol.ComparisonIdentifiers{DefaultSchema: rules.DefaultSchema,
		CatalogCollation: rules.CatalogCollation,
		Rules: &protocol.IdentifierRules{TableNames: comparisons[rules.TableNames],
			ColumnNames:    comparisons[rules.ColumnNames],
			IndexNames:     comparisons[rules.IndexNames],
			IndexNamespace: namespaces[rules.IndexNamespace]}}
	if err := provider.ValidateIdentifierRules(result.Rules); err != nil {
		return nil, err
	}
	for _, resolved := range rules.ResolvedNames {
		result.ResolvedNames = append(result.ResolvedNames, &protocol.ResolvedIdentifier{Name: resolved.Name, Key: resolved.Key})
	}
	return result, nil
}

func comparisonKinds(kinds []schemaext.Kind) []string {
	result := make([]string, len(kinds))
	for i, kind := range kinds {
		result[i] = string(kind)
	}
	return result
}

func boundKinds(kinds []string) []schemaext.Kind {
	result := make([]schemaext.Kind, len(kinds))
	for i, kind := range kinds {
		result[i] = schemaext.Kind(kind)
	}
	return result
}

func comparisonParents(parents []schemaext.ParentState) []*protocol.ComparisonParent {
	result := make([]*protocol.ComparisonParent, len(parents))
	for i, parent := range parents {
		result[i] = &protocol.ComparisonParent{Subject: comparisonIdentity(parent.Subject), Desired: parent.Desired, Current: parent.Current}
	}
	return result
}

func comparisonConstraints(constraints []schemaext.ConstraintContext) []*protocol.ComparisonConstraint {
	result := make([]*protocol.ComparisonConstraint, len(constraints))
	for i, constraint := range constraints {
		result[i] = &protocol.ComparisonConstraint{Subject: comparisonIdentity(constraint.Subject), Type: constraint.Type, Columns: append([]string(nil), constraint.Columns...)}
	}
	return result
}

func boundUndecided(values []*protocol.ComparisonUndecided) []schemaext.UndecidedChange {
	result := make([]schemaext.UndecidedChange, len(values))
	for i, value := range values {
		result[i] = schemaext.UndecidedChange{Kind: schemaext.Kind(value.GetKind()), Subject: boundIdentity(value.GetSubject()), Reason: value.GetReason()}
	}
	return result
}
