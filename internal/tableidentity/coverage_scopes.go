package tableidentity

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

func (b coverageBinder) scopes(scope schemaext.NamespaceScope) ([]schemaext.NamespaceScope, error) {
	if err := b.selected.Validate(); err != nil {
		return nil, err
	}
	scope.Targets = []string{b.selected.Name()}
	// Binding leaves database-qualified and namespace-qualified objects intact.
	// Such regions cannot contain a valid common table, index, or routine.
	if (scope.Database != nil && !scope.Database.Empty()) || !scope.PathPrefix.IsZero() {
		return []schemaext.NamespaceScope{scope}, nil
	}
	if len(scope.SubjectKinds) == 0 {
		return nil, scopeBindingError()
	}
	var result []schemaext.NamespaceScope
	for _, kind := range scope.SubjectKinds {
		single := scope
		single.SubjectKinds = []objectidentity.Kind{kind}
		bound, err := b.scope(single, kind)
		if err != nil {
			return nil, err
		}
		result = append(result, bound)
	}
	return result, nil
}

func (b coverageBinder) scope(scope schemaext.NamespaceScope, kind objectidentity.Kind) (schemaext.NamespaceScope, error) {
	mode := b.mode(kind)
	if mode == unchangedCoverage || (mode == ownedCoverage && scope.Parent != nil && scope.Parent.Empty()) {
		return scope, nil
	}
	// Wildcard database or path bounds mix identities that bind and identities
	// that stay qualified. Refuse that ambiguity instead of moving only one side.
	if scope.Database == nil || !scope.PathExact {
		return schemaext.NamespaceScope{}, scopeBindingError()
	}
	if mode == ownedCoverage && kind != objectidentity.KindColumn && scope.Parent == nil {
		return schemaext.NamespaceScope{}, scopeBindingError()
	}
	if mode != ownedCoverage && (scope.Catalog == nil || !scope.Catalog.Empty()) {
		return schemaext.NamespaceScope{}, scopeBindingError()
	}
	if mode == tableCoverage || mode == routineCoverage || (mode == indexCoverage && b.semantics.IndexNamespace == identifier.IndexNamespaceSchema) {
		if scope.Parent != nil && !scope.Parent.Empty() {
			return schemaext.NamespaceScope{}, scopeBindingError()
		}
		scope.Parent = &objectidentity.Part{}
	}
	return b.bindScopeParts(scope, kind, mode), nil
}

func (b coverageBinder) bindScopeParts(scope schemaext.NamespaceScope, kind objectidentity.Kind, mode coverageBinding) schemaext.NamespaceScope {
	if scope.Schema != nil {
		schema := scope.Schema.Authored()
		var part objectidentity.Part
		if mode == tableCoverage || mode == ownedCoverage {
			part = Subject(schema, "", b.selected.Name(), b.semantics).Schema
		} else {
			part = objectidentity.NewBuilder(b.semantics).SchemaScopedParts(kind, schema, "").Schema
		}
		scope.Schema = &part
	}
	if scope.Parent != nil && !scope.Parent.Empty() {
		var parent objectidentity.Part
		if mode == indexCoverage {
			parent = objectidentity.NewBuilder(b.semantics).TableParts("", scope.Parent.Source).Name
		} else {
			parent = Subject("", scope.Parent.Source, b.selected.Name(), b.semantics).Name
		}
		scope.Parent = &parent
	}
	return scope
}

func scopeBindingError() error {
	return fmt.Errorf("%w: namespace-scoped coverage requires explicit subject kinds and common identity bounds before binding", schemaext.ErrInvalidValue)
}
