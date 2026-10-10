package schemadiff

import (
	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

type comparisonScope struct {
	desired *schemamodel.Database
	current *catalog.Database
	target  schemaext.TargetSelection
}

// resolveComparisonScope captures one naming selection before filtering either
// input. Callers use its canonical name for capabilities and validation too.
func resolveComparisonScope(desired *schemamodel.Database, current *catalog.Database, name string, semantics identifier.Semantics,
	resolver schemaext.TargetResolver,
) (comparisonScope, error) {
	selected, err := resolver.ResolveTarget(name)
	if err != nil {
		return comparisonScope{}, err
	}
	desired, current, err = scopeComparison(desired, current, selected, semantics)
	if err != nil {
		return comparisonScope{}, err
	}
	return comparisonScope{desired: desired, current: current, target: selected}, nil
}

// scopeComparison applies declaration scope to both sides together. Forgetting
// the omitted names before suppressing observed objects turns an exclusion into
// a requested drop. The same projection is used before target checks and in the
// pure comparison funnel. semantics are the identifiers the comparison binds
// feature objects with.
func scopeComparison(desired *schemamodel.Database, current *catalog.Database, target schemaext.TargetSelection,
	semantics identifier.Semantics,
) (*schemamodel.Database, *catalog.Database, error) {
	omitted, err := schemamodel.OmissionsForTarget(desired, target)
	if err != nil {
		return nil, nil, err
	}
	scoped, err := schemamodel.ScopeToTarget(desired, target)
	if err != nil {
		return nil, nil, err
	}
	return scoped, suppressScopedAway(current, omitted, target.Name(), semantics.Normalize(target.Name())), nil
}
