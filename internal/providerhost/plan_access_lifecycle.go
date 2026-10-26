package providerhost

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/featurehost"
)

// Native access follows its object's lifecycle. Its source position among
// unrelated tables cannot constrain a provider that replaces one of them.
// Principal edits and repeated writes to the same access resource retain their
// source order; the native object's effects and owner supply its other edges.
func nativeAccessWindows(request featureplan.Request, nodes []ast.Node, metadata []featureplan.CommonStep, selections *scopeSelections) ([]int, []featurehost.CommonDependency, error) {
	if selections == nil {
		return nil, nil, nil
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	grants := grantObjects{selected: selections.selected}
	var movable []int
	access := make(map[int]objectidentity.Key)
	for index, node := range nodes {
		var objectType string
		switch typed := node.(type) {
		case *ast.GrantPrivilegeNode:
			objectType = typed.ObjectType
		case *ast.RevokePrivilegeNode:
			objectType = typed.ObjectType
		default:
			continue
		}
		if !grants.OwnsGrantType(objectType) {
			continue
		}
		expected, _, err := commonObjectMetadata(builder, nil, node, selections)
		if err != nil {
			return nil, nil, err
		}
		if index >= len(metadata) || len(expected.Effects) != 2 || !slices.EqualFunc(metadata[index].Effects, expected.Effects, func(a, b plangraph.Effect) bool {
			return a.Action == b.Action && a.Subject.Key() == b.Subject.Key()
		}) {
			return nil, nil, fmt.Errorf("%w: native access operand differs from its established footprint", schemaext.ErrInvalidValue)
		}
		movable = append(movable, index)
		access[index] = expected.Effects[1].Subject.Key()
	}
	var dependencies []featurehost.CommonDependency
	for _, index := range movable {
		for other, node := range nodes {
			_, _, principal := principalEffect(builder, node)
			if key, known := access[other]; !principal && (!known || key != access[index]) {
				continue
			}
			before, after := min(index, other), max(index, other)
			edge := featurehost.CommonDependency{Before: before, After: after}
			if before != after && !slices.Contains(dependencies, edge) {
				dependencies = append(dependencies, edge)
			}
		}
	}
	return movable, dependencies, nil
}
