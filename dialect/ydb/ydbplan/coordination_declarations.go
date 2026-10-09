package ydbplan

import (
	"context"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
)

// PlanDeclarations derives CREATE operations from authored node definitions.
// The create operands express the requested command, not an observation about
// a database. They share the owner's operation and graph rules with migrations.
func (s CoordinationService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	return standaloneDeclarations{kind: ydbcoordination.Kind, family: "coordination node",
		strategy: "create the declared configuration through the coordination service",
		create: func(value schemaext.Value) (schemaext.ChangeValue, bool) {
			node, ok := value.(*ydbcoordination.Desired)
			if !ok || node == nil {
				return nil, false
			}
			return &ydbdiff.CoordinationNode{After: new(*node)}, true
		},
		plan: s.PlanFeatures,
	}.planDeclarations(ctx, request)
}
