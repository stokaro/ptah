package ydbplan

import (
	"context"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// PlanDeclarations derives CREATE operations from authored query definitions.
// The create operands express the requested command, not an observation about
// a database. They share the owner's operation and graph rules with migrations.
func (s StreamingService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	return standaloneDeclarations{kind: ydbstreaming.Kind, family: "streaming query",
		strategy: "create the declared configuration with YDB streaming-query statements",
		create: func(value schemaext.Value) (schemaext.ChangeValue, bool) {
			query, ok := value.(*ydbstreaming.Desired)
			if !ok || query == nil {
				return nil, false
			}
			return &ydbdiff.StreamingQuery{After: query.Clone().(*ydbstreaming.Desired)}, true
		},
		plan: s.PlanFeatures,
	}.planDeclarations(ctx, request)
}
