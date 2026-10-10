package ydbplan

import (
	"context"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
)

// PlanDeclarations derives one CREATE SECRET per declared secret, with the
// value its variable holds when the statement runs. The operations share the
// owner's graph rules with migrations, so a secret precedes the data sources
// that read it and a declared table at its path is refused.
func (s SecretService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	return standaloneDeclarations{kind: ydbsecret.Kind, family: "secret",
		strategy: "create the declared secret with the value its variable holds",
		create: func(value schemaext.Value) (schemaext.ChangeValue, bool) {
			secret, ok := value.(*ydbsecret.Desired)
			if !ok || secret == nil {
				return nil, false
			}
			return &ydbdiff.Secret{After: new(*secret)}, true
		},
		plan: s.PlanFeatures,
	}.planDeclarations(ctx, request)
}
