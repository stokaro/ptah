package builtin

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine"
)

// registerSecretServices selects the YDB secret owner for target. Secrets plan
// in a batch of their own: a secret orders itself against the common
// statements that read it by path, and against the drop that frees its path,
// and no other owner's operation reads one.
func registerSecretServices(provider *engine.Provider, target string) {
	provider.Conversions = append(provider.Conversions,
		engine.Conversion{Target: target, Kinds: []schemaext.Kind{ydbsecret.Kind}, Service: ydbconvert.SecretService{}})
	provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: []schemaext.Kind{ydbsecret.Kind},
		ChangeKinds: []schemaext.Kind{ydbdiff.SecretKind}, Service: ydbcompare.SecretService{}})
	provider.Reversals = append(provider.Reversals,
		engine.Reversal{Target: target, Kinds: []schemaext.Kind{ydbdiff.SecretKind}, Service: ydbreverse.SecretService{}})
	provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: []schemaext.Kind{ydbdiff.SecretKind},
		OperationKinds: []schemaext.Kind{ydbast.SecretKind}, Service: ydbplan.SecretService{}})
	provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target, Kinds: []schemaext.Kind{ydbsecret.Kind},
		OperationKinds: []schemaext.Kind{ydbast.SecretKind}, Service: ydbplan.SecretService{}})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting,
			engine.Reporting{Representation: representation, Definitions: ydbreport.SecretDefinitions(), Service: ydbreport.SecretService{}})
	}
}
