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
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine"
)

// Workload and streaming planning share one batch so queries stop before pool
// mutations and resume after them. Separate registrations would hide each
// owner's contributed operations from the other during planning.
func registerWorkloadServices(provider *engine.Provider, target string) {
	provider.Conversions = append(provider.Conversions,
		engine.Conversion{Target: target, Kinds: []schemaext.Kind{ydbworkload.PoolKind}, Service: ydbconvert.PoolService{}},
		engine.Conversion{Target: target, Kinds: []schemaext.Kind{ydbworkload.ClassifierKind}, Service: ydbconvert.ClassifierService{}},
	)
	provider.Comparisons = append(provider.Comparisons,
		engine.ObjectComparison{Target: target, Kinds: []schemaext.Kind{ydbworkload.PoolKind}, ChangeKinds: []schemaext.Kind{ydbdiff.ResourcePoolKind}, Service: ydbcompare.PoolService{}},
		engine.ObjectComparison{Target: target, Kinds: []schemaext.Kind{ydbworkload.ClassifierKind}, ChangeKinds: []schemaext.Kind{ydbdiff.ResourcePoolClassifierKind}, Service: ydbcompare.ClassifierService{}},
	)
	provider.Reversals = append(provider.Reversals,
		engine.Reversal{Target: target, Kinds: []schemaext.Kind{ydbdiff.ResourcePoolKind}, Service: ydbreverse.PoolService{}},
		engine.Reversal{Target: target, Kinds: []schemaext.Kind{ydbdiff.ResourcePoolClassifierKind}, Service: ydbreverse.ClassifierService{}},
	)
	provider.Planning = append(provider.Planning, engine.Planning{Target: target,
		Kinds:          []schemaext.Kind{ydbdiff.ResourcePoolKind, ydbdiff.ResourcePoolClassifierKind, ydbdiff.StreamingQueryKind},
		OperationKinds: []schemaext.Kind{ydbast.ResourcePoolKind, ydbast.ResourcePoolClassifierKind, ydbast.StreamingQueryKind},
		Service:        ydbplan.WorkloadStreamingService{},
	})
	provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target,
		Kinds:          []schemaext.Kind{ydbworkload.PoolKind, ydbworkload.ClassifierKind, ydbstreaming.Kind},
		OperationKinds: []schemaext.Kind{ydbast.ResourcePoolKind, ydbast.ResourcePoolClassifierKind, ydbast.StreamingQueryKind, ydbast.DefaultPoolSettingsKind},
		Service:        ydbplan.WorkloadStreamingService{},
	})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting,
			engine.Reporting{Representation: representation, Definitions: ydbreport.PoolDefinitions(), Service: ydbreport.PoolService{}},
			engine.Reporting{Representation: representation, Definitions: ydbreport.ClassifierDefinitions(), Service: ydbreport.ClassifierService{}},
		)
	}
}
