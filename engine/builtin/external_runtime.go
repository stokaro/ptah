package builtin

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/engine"
)

// registerExternalServices selects the YDB external object owner for target.
// External data sources and external tables share one owner and one batch in
// every pipeline: a data source the plan drops and creates again takes the
// tables over it along.
func registerExternalServices(provider *engine.Provider, target string) {
	kinds := []schemaext.Kind{ydbexternal.SourceKind, ydbexternal.TableKind}
	changes := []schemaext.Kind{ydbdiff.ExternalDataSourceKind, ydbdiff.ExternalTableKind}
	operations := []schemaext.Kind{ydbast.ExternalDataSourceKind, ydbast.ExternalTableKind}
	provider.Conversions = append(provider.Conversions,
		engine.Conversion{Target: target, Kinds: kinds, Service: ydbconvert.ExternalService{}})
	provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: kinds,
		ChangeKinds: changes, Service: ydbcompare.ExternalService{}})
	provider.Reversals = append(provider.Reversals,
		engine.Reversal{Target: target, Kinds: changes, Service: ydbreverse.ExternalService{}})
	provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: changes,
		OperationKinds: operations, Service: ydbplan.ExternalService{}})
	provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target, Kinds: kinds,
		OperationKinds: operations, Service: ydbplan.ExternalService{}})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting,
			engine.Reporting{Representation: representation, Definitions: ydbreport.ExternalDefinitions(), Service: ydbreport.ExternalService{}})
	}
}
