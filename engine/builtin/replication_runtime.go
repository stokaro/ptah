package builtin

import (
	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/engine"
)

// registerReplicationServices selects the YDB async replication and transfer
// owner for target. Each kind plans in a batch of its own: a drop runs early,
// before the tables, topics and changefeeds a transfer used, and a creation or
// a change runs after the common statements, ordered against the secrets,
// topics, changefeeds and tables it reads and the replica paths it writes.
func registerReplicationServices(provider *engine.Provider, target string) {
	for _, owner := range []struct {
		kind, change, operation schemaext.Kind
		conversion              schemaext.ConversionService
		comparison              schemaext.ObjectComparisonService
		reversal                schemaext.ReversalService
		planning                interface {
			featureplan.Service
			featureplan.DeclarationService
		}
	}{
		{kind: ydbreplication.ReplicationKind, change: ydbdiff.AsyncReplicationKind, operation: ydbast.AsyncReplicationKind,
			conversion: ydbconvert.AsyncReplicationService{}, comparison: ydbcompare.AsyncReplicationService{},
			reversal: ydbreverse.AsyncReplicationService{}, planning: ydbplan.AsyncReplicationService{}},
		{kind: ydbreplication.TransferKind, change: ydbdiff.TransferKind, operation: ydbast.TransferKind,
			conversion: ydbconvert.TransferService{}, comparison: ydbcompare.TransferService{},
			reversal: ydbreverse.TransferService{}, planning: ydbplan.TransferService{}},
	} {
		provider.Conversions = append(provider.Conversions,
			engine.Conversion{Target: target, Kinds: []schemaext.Kind{owner.kind}, Service: owner.conversion})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target,
			Kinds: []schemaext.Kind{owner.kind}, ChangeKinds: []schemaext.Kind{owner.change}, Service: owner.comparison})
		provider.Reversals = append(provider.Reversals,
			engine.Reversal{Target: target, Kinds: []schemaext.Kind{owner.change}, Service: owner.reversal})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: []schemaext.Kind{owner.change},
			OperationKinds: []schemaext.Kind{owner.operation}, Service: owner.planning})
		provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target,
			Kinds: []schemaext.Kind{owner.kind}, OperationKinds: []schemaext.Kind{owner.operation}, Service: owner.planning})
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation,
			Definitions: ydbreport.ReplicationDefinitions(), Service: ydbreport.ReplicationService{}})
	}
}
