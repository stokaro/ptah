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
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine"
)

// registerTopicServices selects the YDB topic owner for target. Topics plan in
// a batch of their own: a topic orders itself against the common statements
// that read it by path, a transfer's, and against the drops and creations at
// its path and above it.
func registerTopicServices(provider *engine.Provider, target string) {
	provider.Conversions = append(provider.Conversions,
		engine.Conversion{Target: target, Kinds: []schemaext.Kind{ydbtopic.Kind}, Service: ydbconvert.TopicService{}})
	provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: []schemaext.Kind{ydbtopic.Kind},
		ChangeKinds: []schemaext.Kind{ydbdiff.TopicKind}, Service: ydbcompare.TopicService{}})
	provider.Reversals = append(provider.Reversals,
		engine.Reversal{Target: target, Kinds: []schemaext.Kind{ydbdiff.TopicKind}, Service: ydbreverse.TopicService{}})
	provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: []schemaext.Kind{ydbdiff.TopicKind},
		OperationKinds: []schemaext.Kind{ydbast.TopicKind}, Service: ydbplan.TopicService{}})
	provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target, Kinds: []schemaext.Kind{ydbtopic.Kind},
		OperationKinds: []schemaext.Kind{ydbast.TopicKind}, Service: ydbplan.TopicService{}})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting,
			engine.Reporting{Representation: representation, Definitions: ydbreport.TopicDefinitions(), Service: ydbreport.TopicService{}})
	}
}
