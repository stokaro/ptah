package builtin

import (
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
)

// A YDB vector index's settings are an index facet of the YDB owner. A Go
// annotation, YAML, HCL and YQL declare them through attributes of the index,
// not index properties, so no property source is registered. Every stage is
// registered together, so no stage can accept a value another would refuse
// or ignore.
func registerYDBVectorIndexServices(provider *engine.Provider, target string) {
	provider.Codecs = append(provider.Codecs,
		slices.Concat(ydbschema.VectorIndexCodecs(), []schemaext.Codec{ydbdiff.VectorIndexCodec()}, ydbast.VectorIndexCodecs())...)
	provider.Conversions = append(provider.Conversions, engine.Conversion{
		Target: target, Kinds: []schemaext.Kind{ydbschema.VectorIndexKind}, Service: ydbconvert.VectorIndexService{},
	})
	provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{
		Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindIndex},
		Kinds: []schemaext.Kind{ydbschema.VectorIndexKind}, ChangeKinds: []schemaext.Kind{ydbdiff.VectorIndexKind},
		Service: ydbcompare.VectorIndexService{},
	})
	provider.Reversals = append(provider.Reversals, engine.Reversal{
		Target: target, Kinds: []schemaext.Kind{ydbdiff.VectorIndexKind}, Service: ydbreverse.VectorIndexService{},
	})
	provider.Planning = append(provider.Planning, engine.Planning{
		Target: target, Kinds: []schemaext.Kind{ydbdiff.VectorIndexKind}, ParentKinds: []schemaext.Kind{ydbschema.VectorIndexKind},
		OperationKinds: []schemaext.Kind{ydbast.DropVectorIndexKind, ydbast.AddVectorIndexKind}, Service: ydbplan.VectorIndexService{},
	})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{
			Representation: representation, Definitions: ydbreport.VectorIndexDefinitions(), Service: ydbreport.VectorIndexService{},
		})
	}
}
