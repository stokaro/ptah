package builtin

import (
	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsast"
	"ptah.run/dialect/timescaledb/tscompare"
	"ptah.run/dialect/timescaledb/tsconvert"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsplan"
	"ptah.run/dialect/timescaledb/tsprobe"
	"ptah.run/dialect/timescaledb/tsrelation"
	"ptah.run/dialect/timescaledb/tsreport"
	"ptah.run/dialect/timescaledb/tsreverse"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/timescaledb/tssource"
	"ptah.run/engine"
)

// timescaleProvider assembles the TimescaleDB feature owner as a provider of
// its own, beside the PostgreSQL target rather than inside it: the extension's
// models coexist with PostgreSQL's without either importing the other.
//
// It serves every PostgreSQL-family target, because capability gating is what
// decides whether a target takes a hypertable. A target without the extension
// renders the skip line and records the omission, as it did when these objects
// were common nodes; registering the owner for PostgreSQL alone would refuse
// the same declaration on CockroachDB, YugabyteDB and Spanner instead.
func timescaleProvider() engine.Provider {
	models := []schemaext.Kind{tsschema.HypertableKind, tsschema.ContinuousAggregateKind}
	changes := []schemaext.Kind{tsdiff.HypertableKind, tsdiff.ContinuousAggregateKind}
	provider := engine.Provider{
		ID:          tsschema.Owner,
		Codecs:      append(append(tsschema.Codecs(), tsdiff.Codecs()...), tsast.Codecs()...),
		Annotations: []annotation.Extension{tssource.Annotations()},
	}
	for _, target := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner} {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: models, Service: tsconvert.Service{}})
		provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{Target: target,
			OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Kinds: []schemaext.Kind{tsschema.HypertableKind},
			ChangeKinds: []schemaext.Kind{tsdiff.HypertableKind}, Service: tscompare.HypertableService{}})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target,
			Kinds: []schemaext.Kind{tsschema.ContinuousAggregateKind}, ChangeKinds: []schemaext.Kind{tsdiff.ContinuousAggregateKind},
			Service: tscompare.AggregateService{}})
		provider.Reversals = append(provider.Reversals, engine.Reversal{Target: target, Kinds: changes, Service: tsreverse.Service{}})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: changes,
			ParentKinds:    []schemaext.Kind{tsschema.HypertableKind},
			OperationKinds: []schemaext.Kind{tsast.CreateHypertableKind, tsast.ContinuousAggregateKind}, Service: tsplan.Service{}})
		provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target,
			Kinds: []schemaext.Kind{tsschema.ContinuousAggregateKind}, OperationKinds: []schemaext.Kind{tsast.ContinuousAggregateKind},
			Service: tsplan.Service{}})
		provider.Normalizations = append(provider.Normalizations, engine.Normalization{Target: target,
			Kinds: []schemaext.Kind{tsschema.ContinuousAggregateKind}, Service: tsprobe.Service{}})
		for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
			provider.Relations = append(provider.Relations, engine.RelationDiscovery{Target: target,
				Representation: representation, Kinds: models, Service: tsrelation.Service{}})
		}
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation,
			Definitions: tsreport.Definitions(), Service: tsreport.Service{}})
	}
	return provider
}
