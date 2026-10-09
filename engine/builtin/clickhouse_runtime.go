package builtin

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chconvert"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chplan"
	"ptah.run/dialect/clickhouse/chprepare"
	"ptah.run/dialect/clickhouse/chreport"
	"ptah.run/dialect/clickhouse/chreverse"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
)

// registerClickHouseServices assembles the ClickHouse owner. Table storage and
// skipping-index settings register their source, comparison, planning,
// reversal and reporting services together, so no stage can accept a model
// another stage would refuse or ignore.
func registerClickHouseServices(provider *engine.Provider, name string) {
	provider.Targets[0].Preparation = chprepare.Service{}
	provider.Targets[0].Creations = chprepare.Service{}
	provider.Codecs = append(provider.Codecs, chschema.Codecs()...)
	provider.Codecs = append(provider.Codecs, chschema.IndexCodecs()...)
	provider.Codecs = append(provider.Codecs, chdiff.Codecs()...)
	provider.Codecs = append(provider.Codecs, chdiff.IndexCodecs()...)
	provider.Codecs = append(provider.Codecs, chast.Codecs()...)
	provider.Properties = []engine.PropertySource{
		{Target: name, Format: schemaext.TablePlatformProperties, Definitions: chsource.Definitions(), Service: chsource.Service{}},
		{Target: name, Format: schemaext.IndexPlatformProperties, Definitions: chsource.IndexDefinitions(), Service: chsource.IndexService{}},
	}
	provider.Conversions = []engine.Conversion{{Target: name, Kinds: []schemaext.Kind{chschema.TableKind, chschema.IndexKind}, Service: chconvert.Service{}}}
	provider.FacetComparisons = []engine.FacetComparison{
		{
			Target: name, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
			Kinds: []schemaext.Kind{chschema.TableKind}, ChangeKinds: []schemaext.Kind{chdiff.TableKind}, Service: chcompare.Service{},
		},
		{
			Target: name, OwnerKinds: []objectidentity.Kind{objectidentity.KindIndex},
			Kinds: []schemaext.Kind{chschema.IndexKind}, ChangeKinds: []schemaext.Kind{chdiff.IndexKind}, Service: chcompare.IndexService{},
		},
	}
	provider.Planning = []engine.Planning{
		{
			Target: name, Kinds: []schemaext.Kind{chdiff.TableKind}, ParentKinds: []schemaext.Kind{chschema.TableKind},
			OperationKinds: []schemaext.Kind{chast.AlterTTLKind}, Service: chplan.Service{},
		},
		{
			Target: name, Kinds: []schemaext.Kind{chdiff.IndexKind}, ParentKinds: []schemaext.Kind{chschema.IndexKind},
			OperationKinds: []schemaext.Kind{chast.DropSkippingIndexKind, chast.AddSkippingIndexKind}, Service: chplan.IndexService{},
		},
	}
	provider.Reversals = []engine.Reversal{
		{Target: name, Kinds: []schemaext.Kind{chdiff.TableKind}, Service: chreverse.Service{}},
		{Target: name, Kinds: []schemaext.Kind{chdiff.IndexKind}, Service: chreverse.IndexService{}},
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting,
			engine.Reporting{Representation: representation, Definitions: chreport.Definitions(), Service: chreport.Service{}},
			engine.Reporting{Representation: representation, Definitions: chreport.IndexDefinitions(), Service: chreport.IndexService{}},
		)
	}
}
