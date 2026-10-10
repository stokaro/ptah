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
	"ptah.run/dialect/clickhouse/chprobe"
	"ptah.run/dialect/clickhouse/chreport"
	"ptah.run/dialect/clickhouse/chreverse"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
)

// registerClickHouseServices assembles the ClickHouse owner. Table storage,
// skipping-index settings, materialized view refresh schedules and row
// policies register their comparison, planning, reversal and reporting
// services together, so no stage can accept a model another stage would
// refuse or ignore.
//
// Row policies are registered dormant: no source or reader produces the model
// yet, and the common path still carries ClickHouse policies, so the services
// run only for objects a caller builds itself (ADR 0020).
func registerClickHouseServices(provider *engine.Provider, name string) {
	provider.Targets[0].Preparation = chprepare.Service{}
	provider.Targets[0].Creations = chprepare.Service{}
	provider.Codecs = append(provider.Codecs, chschema.Codecs()...)
	provider.Codecs = append(provider.Codecs, chschema.IndexCodecs()...)
	provider.Codecs = append(provider.Codecs, chschema.RefreshCodecs()...)
	provider.Codecs = append(provider.Codecs, chschema.RowPolicyCodecs()...)
	provider.Codecs = append(provider.Codecs, chdiff.Codecs()...)
	provider.Codecs = append(provider.Codecs, chdiff.IndexCodecs()...)
	provider.Codecs = append(provider.Codecs, chdiff.RefreshCodecs()...)
	provider.Codecs = append(provider.Codecs, chdiff.RowPolicyCodecs()...)
	provider.Codecs = append(provider.Codecs, chast.Codecs()...)
	provider.Properties = []engine.PropertySource{
		{Target: name, Format: schemaext.TablePlatformProperties, Definitions: chsource.Definitions(), Service: chsource.Service{}},
		{Target: name, Format: schemaext.IndexPlatformProperties, Definitions: chsource.IndexDefinitions(), Service: chsource.IndexService{}},
	}
	provider.Conversions = []engine.Conversion{{Target: name,
		Kinds: []schemaext.Kind{chschema.TableKind, chschema.IndexKind, chschema.RefreshKind, chschema.RowPolicyKind}, Service: chconvert.Service{}}}
	provider.Comparisons = []engine.ObjectComparison{{Target: name, Kinds: []schemaext.Kind{chschema.RowPolicyKind},
		ChangeKinds: []schemaext.Kind{chdiff.RowPolicyKind}, Service: chcompare.RowPolicyService{}}}
	provider.Normalizations = []engine.Normalization{{Target: name, Kinds: []schemaext.Kind{chschema.RowPolicyKind}, Service: chprobe.Service{}}}
	provider.FacetComparisons = []engine.FacetComparison{
		{
			Target: name, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
			Kinds: []schemaext.Kind{chschema.TableKind}, ChangeKinds: []schemaext.Kind{chdiff.TableKind}, Service: chcompare.Service{},
		},
		{
			Target: name, OwnerKinds: []objectidentity.Kind{objectidentity.KindIndex},
			Kinds: []schemaext.Kind{chschema.IndexKind}, ChangeKinds: []schemaext.Kind{chdiff.IndexKind}, Service: chcompare.IndexService{},
		},
		{
			Target: name, OwnerKinds: []objectidentity.Kind{objectidentity.KindMatView},
			Kinds: []schemaext.Kind{chschema.RefreshKind}, ChangeKinds: []schemaext.Kind{chdiff.RefreshKind}, Service: chcompare.RefreshService{},
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
		{
			Target: name, Kinds: []schemaext.Kind{chdiff.RefreshKind},
			OperationKinds: []schemaext.Kind{chast.ModifyRefreshKind}, Service: chplan.RefreshService{},
		},
		{
			Target: name, Kinds: []schemaext.Kind{chdiff.RowPolicyKind}, ParentKinds: []schemaext.Kind{chschema.RowPolicyKind},
			OperationKinds: []schemaext.Kind{chast.RowPolicyKind}, Service: chplan.RowPolicyService{},
		},
	}
	provider.Reversals = []engine.Reversal{
		{Target: name, Kinds: []schemaext.Kind{chdiff.TableKind}, Service: chreverse.Service{}},
		{Target: name, Kinds: []schemaext.Kind{chdiff.IndexKind}, Service: chreverse.IndexService{}},
		{Target: name, Kinds: []schemaext.Kind{chdiff.RefreshKind}, Service: chreverse.RefreshService{}},
		{Target: name, Kinds: []schemaext.Kind{chdiff.RowPolicyKind}, Service: chreverse.RowPolicyService{}},
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting,
			engine.Reporting{Representation: representation, Definitions: chreport.Definitions(), Service: chreport.Service{}},
			engine.Reporting{Representation: representation, Definitions: chreport.IndexDefinitions(), Service: chreport.IndexService{}},
			engine.Reporting{Representation: representation, Definitions: chreport.RefreshDefinitions(), Service: chreport.RefreshService{}},
			engine.Reporting{Representation: representation, Definitions: chreport.RowPolicyDefinitions(), Service: chreport.RowPolicyService{}},
		)
	}
}
