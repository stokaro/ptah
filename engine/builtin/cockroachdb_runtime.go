package builtin

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbcompare"
	"ptah.run/dialect/cockroachdb/crdbconvert"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbplan"
	"ptah.run/dialect/cockroachdb/crdbreport"
	"ptah.run/dialect/cockroachdb/crdbreverse"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/crdbsource"
	"ptah.run/engine"
)

// CockroachDB row-level TTL is a table facet. Every stage it passes through is
// registered here, from one descriptor, so no stage can select another owner.
func registerCockroachDBServices(provider *engine.Provider, target string) {
	provider.Codecs = append(provider.Codecs, crdbschema.Codecs()...)
	provider.Codecs = append(provider.Codecs, crdbdiff.Codecs()...)
	provider.Codecs = append(provider.Codecs, crdbast.Codecs()...)
	provider.Properties = append(provider.Properties, engine.PropertySource{
		Target: target, Format: schemaext.TablePlatformProperties, Definitions: crdbsource.Definitions(), Service: crdbsource.Service{},
	})
	provider.Conversions = append(provider.Conversions, engine.Conversion{
		Target: target, Kinds: []schemaext.Kind{crdbschema.RowTTLKind}, Service: crdbconvert.Service{},
	})
	provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{
		Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
		Kinds: []schemaext.Kind{crdbschema.RowTTLKind}, ChangeKinds: []schemaext.Kind{crdbdiff.RowTTLKind}, Service: crdbcompare.Service{},
	})
	provider.Reversals = append(provider.Reversals, engine.Reversal{
		Target: target, Kinds: []schemaext.Kind{crdbdiff.RowTTLKind}, Service: crdbreverse.Service{},
	})
	provider.Planning = append(provider.Planning, engine.Planning{
		Target: target, Kinds: []schemaext.Kind{crdbdiff.RowTTLKind}, ParentKinds: []schemaext.Kind{crdbschema.RowTTLKind},
		OperationKinds: []schemaext.Kind{crdbast.AlterRowTTLKind}, Service: crdbplan.Service{},
	})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{
			Representation: representation, Definitions: crdbreport.Definitions(), Service: crdbreport.Service{},
		})
	}
}
