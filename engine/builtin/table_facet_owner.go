package builtin

import (
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// tableFacetOwner describes an owner whose value is one facet of a table and
// whose every stage is a single service: CockroachDB row-level TTL, the Spanner
// row deletion policy, the YDB TTL and YDB column families. Each is registered from one descriptor,
// so no stage can select another owner.
type tableFacetOwner struct {
	codecs          []schemaext.Codec
	properties      []schemaext.PropertyDefinition
	propertyService schemaext.PropertyService
	facet           schemaext.Kind
	change          schemaext.Kind
	operation       schemaext.Kind
	conversion      schemaext.ConversionService
	comparison      schemaext.FacetComparisonService
	reversal        schemaext.ReversalService
	planning        featureplan.Service
	reports         []schemaext.ReportDefinition
	reporting       schemaext.ReportingService
}

// registerTableFacetOwner adds every stage of owner to provider for target.
func registerTableFacetOwner(provider *engine.Provider, target string, owner tableFacetOwner) {
	provider.Codecs = append(provider.Codecs, owner.codecs...)
	// An owner declared through a directive of its own, such as YDB column
	// families, reads no table property.
	if owner.propertyService != nil {
		provider.Properties = append(provider.Properties, engine.PropertySource{
			Target: target, Format: schemaext.TablePlatformProperties, Definitions: owner.properties, Service: owner.propertyService,
		})
	}
	provider.Conversions = append(provider.Conversions, engine.Conversion{
		Target: target, Kinds: []schemaext.Kind{owner.facet}, Service: owner.conversion,
	})
	provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{
		Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
		Kinds: []schemaext.Kind{owner.facet}, ChangeKinds: []schemaext.Kind{owner.change}, Service: owner.comparison,
	})
	provider.Reversals = append(provider.Reversals, engine.Reversal{
		Target: target, Kinds: []schemaext.Kind{owner.change}, Service: owner.reversal,
	})
	provider.Planning = append(provider.Planning, engine.Planning{
		Target: target, Kinds: []schemaext.Kind{owner.change}, ParentKinds: []schemaext.Kind{owner.facet},
		OperationKinds: []schemaext.Kind{owner.operation}, Service: owner.planning,
	})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{
			Representation: representation, Definitions: owner.reports, Service: owner.reporting,
		})
	}
}
