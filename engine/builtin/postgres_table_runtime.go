package builtin

import (
	"ptah.run/core/annotation"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/dialect/postgres/pgtableprobe"
	"ptah.run/engine"
	"ptah.run/internal/pgtablesource"
)

func postgresTableProvider() engine.Provider {
	provider := engine.Provider{ID: pgtable.Owner, Codecs: pgtable.PartitionCodecs(),
		Annotations: []annotation.Extension{pgtablesource.Annotations()},
		HCL:         []hclext.Extension{pgtablesource.HCL()}, YAML: []yamlext.Extension{pgtablesource.YAML()}, SQL: pgtablesource.SQL(),
	}
	for _, target := range pgtable.PersistenceTargets() {
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, ParentKinds: []schemaext.Kind{pgtable.PartitionKind}, Service: pgtable.PartitionService{}})
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: []schemaext.Kind{pgtable.PartitionKind}, Service: pgtable.PartitionService{}})
		provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{Target: target,
			OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Kinds: []schemaext.Kind{pgtable.PartitionKind}, Service: pgtable.PartitionService{}})
	}
	for _, target := range []string{platform.Postgres, platform.YugabyteDB} {
		provider.FacetNormalizations = append(provider.FacetNormalizations, engine.FacetNormalization{Target: target,
			Kinds: []schemaext.Kind{pgtable.PartitionKind}, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Service: pgtableprobe.Service{}})
	}
	provider.Properties = append(provider.Properties, engine.PropertySource{Target: platform.YugabyteDB,
		Format: schemaext.TablePlatformProperties, Definitions: []schemaext.PropertyDefinition{{Kind: pgtable.PartitionKind, Keys: []string{"partition_by"}}}, Service: pgtablesource.Service{}})
	service := pgtable.PersistenceService{}
	registerTableFacetOwner(&provider, platform.Postgres, tableFacetOwner{
		codecs:     append(pgtable.PersistenceCodecs(), pgtable.PersistenceChangeCodec(), pgtable.PersistenceOperationCodec()),
		properties: pgtablesource.Definitions(), propertyService: pgtablesource.Service{},
		facet: pgtable.PersistenceKind, change: pgtable.PersistenceChangeKind, operation: pgtable.PersistenceOperationKind,
		conversion: service, comparison: service, reversal: service, planning: service, reports: pgtable.PersistenceReports(), reporting: service,
	})
	// The owner accounts for a declaration on every target with a defined
	// omission or refusal policy. Only PostgreSQL produces persistence changes.
	for _, target := range pgtable.PersistenceTargets() {
		if target == platform.Postgres {
			continue
		}
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target,
			Kinds: []schemaext.Kind{pgtable.PersistenceKind}, Service: service})
		provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{Target: target,
			OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Kinds: []schemaext.Kind{pgtable.PersistenceKind},
			Service: service})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target,
			Kinds: []schemaext.Kind{pgtable.PersistenceChangeKind}, ParentKinds: []schemaext.Kind{pgtable.PersistenceKind},
			OperationKinds: []schemaext.Kind{pgtable.PersistenceOperationKind}, Service: service})
	}
	return provider
}
