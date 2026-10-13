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
		Annotations: []annotation.Extension{pgtablesource.Annotations(), pgtablesource.ExclusionAnnotations(), pgtablesource.DeleteColumnsAnnotations()},
		HCL:         []hclext.Extension{pgtablesource.HCL(), pgtablesource.ExclusionHCL(), pgtablesource.DeleteColumnsHCL()},
		YAML:        []yamlext.Extension{pgtablesource.YAML(), pgtablesource.ExclusionYAML(), pgtablesource.DeleteColumnsYAML()},
		SQL:         pgtablesource.SQL(),
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
	registerExclusions(&provider)
	registerDeleteColumns(&provider)
	return provider
}

func registerExclusions(provider *engine.Provider) {
	service := pgtable.ExclusionService{}
	provider.Codecs = append(provider.Codecs, pgtable.ExclusionCodecs()...)
	provider.Codecs = append(provider.Codecs, pgtable.ExclusionChangeCodec(), pgtable.ExclusionOperationCodec())
	for _, target := range pgtable.PersistenceTargets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: []schemaext.Kind{pgtable.ExclusionKind}, Service: service})
	}
	provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: platform.Postgres, Kinds: []schemaext.Kind{pgtable.ExclusionKind}, ChangeKinds: []schemaext.Kind{pgtable.ExclusionChangeKind}, Service: service})
	provider.Normalizations = append(provider.Normalizations, engine.Normalization{Target: platform.Postgres, Kinds: []schemaext.Kind{pgtable.ExclusionKind}, Service: pgtableprobe.ExclusionService{}})
	provider.Planning = append(provider.Planning, engine.Planning{Target: platform.Postgres, Kinds: []schemaext.Kind{pgtable.ExclusionChangeKind}, ParentKinds: []schemaext.Kind{pgtable.ExclusionKind},
		OperationKinds: []schemaext.Kind{pgtable.ExclusionOperationKind}, Service: service})
	provider.Reversals = append(provider.Reversals, engine.Reversal{Target: platform.Postgres, Kinds: []schemaext.Kind{pgtable.ExclusionChangeKind}, Service: service})
	provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{
		Target: platform.Postgres, Kinds: []schemaext.Kind{pgtable.ExclusionKind},
		OperationKinds: []schemaext.Kind{pgtable.ExclusionOperationKind}, Service: service,
	})
	provider.OffTarget = append(provider.OffTarget, engine.OffTargetPolicy{Kinds: []schemaext.Kind{pgtable.ExclusionKind}, Action: schemaext.OffTargetRefuse, Label: "PostgreSQL EXCLUDE constraint"})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: pgtable.ExclusionReports(), Service: service})
	}
}

func registerDeleteColumns(provider *engine.Provider) {
	service := pgtable.DeleteColumnsService{}
	provider.Codecs = append(provider.Codecs, pgtable.DeleteColumnsCodecs()...)
	provider.Codecs = append(provider.Codecs, pgtable.DeleteColumnsChangeCodec(), pgtable.DeleteColumnsOperationCodec())
	for _, target := range pgtable.PersistenceTargets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: []schemaext.Kind{pgtable.DeleteColumnsKind}, Service: service})
	}
	for _, target := range []string{platform.Postgres, platform.YugabyteDB} {
		provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{Target: target,
			OwnerKinds: []objectidentity.Kind{objectidentity.KindConstraint}, Kinds: []schemaext.Kind{pgtable.DeleteColumnsKind},
			ChangeKinds: []schemaext.Kind{pgtable.DeleteColumnsChangeKind}, Service: service})
		provider.Reversals = append(provider.Reversals, engine.Reversal{Target: target, Kinds: []schemaext.Kind{pgtable.DeleteColumnsChangeKind}, Service: service})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: []schemaext.Kind{pgtable.DeleteColumnsChangeKind},
			ParentKinds: []schemaext.Kind{pgtable.DeleteColumnsKind}, OperationKinds: []schemaext.Kind{pgtable.DeleteColumnsOperationKind}, Service: service})
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: pgtable.DeleteColumnsReports(), Service: service})
	}
	provider.OffTarget = append(provider.OffTarget, engine.OffTargetPolicy{Kinds: []schemaext.Kind{pgtable.DeleteColumnsKind}, Action: schemaext.OffTargetRefuse, Label: "PostgreSQL foreign-key ON DELETE column list"})
}
