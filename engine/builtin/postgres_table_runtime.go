package builtin

import (
	"ptah.run/core/annotation"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine"
	"ptah.run/internal/pgtablesource"
)

func postgresTableProvider() engine.Provider {
	provider := engine.Provider{ID: pgtable.Owner,
		Annotations: []annotation.Extension{pgtablesource.Annotations()},
		HCL:         []hclext.Extension{pgtablesource.HCL()}, YAML: []yamlext.Extension{pgtablesource.YAML()}, SQL: pgtablesource.SQL(),
	}
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
