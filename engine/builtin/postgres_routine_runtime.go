package builtin

import (
	"ptah.run/core/annotation"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/engine"
	"ptah.run/internal/pgroutinesource"
)

func postgresRoutineProvider() engine.Provider {
	provider := engine.Provider{ID: pgroutine.PropertiesOwner,
		Codecs:      append(pgroutine.PropertiesCodecs(), pgroutine.PropertiesChangeCodec()),
		Annotations: []annotation.Extension{pgroutinesource.Annotations()},
		HCL:         []hclext.Extension{pgroutinesource.HCL()},
		YAML:        []yamlext.Extension{pgroutinesource.YAML()}, SQL: []sqlext.Extension{pgroutinesource.SQL()},
	}
	for _, target := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB} {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: []schemaext.Kind{pgroutine.PropertiesKind}, Service: pgroutine.PropertiesService{}})
		provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{Target: target,
			OwnerKinds: []objectidentity.Kind{objectidentity.KindFunction}, Kinds: []schemaext.Kind{pgroutine.PropertiesKind},
			ChangeKinds: []schemaext.Kind{pgroutine.PropertiesChangeKind}, Service: pgroutine.PropertiesService{}})
		provider.Reversals = append(provider.Reversals, engine.Reversal{Target: target, Kinds: []schemaext.Kind{pgroutine.PropertiesChangeKind}, Service: pgroutine.PropertiesService{}})
	}
	return provider
}
