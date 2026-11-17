package builtin

import (
	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/postgres/pgrole"
	"ptah.run/engine"
	"ptah.run/internal/pgrolesource"
)

// postgresRoleProvider owns the configuration defaults a PostgreSQL role
// holds in every database. It serves PostgreSQL alone: CockroachDB and
// YugabyteDB are not measured, so a declaration there is refused as a facet
// no comparison of the target owns. Go annotations, YAML and SQL declare the
// settings; Atlas HCL has no spelling for them and declares none.
func postgresRoleProvider() engine.Provider {
	service := pgrole.SettingsService{}
	provider := engine.Provider{ID: pgrole.Owner,
		Annotations: []annotation.Extension{pgrolesource.Annotations()},
		YAML:        []yamlext.Extension{pgrolesource.YAML()},
		SQL:         []sqlext.Extension{pgrolesource.SQL()},
		Codecs:      append(pgrole.SettingsCodecs(), pgrole.SettingsChangeCodec(), pgrole.OperationCodec()),
		Conversions: []engine.Conversion{{Target: platform.Postgres, Kinds: []schemaext.Kind{pgrole.SettingsKind}, Service: service}},
		FacetComparisons: []engine.FacetComparison{{Target: platform.Postgres,
			OwnerKinds: []objectidentity.Kind{objectidentity.KindRole}, Kinds: []schemaext.Kind{pgrole.SettingsKind},
			ChangeKinds: []schemaext.Kind{pgrole.SettingsChangeKind}, Service: service}},
		Planning: []engine.Planning{{Target: platform.Postgres, Kinds: []schemaext.Kind{pgrole.SettingsChangeKind},
			OperationKinds: []schemaext.Kind{pgrole.OperationKind}, Service: service}},
		Reversals: []engine.Reversal{{Target: platform.Postgres, Kinds: []schemaext.Kind{pgrole.SettingsChangeKind}, Service: service}},
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: pgrole.SettingsReports(), Service: service})
	}
	return provider
}
