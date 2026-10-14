// Package pgdefaultprivilegeprovider assembles default-privilege semantics at
// the engine composition boundary. Shared models contain no concrete ACL type.
package pgdefaultprivilegeprovider

import (
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/hclext"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/engine"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegecompare"
	"ptah.run/feature/pgdefaultprivilege/privilegeconvert"
	"ptah.run/feature/pgdefaultprivilege/privilegeplan"
	"ptah.run/feature/pgdefaultprivilege/privilegereport"
	"ptah.run/feature/pgdefaultprivilege/privilegereverse"
	"ptah.run/feature/pgdefaultprivilege/privilegeselect"
	"ptah.run/internal/pgdefaultprivilegesource"
)

// Targets lists the PostgreSQL-family compositions that select this owner.
func Targets() []string {
	return []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner}
}

// Provider returns fresh codecs and stateless services. The composition also
// selects each target and installs the owner's rendering handlers.
func Provider() engine.Provider {
	provider := engine.Provider{ID: pgdefaultprivilege.Owner,
		Selections:  []selectext.Extension{privilegeselect.Extension()},
		SQL:         []sqlext.Extension{pgdefaultprivilegesource.SQL()},
		HCL:         []hclext.Extension{pgdefaultprivilegesource.HCL()},
		Annotations: []annotation.Extension{pgdefaultprivilegesource.Annotations()}, YAML: []yamlext.Extension{pgdefaultprivilegesource.YAML()},
		Codecs:    slices.Concat(pgdefaultprivilege.Codecs(), []schemaext.Codec{pgdefaultprivilege.ChangeCodec(), pgdefaultprivilege.OperationCodec()}),
		OffTarget: []engine.OffTargetPolicy{{Kinds: []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind}, Action: schemaext.OffTargetSkip, Label: "PostgreSQL default privileges"}}}
	models := []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind}
	changes := []schemaext.Kind{pgdefaultprivilege.ChangeKind}
	operations := []schemaext.Kind{pgdefaultprivilege.OperationKind}
	for _, target := range Targets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: models, Service: privilegeconvert.Service{}})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: models, ChangeKinds: changes, Service: privilegecompare.Service{}})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: changes, OperationKinds: operations, Service: privilegeplan.Service{}})
		provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target, Kinds: models, OperationKinds: operations, Service: privilegeplan.Service{}})
		provider.Reversals = append(provider.Reversals, engine.Reversal{Target: target, Kinds: changes, Service: privilegereverse.Service{}})
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: privilegereport.Definitions(), Service: privilegereport.Service{}})
	}
	return provider
}
