// Package pgpublicationprovider assembles the PostgreSQL logical replication
// publication owner of package pgpublication as one engine provider: its
// codecs, its SQL and HCL readers, its scope selection, its conversion,
// comparison, planning, declaration and reversal services on PostgreSQL, and
// its reports in both representations.
package pgpublicationprovider

import (
	"slices"

	"ptah.run/core/hclext"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/feature/pgpublication"
	"ptah.run/feature/pgpublication/publicationcompare"
	"ptah.run/feature/pgpublication/publicationconvert"
	"ptah.run/feature/pgpublication/publicationplan"
	"ptah.run/feature/pgpublication/publicationreport"
	"ptah.run/feature/pgpublication/publicationreverse"
	"ptah.run/internal/pgpublicationsource"
)

// Targets are the targets the services are registered for: PostgreSQL alone.
// CockroachDB has no publication, and YugabyteDB's are not modeled; a
// publication declared for either is refused rather than left out.
func Targets() []string { return []string{platform.Postgres} }

// Provider returns a fresh descriptor of the owner. The runtime that selects
// it must also select a provider of each of [Targets].
func Provider() engine.Provider {
	provider := engine.Provider{
		ID:         pgpublication.Owner,
		Codecs:     slices.Concat(pgpublication.Codecs(), []schemaext.Codec{pgpublication.ChangeCodec()}, pgpublication.OperationCodecs()),
		Selections: []selectext.Extension{pgpublication.Selections()},
		SQL:        []sqlext.Extension{pgpublicationsource.SQL()},
		HCL:        []hclext.Extension{pgpublicationsource.HCL()},
		OffTarget: []engine.OffTargetPolicy{{Kinds: []schemaext.Kind{pgpublication.PublicationKind}, Action: schemaext.OffTargetRefuse,
			Label: "PostgreSQL publication"}},
	}
	models := []schemaext.Kind{pgpublication.PublicationKind}
	changes := []schemaext.Kind{pgpublication.ChangeKind}
	operations := []schemaext.Kind{pgpublication.OperationKind}
	for _, target := range Targets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: models, Service: publicationconvert.Service{}})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: models,
			ChangeKinds: changes, Service: publicationcompare.Service{}})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: changes, OperationKinds: operations,
			Service: publicationplan.Service{}})
		provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target, Kinds: models, OperationKinds: operations,
			Service: publicationplan.Service{}})
		provider.Reversals = append(provider.Reversals, engine.Reversal{Target: target, Kinds: changes, Service: publicationreverse.Service{}})
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation,
			Definitions: publicationreport.Definitions(), Service: publicationreport.Service{}})
	}
	return provider
}
