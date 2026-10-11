// Package pgtypeprovider assembles the PostgreSQL user type owner of package
// pgtype as one engine provider: its codecs, and its conversion, comparison
// and domain normalization services on every PostgreSQL-family target. The
// bundled runtime does not select it yet.
package pgtypeprovider

import (
	"slices"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgtype"
	"ptah.run/feature/pgtype/typecompare"
	"ptah.run/feature/pgtype/typeconvert"
	"ptah.run/feature/pgtype/typeprobe"
)

// Targets are the targets the services are registered for: every target of
// the PostgreSQL family. CockroachDB holds composite types, and domains from
// 26.3 on; Spanner holds none of the three. What a target cannot create is
// refused when a plan creates it, not when it is compared.
func Targets() []string {
	return []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner}
}

// Provider returns a fresh descriptor of the owner. The runtime that selects
// it must also select a provider of each of [Targets].
func Provider() engine.Provider {
	provider := engine.Provider{
		ID:     pgtype.Owner,
		Codecs: slices.Concat(pgtype.Codecs(), pgtype.ChangeCodecs(), pgtype.OperationCodecs()),
		// A description that does not describe a kind, such as a document
		// whose blocks of that kind the compatibility surface left out,
		// describes none of the owner's types of the kind either.
		CoverageLimits: []schemaext.LimitMapping{
			{Header: coverage.Domain, Model: pgtype.DomainKind, Names: typeNames},
			{Header: coverage.Composite, Model: pgtype.CompositeKind, Names: typeNames},
			{Header: coverage.Range, Model: pgtype.RangeKind, Names: typeNames},
		},
	}
	models := pgtype.Kinds()
	for _, target := range Targets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: models, Service: typeconvert.Service{}})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: models,
			ChangeKinds: pgtype.ChangeKinds(), Service: typecompare.Service{}})
		provider.Normalizations = append(provider.Normalizations, engine.Normalization{Target: target,
			Kinds: []schemaext.Kind{pgtype.DomainKind}, Service: typeprobe.Service{}})
	}
	return provider
}

// typeNames are the names a not-described record may use for a type: its
// name qualified by the schema the source wrote, and its bare name.
func typeNames(ref objectidentity.ID) []string {
	return slices.Compact([]string{pgtype.QualifiedName(ref), ref.Name.Source})
}
