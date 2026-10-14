// Package pgtypeprovider assembles the PostgreSQL user type owner of package
// pgtype as one engine provider: its codecs, its conversion, comparison,
// domain normalization, planning, declaration and reversal services on every
// PostgreSQL-family target, and its reports in both representations. It also
// wires the Go, HCL and SQL source adapters of the three kinds.
package pgtypeprovider

import (
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/feature/pgtype"
	"ptah.run/feature/pgtype/typecompare"
	"ptah.run/feature/pgtype/typeconvert"
	"ptah.run/feature/pgtype/typeplan"
	"ptah.run/feature/pgtype/typeprobe"
	"ptah.run/feature/pgtype/typereport"
	"ptah.run/feature/pgtype/typereverse"
	"ptah.run/internal/pgtypesource"
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
		// describes none of the owner's types of the kind either. Domains
		// and composite types keep the common not-described kinds because
		// Oracle's, which the common model holds, read the same records.
		CoverageLimits: []schemaext.LimitMapping{
			{Header: coverage.Domain, Model: pgtype.DomainKind, Names: typeNames},
			{Header: coverage.Composite, Model: pgtype.CompositeKind, Names: typeNames},
		},
		// A scope names a type with the selector type of the common one.
		Selections:  []selectext.Extension{pgtype.Selections()},
		HCL:         []hclext.Extension{pgtypesource.HCL(Targets())},
		Annotations: []annotation.Extension{pgtypesource.Annotations(Targets())},
		SQL:         []sqlext.Extension{pgtypesource.SQL(Targets())},
		// A type on a target outside the family is refused, naming the scope
		// that keeps it to the targets that host it: a column declared with
		// the type would otherwise name something the server has no
		// definition of (stokaro/ptah#1717).
		OffTarget: []engine.OffTargetPolicy{
			{Kinds: []schemaext.Kind{pgtype.DomainKind}, Action: schemaext.OffTargetRefuse, Label: "PostgreSQL domain"},
			{Kinds: []schemaext.Kind{pgtype.CompositeKind}, Action: schemaext.OffTargetRefuse, Label: "PostgreSQL composite type"},
			{Kinds: []schemaext.Kind{pgtype.RangeKind}, Action: schemaext.OffTargetRefuse, Label: "PostgreSQL range type"},
		},
	}
	models := pgtype.Kinds()
	for _, target := range Targets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: models, Service: typeconvert.Service{}})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: models,
			ChangeKinds: pgtype.ChangeKinds(), Service: typecompare.Service{}})
		provider.Normalizations = append(provider.Normalizations, engine.Normalization{Target: target,
			Kinds: []schemaext.Kind{pgtype.DomainKind}, Service: typeprobe.Service{}})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: pgtype.ChangeKinds(),
			OperationKinds: pgtype.OperationKinds(), Service: typeplan.Service{}})
		provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target, Kinds: models,
			OperationKinds: pgtype.OperationKinds(), Service: typeplan.Service{}})
		provider.Reversals = append(provider.Reversals, engine.Reversal{Target: target, Kinds: pgtype.ChangeKinds(), Service: typereverse.Service{}})
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation,
			Definitions: typereport.Definitions(), Service: typereport.Service{}})
	}
	return provider
}

// typeNames are the names a not-described record may use for a type: its
// name qualified by the schema the source wrote, and its bare name.
func typeNames(ref objectidentity.ID) []string {
	return slices.Compact([]string{pgtype.QualifiedName(ref), ref.Name.Source})
}
