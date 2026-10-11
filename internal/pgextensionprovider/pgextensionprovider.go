// Package pgextensionprovider assembles the PostgreSQL extension owner of
// package pgextension as one engine provider: its codecs, and its conversion,
// comparison, planning, declaration and reversal services on every
// PostgreSQL-family target, and its reports in both representations. The
// bundled runtime selects it without the reports.
package pgextensionprovider

import (
	"slices"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgextension/extensioncompare"
	"ptah.run/feature/pgextension/extensionconvert"
	"ptah.run/feature/pgextension/extensionplan"
	"ptah.run/feature/pgextension/extensionreport"
	"ptah.run/feature/pgextension/extensionreverse"
)

// Targets are the targets the services are registered for: every target of
// the PostgreSQL family. CockroachDB and Spanner create an extension and do
// nothing else with one; the planning of the owner refuses the rest there.
func Targets() []string {
	return []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner}
}

// Provider returns a fresh descriptor of the owner. The runtime that selects
// it must also select a provider of each of [Targets].
func Provider() engine.Provider {
	provider := engine.Provider{
		ID:     pgextension.Owner,
		Codecs: slices.Concat(pgextension.Codecs(), []schemaext.Codec{pgextension.ChangeCodec()}, pgextension.OperationCodecs()),
		// A description that does not describe extensions, such as a document
		// whose extension blocks the compatibility surface left out,
		// describes none of the owner's extensions either.
		CoverageLimits: []schemaext.LimitMapping{{Header: coverage.Extension, Model: pgextension.ExtensionKind, Names: extensionNames}},
		// An extension on a target outside the family is skipped with a note:
		// the target does not miss it, and a Go tree that targets several
		// engines shares one set of annotations.
		OffTarget: []engine.OffTargetPolicy{{Kinds: []schemaext.Kind{pgextension.ExtensionKind}, Action: schemaext.OffTargetSkip,
			Label: "PostgreSQL extension"}},
	}
	models := []schemaext.Kind{pgextension.ExtensionKind}
	changes := []schemaext.Kind{pgextension.ChangeKind}
	operations := []schemaext.Kind{pgextension.OperationKind, pgextension.CommentOperationKind}
	for _, target := range Targets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: models, Service: extensionconvert.Service{}})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: models,
			ChangeKinds: changes, Service: extensioncompare.Service{}})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: changes, OperationKinds: operations,
			Service: extensionplan.Service{}})
		provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target, Kinds: models, OperationKinds: operations,
			Service: extensionplan.Service{}})
		provider.Reversals = append(provider.Reversals, engine.Reversal{Target: target, Kinds: changes, Service: extensionreverse.Service{}})
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation,
			Definitions: extensionreport.Definitions(), Service: extensionreport.Service{}})
	}
	return provider
}

// extensionNames are the names a not-described record may use for an
// extension: its own name, which no folding changes.
func extensionNames(ref objectidentity.ID) []string {
	return []string{pgextension.Name(ref)}
}
