// Package pgextensionprovider assembles the PostgreSQL extension owner of
// package pgextension as one engine provider: its codecs, and its conversion
// and comparison services on every PostgreSQL-family target. The bundled
// runtime does not select it yet.
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
	}
	models := []schemaext.Kind{pgextension.ExtensionKind}
	for _, target := range Targets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: models, Service: extensionconvert.Service{}})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: models,
			ChangeKinds: []schemaext.Kind{pgextension.ChangeKind}, Service: extensioncompare.Service{}})
	}
	return provider
}

// extensionNames are the names a not-described record may use for an
// extension: its own name, which no folding changes.
func extensionNames(ref objectidentity.ID) []string {
	return []string{pgextension.Name(ref)}
}
