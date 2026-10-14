// Package builtindialects holds the descriptors of the database engines Ptah
// ships, one file per engine. core/platform seeds the process dialect
// registry from Descriptors, and engine/builtin registers a provider for each.
//
// The package is data. It imports only the descriptor contract and
// internal/capabilityline, the release-line names the capability matrix
// shares, so the shared packages that read the process registry link no
// engine's code through it,
// and the registry is ready before any package-level variable reads it,
// without an init function. An engine Ptah does not ship describes itself
// through platform.Register instead of through this package.
package builtindialects

import "ptah.run/core/platform/descriptor"

// Descriptors returns the descriptor of every built-in engine, in a fixed
// order: the order platform.DialectSpellings lists their spellings and
// engine/builtin registers their providers. Each call returns new values.
func Descriptors() []descriptor.Dialect {
	return []descriptor.Dialect{
		postgres(),
		mysql(),
		mariaDB(),
		clickHouse(),
		sqlite(),
		sqlServer(),
		cockroachDB(),
		yugabyteDB(),
		spanner(),
		oracle(),
		ydb(),
	}
}
