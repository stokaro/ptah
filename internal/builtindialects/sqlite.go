package builtindialects

import "ptah.run/core/platform/descriptor"

// sqlite is SQLite. libsql and libsql+ws reach a libSQL server, which keeps
// SQLite's schema semantics over its own transport (#1615).
func sqlite() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "sqlite",
		Aliases:       []string{"sqlite3", "libsql", "libsql+ws"},
		DefaultSchema: "main",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "schema",
			IndexNames:     "ascii_case_insensitive",
			TableNames:     "ascii_case_insensitive",
			ColumnNames:    "ascii_case_insensitive",
		},
		Placeholder: descriptor.PlaceholderQuestion,

		// sqlite_version() answers a bare dotted version, so no banner names
		// SQLite. RENAME COLUMN arrived in 3.25, and ALTER COLUMN ... SET NOT
		// NULL in 3.53. Every version is credited to its line and none is
		// past the top: the matrix declares one SQLite cell, with no container
		// to measure a newest line on (stokaro/ptah#916). Ptah models no
		// identifier limit for SQLite.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "SQLite3",
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "SQLite324",
				Steps: []descriptor.VersionStep{
					{Min: "3.25", Preset: "SQLite352"},
					{Min: "3.53", Preset: "SQLite3"},
				},
				Attribution: "through-newest",
			},
		},

		// How a connection reaches the server.
		Driver: "sqlite",
	}
}
