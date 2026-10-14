package builtindialects

import "ptah.run/core/platform/descriptor"

// postgres is PostgreSQL, the head of the family of engines that speak its
// wire protocol and catalog. pgx is the driver's name and postgresql the URL
// scheme libpq accepts.
func postgres() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "postgres",
		Aliases:       []string{"pgx", "postgresql"},
		Family:        "postgres",
		DefaultSchema: "public",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "schema",
			IndexNames:     "exact",
			TableNames:     "exact",
			ColumnNames:    "exact",
		},
		Placeholder: descriptor.PlaceholderDollar,

		// The ladder reads the first dotted number, which a PostgreSQL banner
		// carries first: "PostgreSQL 16.3 (Debian ...)". Every major up to the
		// newest measured one is credited to its own line. Postgres14 is the
		// first with CREATE OR REPLACE TRIGGER, Postgres16 the first with the
		// PostgreSQL 15 grammar, and Postgres18 differs from Postgres17 by the
		// named NOT NULL constraint a live 18 persists (stokaro/ptah#2161).
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "Postgres17",
			BannerTokens:  []string{"postgres"},
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "Postgres13",
				Steps: []descriptor.VersionStep{
					{Min: "14", Preset: "Postgres14"},
					{Min: "15", Preset: "Postgres16"},
					{Min: "17", Preset: "Postgres17"},
					{Min: "18", Preset: "Postgres18"},
				},
				Attribution: "through-newest",
				Newest:      "18",
			},
		},
		IdentifierLimit: descriptor.IdentifierLimit{Max: 63, Unit: "bytes"},

		// How a connection reaches the server.
		Driver: "pgx",
	}
}
