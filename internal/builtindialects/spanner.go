package builtindialects

import "ptah.run/core/platform/descriptor"

// spanner is Cloud Spanner through its PostgreSQL interface, in the
// PostgreSQL family, with PostgreSQL's identifier rules.
func spanner() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "spanner",
		Aliases:       []string{"cloudspanner", "google-spanner", "google_spanner"},
		Family:        "postgres",
		DefaultSchema: "public",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "schema",
			IndexNames:     "exact",
			TableNames:     "exact",
			ColumnNames:    "exact",
		},
		Placeholder: descriptor.PlaceholderDollar,

		// Spanner has one preset and no ladder: a banner naming it selects
		// that preset without reading a version.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "SpannerPostgres",
			BannerTokens:  []string{"spanner"},
		},
		IdentifierLimit: descriptor.IdentifierLimit{Max: 128, Unit: "characters"},

		// How a connection reaches the server.
		Driver:     "pgx",
		WireScheme: "postgres",

		Query: descriptor.Query{
			Upsert: descriptor.UpsertOnConflict,
		},
		Scripts: descriptor.Scripts{ClientDelimiters: true},

		// The grammars the planner asks for non-blocking DDL in.
		ConcurrentIndexes: true,
		OnlineAlter:       descriptor.OnlineAlterNotValid,
	}
}
