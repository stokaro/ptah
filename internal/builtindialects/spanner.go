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
	}
}
