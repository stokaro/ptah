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
	}
}
