package builtindialects

import "ptah.run/core/platform/descriptor"

// yugabyteDB is YugabyteDB through its PostgreSQL-compatible YSQL API, in the
// PostgreSQL family, with PostgreSQL's identifier rules.
func yugabyteDB() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "yugabytedb",
		Aliases:       []string{"yugabyte", "ysql"},
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
