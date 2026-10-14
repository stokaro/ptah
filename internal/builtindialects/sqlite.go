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
	}
}
