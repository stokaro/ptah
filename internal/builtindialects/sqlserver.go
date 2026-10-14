package builtindialects

import "ptah.run/core/platform/descriptor"

// sqlServer is Microsoft SQL Server. It compares names under a catalog
// collation that only the live server knows, so offline every distinct name
// is a possible conflict; core/platform/identifier.ForSQLServerCatalog
// carries what a live catalog resolved.
func sqlServer() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "sqlserver",
		Aliases:       []string{"mssql", "sql-server", "sql_server", "tsql"},
		DefaultSchema: "dbo",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "table",
			IndexNames:     "catalog_unknown",
			TableNames:     "catalog_unknown",
			ColumnNames:    "catalog_unknown",
		},
		Placeholder: descriptor.PlaceholderAtP,
	}
}
