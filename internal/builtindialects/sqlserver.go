package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

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

		// @@VERSION opens with the marketing year, "Microsoft SQL Server 2025
		// (RTM-CU7) ... - 17.0.4065.4", so the version is read after the dash.
		// The ladder has one arm: the three declared lines answered the
		// capability probe identically, and the version selects which line an
		// observation belongs to (stokaro/ptah#3190). A banner with no number
		// still named the server.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "SQLServer2022",
			BannerTokens:  []string{"sql server"},
			Versions: descriptor.Versions{
				Scheme:      "after-dash",
				Floor:       "SQLServer2022",
				Attribution: "measured-lines",
				Measured:    capabilityline.SQLServerMeasured(),
				Newest:      capabilityline.SQLServer2025,
				Unreadable:  descriptor.UnreadableVersion{Recognized: true},
			},
		},
		IdentifierLimit: descriptor.IdentifierLimit{Max: 128, Unit: "characters"},

		// How a connection reaches the server.
		Driver: "sqlserver",

		// T-SQL has no LIMIT. It accepts OFFSET and FETCH, and only after an
		// ORDER BY, so a row limit the caller ordered by nothing gets `ORDER
		// BY (SELECT NULL)`, its spelling of "no particular order"
		// (stokaro/ptah#941). GO ends a batch in sqlcmd and SSMS.
		Query: descriptor.Query{
			OffsetFetch:  true,
			FetchOrderBy: "(SELECT NULL)",
			UpsertRefusal: "SQL Server has no ON CONFLICT: it expresses an upsert as MERGE, " +
				"a different statement with its own source, join condition and match clauses",
		},
		Scripts: descriptor.Scripts{BatchSeparator: "GO", ClientDelimiters: true},
	}
}
