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

		// Migrations: what the migrator reads.
		DDLTransactions: "transactional",
		RevisionTable:   sqlServerRevisionTable(),
	}
}

// sqlServerRevisionTable is how SQL Server keeps the migrator's tables. T-SQL
// has no CREATE TABLE IF NOT EXISTS, so each statement asks OBJECT_ID first,
// and no CREATE SCHEMA IF NOT EXISTS, so the schema statement asks SCHEMA_ID
// and runs CREATE SCHEMA through EXEC, which must be the only statement of its
// batch. The tables are always schema-qualified, with dbo when nothing else
// names a schema. The Atlas version column is binary-collated, so two tokens
// that differ only in case stay two revisions. The SQL Server writer runs a
// real transaction that DDL joins; no live revision-completion test drives it
// yet.
func sqlServerRevisionTable() descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create: `IF OBJECT_ID({object}, N'U') IS NULL
BEGIN
    CREATE TABLE {table} (
        version BIGINT PRIMARY KEY,
        description NVARCHAR(MAX) NOT NULL,
        applied_at DATETIME2 NOT NULL,
        state NVARCHAR(32) NOT NULL DEFAULT 'applied',
        applied INT NOT NULL DEFAULT 1,
        total INT NOT NULL DEFAULT 1,
        error NVARCHAR(MAX) NULL,
        error_stmt NVARCHAR(MAX) NULL,
        execution_time_ms BIGINT NOT NULL DEFAULT 0,
        checksum NVARCHAR(64) NOT NULL DEFAULT ''
    )
END`,
		CreateAtlas: `IF OBJECT_ID({object}, N'U') IS NULL
BEGIN
    CREATE TABLE {table} (
        version NVARCHAR(255) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
        description NVARCHAR(MAX) NOT NULL,
        type BIGINT NOT NULL DEFAULT 2,
        applied BIGINT NOT NULL DEFAULT 0,
        total BIGINT NOT NULL DEFAULT 0,
        executed_at DATETIME2 NOT NULL,
        execution_time BIGINT NOT NULL,
        error NVARCHAR(MAX) NULL,
        error_stmt NVARCHAR(MAX) NULL,
        hash NVARCHAR(255) NOT NULL,
        partial_hashes NVARCHAR(MAX) NULL,
        operator_version NVARCHAR(255) NOT NULL
    )
END`,
		CreateTags: `IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID({object}) AND type = 'U')
BEGIN
    CREATE TABLE {table} (
        tag NVARCHAR(255) NOT NULL PRIMARY KEY,
        version BIGINT NOT NULL,
        recorded_at DATETIME2 NOT NULL
    )
END`,
		CreateLog: `IF OBJECT_ID({object}, N'U') IS NULL
BEGIN
    CREATE TABLE {table} (
        run_id NVARCHAR(64) NOT NULL,
        seq BIGINT NOT NULL,
        operation NVARCHAR(32) NOT NULL,
        version BIGINT NOT NULL,
        state NVARCHAR(32) NOT NULL,
        actor NVARCHAR(256) NULL,
        actor_source NVARCHAR(32) NOT NULL,
        checksum NVARCHAR(64) NULL,
        logged_at DATETIME2 NOT NULL,
        error NVARCHAR(MAX) NULL,
        PRIMARY KEY (run_id, seq)
    )
END`,
		CreateSchema:       "IF SCHEMA_ID({schema_object}) IS NULL EXEC(N'CREATE SCHEMA {schema_escaped}')",
		QualifiedByDefault: true,
		AddedColumns: map[string]string{
			"state":      "NVARCHAR(32) NOT NULL DEFAULT 'applied'",
			"error":      "NVARCHAR(MAX) NULL",
			"error_stmt": "NVARCHAR(MAX) NULL",
			"checksum":   "NVARCHAR(64) NOT NULL DEFAULT ''",
		},
		FirstRow: "top",
		RowLock:  "table-hint",
		Update:   updateRevision,
		Respell:  respellRevision(),
		UpsertTag: "MERGE {table} AS target USING (SELECT ? AS tag, ? AS version, ? AS recorded_at) AS source" +
			" ON target.tag = source.tag" +
			" WHEN MATCHED THEN UPDATE SET version = source.version, recorded_at = source.recorded_at" +
			" WHEN NOT MATCHED THEN INSERT (tag, version, recorded_at)" +
			" VALUES (source.tag, source.version, source.recorded_at);",
		VersionCast:    "BIGINT",
		VersionLiteral: "national",
		DerivedTableAs: " AS",
		JSONAsText:     true,
	}
}
