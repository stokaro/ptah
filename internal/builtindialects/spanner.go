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

		// Migrations: what the migrator reads.
		DDLTransactions: "no_transaction",
		RevisionTable:   spannerRevisionTable(),

		// Unlike the rest of its family, Spanner does not place a desired
		// object declared without a schema in public.
		Schemas: descriptor.Schemas{Selection: descriptor.SchemaSelectionSearchPath},
		Roles:   descriptor.Roles{OutOfScope: descriptor.RoleScopeReferenced, Widenable: true},
	}
}

// spannerRevisionTable is how Spanner's PostgreSQL interface keeps the
// migrator's tables. It has neither TIMESTAMP nor JSON -- `Type <timestamp>
// is not supported`, `Type <json> is not supported; use jsonb instead` -- so
// the revision tables say TIMESTAMPTZ and JSONB, measured against the Cloud
// Spanner emulator behind PGAdapter 0.55.2 (stokaro/ptah#2233). The column
// is a point in time either way: Spanner stores timestamps in UTC.
//
// It refuses a schema statement inside an explicit transaction (`DDL
// statements are only allowed outside explicit transactions`, SQLSTATE 25000,
// stokaro/ptah#1793), so every statement commits on its own, body and
// revision write alike; TestSpannerMigratorKeepsEveryStatementItRanLive
// measures that (stokaro/ptah#3320).
func spannerRevisionTable() descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create: `CREATE TABLE IF NOT EXISTS {table} (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
){engine}`,
		CreateAtlas: createPostgresAtlasRevisions,
		CreateTags:  createTags,
		CreateLog: `CREATE TABLE IF NOT EXISTS {table} (
    run_id VARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL,
    operation VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL,
    state VARCHAR(32) NOT NULL,
    actor VARCHAR(256) NULL,
    actor_source VARCHAR(32) NOT NULL,
    checksum VARCHAR(64) NULL,
    logged_at TIMESTAMPTZ NOT NULL,
    error TEXT NULL,
    PRIMARY KEY (run_id, seq)
){engine}`,
		CreateSchema:   createSchema,
		FirstRow:       "limit",
		Update:         updateRevision,
		Respell:        respellRevision(),
		UpsertTag:      upsertTagOnDuplicateKey,
		VersionCast:    "BIGINT",
		VersionLiteral: "dollar",
		DerivedTableAs: " AS",
		AtlasSchema:    "search-path",
	}
}
