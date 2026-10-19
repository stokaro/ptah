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
		// The bytea input reads a backslash as an escape and refuses a byte
		// that is not UTF-8 (SQLSTATE 22021), so a binary value is spelled in
		// hex digits, relying on standard_conforming_strings. A timestamp
		// literal keeps the instant its offset names.
		Literals: descriptor.Literals{
			BinaryPrefix:    `'\x`,
			BinarySuffix:    `'::bytea`,
			TimestampOffset: true,
		},
		ManagedData: descriptor.ManagedData{
			IdentityRefusesInsert: descriptor.IdentityInsertGeneratedAlways,
		},
		// The PostgreSQL interface has neither type the portable tracker
		// uses. CHAR(64) arrives as bpchar and the endpoint answers `Type
		// <bpchar> is not supported. (SQLSTATE P0001)`, and TIMESTAMP is
		// refused next. Measured on the emulator: this statement is accepted,
		// running it twice is a no-op, and a row written through it reads back
		// (stokaro/ptah#3325).
		Seeds: descriptor.Seeds{
			Tracker: `CREATE TABLE IF NOT EXISTS schema_seeds (
    seed_path VARCHAR(512) PRIMARY KEY,
    env VARCHAR(128) NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL
)`,
			Transactional: true,
			Savepoint:     descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"},
		},

		// How an identifier is written, and how a column's own key is named.
		Quoting: descriptor.Quoting{Style: descriptor.QuoteDouble},

		// Cleanup: what a development database cleanup reads.
		// Spanner's PostgreSQL interface does not accept the CASCADE and DROP
		// TYPE forms the PostgreSQL family's cleanup renders, and no instance
		// was available to measure its coverage, so its plan reports tables
		// only.
		Cleanup: descriptor.Cleanup{},

		// Lint: what migration lint reads.
		Lint: descriptor.Lint{Analyzed: true},

		// How a schema file's names are read, and how unnamed indexes and CHECKs are named.
		IndexNaming: descriptor.IndexNaming{PrimaryKeySuffix: "pkey"},
		Names:       descriptor.Names{PublicKeyword: true},
		Statements:  descriptor.Statements{ViewsBlockDropTable: true},

		// Dev databases.
		DevRealm: descriptor.DevRealm{
			Identity:        "query",
			Query:           "SELECT current_database()",
			ScratchDatabase: true,
		},

		// How the common model is lowered for the engine.
		GeneratedColumnDefault: "STORED",
		InlineEnum:             descriptor.InlineEnum{Type: "TEXT", Check: descriptor.InlineEnumCheckBare},
		// Comparison: what comparing a declaration with a catalog reads.
		// Spanner reports a backing index for a foreign key.
		Comparison: descriptor.Comparison{
			ForeignKeyBackingIndex: true,
			RolePasswords:          true,
			RoutineTypeAliases:     "pg-aliases",
			SingleStringType:       true,
		},

		// The PostgreSQL interface has no enum type, so an enum column is TEXT
		// with a CHECK. A foreign key name is unique in its schema and compared
		// without regard to case. A bare type name PostgreSQL defines is the
		// built-in type.
		Preparation: descriptor.Preparation{
			Enums:                   descriptor.EnumCheck,
			EnumCheckType:           "TEXT",
			ForeignKeyNamespace:     descriptor.NamespaceSchema,
			ForeignKeyNamesFoldCase: true,
			PostgresBuiltInTypes:    true,
		},

		// A serial column owns its sequence, every routine carries EXECUTE
		// for PUBLIC, and an index's declared type is its access method, as a
		// read reports them. The family holds the shared row-level security
		// model, where a policy that names no role applies to PUBLIC. An
		// imported auto-increment column is an identity generated BY DEFAULT,
		// the one spelling that keeps the declared type.
		Conversion: descriptor.Conversion{
			OwnedSequences:            true,
			ImplicitRoutineGrants:     true,
			IndexTypeIsMethod:         true,
			RowLevelSecurity:          true,
			EmptyPolicyRolesArePublic: true,
			AutoIncrementIdentity:     true,
		},

		// Lexing and reach: what internal/dialectlexer and internal/sqlreach
		// read. Nobody has measured PostgreSQL's continued and U&'...' strings
		// on Spanner, so a text that writes one is two tokens and is refused
		// rather than read by a rule the server may not share.
		Lexical: descriptor.Lexical{EscapeStrings: true},
		Reach:   postgresReach(),

		// Grammar: how the shared DDL parser reads the engine's DDL. Statements,
		// routines and triggers are read in the PostgreSQL family's grammar.
		Grammar: descriptor.Grammar{
			Columns:    descriptor.ColumnGrammar{CurrentTimeKeywords: true, StoredStringDefaults: true},
			Statements: descriptor.StatementGrammar{Extensions: true, UserTypes: true, GrantTargetLists: true},
			Routines: descriptor.RoutineGrammar{
				Body:              descriptor.RoutineQuotedBody,
				TriggerConditions: true,
			},
		},

		// Clean checks: what internal/migrateclean reads. The pinned
		// community binary does not open a Spanner URL (`unknown driver`), so
		// a dev database is refused in the shape of the PostgreSQL refusals.
		CleanCheck: descriptor.CleanCheck{Dev: true, ConnectedSchema: true},

		// What a schema qualifier on a planned migration, as `migrate diff
		// --qualifier` writes one, would need on Spanner.
		QualifierRefusal: "Spanner renders through the PostgreSQL path but has no live coverage at all " +
			"(stokaro/ptah#942), so a qualified plan could not be shown to apply",

		// System schemas: what internal/pgsystemschema reads. Spanner lists
		// spanner_sys in pg_namespace beside public, measured on the PGAdapter
		// emulator v0.55.3, and it is the server's own, so a read leaves it
		// out. Neither schema can be created: `CREATE SCHEMA IF NOT EXISTS
		// "public"` is refused with `Schema name not valid: public`, measured
		// on v0.55.2, and the IF NOT EXISTS does not help because the refusal
		// is about the name (stokaro/ptah#2072). `public` holds the user's own
		// tables, and `ptah schema inspect` writes a `schema "public"` block
		// for it, so a document may declare it.
		SystemSchemas: descriptor.SystemSchemas{
			Catalog:  []string{"spanner_sys"},
			Implicit: []string{"public", "spanner_sys"},
		},
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
