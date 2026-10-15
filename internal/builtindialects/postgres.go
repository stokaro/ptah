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

		// The ladder reads the first dotted number, which a PostgreSQL banner
		// carries first: "PostgreSQL 16.3 (Debian ...)". Every major up to the
		// newest measured one is credited to its own line. Postgres14 is the
		// first with CREATE OR REPLACE TRIGGER, Postgres16 the first with the
		// PostgreSQL 15 grammar, and Postgres18 differs from Postgres17 by the
		// named NOT NULL constraint a live 18 persists (stokaro/ptah#2161).
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "Postgres17",
			BannerTokens:  []string{"postgres"},
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "Postgres13",
				Steps: []descriptor.VersionStep{
					{Min: "14", Preset: "Postgres14"},
					{Min: "15", Preset: "Postgres16"},
					{Min: "17", Preset: "Postgres17"},
					{Min: "18", Preset: "Postgres18"},
				},
				Attribution: "through-newest",
				Newest:      "18",
			},
		},
		IdentifierLimit: descriptor.IdentifierLimit{Max: 63, Unit: "bytes"},

		// How a connection reaches the server.
		Driver: "pgx",

		Query: descriptor.Query{
			Upsert: descriptor.UpsertOnConflict,
		},
		Scripts: descriptor.Scripts{ClientDelimiters: true},

		// The grammars the planner asks for non-blocking DDL in.
		ConcurrentIndexes: true,
		OnlineAlter:       descriptor.OnlineAlterNotValid,

		// Migrations: what the migrator reads.
		// DDL joins the client's transaction, so a body and the revision that
		// records it commit together, measured by
		// TestRevisionCompletionFailure_PostgresTransactionalLive.
		DDLTransactions: "transactional",
		RevisionTable:   postgresFamilyRevisionTable(),

		Schemas: descriptor.Schemas{Selection: descriptor.SchemaSelectionSearchPath, Implicit: true},
		Roles:   descriptor.Roles{OutOfScope: descriptor.RoleScopeReferenced, Widenable: true},
		Lock:    descriptor.Lock{Protocol: descriptor.LockPostgresAdvisory},
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
		Seeds: descriptor.Seeds{
			Transactional: true,
			Savepoint:     descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"},
		},

		// How an identifier is written, and how a column's own key is named.
		Quoting:    descriptor.Quoting{Style: descriptor.QuoteDouble},
		ColumnKeys: descriptor.ColumnKeyTableColumnKey,

		// Cleanup: what a development database cleanup reads.
		// The writer-only catalog inventory is PostgreSQL proper's alone:
		// it is the only engine of the family the cleanup has been measured
		// against, and CockroachDB and YugabyteDB plans stay conservative
		// until theirs are.
		Cleanup: descriptor.Cleanup{
			ForeignKeys:       true,
			Views:             true,
			MaterializedViews: true,
			Enums:             true,
			Domains:           true,
			Composites:        true,
			Ranges:            true,
			Functions:         true,
			CatalogInventory:  true,
			RevisionTables:    true,
			RevisionProbe:     postgresRevisionProbe,
			SchemaScoped:      true,
			Restrict:          true,
		},

		// Lint: what migration lint reads.
		Lint: descriptor.Lint{
			Analyzed:         true,
			Scanner:          "dollar",
			DynamicSQL:       "execute",
			InjectionAdvice:  "quote an identifier with quote_ident() or format('%I'), a literal with quote_literal() or format('%L'), and pass values through USING with $1 placeholders",
			OnlineMeasured:   true,
			TypeRestatements: "pgtype",
			AtlasAnalyzer:    "PG",
		},

		// How a schema file's names are read, and how unnamed indexes and CHECKs are named.
		Names:            descriptor.Names{Unquoted: "ascii_case_insensitive", PublicKeyword: true},
		IndexNaming:      descriptor.IndexNaming{PrimaryKeySuffix: "pkey"},
		ConstraintNaming: descriptor.ConstraintNamingTableColumnsSuffix,
		Columns:          descriptor.Columns{IdentityNotNull: true},
		Statements:       descriptor.Statements{ViewsBlockDropTable: true, CreateRoleOnce: true, DoBlocks: "plpgsql"},

		// Dev databases.
		DevRealm: descriptor.DevRealm{
			Identity:        "query",
			Query:           "SELECT current_database()",
			ScratchDatabase: true,
			DropSchema:      "DROP SCHEMA IF EXISTS %s CASCADE",
		},

		// How the common model is lowered for the engine.
		GeneratedTimeouts:      descriptor.GeneratedTimeouts{Lock: "3s", Statement: "30s"},
		GeneratedColumnDefault: "STORED",
		// Comparison: what comparing a declaration with a catalog reads.
		Comparison: descriptor.Comparison{
			ForeignKeysOutside:   true,
			ExpressionProbes:     "pg-catalog",
			RolePasswords:        true,
			DefaultGeneratedKind: "STORED",
			RoutineTypeAliases:   "pg-aliases",
			QualifiedTypeNames:   true,
		},
	}
}
