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
		Statements:       descriptor.Statements{ViewsBlockDropTable: true, CreateRoleOnce: true, DoBlocks: descriptor.DoBlocksPLpgSQL},

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

		// PostgreSQL keeps one index for equal UNIQUE and PRIMARY KEY
		// constraints of a table. A
		// bare type name its catalog or grammar defines is the built-in type:
		// measured on 17.10, a user domain named money beside a pg_catalog
		// money column (stokaro/ptah#1138).
		Preparation: descriptor.Preparation{
			FoldsIndexConstraints: true,
			PostgresBuiltInTypes:  true,
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
		// read. PostgreSQL's scanner reads E'...', continued strings and
		// U&'...' strings.
		Lexical: descriptor.Lexical{EscapeStrings: true, ContinuedStrings: true, UnicodeStrings: true},
		Reach:   postgresReach(),

		// Database URLs and the Atlas contract: what internal/atlasurl and the
		// Atlas HCL reader and writer read. postgis and pgvector start their
		// projects' PostgreSQL images (stokaro/ptah#4066).
		URLs: descriptor.URLs{DefaultPort: 5432},
		Atlas: descriptor.Atlas{
			DockerImages:  []string{"postgres", "postgis", "pgvector"},
			DockerDrivers: []string{"postgres"},
			HCL: descriptor.AtlasHCL{
				RefusedBlocks: []string{"extension", "policy", "sequence"},
				ColumnTypes:   postgresAtlasColumnTypes(),
			},
		},

		// Grammar: how the shared DDL parser reads the engine's DDL.
		// PostgreSQL 18.6 takes [NOT] ENFORCED after a CHECK or a foreign key,
		// one per constraint, NOT VALID with VALIDATE CONSTRAINT, MATCH FULL,
		// and answers `MATCH PARTIAL not yet implemented`. The current-time
		// words are keywords: CURRENT_TIMESTAMP() is refused. The server stores
		// one spelling of a string default, ALTER INDEX renames an index, and
		// a routine's body is a string constant in a LANGUAGE.
		Grammar: descriptor.Grammar{
			Columns: descriptor.ColumnGrammar{CurrentTimeKeywords: true, StoredStringDefaults: true},
			Constraints: descriptor.ConstraintGrammar{
				Enforcement: descriptor.Enforcement{Checks: true, ForeignKeys: true, Once: true},
				NotValid:    true,
				Match:       descriptor.Match{Clause: true},
			},
			Indexes:    descriptor.IndexGrammar{Alter: descriptor.AlterIndexRename},
			Statements: descriptor.StatementGrammar{Extensions: true, UserTypes: true, GrantTargetLists: true},
			Routines: descriptor.RoutineGrammar{
				Body:              descriptor.RoutineQuotedBody,
				TriggerConditions: true,
			},
		},

		// Clean checks: what internal/migrateclean reads. The pinned
		// community binary refuses to adopt a database that holds a table,
		// measured on PostgreSQL 17 on 2026-08-07, and refuses a dev database
		// that holds one "in connected schema", measured on PostgreSQL 18 on
		// 2026-09-26. A dev database an atlas.hcl docker block provisioned
		// starts from the state the block leaves (stokaro/ptah#4056).
		CleanCheck: descriptor.CleanCheck{
			Adoption:        true,
			Dev:             true,
			ConnectedSchema: true,
			StartingPoint:   true,
		},

		// Migrations: what internal/ptahdirective, internal/migrationreplay
		// and internal/txrequire read. The engine's reading decides a migration
		// file's directives when its dialect is unknown. A value added to an
		// existing enum type is refused in a statement of the same transaction,
		// measured on PostgreSQL 18.4: `unsafe use of new value` (55P04).
		Migration: descriptor.Migration{
			DirectivesWithoutDialect: true,
			EnumValuesCommitFirst:    true,
		},
	}
}

// postgresAtlasColumnTypes are the column type names the pinned Atlas
// community binary v1.3.0 models as a bare HCL type for PostgreSQL, measured
// by feeding each name to it bare, as `type = <name>`, against a PostgreSQL
// 17 dev database. Names refused there are absent: hstore, citext, ltree and
// geometry are extension types the dev database does not carry, and they are
// refused when wrapped as well. The multi-word spellings PostgreSQL's catalog
// reports, such as `character varying` and `timestamp with time zone`, are
// absent because two identifiers separated by a space are not one HCL
// expression; they reach the binary through sql(). internal/atlashclrender
// says why a list that is too short is safe and one that is too long is not,
// and the Atlas CE oracle job re-measures every entry.
func postgresAtlasColumnTypes() []string {
	return []string{
		"bigint", "bigserial", "bit", "bool", "boolean", "box", "bytea", "char", "character", "cidr",
		"circle", "date", "daterange", "decimal", "float", "float4", "float8", "inet", "int", "int2",
		"int4", "int4range", "int8", "int8range", "integer", "interval", "json", "jsonb", "line", "lseg",
		"macaddr", "macaddr8", "money", "name", "numeric", "numrange", "oid", "path", "point", "polygon",
		"real", "regclass", "serial", "smallint", "smallserial", "text", "time", "timestamp", "timestamptz", "timetz",
		"tsquery", "tsrange", "tstzrange", "tsvector", "uuid", "varchar", "xml",
	}
}
