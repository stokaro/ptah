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
		Placeholder:       descriptor.PlaceholderDollar,
		VulnerabilityScan: "supported",

		// The ladder reads the first dotted number, which a PostgreSQL banner
		// carries first: "PostgreSQL 16.3 (Debian ...)". Every major up to the
		// newest measured one is credited to its own line. Postgres14 is the
		// first with CREATE OR REPLACE TRIGGER, Postgres16 the first with the
		// PostgreSQL 15 grammar, and Postgres18 differs from Postgres17 by the
		// named NOT NULL constraint a live 18 persists (stokaro/ptah#2161).
		// PostgreSQL 19 is a line of its own planned as Postgres18: measured on
		// 19.0, it agreed with Postgres18 on all 131 decided rows.
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
					{Min: "19", Preset: "Postgres18"},
				},
				Attribution: "through-newest",
				Newest:      "19",
			},
		},
		IdentifierLimit: descriptor.IdentifierLimit{Max: 63, Unit: "bytes", Truncates: true},

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
			// The advisory data reviews PostgreSQL's own contrib modules,
			// fixed in its server releases, and the dev database's catalog
			// says what an extension statement installs.
			ExtensionAdvisories: true,
			// The read lists every enum, domain, composite and range type,
			// view, materialized view, trigger and sequence of the schemas it
			// reads. Lint takes a continuous aggregate, which the read records
			// as a TimescaleDB object, as a materialized view, and a serial or
			// identity sequence off the column that owns it. What the read
			// leaves out is what a drop cannot reach: a type or a function an
			// extension owns, an internal constraint trigger, a partition's
			// copy of its parent's trigger. Routines are not listed: the read
			// leaves out aggregates and Ptah's own trigger functions, which
			// DROP FUNCTION can drop.
			ListedObjects: []string{"type", "view", "materialized_view", "trigger", "sequence"},
		},

		// How a schema file's names are read, and how unnamed indexes and CHECKs are named.
		//
		// Measured on 18.6: a column default, a view and an SQL function body
		// that name app.ids in nextval('app.ids'), currval, setval, a ::regclass
		// cast or CAST(... AS regclass) are refused with `relation "app.ids"
		// does not exist` while the database lacks it; nextval('app.ids'::text)
		// is accepted.
		Names:            descriptor.Names{Unquoted: "ascii_case_insensitive", PublicKeyword: true, RegclassConstants: true},
		IndexNaming:      descriptor.IndexNaming{PrimaryKeySuffix: "pkey"},
		ConstraintNaming: descriptor.ConstraintNamingTableColumnsSuffix,
		// Measured on 18.6: two tables of a schema each hold a CHECK named ck,
		// and one table refuses dd beside DD, which is stored as dd, and holds
		// dd beside "DD". A schema file's unquoted name is read folded, so the
		// model's names compare exactly.
		CheckNames: descriptor.CheckNames{
			Scope:   descriptor.CheckNameScopeTable,
			Refusal: `ERROR: check constraint "{name}" already exists (SQLSTATE 42710)`,
		},
		// A column's collation is written after its type and read back from
		// pg_attribute.attcollation. CockroachDB and YugabyteDB leave it
		// unset: neither renderer path nor reader has been measured there.
		Columns: descriptor.Columns{
			NationalCharAsChar: true, IdentityNotNull: true, SerialNotNull: true, Collations: true,
		},
		// A second CREATE EXTENSION of one extension is refused, measured on
		// 18.6: `extension "pgcrypto" already exists`.
		//
		// Measured on 18: a guarded CREATE TABLE or CREATE SEQUENCE after a
		// table, view, materialized view or sequence of its name is skipped.
		Statements: descriptor.Statements{
			ViewsBlockDropTable: true, CreateRoleOnce: true, OwnerCreateOnce: true, DoBlocks: descriptor.DoBlocksPLpgSQL, RelationsShareNames: true,
		},

		// Dev databases.
		DevRealm: descriptor.DevRealm{
			Identity:        "query",
			Query:           "SELECT current_database()",
			ClusterQuery:    "SELECT system_identifier::text FROM pg_control_system()",
			ScratchDatabase: true,
			DropSchema:      "DROP SCHEMA IF EXISTS %s CASCADE",
		},

		// How the common model is lowered for the engine.
		GeneratedTimeouts:      descriptor.GeneratedTimeouts{Lock: "3s", Statement: "30s"},
		GeneratedColumnDefault: "STORED",
		// Measured on PostgreSQL 18: a foreign key to a column no unique key covers
		// fails at CREATE TABLE with 42830, indexed or not.
		ForeignKeyReferencesCheckedAtCreate: true,
		// Measured on PostgreSQL 18.6: ALTER COLUMN TYPE rounds numeric, money,
		// timestamp, timestamptz, time, timetz and interval to the new digits, and
		// numeric under an integer, and succeeds.
		Rounding: postgresFamilyRounding,
		// Comparison: what comparing a declaration with a catalog reads.
		Comparison: descriptor.Comparison{
			// A bare name folds to lower case and a quoted one keeps its case,
			// section 4.1.1 of the PostgreSQL manual.
			QuotedNamesKeepCase: true,

			DeclaredSchemas:    true,
			TimePrecision:      descriptor.TimePrecision{Compared: true, Default: 6},
			BareTypes:          descriptor.BareTypes{Stated: true, FloatBits: 53, RealBits: 24, MoneyScale: 2},
			ForeignKeysOutside: true,
			// pg_constraint.conindid names the index, and `DROP CONSTRAINT`
			// on it answers `cannot drop constraint parent_pkey on table
			// parent because other objects depend on it`, measured on 18.6.
			// The rest of the family has not been measured.
			// Measured on 18.6. The rest of the family has not been measured.
			ForeignKeyMatchesKeyInAnyOrder: true,
			ForeignKeyHoldsItsKey:          true,
			// Without this `code bigint PRIMARY KEY` on a table that holds
			// another key, or none, plans no key and reads as synced, measured
			// on 18.6. The rest of the family has not been measured.
			ColumnPrimaryKeyIsTheTablesKey: true,
			PrimaryKeyRenames:              true,
			IdentityColumns:                true,
			SerialOwnsSequence:             true,
			ExpressionProbes:               "pg-catalog",
			RolePasswords:                  true,
			DefaultGeneratedKind:           "STORED",
			RoutineTypeAliases:             "pg-aliases",
			RoutineOverloads:               true,
			QualifiedTypeNames:             true,
			// YugabyteDB leaves this off: a routine created in pg_temp
			// outlives the rollback there, and a copy whose body reads a
			// table would hold it.
			ProbesSQLStandardBodies: true,
		},

		// PostgreSQL keeps one index for equal UNIQUE and PRIMARY KEY
		// constraints of a table. A
		// bare type name its catalog or grammar defines is the built-in type:
		// measured on 17.10, a user domain named money beside a pg_catalog
		// money column (stokaro/ptah#1138). Of the built-in access methods
		// only btree builds a unique index: measured on 18.6, each of the
		// others answers `access method "hash" does not support unique
		// indexes` in its own name.
		Preparation: descriptor.Preparation{
			FoldsIndexConstraints: true,
			PostgresBuiltInTypes:  true,
			NonUniqueIndexMethods: []string{"brin", "gin", "gist", "hash", "spgist"},
		},

		// A serial column owns its sequence, every routine carries EXECUTE
		// for PUBLIC, and an index's declared type is its access method, as a
		// read reports them. The family holds the shared row-level security
		// model, where a policy that names no role applies to PUBLIC. An
		// imported auto-increment column is an identity generated BY DEFAULT,
		// the one spelling that keeps the declared type. Every database holds
		// schema public with USAGE for PUBLIC and the comment `standard public
		// schema`, measured on 18.6. `GRANT SELECT ON s1` and `GRANT SELECT ON
		// TABLE s1` grant on a sequence s1, which the read reports as a
		// sequence grant, measured on 19.
		Conversion: descriptor.Conversion{
			OwnedSequences:            true,
			SequenceGrantsByName:      true,
			ImplicitRoutineGrants:     true,
			IndexTypeIsMethod:         true,
			RowLevelSecurity:          true,
			EmptyPolicyRolesArePublic: true,
			AutoIncrementIdentity:     true,
			DefaultSchemaUsage:        true,
			DefaultSchemaComment:      "standard public schema",
		},

		// Lexing and reach: what internal/dialectlexer and internal/sqlreach
		// read. PostgreSQL's scanner reads E'...', continued strings and
		// U&'...' strings.
		// 18.6 reads a block comment that holds another as one comment.
		Lexical: descriptor.Lexical{EscapeStrings: true, ContinuedStrings: true, UnicodeStrings: true, NestedComments: true},
		Reach:   postgresReach(),

		// Database URLs and the Atlas contract: what internal/atlasurl and the
		// Atlas HCL reader and writer read. postgis and pgvector start their
		// projects' PostgreSQL images (stokaro/ptah#4066).
		URLs: descriptor.URLs{DefaultPort: 5432},
		Atlas: descriptor.Atlas{
			CatalogForm:   descriptor.PostgresCatalogForm,
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
		// a routine's body is a string constant in a LANGUAGE. A DO block that
		// writes no LANGUAGE is PL/pgSQL. ALTER TABLE ... ADD CONSTRAINT k
		// UNIQUE USING INDEX i makes the unique index i the key k, renaming it.
		Grammar: descriptor.Grammar{
			Columns: descriptor.ColumnGrammar{
				CurrentTimeKeywords:  true,
				StoredStringDefaults: true,
				QualifiedCollations:  true,
				DefaultPredicates: descriptor.DefaultPredicates{
					Refusal: "PostgreSQL 18 reads a comparison, a shift and IS [NOT] DISTINCT FROM in a default written " +
						"without parentheses, and answers a syntax error to every other IS test, to AND, OR and NOT, and to " +
						"BETWEEN, IN and LIKE there; write the whole expression in parentheses, as DEFAULT (1 + 2 IS NOT NULL). " +
						"It answers operator does not exist to ==, in parentheses too; write =",
					Comparison:   true,
					Shift:        true,
					DistinctFrom: true,
				},
				// Measured on PostgreSQL 18: DEFAULT 'now' on a timestamptz
				// stores the creation time, and 'today' on a date the
				// creation date.
				RelativeTimeLiterals: []string{"NOW", "TODAY", "TOMORROW", "YESTERDAY"},
			},
			Constraints: descriptor.ConstraintGrammar{
				TableNotNull: true,
				Enforcement:  descriptor.Enforcement{Checks: true, ForeignKeys: true, Once: true},
				NotValid:     true,
				Match:        descriptor.Match{Clause: true},
			},
			Indexes:    descriptor.IndexGrammar{Alter: descriptor.AlterIndexRename},
			AlterTable: descriptor.AlterTableGrammar{KeyUsingIndex: true, AddNotNull: true},
			Statements: descriptor.StatementGrammar{Extensions: true, UserTypes: true, GrantTargetLists: true},
			Forms:      postgresStatementForms(),
			Routines: descriptor.RoutineGrammar{
				Body:                   descriptor.RoutineQuotedBody,
				TriggerConditions:      true,
				DefaultDoBlockLanguage: descriptor.DoBlocksPLpgSQL,
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
		// measured on PostgreSQL 18.4: `unsafe use of new value` (55P04). The
		// statements refused inside a transaction block were measured on
		// PostgreSQL 18.6, each answering `... cannot run inside a transaction
		// block` (25001). ANALYZE, REINDEX INDEX and REINDEX TABLE without
		// CONCURRENTLY, CLUSTER of a named table and DISCARD PLANS are
		// accepted there. CLUSTER with no table is refused too, but nothing
		// that follows a statement's first words tells it from CLUSTER of a
		// table, so it is not stated.
		Migration: descriptor.Migration{
			DirectivesWithoutDialect: true,
			EnumValuesCommitFirst:    true,
			RefusedInTransaction: []descriptor.TransactionRefusal{
				{Statement: "VACUUM", StartsWith: []string{"VACUUM"}},
				{Statement: "REINDEX ... CONCURRENTLY", StartsWith: []string{"REINDEX"}, Keywords: []string{"CONCURRENTLY"}},
				{Statement: "REINDEX SCHEMA, DATABASE or SYSTEM", StartsWith: []string{"REINDEX"}, Keywords: []string{"SCHEMA", "DATABASE", "SYSTEM"}},
				{Statement: "CREATE DATABASE", StartsWith: []string{"CREATE", "DATABASE"}},
				{Statement: "DROP DATABASE", StartsWith: []string{"DROP", "DATABASE"}},
				{Statement: "ALTER DATABASE ... SET TABLESPACE", StartsWith: []string{"ALTER", "DATABASE"}, Keywords: []string{"TABLESPACE"}},
				{Statement: "ALTER SYSTEM", StartsWith: []string{"ALTER", "SYSTEM"}},
				{Statement: "CREATE TABLESPACE", StartsWith: []string{"CREATE", "TABLESPACE"}},
				{Statement: "DROP TABLESPACE", StartsWith: []string{"DROP", "TABLESPACE"}},
				{Statement: "DISCARD ALL", StartsWith: []string{"DISCARD", "ALL"}},
			},
		},

		// Transactions: what migration/seeder and internal/schemaclean read.
		Savepoint: descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"},
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

// postgresFamilyRounding reads a value the PostgreSQL family's conversion would
// change, with the server's own CAST, which rounds as ALTER COLUMN TYPE does: measured
// on PostgreSQL 18.6, YugabyteDB 2026.1 and CockroachDB 26.4, CAST and the ALTER read
// 1234.6, -1234.6 and 1235 back for 1234.567, -1234.567 and an integer, 10:00:01 for
// 10:00:00.987654, '1 day 02:00:00' for '1 day 02:03:04' under interval hour, and
// infinity for infinity. A decimal is compared as an unlimited numeric, since money
// has no comparison with numeric. A serial is not a type CAST takes.
//
// Temporal precision is compared in the declared type with no precision.
// An interval compares with no field limit, so a coarser last field reads as
// a change. A conversion removing a zone is checked separately by counting
// non-NULL values: a clock time does not preserve its offset or instant.
var postgresFamilyRounding = descriptor.Rounding{
	Decimal:  "CAST({column} AS numeric) IS DISTINCT FROM CAST({column} AS {type})",
	Temporal: "CAST({column} AS {domain}) IS DISTINCT FROM CAST({column} AS {type})",
	CastTypes: map[string]string{
		"serial": "integer", "serial4": "integer", "bigserial": "bigint", "serial8": "bigint",
		"smallserial": "smallint", "serial2": "smallint",
	},
	Domains: map[string]string{"interval": "interval"},
}
