package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

// cockroachDB is CockroachDB, in the PostgreSQL family.
//
// It resolves an unqualified name in `public`, as PostgreSQL does: measured
// on v26.3.2, search_path is `"$user", public` and a table created as `t` is
// read as `public.t`. Without the default, `t` and `public.t` are two names
// offline, and a render creates a routine returning `SETOF public.t` before
// the table `t` (stokaro/ptah#4065). Index names stay table-scoped: the same
// server accepts one index name on two tables.
func cockroachDB() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "cockroachdb",
		Aliases:       []string{"cockroach", "crdb"},
		Family:        "postgres",
		DefaultSchema: "public",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "table",
			IndexNames:     "exact",
			TableNames:     "exact",
			ColumnNames:    "exact",
		},
		Placeholder: descriptor.PlaceholderDollar,

		// The whole 25 line takes CockroachDB25, not only the measured 25.4
		// minor: autocommit_before_ddl defaults on across the line, measured
		// on v25.1.10, v25.2.22, v25.3.7 and v25.4.5 (stokaro/ptah#1849).
		// 26.3 is the first line carrying CREATE DOMAIN (stokaro/ptah#1735).
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "CockroachDB26",
			BannerTokens:  []string{"cockroachdb"},
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "CockroachDB23",
				Steps: []descriptor.VersionStep{
					{Min: "25", Preset: "CockroachDB25"},
					{Min: capabilityline.CockroachDB26, Preset: "CockroachDB26"},
					{Min: capabilityline.CockroachDB263, Preset: "CockroachDB263"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.CockroachDBMeasured(),
				Newest:      capabilityline.CockroachDB263,
			},
		},
		IdentifierLimit: descriptor.IdentifierLimit{Max: 63, Unit: "bytes"},

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
		// The PostgreSQL driver and the transactional migrator path PostgreSQL
		// takes, so this is the contract that code implements; no live
		// revision-completion test drives it yet.
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
			Savepoint:     descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"},
		},

		// How an identifier is written, and how a column's own key is named.
		Quoting: descriptor.Quoting{Style: descriptor.QuoteDouble},

		// Cleanup: what a development database cleanup reads.
		Cleanup: descriptor.Cleanup{
			ForeignKeys:       true,
			Views:             true,
			MaterializedViews: true,
			Enums:             true,
			Domains:           true,
			Composites:        true,
			Ranges:            true,
			Functions:         true,
			RevisionTables:    true,
			RevisionProbe:     postgresRevisionProbe,
			SchemaScoped:      true,
			Restrict:          true,
		},

		// Lint: what migration lint reads.
		Lint: descriptor.Lint{Analyzed: true},

		// How a schema file's names are read, and how unnamed indexes and CHECKs are named.
		Names:       descriptor.Names{Unquoted: "unicode_case_insensitive", Roles: "unicode_case_insensitive", PublicKeyword: true},
		IndexNaming: descriptor.IndexNaming{PrimaryKeySuffix: "pkey"},
		Statements:  descriptor.Statements{ViewsBlockDropTable: true},

		// Dev databases. The server takes no advisory lock Ptah can hold, so runs on
		// one dev database are serialized with a lock file, which only serializes
		// runs on this machine.
		DevRealm: descriptor.DevRealm{
			Identity:        "query",
			Query:           "SELECT current_database()",
			FileLock:        "loopback",
			ScratchDatabase: true,
		},

		// How the common model is lowered for the engine.
		GeneratedColumnDefault: "STORED",
		// Comparison: what comparing a declaration with a catalog reads.
		// A probe's view goes in a schema, and a routine body is stored
		// reflowed and qualified: measured on v26.3.2, `SELECT * FROM
		// public.items` is stored as `SELECT public.items.id, ... FROM
		// f1.public.items;` (stokaro/ptah#4058).
		Comparison: descriptor.Comparison{
			ForeignKeysOutside:      true,
			ExpressionProbes:        "pg-catalog",
			ProbeViewsInSchema:      true,
			RewritesRoutineBodies:   true,
			RolePasswords:           true,
			RoutineTypeAliases:      "pg-aliases",
			QualifiedTypeNames:      true,
			CatalogTypeNames:        true,
			PrimaryKeyStoresColumns: true,
		},

		// A bare type name PostgreSQL defines is the built-in type here too.
		Preparation: descriptor.Preparation{
			PostgresBuiltInTypes: true,
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
		// read. CockroachDB 26.3, measured, refuses a -- comment between
		// continued strings and has no U&'...' strings, so it keeps only the
		// E'...' prefix.
		Lexical: descriptor.Lexical{EscapeStrings: true},
		Reach:   postgresReach(),

		// Database URLs: what internal/atlasurl reads.
		URLs: descriptor.URLs{DefaultPort: 26257},

		// Grammar: how the shared DDL parser reads the engine's DDL.
		// v26.3.2 takes NOT VALID and MATCH FULL, refuses every deferral
		// clause, and reads INDEX in a table body as an index only before a
		// column list. An index may end with its visibility, which ALTER
		// INDEX table@index changes.
		Grammar: descriptor.Grammar{
			Columns: descriptor.ColumnGrammar{CurrentTimeKeywords: true, StoredStringDefaults: true},
			Constraints: descriptor.ConstraintGrammar{
				NotValid: true,
				Match:    descriptor.Match{Clause: true},
				Deferral: descriptor.Deferral{
					Refusal: "CockroachDB has no deferrable constraints, and v26.3.2 refuses the clause, NOT DEFERRABLE included",
				},
			},
			Keys:       descriptor.KeyGrammar{IndexKeywords: []string{"INDEX"}, IndexKeywordsNeedColumns: true},
			Indexes:    descriptor.IndexGrammar{TrailingVisibility: true, Alter: descriptor.AlterIndexVisibility},
			Statements: descriptor.StatementGrammar{Extensions: true, UserTypes: true, GrantTargetLists: true},
			Routines: descriptor.RoutineGrammar{
				Body:              descriptor.RoutineQuotedBody,
				TriggerConditions: true,
			},
		},

		// Clean checks: what internal/migrateclean reads. The pinned
		// community binary reaches CockroachDB through a postgres:// URL and
		// judges a dev database by the PostgreSQL rules, measured on
		// CockroachDB 26.2 on 2026-09-27: it refuses a table in the connected
		// schema and, with no search_path, any schema but an empty public. It
		// does not open cockroachdb://, and its `migrate apply` gate was never
		// measured here, so Ptah does not refuse an adoption.
		CleanCheck: descriptor.CleanCheck{Dev: true, ConnectedSchema: true},

		// System schemas: what internal/pgsystemschema reads. CockroachDB
		// exposes crdb_internal through the PostgreSQL catalog surface, but its
		// virtual relations are not ordinary user tables and the PostgreSQL
		// reader cannot inspect them as comparison input, so a read leaves the
		// schema out and a document may not hold it. Matching is exact: a
		// quoted CRDB_INTERNAL is the user's.
		SystemSchemas: descriptor.SystemSchemas{
			Catalog:  []string{"crdb_internal"},
			Reserved: []string{"crdb_internal"},
		},

		// Migrations: the engine's reading decides a migration file's
		// directives when its dialect is unknown, and a generated ADD VALUE is
		// planned by PostgreSQL's rule: it commits before a statement uses the
		// value.
		Migration: descriptor.Migration{
			DirectivesWithoutDialect: true,
			EnumValuesCommitFirst:    true,
		},
	}
}
