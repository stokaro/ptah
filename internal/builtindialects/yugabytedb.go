package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

// yugabyteDB is YugabyteDB through its PostgreSQL-compatible YSQL API, in the
// PostgreSQL family, with PostgreSQL's identifier rules.
func yugabyteDB() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "yugabytedb",
		Aliases:       []string{"yugabyte", "ysql"},
		Family:        "postgres",
		DefaultSchema: "public",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "schema",
			IndexNames:     "exact",
			TableNames:     "exact",
			ColumnNames:    "exact",
		},
		Placeholder: descriptor.PlaceholderDollar,

		// A YugabyteDB banner opens with the PostgreSQL compatibility version,
		// "PostgreSQL 11.2-YB-2024.2.4.0-b0", and names its own release after
		// the "-YB-" marker. The PostgreSQL 11 to 15 engine swap landed in the
		// 2025 line. A string with no readable release takes the lower arm,
		// whose plan runs on both engines (stokaro/ptah#916).
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "YugabyteDB25",
			BannerTokens:  []string{"yugabyte", "-yb-"},
			Versions: descriptor.Versions{
				Scheme: "yb-release",
				Floor:  "YugabyteDB24",
				Steps: []descriptor.VersionStep{
					{Min: "2025", Preset: "YugabyteDB25"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.YugabyteDBMeasured(),
				Newest:      capabilityline.YugabyteDB2026,
				Unreadable: descriptor.UnreadableVersion{
					Preset:     "YugabyteDB24",
					Recognized: true,
				},
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
		Names:       descriptor.Names{Unquoted: "ascii_case_insensitive", PublicKeyword: true},
		IndexNaming: descriptor.IndexNaming{PrimaryKeySuffix: "pkey"},
		Statements:  descriptor.Statements{ViewsBlockDropTable: true},

		// Dev databases.
		DevRealm: descriptor.DevRealm{
			Identity:        "query",
			Query:           "SELECT current_database()",
			ScratchDatabase: true,
		},

		// How the common model is lowered for the engine.
		GeneratedColumnDefault: "STORED",
		// Comparison: what comparing a declaration with a catalog reads.
		Comparison: descriptor.Comparison{
			ExpressionProbes:   "pg-catalog",
			RolePasswords:      true,
			RoutineTypeAliases: "pg-aliases",
			QualifiedTypeNames: true,
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
		// read. Nobody has measured PostgreSQL's continued and U&'...' strings
		// on YugabyteDB, so a text that writes one is two tokens and is
		// refused rather than read by a rule the server may not share.
		Lexical: descriptor.Lexical{EscapeStrings: true},
		Reach:   postgresReach(),

		// Database URLs: what internal/atlasurl reads.
		URLs: descriptor.URLs{DefaultPort: 5433},

		// Grammar: how the shared DDL parser reads the engine's DDL.
		// 2026.1.2 takes NOT VALID and MATCH FULL.
		Grammar: descriptor.Grammar{
			Columns: descriptor.ColumnGrammar{CurrentTimeKeywords: true, StoredStringDefaults: true},
			Constraints: descriptor.ConstraintGrammar{
				NotValid: true,
				Match:    descriptor.Match{Clause: true},
			},
			Statements: descriptor.StatementGrammar{Extensions: true, UserTypes: true, GrantTargetLists: true},
			Routines: descriptor.RoutineGrammar{
				Body:              descriptor.RoutineQuotedBody,
				TriggerConditions: true,
			},
		},
	}
}
