package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

// mariaDB is MariaDB, in the MySQL family. The pinned community binary
// accepts the maria spelling (#3744), and the +unix spellings are its socket
// transport (#3755). Its identifier rules are MySQL's, for the reason given
// there.
func mariaDB() descriptor.Dialect {
	return descriptor.Dialect{
		Name:    "mariadb",
		Aliases: []string{"maria", "mariadb+unix", "maria+unix"},
		Family:  "mysql",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "table",
			IndexNames:     "ascii_folded_non_ascii_unknown",
			TableNames:     "exact",
			ColumnNames:    "ascii_folded_non_ascii_unknown",
		},
		Placeholder: descriptor.PlaceholderQuestion,

		// MariaDB names itself in its version string even over the MySQL
		// protocol, behind a "5.5.5-" replication prefix that is not the
		// version. 10.2 brought generic DROP CONSTRAINT, enforced CHECK and
		// the IF EXISTS guards. A banner with no number still reports the
		// newest measured line.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "MariaDB1011",
			BannerTokens:  []string{"mariadb"},
			Versions: descriptor.Versions{
				Scheme: "replication-prefix",
				Floor:  "MariaDBLegacy",
				Steps: []descriptor.VersionStep{
					{Min: "10.2", Preset: "MariaDB1011"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.MariaDBMeasured(),
				Newest:      capabilityline.MariaDB12,
				Unreadable:  descriptor.UnreadableVersion{ReportNewest: true},
			},
		},
		IdentifierLimit:      descriptor.IdentifierLimit{Max: 64, Unit: "characters"},
		GrantOptionPerObject: true,

		// How a connection reaches the server.
		Driver: "mysql",

		// No release of the family has a FULL OUTER JOIN, and OFFSET is
		// accepted only after a LIMIT: the largest BIGINT UNSIGNED is the
		// documented "every row from the offset on". ON DUPLICATE KEY UPDATE
		// fires for every unique key on the table, so it cannot be scoped to
		// the columns a caller names.
		Query: descriptor.Query{
			UnsupportedJoins: []descriptor.Join{descriptor.JoinFull},
			OffsetOnlyLimit:  "18446744073709551615",
			Upsert:           descriptor.UpsertOnDuplicateKey,
		},
		Scripts: descriptor.Scripts{ClientDelimiters: true},

		// The grammars the planner asks for non-blocking DDL in.
		OnlineAlter: descriptor.OnlineAlterAlgorithmLock,

		// Migrations: what the migrator reads.
		DDLTransactions: "implicit_commit",
		RevisionTable:   mysqlFamilyRevisionTable(),

		Schemas: descriptor.Schemas{Selection: descriptor.SchemaSelectionDatabase},
		// String literals process C-style backslash escapes by default, so a
		// backslash in a value is doubled, and a boolean is the TINYINT(1) the
		// family creates it as. Neither engine's DATETIME literal accepts an
		// offset; the wall clock round-trips in the session's time zone.
		Literals: descriptor.Literals{
			NumericBool:      true,
			BackslashEscapes: true,
			BinaryPrefix:     "X'",
			BinarySuffix:     "'",
		},
		Seeds: descriptor.Seeds{
			Transactional: true,
			Savepoint:     descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"},
			TablesQuery:   "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'",
		},

		// How an identifier is written, and how a column's own key is named.
		Quoting:    descriptor.Quoting{Style: descriptor.QuoteBacktick},
		ColumnKeys: descriptor.ColumnKeyAfterColumn,

		// Cleanup: what a development database cleanup reads.
		Cleanup: descriptor.Cleanup{
			ForeignKeys:    true,
			Views:          true,
			StoredObjects:  true,
			RevisionTables: true,
			RevisionProbe:  mysqlRevisionProbe,
			ForeignKeyDrop: "DROP FOREIGN KEY",
			DropsDatabases: true,
			ViewsFirst:     true,
		},

		// Lint: what migration lint reads.
		Lint: descriptor.Lint{
			Analyzed:        true,
			Scanner:         "hash",
			DynamicSQL:      "prepare-from",
			InjectionAdvice: "prepare a literal statement with ? placeholders and pass the values through EXECUTE ... USING; an identifier that must vary has no placeholder and needs a whitelist",
			NotNullConsequence: "a row holding NULL fails the rebuild under the strict SQL mode MariaDB defaults to (Data truncated for column), " +
				"and is silently rewritten to the type's implicit default (0, '', or the zero date) when strict mode is off; " +
				"a DEFAULT in the same clause applies to new rows only. The rebuild itself runs in place with writes allowed (ALGORITHM=INPLACE, LOCK=NONE)",
			OnlineMeasured:   true,
			TypeRestatements: "mytype",
			AtlasAnalyzer:    "MY",
		},

		// How a schema file's names are read, and how unnamed indexes and CHECKs are named.
		Names: descriptor.Names{Lookup: "unicode_case_insensitive"},
		// A name an index takes after a column keeps the whole column before
		// `_2` and on, where MySQL cuts it.
		IndexNaming: descriptor.IndexNaming{MaxBytes: 64, DescendingCovers: true, UniqueIsIndex: true},
		CheckNaming: descriptor.CheckNamingColumnOrConstraint,

		// Dev databases. MariaDB has no server UUID; its host name, port and data
		// directory tell one server from another.
		DevRealm: descriptor.DevRealm{
			Identity:        "query",
			Query:           "SELECT DATABASE()",
			ServerQuery:     "SELECT CONCAT_WS(':', @@hostname, @@port, @@datadir)",
			ScratchDatabase: true,
		},
		SchemaIsDatabase: true,

		// How the common model is lowered for the engine.
		GeneratedTimeouts:        descriptor.GeneratedTimeouts{Lock: "3s", Statement: "30s"},
		GeneratedColumnDefault:   "VIRTUAL",
		InlineEnum:               descriptor.InlineEnum{Type: descriptor.InlineEnumValues},
		IndexesBeforeForeignKeys: true,
		// Comparison: what comparing a declaration with a catalog reads.
		Comparison: descriptor.Comparison{
			ForeignKeysOutside:       true,
			ForeignKeyBackingIndex:   true,
			DefaultGeneratedKind:     "VIRTUAL",
			DoubleQuotedStrings:      true,
			RestrictIsNoAction:       true,
			PrimaryKeyName:           "PRIMARY",
			ConstraintComments:       true,
			RoutineNamesFold:         true,
			RoutineTypeAliases:       "mysqlroutine",
			ViewPredicatesAsIntegers: true,
			AutoIncrementFlag:        true,
			// A whole server compares its databases too.
			ServerDatabases: true,
		},

		// As MySQL.
		Preparation: descriptor.Preparation{
			Enums:                   descriptor.EnumColumnType,
			SerialAutoIncrement:     true,
			ForeignKeyNamespace:     descriptor.NamespaceDatabase,
			ForeignKeyNamesFoldCase: true,
		},

		// A table directive's engine and comment attributes, and a field's
		// comment, are the family's platform properties, and so is a table's
		// row format, which a converted declaration keeps for the index hints
		// that need it. A primary key carries a comment of its own. An
		// embedded relation's INTEGER key column is INT, the type a SERIAL key
		// is written as here.
		Conversion: descriptor.Conversion{
			PropertyShortcuts:    true,
			SerialForeignKeyType: "INT",
			RowFormatProperty:    true,
			ConstraintComments:   true,
		},

		// Lexing: what internal/dialectlexer and the body scanner of
		// internal/deporder read. Measured on 11.8.9, a # comment runs to the
		// end of the line, 1--1 is a subtraction, and with the default
		// sql_mode a backslash escapes in '...' and "..." but not in a
		// backtick-quoted name: `a\` names a table whose name ends in one.
		// The catalog writes a view's strings with backslash escapes, as in
		// 'it\'s'.
		Lexical: descriptor.Lexical{
			LineComments:          []string{"#"},
			BackslashEscapes:      true,
			DashCommentNeedsSpace: true,
			ExecutableComments:    descriptor.ExecutableCommentsMBang,
		},

		// Database URLs and the Atlas contract: what internal/atlasurl and the
		// Atlas HCL reader read. The MariaDB schemes open the MySQL driver, so
		// a URL cannot tell a MariaDB server from a MySQL one, and the pinned
		// community binary accepts any pair of the three schemes for a target
		// and its dev database (stokaro/ptah#3756).
		URLs: descriptor.URLs{DefaultPort: 3306, ComparedAs: "mysql", Grammar: descriptor.URLGrammarGoSQLDriver},
		Atlas: descriptor.Atlas{
			DockerImages:  []string{"maria", "mariadb"},
			DockerDrivers: []string{"maria", "mariadb"},
			HCL:           mysqlFamilyAtlasHCL(),
		},

		// Grammar: how the shared DDL parser reads the engine's DDL.
		// MariaDB 11.8 takes no [NOT] ENFORCED, accepts MATCH and records
		// NONE, takes one CHECK on a column, and takes a column CONSTRAINT
		// before REFERENCES alone and before nothing in a MODIFY
		// (stokaro/ptah#3745, #3759). It spells a hidden index IGNORED, and
		// takes IF [NOT] EXISTS on a column (stokaro/ptah#3877).
		Grammar: descriptor.Grammar{
			Columns: descriptor.ColumnGrammar{
				SecondCheckRefusal: "MariaDB accepts one CHECK on a column and answers ERROR 1064 (42000) to the " +
					"second; write it at table level",
				Constraint:              descriptor.ColumnConstraint{Define: descriptor.ConstraintKindReferences},
				ModifyRefusesReferences: true,
			},
			Constraints: descriptor.ConstraintGrammar{
				Match: descriptor.Match{
					Clause: true,
					Refusal: "MariaDB 11.8.9 accepts the clause and records NONE, so the key it builds is MATCH " +
						"SIMPLE; declare the key without the clause",
				},
				Deferral:       descriptor.Deferral{Refusal: mysqlFamilyDeferralRefusal},
				SymbolOptional: true,
			},
			Keys: descriptor.KeyGrammar{
				IndexKeywords: []string{"KEY", "INDEX", "FULLTEXT", "SPATIAL"},
				Options:       true,
				Visibility: descriptor.KeyVisibility{
					Refused: []string{"INVISIBLE"},
					Refusal: "it is MySQL's clause, and MariaDB 11.8.9 answers ERROR 1064; MariaDB spells it IGNORED",
				},
				BareUniqueName:      true,
				ForeignKeyIndexName: true,
				// Measured on 11.8.9, `FOREIGN KEY idx_c_p (p_id) REFERENCES
				// p(id)` with no CONSTRAINT symbol holds the key idx_c_p over
				// the index idx_c_p, in CREATE TABLE and in ALTER TABLE ... ADD.
				// MySQL 8.4.11 and 26.7.0 hold the key c_ibfk_1 over the index.
				ForeignKeyIndexNameNamesKey: true,
				RefusesNullsDistinct:        true,
			},
			AlterTable: descriptor.AlterTableGrammar{
				AddIndex:           true,
				RenameIndex:        true,
				Modify:             descriptor.ModifyDefinition,
				ChecksCapabilities: true,
				ColumnGuards:       true,
			},
			Statements: descriptor.StatementGrammar{UseSelectsDatabase: true, RoleIfNotExists: true},
			Routines: descriptor.RoutineGrammar{
				Body:                 descriptor.RoutineCompoundStatement,
				TriggerStatementBody: true,
			},
		},

		// Clean checks: what internal/migrateclean reads. MariaDB's adoption
		// is refused because it reads the same information_schema through
		// the same code path as MySQL, which is inferred rather than
		// measured. The pinned community binary refuses a dev database that
		// holds a table, measured on MariaDB 11.8 on 2026-09-26.
		CleanCheck: descriptor.CleanCheck{Adoption: true, Dev: true},
		// A database carries a character set and a collation, as on MySQL.
		SchemaAttributesQuery: mysqlSchemaAttributes,

		// Foreign key naming: as MySQL, measured on 11.8.9. From 12.1 the
		// server writes the key's number alone, `1`, which
		// internal/mysqlname reads as the same key.
		ForeignKeyNaming: descriptor.ForeignKeyNamingTableNumbered,

		// Migrations: the engine's reading decides a migration file's
		// directives when its dialect is unknown.
		Migration: descriptor.Migration{DirectivesWithoutDialect: true},

		// Reach: none of its own. MariaDB's constructs that reach outside the
		// database are MySQL's, which MySQL's descriptor states and a scan
		// refuses on every engine.

		// Foreign key name rules: where MariaDB keeps other names than MySQL.
		// Measured over a utf8mb4 connection on 11.8.9, CREATE TABLE answers
		// ERROR 1059 to a derived name of 64 characters, and ALTER TABLE cuts
		// a longer one to 64 without a warning. Measured on 12.1.2, 12.2.2 and
		// 12.3.3, an unnamed key is `<n>` where 12.0.2 and earlier write
		// `<table>_ibfk_<n>`.
		ForeignKeyNameRules: descriptor.ForeignKeyNameRules{CreateTableMaxChars: 63, BareNumbers: true},
	}
}
