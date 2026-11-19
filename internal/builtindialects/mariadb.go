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
		Name:              "mariadb",
		VulnerabilityScan: "supported",
		Aliases:           []string{"maria", "mariadb+unix", "maria+unix"},
		Family:            "mysql",
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
		RevisionTable:   mysqlFamilyRevisionTable("SET STATEMENT sql_mode='STRICT_ALL_TABLES' FOR INSERT"),

		Schemas: descriptor.Schemas{
			Selection:                   descriptor.SchemaSelectionDatabase,
			StoredViewBodyNamesDatabase: true,
		},
		// mysql.user is server-wide, and a read lists every role in it.
		Roles: descriptor.Roles{OutOfScope: descriptor.RoleScopeNone},
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
		ManagedData: descriptor.ManagedData{DateTimeWallClock: true},
		Seeds: descriptor.Seeds{
			Transactional: true,
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
		// Measured on 12.3.3: two tables of a database each hold a CHECK named
		// ck, and one table refuses dd beside DD. It also refuses é beside É
		// and s beside ſ, and accepts i beside İ (U+0130). Ptah folds ASCII
		// letters only, so it never refuses a pair the server accepts and
		// leaves the server to refuse the rest.
		CheckNames: descriptor.CheckNames{
			Scope: descriptor.CheckNameScopeTable, Comparison: "ascii_case_insensitive",
			Refusal: "Error 1826 (HY000): Duplicate CHECK constraint name '{name}'",
		},
		Columns: descriptor.Columns{
			NationalCharAsChar: true, NationalCharCharset: "utf8mb3",
			Collations: true, InheritedCollationVaries: true,
			// Measured on MariaDB 11.8.9 and 12.3.3: utf8_bin is stored as
			// utf8mb3_bin, uca1400_ai_ci and DEFAULT on a utf8mb4 column as
			// utf8mb4_uca1400_ai_ci, and int COLLATE utf8mb4_bin keeps no
			// collation.
			CollationNames: descriptor.CollationNames{
				CharsetAliases:  map[string]string{"utf8": "utf8mb3"},
				CharsetDefaults: mariaDBCharsetDefaults(),
				WithoutCharset:  true, CharsetDefault: "DEFAULT",
				ListedWithoutCharset: []string{"uca1400_"},
				Types: []string{
					"char", "character", "nchar", "national", "varchar", "nvarchar",
					"tinytext", "text", "mediumtext", "longtext", "enum", "set",
				},
			},
			// Measured on MariaDB 12.3: DEFAULT (upper('abc')) and
			// DEFAULT (ucase('abc')) are both reported as ucase('abc').
			FunctionAliases: map[string]string{"ucase": "upper", "lcase": "lower"},
		},

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
		// Measured on MariaDB 11.8.9: a foreign key to a column no index covers
		// fails at CREATE TABLE with errno 150, and one to an indexed column no
		// unique key covers is created, as the indexed-reference policy says.
		ForeignKeyReferencesCheckedAtCreate: true,
		// Comparison: what comparing a declaration with a catalog reads.
		Comparison: descriptor.Comparison{
			ColumnCheckDropByModify:        true,
			TimePrecision:                  descriptor.TimePrecision{Compared: true},
			BareTypes:                      descriptor.BareTypes{Stated: true, DecimalPrecision: 10, FloatBits: 24, RealBits: 53, LobTiers: true},
			ColumnPrimaryKeyIsTheTablesKey: true,
			ForeignKeysOutside:             true,
			ForeignKeyBackingIndex:         true,
			DefaultGeneratedKind:           "VIRTUAL",
			DoubleQuotedStrings:            true,
			RestrictIsNoAction:             true,
			PrimaryKeyName:                 "PRIMARY",
			ConstraintComments:             true,
			RoutineNamesFold:               true,
			RoutineTypeAliases:             "mysqlroutine",
			ViewPredicatesAsIntegers:       true,
			ViewSQLSecurity:                true,
			AutoIncrementFlag:              true,
			// MariaDB 12.3 stores the default (7 % 4 * 2) as (7 MOD 4 * 2)
			// and a column a as `a`.
			OperatorPrecedence: "mysql-operators",
			// A whole server compares its databases too.
			ServerDatabases: true,
		},

		// As MySQL.
		Preparation: descriptor.Preparation{
			Enums:                   descriptor.EnumColumnType,
			SerialAutoIncrement:     true,
			ForeignKeyNamespace:     descriptor.NamespaceDatabase,
			ForeignKeyNamesFoldCase: true,
			JSONHeldAsText:          true,
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
			CatalogForm:   descriptor.MariaDBCatalogForm,
			DockerImages:  []string{"maria", "mariadb"},
			DockerDrivers: []string{"maria", "mariadb"},
			ChangeWords:   descriptor.MySQLDriverChangeWords,
			CheckSpelling: descriptor.DevCheckSpelling,
			HCL:           mysqlFamilyAtlasHCL(),
		},

		// Grammar: how the shared DDL parser reads the engine's DDL.
		// MariaDB 11.8 takes no [NOT] ENFORCED, accepts MATCH and records
		// NONE, takes one CHECK on a column, and takes a column CONSTRAINT
		// before REFERENCES alone and before nothing in a MODIFY
		// (stokaro/ptah#3745, #3759). It spells a hidden index IGNORED, and
		// takes IF [NOT] EXISTS on a column (stokaro/ptah#3877).
		Statements: descriptor.Statements{DropViewIfExistsSkipsOtherKinds: true},
		Grammar: descriptor.Grammar{
			Columns: descriptor.ColumnGrammar{
				DefaultOperatorRefusal: "MariaDB 12.3 takes an expression default only in parentheses and answers " +
					"ERROR 1064 (42000) to an operator outside them",
				DefaultPipesRefusal: "MariaDB 11.8 to 12.3 read || as OR under their default SQL mode and store " +
					"DEFAULT ('a' || 'b') as ('a' or 'b'), which inserts 0; under PIPES_AS_CONCAT it is a " +
					"concatenation",
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
			Forms:      mariaDBStatementForms(),
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
		Migration: descriptor.Migration{DirectivesWithoutDialect: true, SameNamedForeignKeysApart: true},

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

		// Transactions: what migration/seeder and internal/schemaclean read.
		Savepoint: descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"},
	}
}

// mariaDBCharsetDefaults is the descriptor's
// [descriptor.CollationNames.CharsetDefaults]: information_schema.CHARACTER_SETS
// on MariaDB 11.8 and 12.3. MariaDB 10.11 defaults the Unicode character sets
// to their _general_ci collations.
func mariaDBCharsetDefaults() map[string]string {
	return map[string]string{
		"armscii8": "armscii8_general_ci", "ascii": "ascii_general_ci",
		"big5": "big5_chinese_ci", "binary": "binary", "cp1250": "cp1250_general_ci",
		"cp1251": "cp1251_general_ci", "cp1256": "cp1256_general_ci",
		"cp1257": "cp1257_general_ci", "cp850": "cp850_general_ci", "cp852": "cp852_general_ci",
		"cp866": "cp866_general_ci", "cp932": "cp932_japanese_ci", "dec8": "dec8_swedish_ci",
		"eucjpms": "eucjpms_japanese_ci", "euckr": "euckr_korean_ci",
		"gb2312": "gb2312_chinese_ci", "gbk": "gbk_chinese_ci", "geostd8": "geostd8_general_ci",
		"greek": "greek_general_ci", "hebrew": "hebrew_general_ci", "hp8": "hp8_english_ci",
		"keybcs2": "keybcs2_general_ci", "koi8r": "koi8r_general_ci",
		"koi8u": "koi8u_general_ci", "latin1": "latin1_swedish_ci",
		"latin2": "latin2_general_ci", "latin5": "latin5_turkish_ci",
		"latin7": "latin7_general_ci", "macce": "macce_general_ci",
		"macroman": "macroman_general_ci", "sjis": "sjis_japanese_ci",
		"swe7": "swe7_swedish_ci", "tis620": "tis620_thai_ci", "ucs2": "ucs2_uca1400_ai_ci",
		"ujis": "ujis_japanese_ci", "utf16": "utf16_uca1400_ai_ci",
		"utf16le": "utf16le_general_ci", "utf32": "utf32_uca1400_ai_ci",
		"utf8mb3": "utf8mb3_uca1400_ai_ci", "utf8mb4": "utf8mb4_uca1400_ai_ci",
	}
}
