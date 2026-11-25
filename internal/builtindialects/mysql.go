package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

// mysql is MySQL, the head of the family MariaDB belongs to. mysql+unix is
// the socket transport the pinned community binary opens (#3755); it selects
// the same schema semantics.
//
// Both engines of the family fold index and column names with a collation
// table Ptah has no offline copy of, and they disagree about every non-ASCII
// pair measured. ASCII folding is shared and exact, so it is kept; beyond
// ASCII the equivalence class is unknown and is reported as a possible
// conflict rather than guessed at in either direction (stokaro/ptah#2768).
func mysql() descriptor.Dialect {
	return descriptor.Dialect{
		Name:              "mysql",
		VulnerabilityScan: "supported",
		Aliases:           []string{"mysql+unix"},
		Family:            "mysql",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "table",
			IndexNames:     "ascii_folded_non_ascii_unknown",
			TableNames:     "exact",
			ColumnNames:    "ascii_folded_non_ascii_unknown",
		},
		Placeholder: descriptor.PlaceholderQuestion,

		// MySQL's version string names no product: SELECT VERSION() answers a
		// bare "9.7.2", so the ladder is reached only through the declared
		// dialect. The 8.0 steps are patch releases, each the threshold of the
		// gate it replaced: VIEW_TABLE_USAGE at 8.0.13, enforced CHECK at
		// 8.0.16, DROP CONSTRAINT at 8.0.19, SHOW_ROUTINE at 8.0.20
		// (stokaro/ptah#916) and an instant column placed before existing
		// ones at 8.0.29.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "MySQL84",
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "MySQLLegacy",
				Steps: []descriptor.VersionStep{
					{Min: "8.0.13", Preset: "MySQL8013"},
					{Min: "8.0.16", Preset: "MySQL8016"},
					{Min: "8.0.19", Preset: "MySQL8019"},
					{Min: "8.0.20", Preset: "MySQL8020"},
					{Min: "8.0.29", Preset: "MySQL8029"},
					{Min: "8.4", Preset: "MySQL84"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.MySQLMeasured(),
				Newest:      capabilityline.MySQL2610,
			},
		},
		IdentifierLimit:      descriptor.IdentifierLimit{Max: 64, Unit: "characters"},
		GrantOptionPerObject: true,
		// A REVOKE on a table takes only a grant on the table, and one of a
		// privilege the role holds there through no grant fails with Error
		// 1147: measured on 8.4.13, also under a grant on the database or on
		// *.*.
		TableRevokeTakesOnlyTableGrants: true,
		// lower_case_table_names decides it per server; a connection reads
		// it into catalog.ServerInfo.NameFolding.
		NamesFoldBySetting: true,
		// A column privilege held through a role serves only the spelling
		// the GRANT used; see platform.ColumnPrivilegeIsBySpelling. MariaDB
		// serves either, so its descriptor leaves this out.
		ColumnPrivilegeBySpelling: true,
		// What GRANT ALL grants on a table and on a database, one row each in
		// information_schema: measured on MySQL 8.0.46, 8.4.13, 9.7.4 and
		// 26.10.0, which grant the same lists.
		AllPrivileges: mysqlAllPrivileges(),

		// How a connection reaches the server.
		Driver: "mysql",

		Query:   mysqlFamilyQuery(),
		Scripts: descriptor.Scripts{ClientDelimiters: true},

		// The grammars the planner asks for non-blocking DDL in.
		OnlineAlter: descriptor.OnlineAlterAlgorithmLock,

		// Migrations: what the migrator reads.
		DDLTransactions:     "implicit_commit",
		AtomicIndexCreation: true,
		RevisionTable:       mysqlFamilyRevisionTable("INSERT /*+ SET_VAR(sql_mode='STRICT_ALL_TABLES') */"),

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
			NotNullConsequence: "a row holding NULL fails the rebuild under the strict SQL mode MySQL defaults to (Invalid use of NULL value), " +
				"and is silently rewritten to the type's implicit default (0, '', or the zero date) when strict mode is off; " +
				"a DEFAULT in the same clause applies to new rows only. The rebuild itself runs in place with writes allowed (ALGORITHM=INPLACE, LOCK=NONE)",
			OnlineMeasured:   true,
			TypeRestatements: "mytype",
			AtlasAnalyzer:    "MY",
			// The read lists every view and trigger of the database it reads.
			ListedObjects: []string{"view", "trigger"},
		},

		// How a schema file's names are read, and how unnamed indexes and CHECKs are named.
		Names: descriptor.Names{Lookup: "unicode_case_insensitive"},
		// A name an index takes after a column keeps 61 bytes of the column
		// before `_2` and on: measured on 9.7.2, a 64-character column yields a
		// 63-character `_2` and a 64-character `_10`.
		IndexNaming: descriptor.IndexNaming{
			MaxBytes: 64, FunctionalBase: "functional_index", UniqueIsIndex: true, BaseBytes: 61,
		},
		CheckNaming: descriptor.CheckNamingTableNumbered,
		CheckNames:  mysqlCheckNames,
		Columns: descriptor.Columns{
			NationalCharAsChar: true, NationalCharCharset: "utf8mb3", CheckOwnColumnOnly: true,
			Collations: true, InheritedCollationVaries: true,
			// Measured on MySQL 8.4.13 and 26.10.0: utf8_bin is stored as
			// utf8mb3_bin, and int COLLATE utf8mb4_bin keeps no collation.
			// MySQL has no UCA 14.0.0 collation. The family is declared all
			// the same, because a mysql:// URL may name a MariaDB server,
			// which lists it with no character set.
			CollationNames: descriptor.CollationNames{
				CharsetAliases:       map[string]string{"utf8": "utf8mb3"},
				CharsetDefaults:      mysqlCharsetDefaults(),
				ListedWithoutCharset: []string{"uca1400_"},
				Types: []string{
					"char", "character", "nchar", "national", "varchar", "nvarchar",
					"tinytext", "text", "mediumtext", "longtext", "enum", "set",
				},
			},
			// Measured on MySQL 26.10: DEFAULT (ucase('abc')) is reported
			// as upper(...).
			FunctionAliases: map[string]string{"ucase": "upper", "lcase": "lower"},
		},

		// Dev databases.
		DevRealm: descriptor.DevRealm{
			Identity:        "query",
			Query:           "SELECT DATABASE()",
			ServerQuery:     "SELECT @@server_uuid",
			ScratchDatabase: true,
		},
		SchemaIsDatabase: true,

		// How the common model is lowered for the engine.
		GeneratedTimeouts:        descriptor.GeneratedTimeouts{Lock: "3s", Statement: "30s"},
		GeneratedColumnDefault:   "VIRTUAL",
		InlineEnum:               descriptor.InlineEnum{Type: descriptor.InlineEnumValues, TrailingSpacesRemoved: true},
		IndexesBeforeForeignKeys: true,
		// Measured on MySQL 8.4.13, where the target requires a unique reference,
		// with Error 6125 for an indexed column no unique key covers, and on 8.0.46,
		// where it requires an indexed one, with Error 1822 for a column no index
		// covers: the server applies the policy the capabilities state at CREATE
		// TABLE.
		ForeignKeyReferencesCheckedAtCreate: true,
		// Comparison: what comparing a declaration with a catalog reads.
		// MySQL creates an index for every foreign key unasked, and a
		// database is the schema, so a key into another database is accepted
		// and read back with that database's name (stokaro/ptah#3891).
		Comparison: descriptor.Comparison{
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
			// MySQL 26.10 stores the default (2 * 3 + 1) as ((2 * 3) + 1).
			OperatorPrecedence: "mysql-operators",
			// A whole server compares its databases too.
			ServerDatabases: true,
		},

		// An enum column is the ENUM column type, and a serial type is the
		// integer with AUTO_INCREMENT: written as INT alone, a SERIAL key took
		// no value of its own. A foreign key name is unique in the database
		// and compared without regard to case.
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
		// internal/deporder read. Measured on 8.4.11, a # comment runs to the
		// end of the line, 1--1 is a subtraction, and with the default
		// sql_mode a backslash escapes in '...' and "..." but not in a
		// backtick-quoted name: `a\` names a table whose name ends in one.
		// The catalog writes a view's strings with backslash escapes, as in
		// 'it\'s'.
		Lexical: descriptor.Lexical{
			LineComments:          []string{"#"},
			BackslashEscapes:      true,
			DashCommentNeedsSpace: true,
			ExecutableComments:    descriptor.ExecutableCommentsBang,
		},

		// Database URLs and the Atlas contract: what internal/atlasurl and the
		// Atlas HCL reader read.
		URLs: descriptor.URLs{DefaultPort: 3306, Grammar: descriptor.URLGrammarGoSQLDriver},
		Atlas: descriptor.Atlas{
			CatalogForm:   descriptor.MySQLCatalogForm,
			DockerImages:  []string{"mysql"},
			DockerDrivers: []string{"mysql"},
			ChangeWords:   descriptor.MySQLDriverChangeWords,
			CheckSpelling: descriptor.DevCheckSpellingUnwrapped,
			HCL:           mysqlFamilyAtlasHCL(),
		},

		// Grammar: how the shared DDL parser reads the engine's DDL.
		// MySQL 8.4 keeps the last [NOT] ENFORCED after a CHECK and takes none
		// after a foreign key, and records MATCH FULL and PARTIAL without
		// enforcing either. A column CONSTRAINT precedes a CHECK alone, in a
		// MODIFY too, and a MODIFY takes no REFERENCES (stokaro/ptah#3745,
		// #3759). What a column-level REFERENCES builds depends on the release
		// (stokaro/ptah#3760). Index keywords, options, the bare UNIQUE name,
		// the FOREIGN KEY index name and functional key parts are the family's
		// table body; MariaDB has no functional key parts. MySQL takes no IF
		// [NOT] EXISTS on a column (stokaro/ptah#3877).
		Statements: descriptor.Statements{DropViewIfExistsSkipsOtherKinds: true},
		Grammar: descriptor.Grammar{
			Columns: descriptor.ColumnGrammar{
				DefaultOperatorRefusal: "MySQL 26.10 takes an expression default only in parentheses and answers " +
					"ERROR 1064 (42000) to an operator outside them",
				DefaultPipesRefusal: "MySQL 8.4 to 26.10 read || as OR under their default SQL mode and store " +
					"DEFAULT ('a' || 'b') as ((0 <> 'a') or (0 <> 'b')), so an INSERT that takes the default " +
					"answers ERROR 1292 (22007); under PIPES_AS_CONCAT it is a concatenation",
				ReferencesRefusal: "MySQL 8.4 builds nothing from the clause, while MySQL 9.7 and 26.7 build a " +
					"foreign key and its index, and the SQL file is read without the server version, so Ptah " +
					"refuses the clause rather than guess which schema it declares; write a table-level FOREIGN " +
					"KEY clause, which every MySQL line builds",
				Constraint:              descriptor.ColumnConstraint{Define: descriptor.ConstraintKindCheck, Modify: descriptor.ConstraintKindCheck},
				ModifyRefusesReferences: true,
			},
			Constraints: descriptor.ConstraintGrammar{
				Enforcement:    descriptor.Enforcement{Checks: true},
				Match:          descriptor.Match{Clause: true, Partial: true},
				Deferral:       descriptor.Deferral{Refusal: mysqlFamilyDeferralRefusal},
				SymbolOptional: true,
			},
			Keys: descriptor.KeyGrammar{
				IndexKeywords: []string{"KEY", "INDEX", "FULLTEXT", "SPATIAL"},
				Options:       true,
				Visibility: descriptor.KeyVisibility{
					Refused: []string{"IGNORED", "NOT IGNORED"},
					Refusal: "it is MariaDB's clause, and MySQL 8.4 answers ERROR 1064",
				},
				BareUniqueName:       true,
				ForeignKeyIndexName:  true,
				FunctionalParts:      true,
				RefusesNullsDistinct: true,
			},
			AlterTable: descriptor.AlterTableGrammar{
				AddIndex:           true,
				RenameIndex:        true,
				Modify:             descriptor.ModifyDefinition,
				ChecksCapabilities: true,
			},
			Statements: mysqlStatementGrammar(),
			Forms:      mysqlStatementForms(),
			Routines: descriptor.RoutineGrammar{
				Body:                 descriptor.RoutineCompoundStatement,
				TriggerStatementBody: true,
			},
		},

		// Clean checks: what internal/migrateclean reads. The pinned
		// community binary refuses to adopt a database that holds a table,
		// measured on MySQL 9.7 on 2026-08-07, and refuses a dev database
		// that holds one, measured on MySQL 8.4 on 2026-09-26.
		CleanCheck: descriptor.CleanCheck{Adoption: true, Dev: true},
		// A database carries a character set and a collation, which a schema
		// read fills in where the reader did not describe them.
		SchemaAttributesQuery: mysqlSchemaAttributes,

		// Foreign key naming: what the SQL reader, the comparison and the
		// generator's backing index cleanup read. Measured on 8.4.11 and
		// 26.7.0, `FOREIGN KEY (p_id) REFERENCES p(id)` on `c` holds
		// `c_ibfk_1` over an index called `p_id`.
		ForeignKeyNaming: descriptor.ForeignKeyNamingTableNumbered,

		// Migrations: the engine's reading decides a migration file's
		// directives when its dialect is unknown.
		Migration: descriptor.Migration{DirectivesWithoutDialect: true, SameNamedForeignKeysApart: true},

		// Reach: the constructs of the MySQL family that reach outside the
		// database, which a scan refuses on every engine.
		Reach: descriptor.Reach{Everywhere: mysqlEverywhere()},

		// Foreign key name rules: none beyond the naming. Measured on 8.4.11
		// and 26.7.0, CREATE TABLE and ALTER TABLE both keep a derived name of
		// 64 characters and answer ERROR 1059 to one of 65, and an unnamed key
		// is never named by its number alone.

		// Transactions: what migration/seeder and internal/schemaclean read.
		Savepoint: descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"},
	}
}

// mysqlFamilyRevisionTable is how MySQL and MariaDB keep the migrator's
// tables. Both commit the open transaction before DDL runs, which
// TestRevisionCompletionFailure_MySQLImplicitCommitLive and its MariaDB twin
// measure, and the migrator's progress witness depends on the revision table
// being InnoDB: it records that a migration was applied even when the
// statement around it fails. The Atlas version column is binary-collated, so
// two tokens that differ only in case stay two revisions.
// mysqlCreateApplyLog is the schema apply log on the MySQL family. TEXT holds
// 65,535 bytes, and in strict mode a longer value fails the insert, so the
// outcome entry of a run that stopped at a long statement would be the one
// entry lost: error and error_stmt are LONGTEXT. The table states utf8mb4, as
// the revision table does, so a statement's non-ASCII text is stored on a
// server whose default character set is narrower.
const mysqlCreateApplyLog = `CREATE TABLE IF NOT EXISTS {table} (
    run_id VARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL,
    run_reference VARCHAR(255) NULL,
    state VARCHAR(32) NOT NULL,
    actor VARCHAR(256) NULL,
    actor_source VARCHAR(32) NOT NULL,
    actor_origin VARCHAR(64) NULL,
    plan_name VARCHAR(255) NULL,
    plan_digest VARCHAR(80) NULL,
    statement_count INTEGER NOT NULL,
    error_position INTEGER NULL,
    logged_at TIMESTAMP NOT NULL,
    error LONGTEXT NULL,
    error_stmt LONGTEXT NULL,
    PRIMARY KEY (run_id, seq)
){engine} DEFAULT CHARSET=utf8mb4`

// mysqlCreateRepeatables is the record of repeatable migrations on the MySQL
// family. The identity is binary-collated, as the Atlas version column is, so
// a row matches the file whose identity it spells byte for byte: under the
// default case-insensitive collation an orphaned row named Widget would
// answer the UPDATE of a new file named widget.
//
// attempted_at states a default because a session with
// explicit_defaults_for_timestamp off, the MariaDB default before 10.10 and a
// setting on every server of the family, gives the first TIMESTAMP NOT NULL
// column without one ON UPDATE CURRENT_TIMESTAMP (measured on MySQL 26.10 and
// MariaDB 10.11.19). Every outcome written to the row would then move the
// time its attempt started.
//
// error is LONGTEXT for the reason the apply log's is: in strict mode a
// failure longer than TEXT holds would fail the write that records it. body is
// LONGBLOB for the same reason: a BLOB holds 65,535 bytes, and a longer body
// would fail the write that records it as applied, inside its transaction.
const mysqlCreateRepeatables = `CREATE TABLE IF NOT EXISTS {table} (
    identity VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
    checksum VARCHAR(64) NOT NULL DEFAULT '',
    state VARCHAR(32) NOT NULL,
    attempt_checksum VARCHAR(64) NOT NULL,
    attempt_order INTEGER NOT NULL,
    transactional INTEGER NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    attempted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    applied_at TIMESTAMP NULL,
    error LONGTEXT NULL,
    body LONGBLOB NULL
){engine} DEFAULT CHARSET=utf8mb4`

func mysqlFamilyRevisionTable(logInsertPrefix string) descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create: createRevisions + " DEFAULT CHARSET=utf8mb4",
		CreateAtlas: `CREATE TABLE IF NOT EXISTS {table} (
    version VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY,
    description TEXT NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at TIMESTAMP NOT NULL,
    execution_time BIGINT NOT NULL,
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR(255) NOT NULL,
    partial_hashes JSON NULL,
    operator_version VARCHAR(255) NOT NULL
){engine} DEFAULT CHARSET=utf8mb4`,
		CreateTags:        createTags + " ENGINE=InnoDB",
		CreateLog:         createLog,
		CreateApplyLog:    mysqlCreateApplyLog,
		CreateRepeatables: mysqlCreateRepeatables,
		LogInsertPrefix:   logInsertPrefix,
		CreateSchema:      createSchema,
		EngineClause:      " ENGINE=InnoDB",
		RequiredEngine:    "InnoDB",
		FirstRow:          "limit",
		RowLock:           "for-update",
		Update:            updateRevision,
		Respell:           respellRevision(),
		Conflict: `ON DUPLICATE KEY UPDATE
description = VALUES(description),
applied_at = VALUES(applied_at),
state = VALUES(state),
applied = VALUES(applied),
total = VALUES(total),
error = NULL,
error_stmt = NULL,
execution_time_ms = VALUES(execution_time_ms),
checksum = VALUES(checksum)`,
		ConflictAtlas: `ON DUPLICATE KEY UPDATE
description = VALUES(description),
type = VALUES(type),
applied = VALUES(applied),
total = VALUES(total),
executed_at = VALUES(executed_at),
execution_time = VALUES(execution_time),
error = '',
error_stmt = '',
hash = VALUES(hash),
partial_hashes = VALUES(partial_hashes),
operator_version = VALUES(operator_version)`,
		UpsertTag:   upsertTagOnDuplicateKey,
		VersionCast: "SIGNED",
		// A hexadecimal binary literal is independent of sql_mode. Escaping a
		// backslash as `\\` works under MySQL's default mode but changes the
		// value when NO_BACKSLASH_ESCAPES is enabled.
		VersionLiteral: "hex",
		DerivedTableAs: " AS",
		// A URL that names no database is the whole server, where the pinned
		// community binary keeps its revision table in a database of its own,
		// measured on MySQL 8.4.11 and MariaDB 11.8.9 (stokaro/ptah#3789).
		AtlasSchema: "database",
	}
}

// mysqlFamilyAtlasHCL is what an Atlas HCL schema holds for MySQL and MariaDB
// alone: the attributes the pinned community binary reads from its MySQL
// driver only. They become the platform properties of the mysql and mariadb
// targets, where the MySQL owner reads them.
func mysqlFamilyAtlasHCL() descriptor.AtlasHCL {
	return descriptor.AtlasHCL{
		TableProperties:  []string{"auto_increment", "charset"},
		ColumnProperties: []string{"charset", "on_update"},
		IndexProperties:  []string{"parser"},
	}
}

// mysqlFamilyDeferralRefusal is why MySQL and MariaDB refuse every deferral
// clause, which neither engine reads.
const mysqlFamilyDeferralRefusal = "MySQL and MariaDB have no deferrable constraints, and MySQL 8.4 and " +
	"MariaDB 11.8 answer ERROR 1064 to the clause"

// mysqlCharsetDefaults is the descriptor's
// [descriptor.CollationNames.CharsetDefaults]: information_schema.CHARACTER_SETS
// on MySQL 8.4 and 26.10.
func mysqlCharsetDefaults() map[string]string {
	return map[string]string{
		"armscii8": "armscii8_general_ci", "ascii": "ascii_general_ci",
		"big5": "big5_chinese_ci", "binary": "binary", "cp1250": "cp1250_general_ci",
		"cp1251": "cp1251_general_ci", "cp1256": "cp1256_general_ci",
		"cp1257": "cp1257_general_ci", "cp850": "cp850_general_ci", "cp852": "cp852_general_ci",
		"cp866": "cp866_general_ci", "cp932": "cp932_japanese_ci", "dec8": "dec8_swedish_ci",
		"eucjpms": "eucjpms_japanese_ci", "euckr": "euckr_korean_ci",
		"gb18030": "gb18030_chinese_ci", "gb2312": "gb2312_chinese_ci", "gbk": "gbk_chinese_ci",
		"geostd8": "geostd8_general_ci", "greek": "greek_general_ci",
		"hebrew": "hebrew_general_ci", "hp8": "hp8_english_ci", "keybcs2": "keybcs2_general_ci",
		"koi8r": "koi8r_general_ci", "koi8u": "koi8u_general_ci", "latin1": "latin1_swedish_ci",
		"latin2": "latin2_general_ci", "latin5": "latin5_turkish_ci",
		"latin7": "latin7_general_ci", "macce": "macce_general_ci",
		"macroman": "macroman_general_ci", "sjis": "sjis_japanese_ci",
		"swe7": "swe7_swedish_ci", "tis620": "tis620_thai_ci", "ucs2": "ucs2_general_ci",
		"ujis": "ujis_japanese_ci", "utf16": "utf16_general_ci",
		"utf16le": "utf16le_general_ci", "utf32": "utf32_general_ci",
		"utf8mb3": "utf8mb3_general_ci", "utf8mb4": "utf8mb4_0900_ai_ci",
	}
}

// Measured on 26.10.0 over every code point below U+20000 that has a
// simple lowercase: a CHECK named ck on one table and ck or CK on
// another of the database is refused, and so are ÉK after ék, İ after i
// and the Kelvin sign after k, while ek and ék are two names, and ẞ
// (U+1E9E) beside ß and ϴ (U+03F4) beside θ are accepted. The server
// folds the Latin-1 and Latin Extended letters, the basic Greek,
// Cyrillic and Armenian capitals, the polytonic Greek block, the
// letterlike signs, the Roman numerals, the circled and the fullwidth
// capitals. In each range below the run refused every pair, so Ptah
// never refuses a pair the server accepts. A pair the server refuses
// outside them reaches the server, which refuses it after the
// statements before it have run, since MySQL DDL does not roll back.
// mysqlCheckNames is how MySQL holds a CHECK name, measured on 26.10.0: once
// per schema, compared without case over ASCII and over mysqlCheckNameFolds.
var mysqlCheckNames = descriptor.CheckNames{
	Scope: descriptor.CheckNameScopeSchema, Comparison: "ascii_case_insensitive",
	FoldedRanges: mysqlCheckNameFolds,
	Refusal:      "Error 3822 (HY000): Duplicate check constraint name '{name}'.",
}

var mysqlCheckNameFolds = []descriptor.CodePointRange{
	{First: 0xC0, Last: 0xDE}, {First: 0x100, Last: 0x1FF}, {First: 0x386, Last: 0x3AB},
	{First: 0x400, Last: 0x42F}, {First: 0x531, Last: 0x556}, {First: 0x1F00, Last: 0x1FFF},
	{First: 0x2126, Last: 0x212B}, {First: 0x2160, Last: 0x216F}, {First: 0x24B6, Last: 0x24CF},
	{First: 0xFF21, Last: 0xFF3A},
}

// mysqlStatementGrammar is how MySQL spells the statements a schema file
// holds: USE selects the database, CREATE ROLE takes IF NOT EXISTS, and a
// grant names a database as `db`.*.
func mysqlStatementGrammar() descriptor.StatementGrammar {
	return descriptor.StatementGrammar{UseSelectsDatabase: true, RoleIfNotExists: true, DatabaseWildcards: true}
}

// mysqlMultiWordPrivileges are the privileges of more than one word MySQL
// grants on a table, a routine and a database, as SHOW GRANTS and
// information_schema report them, measured on MySQL 8.4.13 and 26.10.0.
func mysqlMultiWordPrivileges() []string {
	return []string{"ALTER ROUTINE", "CREATE ROUTINE", "CREATE TEMPORARY TABLES", "CREATE VIEW", "LOCK TABLES", "SHOW VIEW"}
}

// mysqlAllPrivileges is what GRANT ALL grants on a MySQL table and database,
// as information_schema.TABLE_PRIVILEGES and SCHEMA_PRIVILEGES report it.
// MariaDB grants these and more, so its descriptor starts from them.
func mysqlAllPrivileges() descriptor.AllPrivileges {
	table := []string{
		"ALTER", "CREATE", "CREATE VIEW", "DELETE", "DROP", "INDEX",
		"INSERT", "REFERENCES", "SELECT", "SHOW VIEW", "TRIGGER", "UPDATE",
	}
	return descriptor.AllPrivileges{Kinds: map[string]descriptor.AllPrivilegeList{
		"TABLE": {Privileges: table},
		"SCHEMA": {Privileges: append(append([]string(nil), table...),
			"ALTER ROUTINE", "CREATE ROUTINE", "CREATE TEMPORARY TABLES", "EVENT", "EXECUTE", "LOCK TABLES")},
	}}
}

// mysqlFamilyQuery is how the query builder spells what MySQL and MariaDB
// spell differently from other engines. No release of the family has a FULL
// OUTER JOIN, and OFFSET is accepted only after a LIMIT: the largest BIGINT
// UNSIGNED is the documented "every row from the offset on". ON DUPLICATE KEY
// UPDATE fires for every unique key on the table, so it cannot be scoped to
// the columns a caller names.
func mysqlFamilyQuery() descriptor.Query {
	return descriptor.Query{
		UnsupportedJoins: []descriptor.Join{descriptor.JoinFull},
		OffsetOnlyLimit:  "18446744073709551615",
		Upsert:           descriptor.UpsertOnDuplicateKey,
	}
}
