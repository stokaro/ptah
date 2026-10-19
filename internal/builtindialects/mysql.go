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
		Name:    "mysql",
		Aliases: []string{"mysql+unix"},
		Family:  "mysql",
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
		// 8.0.16, DROP CONSTRAINT at 8.0.19 and SHOW_ROUTINE at 8.0.20
		// (stokaro/ptah#916).
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
					{Min: "8.4", Preset: "MySQL84"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.MySQLMeasured(),
				Newest:      capabilityline.MySQL26,
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
		Lock:    descriptor.Lock{Protocol: descriptor.LockGetLock},
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
			NotNullConsequence: "a row holding NULL fails the rebuild under the strict SQL mode MySQL defaults to (Invalid use of NULL value), " +
				"and is silently rewritten to the type's implicit default (0, '', or the zero date) when strict mode is off; " +
				"a DEFAULT in the same clause applies to new rows only. The rebuild itself runs in place with writes allowed (ALGORITHM=INPLACE, LOCK=NONE)",
			OnlineMeasured:   true,
			TypeRestatements: "mytype",
			AtlasAnalyzer:    "MY",
		},

		// How a schema file's names are read, and how unnamed indexes and CHECKs are named.
		Names:       descriptor.Names{Lookup: "unicode_case_insensitive"},
		IndexNaming: descriptor.IndexNaming{MaxBytes: 64, FunctionalBase: "functional_index", UniqueIsIndex: true},
		CheckNaming: descriptor.CheckNamingTableNumbered,
		Columns:     descriptor.Columns{CheckOwnColumnOnly: true},

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
		InlineEnum:               descriptor.InlineEnum{Type: descriptor.InlineEnumValues},
		IndexesBeforeForeignKeys: true,
		// Comparison: what comparing a declaration with a catalog reads.
		// MySQL creates an index for every foreign key unasked, and a
		// database is the schema, so a key into another database is accepted
		// and read back with that database's name (stokaro/ptah#3891).
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
	}
}

// mysqlFamilyRevisionTable is how MySQL and MariaDB keep the migrator's
// tables. Both commit the open transaction before DDL runs, which
// TestRevisionCompletionFailure_MySQLImplicitCommitLive and its MariaDB twin
// measure, and the migrator's progress witness depends on the revision table
// being InnoDB: it records that a migration was applied even when the
// statement around it fails. The Atlas version column is binary-collated, so
// two tokens that differ only in case stay two revisions.
func mysqlFamilyRevisionTable() descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create: createRevisions,
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
){engine}`,
		CreateTags:     createTags + " ENGINE=InnoDB",
		CreateLog:      createLog,
		CreateSchema:   createSchema,
		EngineClause:   " ENGINE=InnoDB",
		RequiredEngine: "InnoDB",
		FirstRow:       "limit",
		RowLock:        "for-update",
		Update:         updateRevision,
		Respell:        respellRevision(),
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
