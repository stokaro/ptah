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
		// MariaDB refuses the negative timeout MySQL's GET_LOCK takes for a
		// wait without limit, so a year stands in for it.
		Lock: descriptor.Lock{Protocol: descriptor.LockGetLock, UnboundedWaitSeconds: 31_536_000},
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
		Names:       descriptor.Names{Lookup: "unicode_case_insensitive"},
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
			ForeignKeysOutside:     true,
			ForeignKeyBackingIndex: true,
			DefaultGeneratedKind:   "VIRTUAL",
		},
	}
}
