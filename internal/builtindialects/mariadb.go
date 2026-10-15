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
	}
}
