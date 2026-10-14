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
	}
}
