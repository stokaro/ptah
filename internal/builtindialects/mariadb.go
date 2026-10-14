package builtindialects

import "ptah.run/core/platform/descriptor"

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
	}
}
