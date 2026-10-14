package builtindialects

import "ptah.run/core/platform/descriptor"

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
	}
}
