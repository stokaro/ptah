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
	}
}
