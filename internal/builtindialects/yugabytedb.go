package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

// yugabyteDB is YugabyteDB through its PostgreSQL-compatible YSQL API, in the
// PostgreSQL family, with PostgreSQL's identifier rules.
func yugabyteDB() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "yugabytedb",
		Aliases:       []string{"yugabyte", "ysql"},
		Family:        "postgres",
		DefaultSchema: "public",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "schema",
			IndexNames:     "exact",
			TableNames:     "exact",
			ColumnNames:    "exact",
		},
		Placeholder: descriptor.PlaceholderDollar,

		// A YugabyteDB banner opens with the PostgreSQL compatibility version,
		// "PostgreSQL 11.2-YB-2024.2.4.0-b0", and names its own release after
		// the "-YB-" marker. The PostgreSQL 11 to 15 engine swap landed in the
		// 2025 line. A string with no readable release takes the lower arm,
		// whose plan runs on both engines (stokaro/ptah#916).
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "YugabyteDB25",
			BannerTokens:  []string{"yugabyte", "-yb-"},
			Versions: descriptor.Versions{
				Scheme: "yb-release",
				Floor:  "YugabyteDB24",
				Steps: []descriptor.VersionStep{
					{Min: "2025", Preset: "YugabyteDB25"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.YugabyteDBMeasured(),
				Newest:      capabilityline.YugabyteDB2026,
				Unreadable: descriptor.UnreadableVersion{
					Preset:     "YugabyteDB24",
					Recognized: true,
				},
			},
		},
		IdentifierLimit: descriptor.IdentifierLimit{Max: 63, Unit: "bytes"},

		// How a connection reaches the server.
		Driver:     "pgx",
		WireScheme: "postgres",
	}
}
