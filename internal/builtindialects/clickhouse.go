package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

// clickHouse is ClickHouse. Its identifier rules are the conservative ones
// for an engine Ptah has not measured: exact comparison, index names scoped
// to their table, and no default schema.
func clickHouse() descriptor.Dialect {
	return descriptor.Dialect{
		Name:    "clickhouse",
		Aliases: []string{"ch"},
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "table",
			IndexNames:     "exact",
			TableNames:     "exact",
			ColumnNames:    "exact",
		},
		Placeholder: descriptor.PlaceholderQuestion,

		// SELECT version() answers a bare "26.7.3.19"; system.build_options
		// reports VERSION_FULL "ClickHouse 26.7.3.19", which is the server
		// naming itself. CHECK GRANT arrived in 24.11. A string with no
		// readable version takes the lower arm: a database-realm cleanup that
		// the CHECK GRANT key guards is one Ptah would rather decline than run
		// on a privilege answer it could not obtain (stokaro/ptah#916).
		// Ptah models no identifier limit for ClickHouse.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "ClickHouse24",
			BannerTokens:  []string{"clickhouse"},
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "ClickHouse24",
				Steps: []descriptor.VersionStep{
					{Min: "24.11", Preset: "ClickHouse2411"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.ClickHouseMeasured(),
				Newest:      capabilityline.ClickHouse269,
			},
		},
	}
}
