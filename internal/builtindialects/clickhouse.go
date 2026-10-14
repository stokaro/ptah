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

		// How a connection reaches the server.
		Driver: "clickhouse",

		// ClickHouse runs DELETE, but a plain UPDATE answers `Lightweight
		// updates are not supported. Lightweight updates are supported only
		// for tables with materialized _block_number column`, a per-table
		// setting a statement cannot declare. Its own spelling, ALTER TABLE
		// ... UPDATE, is an asynchronous mutation outside any transaction, so
		// the portable builder does not write it (stokaro/ptah#941). Measured
		// on 24.10.4.191 and 25.8.30.
		Query: descriptor.Query{
			UpsertRefusal: "ClickHouse has no upsert statement: a ReplacingMergeTree deduplicates in the background " +
				"rather than at insert time, which is a table design and not a clause on this INSERT",
			UpdateRefusal: "UPDATE is not a portable statement on ClickHouse: a plain UPDATE runs only on a table " +
				"with the materialized _block_number column, which a statement cannot declare; use " +
				"ALTER TABLE … UPDATE, or enable enable_block_number_column on the table",
		},
		Scripts: descriptor.Scripts{ClientDelimiters: true},
	}
}
