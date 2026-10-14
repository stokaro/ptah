package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

// oracle is Oracle Database.
//
// Names compare case-insensitively because the renderer writes a plain name
// bare and Oracle folds it to upper case: a declared `view_count` is stored as
// VIEW_COUNT, and comparing exactly would report a difference on every run
// against a catalog Ptah itself wrote. Measured -- user_tables reports
// PTAH_FOLD for a bare CREATE and ptah_fold for a quoted one, and the two
// coexist as separate tables.
//
// Index names are schema-scoped, not table-scoped: `DROP INDEX <name>` is the
// whole statement here, with no ON clause naming a table, so two tables in one
// schema cannot both carry an index called `idx_name` (stokaro/ptah#1875).
//
// Measured on 23.26: an INSERT bound with :1 is accepted, while ? and $1 both
// answer ORA-00911, invalid character.
func oracle() descriptor.Dialect {
	return descriptor.Dialect{
		Name:    "oracle",
		Aliases: []string{"oracledb"},
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "schema",
			IndexNames:     "ascii_case_insensitive",
			TableNames:     "ascii_case_insensitive",
			ColumnNames:    "ascii_case_insensitive",
		},
		Placeholder: descriptor.PlaceholderColon,

		// An Oracle banner carries two numbers, and the first is not the
		// version: "Oracle AI Database 26ai Free Release 23.26.2.0.0". The
		// version is read after "Release". The IF [NOT] EXISTS guards arrived
		// with the 23 line, and the step is the whole major: 23.0 through
		// 23.25 were never measured. Ptah models no identifier limit for
		// Oracle.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "Oracle23",
			BannerTokens:  []string{"oracle"},
			Versions: descriptor.Versions{
				Scheme: "after-release",
				Floor:  "Oracle21",
				Steps: []descriptor.VersionStep{
					{Min: "23", Preset: "Oracle23"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.OracleMeasured(),
				Newest:      capabilityline.Oracle23,
			},
		},

		// How a connection reaches the server.
		Driver: "oracle",

		// Measured on 23.26: LIMIT answers ORA-03049, while `OFFSET n ROWS
		// FETCH NEXT m ROWS ONLY` is accepted without an ORDER BY, so Oracle
		// gets no ORDER BY sentinel and runs on every line Ptah targets.
		Query: descriptor.Query{
			OffsetFetch: true,
		},
		Scripts: descriptor.Scripts{ClientDelimiters: true},
	}
}
