package builtindialects

import "ptah.run/core/platform/descriptor"

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
	}
}
