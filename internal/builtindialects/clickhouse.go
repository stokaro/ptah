package builtindialects

import "ptah.run/core/platform/descriptor"

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
	}
}
