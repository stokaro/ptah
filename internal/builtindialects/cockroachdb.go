package builtindialects

import "ptah.run/core/platform/descriptor"

// cockroachDB is CockroachDB, in the PostgreSQL family.
//
// It resolves an unqualified name in `public`, as PostgreSQL does: measured
// on v26.3.2, search_path is `"$user", public` and a table created as `t` is
// read as `public.t`. Without the default, `t` and `public.t` are two names
// offline, and a render creates a routine returning `SETOF public.t` before
// the table `t` (stokaro/ptah#4065). Index names stay table-scoped: the same
// server accepts one index name on two tables.
func cockroachDB() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "cockroachdb",
		Aliases:       []string{"cockroach", "crdb"},
		Family:        "postgres",
		DefaultSchema: "public",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "table",
			IndexNames:     "exact",
			TableNames:     "exact",
			ColumnNames:    "exact",
		},
		Placeholder: descriptor.PlaceholderDollar,
	}
}
