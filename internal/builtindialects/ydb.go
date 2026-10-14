package builtindialects

import "ptah.run/core/platform/descriptor"

// ydb is YDB, read and written in its own query language, YQL, over gRPC. It
// is not in the PostgreSQL family: YDB serves no PostgreSQL wire protocol or
// syntax, and in YQL a double-quoted "x" is a string rather than a name. ydbs
// is the TLS URL scheme of a YDB server (#4015). grpc and grpcs are the SDK's
// schemes and are not aliases: they name a protocol, and every gRPC URL would
// read as a database URL.
//
// Every YDB name is case-sensitive, quoted or not: measured on 26.2.1.14,
// tables `Users` and `users` coexist, and so do columns `id`, `ID` and `Id`
// in one table. There is no schema to default to: a Ptah schema is a
// directory, and the database root is the empty one. An index belongs to its
// table -- it is the path `table/index`, and two tables carry an index of the
// same name without a clash.
func ydb() descriptor.Dialect {
	return descriptor.Dialect{
		Name:    "ydb",
		Aliases: []string{"ydbs"},
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "table",
			IndexNames:     "exact",
			TableNames:     "exact",
			ColumnNames:    "exact",
		},
		Placeholder: descriptor.PlaceholderDollarNamed,
	}
}
