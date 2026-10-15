package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

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

		// SELECT Version() names no product: it answers the dotted release,
		// "26.2.1.14", or the build branch, "stable-25-4-1" on 25.4.1.15. The
		// dialect comes from the ydb:// URL scheme or from --dialect. A
		// version below the oldest measured line takes that line's preset:
		// nothing older was measured. Ptah models no identifier limit for YDB.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "YDB262",
			Versions: descriptor.Versions{
				Scheme: "release-branch",
				Floor:  "YDB251",
				Steps: []descriptor.VersionStep{
					{Min: capabilityline.YDB252, Preset: "YDB252"},
					{Min: capabilityline.YDB253, Preset: "YDB253"},
					{Min: capabilityline.YDB254, Preset: "YDB254"},
					{Min: capabilityline.YDB261, Preset: "YDB261"},
					{Min: capabilityline.YDB262, Preset: "YDB262"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.YDBMeasured(),
				Newest:      capabilityline.YDB262,
			},
		},

		// How a connection reaches the server.
		Driver: "ydb",

		// What a YDB line runs differs by release, so the query builder reads
		// the target's capabilities. YQL types LIMIT and OFFSET as Uint64 and
		// converts no signed parameter: measured on 26.2.1.14 and 25.1.4.7,
		// `LIMIT $p1` bound to an Int64 answers `Failed to convert type: Int64
		// to Uint64`. UPSERT INTO is YQL's own statement; on the same lines
		// `INSERT OR IGNORE INTO` answers `INSERT OR IGNORE is not yet
		// supported for Kikimr`, and YQL has no ON CONFLICT.
		Query: descriptor.Query{
			ChecksCapabilities: true,
			UnsignedBounds:     true,
			Upsert:             descriptor.UpsertInto,
			UpsertRefusal: "YDB has no ON CONFLICT: UpsertInto writes each row over the row with the same " +
				"primary key, and the server refuses INSERT OR IGNORE",
		},
		// No YDB client writes a delimiter directive, and the shared rewriter
		// reads strings and comments by rules YQL does not share: it would
		// rewrite a delimiter inside an @@...@@ literal.
		Scripts: descriptor.Scripts{ClientDelimiters: false},
	}
}
