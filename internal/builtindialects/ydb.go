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

		// Migrations: what the migrator reads.
		DDLTransactions: "no_transaction",
		RevisionTable:   ydbRevisionTable(),

		Roles:               descriptor.Roles{OutOfScope: descriptor.RoleScopeUserMade},
		Lock:                descriptor.Lock{Protocol: descriptor.LockCoordinationSemaphore},
		URLValuedParameters: []string{"monitoring"},
	}
}

// ydbRevisionTable is how YDB keeps the migrator's tables.
//
// YQL has no inline PRIMARY KEY, so each key is a table-level clause. Every
// integer column is Int64, because the YDB connection widens a Go int to 64
// bits and YDB refuses an Int64 value for an Int32 column. Every text column
// is Utf8; partial_hashes too, since a Utf8 value cannot be written to a Json
// or JsonDocument column (measured on 26.2.1.14: `Failed to convert 'j': Utf8
// to Optional<JsonDocument>`), so the column holds the JSON document as text.
// Timestamp rather than Timestamp64, because every line Ptah supports has it
// and a revision is never older than 1970. No column has a default: the
// migrator writes every column of every row it inserts. A schema is a
// directory, and CREATE TABLE creates the directories its path names.
//
// YDB runs no schema statement inside a transaction (`Scheme operations
// cannot be executed inside transaction`, measured on 26.2.1.14), DDL and DML
// cannot share one query, and a query of several DDL statements is not
// atomic, so every query commits on its own, body and revision write alike;
// TestYDBMigrator_ResumesWhereAFailedMigrationStopped measures it. YQL has
// UPSERT and no ON CONFLICT, no FOR UPDATE -- a serializable read aborts the
// second of two conflicting transactions instead -- and UNION ALL pairs
// columns by name. It binds every value with a type and refuses an untyped
// NULL (`Failed to convert 'txt': Void to Optional<Utf8>`). ydb-go-sdk counts
// the rows a DELETE's plan deleted, so a DELETE by key reports its key whether
// a row held it or not.
func ydbRevisionTable() descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create: `CREATE TABLE IF NOT EXISTS {table} (
    version Int64 NOT NULL,
    description Utf8 NOT NULL,
    applied_at Timestamp NOT NULL,
    state Utf8 NOT NULL,
    applied Int64 NOT NULL,
    total Int64 NOT NULL,
    error Utf8,
    error_stmt Utf8,
    execution_time_ms Int64 NOT NULL,
    checksum Utf8 NOT NULL,
    PRIMARY KEY (version)
)`,
		CreateAtlas: `CREATE TABLE IF NOT EXISTS {table} (
    version Utf8 NOT NULL,
    description Utf8 NOT NULL,
    type Int64 NOT NULL,
    applied Int64 NOT NULL,
    total Int64 NOT NULL,
    executed_at Timestamp NOT NULL,
    execution_time Int64 NOT NULL,
    error Utf8,
    error_stmt Utf8,
    hash Utf8 NOT NULL,
    partial_hashes Utf8,
    operator_version Utf8 NOT NULL,
    PRIMARY KEY (version)
)`,
		CreateTags: `CREATE TABLE IF NOT EXISTS {table} (
    tag Utf8 NOT NULL,
    version Int64 NOT NULL,
    recorded_at Timestamp NOT NULL,
    PRIMARY KEY (tag)
)`,
		CreateLog: `CREATE TABLE IF NOT EXISTS {table} (
    run_id Utf8 NOT NULL,
    seq Int64 NOT NULL,
    operation Utf8 NOT NULL,
    version Int64 NOT NULL,
    state Utf8 NOT NULL,
    actor Utf8,
    actor_source Utf8 NOT NULL,
    checksum Utf8,
    logged_at Timestamp NOT NULL,
    error Utf8,
    PRIMARY KEY (run_id, seq)
)`,
		FirstRow:  "limit",
		Update:    updateRevision,
		Respell:   respellRevision(),
		Upserts:   true,
		UpsertTag: "UPSERT INTO {table} (tag, version, recorded_at) VALUES (?, ?, ?)",
		// YQL's own name for the type. A cast of a column is safe, unlike a
		// cast of a parameter: every token that is not a number is already
		// NULL, and NULL is what YQL's cast answers for text it cannot read.
		VersionCast:      "Int64",
		VersionLiteral:   "yql",
		DerivedTableAs:   " AS",
		UnionByName:      true,
		JSONAsText:       true,
		TypedNulls:       true,
		DeleteCountsKeys: true,
	}
}
