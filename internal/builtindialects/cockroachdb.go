package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

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

		// The whole 25 line takes CockroachDB25, not only the measured 25.4
		// minor: autocommit_before_ddl defaults on across the line, measured
		// on v25.1.10, v25.2.22, v25.3.7 and v25.4.5 (stokaro/ptah#1849).
		// 26.3 is the first line carrying CREATE DOMAIN (stokaro/ptah#1735).
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "CockroachDB26",
			BannerTokens:  []string{"cockroachdb"},
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "CockroachDB23",
				Steps: []descriptor.VersionStep{
					{Min: "25", Preset: "CockroachDB25"},
					{Min: capabilityline.CockroachDB26, Preset: "CockroachDB26"},
					{Min: capabilityline.CockroachDB263, Preset: "CockroachDB263"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.CockroachDBMeasured(),
				Newest:      capabilityline.CockroachDB263,
			},
		},
		IdentifierLimit: descriptor.IdentifierLimit{Max: 63, Unit: "bytes"},

		// How a connection reaches the server.
		Driver:     "pgx",
		WireScheme: "postgres",

		Query: descriptor.Query{
			Upsert: descriptor.UpsertOnConflict,
		},
		Scripts: descriptor.Scripts{ClientDelimiters: true},

		// The grammars the planner asks for non-blocking DDL in.
		ConcurrentIndexes: true,
		OnlineAlter:       descriptor.OnlineAlterNotValid,

		// Migrations: what the migrator reads.
		// The PostgreSQL driver and the transactional migrator path PostgreSQL
		// takes, so this is the contract that code implements; no live
		// revision-completion test drives it yet.
		DDLTransactions: "transactional",
		RevisionTable:   postgresFamilyRevisionTable(),

		Schemas: descriptor.Schemas{Selection: descriptor.SchemaSelectionSearchPath, Implicit: true},
		Roles:   descriptor.Roles{OutOfScope: descriptor.RoleScopeReferenced, Widenable: true},
	}
}
