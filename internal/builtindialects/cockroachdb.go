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
		// The bytea input reads a backslash as an escape and refuses a byte
		// that is not UTF-8 (SQLSTATE 22021), so a binary value is spelled in
		// hex digits, relying on standard_conforming_strings. A timestamp
		// literal keeps the instant its offset names.
		Literals: descriptor.Literals{
			BinaryPrefix:    `'\x`,
			BinarySuffix:    `'::bytea`,
			TimestampOffset: true,
		},
		ManagedData: descriptor.ManagedData{
			IdentityRefusesInsert: descriptor.IdentityInsertGeneratedAlways,
		},
		Seeds: descriptor.Seeds{
			Transactional: true,
			Savepoint:     descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"},
		},

		// How an identifier is written, and how a column's own key is named.
		Quoting: descriptor.Quoting{Style: descriptor.QuoteDouble},

		// Cleanup: what a development database cleanup reads.
		Cleanup: descriptor.Cleanup{
			ForeignKeys:       true,
			Views:             true,
			MaterializedViews: true,
			Enums:             true,
			Domains:           true,
			Composites:        true,
			Ranges:            true,
			Functions:         true,
			RevisionTables:    true,
			RevisionProbe:     postgresRevisionProbe,
			SchemaScoped:      true,
			Restrict:          true,
		},

		// Lint: what migration lint reads.
		Lint: descriptor.Lint{Analyzed: true},

		// How a schema file's names are read, and how unnamed indexes and CHECKs are named.
		Names:       descriptor.Names{Unquoted: "unicode_case_insensitive", Roles: "unicode_case_insensitive", PublicKeyword: true},
		IndexNaming: descriptor.IndexNaming{PrimaryKeySuffix: "pkey"},
		Statements:  descriptor.Statements{ViewsBlockDropTable: true},

		// Dev databases. The server takes no advisory lock Ptah can hold, so runs on
		// one dev database are serialized with a lock file, which only serializes
		// runs on this machine.
		DevRealm: descriptor.DevRealm{
			Identity:        "query",
			Query:           "SELECT current_database()",
			FileLock:        "loopback",
			ScratchDatabase: true,
		},

		// How the common model is lowered for the engine.
		GeneratedColumnDefault: "STORED",
		// Comparison: what comparing a declaration with a catalog reads.
		// A probe's view goes in a schema, and a routine body is stored
		// reflowed and qualified: measured on v26.3.2, `SELECT * FROM
		// public.items` is stored as `SELECT public.items.id, ... FROM
		// f1.public.items;` (stokaro/ptah#4058).
		Comparison: descriptor.Comparison{
			ForeignKeysOutside:    true,
			ExpressionProbes:      "pg-catalog",
			ProbeViewsInSchema:    true,
			RewritesRoutineBodies: true,
			RolePasswords:         true,
		},
	}
}
