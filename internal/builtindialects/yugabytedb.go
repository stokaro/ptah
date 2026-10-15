package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

// yugabyteDB is YugabyteDB through its PostgreSQL-compatible YSQL API, in the
// PostgreSQL family, with PostgreSQL's identifier rules.
func yugabyteDB() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "yugabytedb",
		Aliases:       []string{"yugabyte", "ysql"},
		Family:        "postgres",
		DefaultSchema: "public",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "schema",
			IndexNames:     "exact",
			TableNames:     "exact",
			ColumnNames:    "exact",
		},
		Placeholder: descriptor.PlaceholderDollar,

		// A YugabyteDB banner opens with the PostgreSQL compatibility version,
		// "PostgreSQL 11.2-YB-2024.2.4.0-b0", and names its own release after
		// the "-YB-" marker. The PostgreSQL 11 to 15 engine swap landed in the
		// 2025 line. A string with no readable release takes the lower arm,
		// whose plan runs on both engines (stokaro/ptah#916).
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "YugabyteDB25",
			BannerTokens:  []string{"yugabyte", "-yb-"},
			Versions: descriptor.Versions{
				Scheme: "yb-release",
				Floor:  "YugabyteDB24",
				Steps: []descriptor.VersionStep{
					{Min: "2025", Preset: "YugabyteDB25"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.YugabyteDBMeasured(),
				Newest:      capabilityline.YugabyteDB2026,
				Unreadable: descriptor.UnreadableVersion{
					Preset:     "YugabyteDB24",
					Recognized: true,
				},
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
		Lock:    descriptor.Lock{Protocol: descriptor.LockPostgresAdvisory},
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
	}
}
