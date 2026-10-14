package builtindialects

import "ptah.run/core/platform/descriptor"

// sqlite is SQLite. libsql and libsql+ws reach a libSQL server, which keeps
// SQLite's schema semantics over its own transport (#1615).
func sqlite() descriptor.Dialect {
	return descriptor.Dialect{
		Name:          "sqlite",
		Aliases:       []string{"sqlite3", "libsql", "libsql+ws"},
		DefaultSchema: "main",
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "schema",
			IndexNames:     "ascii_case_insensitive",
			TableNames:     "ascii_case_insensitive",
			ColumnNames:    "ascii_case_insensitive",
		},
		Placeholder: descriptor.PlaceholderQuestion,

		// sqlite_version() answers a bare dotted version, so no banner names
		// SQLite. RENAME COLUMN arrived in 3.25, and ALTER COLUMN ... SET NOT
		// NULL in 3.53. Every version is credited to its line and none is
		// past the top: the matrix declares one SQLite cell, with no container
		// to measure a newest line on (stokaro/ptah#916). Ptah models no
		// identifier limit for SQLite.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "SQLite3",
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "SQLite324",
				Steps: []descriptor.VersionStep{
					{Min: "3.25", Preset: "SQLite352"},
					{Min: "3.53", Preset: "SQLite3"},
				},
				Attribution: "through-newest",
			},
		},

		// How a connection reaches the server.
		Driver: "sqlite",

		// SQLite gained RIGHT and FULL [OUTER] JOIN only in 3.39 (2022), and
		// Ptah cannot assume a release that new, so both are refused. OFFSET
		// is accepted only after a LIMIT, and -1 is the documented "no limit".
		Query: descriptor.Query{
			UnsupportedJoins: []descriptor.Join{descriptor.JoinRight, descriptor.JoinFull},
			OffsetOnlyLimit:  "-1",
			Upsert:           descriptor.UpsertOnConflict,
		},
		Scripts: descriptor.Scripts{ClientDelimiters: true},

		// Migrations: what the migrator reads.
		DDLTransactions: "transactional",
		RevisionTable:   sqliteRevisionTable(),

		Schemas: descriptor.Schemas{Selection: descriptor.SchemaSelectionSingle, Implicit: true},
		// SQLite stores a timestamp's text verbatim, offset included.
		Literals: descriptor.Literals{
			BinaryPrefix:    "X'",
			BinarySuffix:    "'",
			TimestampOffset: true,
		},
		Seeds: descriptor.Seeds{
			Transactional: true,
			Savepoint:     descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"},
		},
	}
}

// sqliteRevisionTable is how SQLite keeps the migrator's tables. A schema
// there is an attached database file, which the migrator does not create, and
// a revision timestamp is bound as RFC 3339 text. Its DDL joins the
// transaction, which the SQLite case of the revision-completion matrix
// measures in process.
func sqliteRevisionTable() descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create:         createRevisions,
		CreateAtlas:    createAtlasRevisions,
		CreateTags:     createTags,
		CreateLog:      createLog,
		FirstRow:       "limit",
		Update:         updateRevision,
		Respell:        respellRevision(),
		UpsertTag:      upsertTagOnConflict,
		VersionCast:    "BIGINT",
		VersionLiteral: "standard",
		DerivedTableAs: " AS",
		TimeAsText:     true,
	}
}
