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

		// How an identifier is written, and how a column's own key is named.
		Quoting: descriptor.Quoting{Style: descriptor.QuoteDouble, Keywords: sqliteKeywords()},

		// Cleanup: what a development database cleanup reads.
		// The writer reads pragma_table_list and drops tables and views. The
		// reader hides schema_migrations and the writer keeps it, so the
		// revision tables stay out of the plan.
		Cleanup: descriptor.Cleanup{Views: true},

		// Lint: what migration lint reads.
		Lint: descriptor.Lint{
			Analyzed:           true,
			SetNotNullRebuilds: true,
		},

		// How a schema file's names are read, and how unnamed indexes and CHECKs are named.
		Names: descriptor.Names{Lookup: "ascii_case_insensitive"},
		TableOptions: []descriptor.TableOption{
			{Option: "STRICT", Property: "strict"},
			{Option: "WITHOUT_ROWID", Property: "without_rowid"},
		},

		// Dev databases. The database is a file, named by its identity on disk.
		DevRealm: descriptor.DevRealm{
			Identity: "file",
			Query:    "SELECT file FROM pragma_database_list WHERE name = 'main'",
			FileLock: "local",
		},

		// How the common model is lowered for the engine.
		GeneratedColumnDefault:   "VIRTUAL",
		InlineEnum:               descriptor.InlineEnum{Type: "TEXT", Check: descriptor.InlineEnumCheckBare},
		ForeignKeysInCreateTable: true,
		// Comparison: what comparing a declaration with a catalog reads.
		// SQLite keeps a table's name as the bytes it was given, and names
		// its own backing structures sqlite_autoindex_*, which no statement
		// can refer to.
		Comparison: descriptor.Comparison{
			VerbatimTableNames:   true,
			InternalIndexPrefix:  "sqlite_autoindex_",
			DefaultGeneratedKind: "VIRTUAL",
			AutoIncrementFlag:    true,
			PrimaryKeyNullable:   true,
			RowidTables:          true,
			TypeAffinity:         true,
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

// sqliteKeywords are the words SQLite parses as syntax where an identifier
// stands. SQLite publishes one exhaustive list for every supported build.
// Virtual table module names occupy an identifier position after USING, so a
// module named `select` must render as `"select"`; the bare form is a syntax
// error.
func sqliteKeywords() []string {
	return []string{
		"abort", "action", "add", "after", "all", "alter", "always", "analyze", "and", "as", "asc",
		"attach", "autoincrement", "before", "begin", "between", "by", "cascade", "case", "cast",
		"check", "collate", "column", "commit", "conflict", "constraint", "create", "cross",
		"current", "current_date", "current_time", "current_timestamp", "database", "default",
		"deferrable", "deferred", "delete", "desc", "detach", "distinct", "do", "drop", "each",
		"else", "end", "escape", "except", "exclude", "exclusive", "exists", "explain", "fail",
		"filter", "first", "following", "for", "foreign", "from", "full", "generated", "glob",
		"group", "groups", "having", "if", "ignore", "immediate", "in", "index", "indexed",
		"initially", "inner", "insert", "instead", "intersect", "into", "is", "isnull", "join",
		"key", "last", "left", "like", "limit", "match", "materialized", "natural", "no", "not",
		"nothing", "notnull", "null", "nulls", "of", "offset", "on", "or", "order", "others",
		"outer", "over", "partition", "plan", "pragma", "preceding", "primary", "query", "raise",
		"range", "recursive", "references", "regexp", "reindex", "release", "rename", "replace",
		"restrict", "returning", "right", "rollback", "row", "rows", "savepoint", "select", "set",
		"table", "temp", "temporary", "then", "ties", "to", "transaction", "trigger", "unbounded",
		"union", "unique", "update", "using", "vacuum", "values", "view", "virtual", "when", "where",
		"window", "with", "without",
	}
}
