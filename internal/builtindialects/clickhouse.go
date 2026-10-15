package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

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

		// SELECT version() answers a bare "26.7.3.19"; system.build_options
		// reports VERSION_FULL "ClickHouse 26.7.3.19", which is the server
		// naming itself. CHECK GRANT arrived in 24.11. A string with no
		// readable version takes the lower arm: a database-realm cleanup that
		// the CHECK GRANT key guards is one Ptah would rather decline than run
		// on a privilege answer it could not obtain (stokaro/ptah#916).
		// Ptah models no identifier limit for ClickHouse.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "ClickHouse24",
			BannerTokens:  []string{"clickhouse"},
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "ClickHouse24",
				Steps: []descriptor.VersionStep{
					{Min: "24.11", Preset: "ClickHouse2411"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.ClickHouseMeasured(),
				Newest:      capabilityline.ClickHouse269,
			},
		},

		// How a connection reaches the server.
		Driver: "clickhouse",

		// ClickHouse runs DELETE, but a plain UPDATE answers `Lightweight
		// updates are not supported. Lightweight updates are supported only
		// for tables with materialized _block_number column`, a per-table
		// setting a statement cannot declare. Its own spelling, ALTER TABLE
		// ... UPDATE, is an asynchronous mutation outside any transaction, so
		// the portable builder does not write it (stokaro/ptah#941). Measured
		// on 24.10.4.191 and 25.8.30.
		Query: descriptor.Query{
			UpsertRefusal: "ClickHouse has no upsert statement: a ReplacingMergeTree deduplicates in the background " +
				"rather than at insert time, which is a table design and not a clause on this INSERT",
			UpdateRefusal: "UPDATE is not a portable statement on ClickHouse: a plain UPDATE runs only on a table " +
				"with the materialized _block_number column, which a statement cannot declare; use " +
				"ALTER TABLE … UPDATE, or enable enable_block_number_column on the table",
		},
		Scripts: descriptor.Scripts{ClientDelimiters: true},

		// Migrations: what the migrator reads.
		DDLTransactions: "no_transaction",
		RevisionTable:   clickHouseRevisionTable(),

		Roles: descriptor.Roles{OutOfScope: descriptor.RoleScopeReferenced},
		// String literals process C-style backslash escapes. A plain UPDATE
		// answers "Lightweight updates are not supported" unless the table
		// carries a materialized _block_number column, which a reference
		// table does not, so a row change is the ALTER TABLE ... UPDATE
		// mutation; the lightweight DELETE has been on by default since 23.3
		// (stokaro/ptah#3281). ClickHouse has neither transactions nor
		// savepoints, so a seed runs statement by statement into a MergeTree
		// tracker whose ORDER BY key constrains nothing.
		Literals: descriptor.Literals{
			NumericBool:      true,
			BackslashEscapes: true,
			BinaryPrefix:     "X'",
			BinarySuffix:     "'",
		},
		ManagedData: descriptor.ManagedData{
			AlterTableUpdate: true,
		},
		Seeds: descriptor.Seeds{
			Tracker: `CREATE TABLE IF NOT EXISTS schema_seeds (
    seed_path String,
    env String,
    checksum String,
    applied_at DateTime
) ENGINE = MergeTree ORDER BY seed_path`,
			TablesQuery: "SELECT name FROM system.tables WHERE database = currentDatabase() AND is_temporary = 0",
		},

		// How an identifier is written, and how a column's own key is named.
		Quoting: descriptor.Quoting{Style: descriptor.QuoteBacktick},

		// Cleanup: what a development database cleanup reads.
		// The writer drops tables, views and materialized views, and the
		// reader already reports both revision tables.
		Cleanup: descriptor.Cleanup{
			Views:             true,
			MaterializedViews: true,
			Sync:              true,
		},

		// Lint: what migration lint reads.
		Lint: descriptor.Lint{
			Analyzed: true,
			NotNullConsequence: "ClickHouse runs the change as a mutation that fails on the first NULL (CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN) and " +
				"stays unfinished while the column is already declared non-Nullable, so every read of the table fails with the same " +
				"error until the mutation is killed and the column modified back to Nullable; a DEFAULT in the clause does not rescue the NULL",
			NotNullByType: true,
		},

		// Dev databases. The server takes no advisory lock Ptah can hold, so runs on
		// one dev database are serialized with a lock file, which only serializes
		// runs on this machine.
		DevRealm: descriptor.DevRealm{
			Identity: "query",
			Query:    "SELECT currentDatabase()",
			FileLock: "loopback",
		},
		SchemaIsDatabase: true,

		// How the common model is lowered for the engine.
		InlineEnum: descriptor.InlineEnum{Type: descriptor.InlineEnumNumbered},
		Lexical:    descriptor.Lexical{LineComments: []string{"#", "//"}, BackslashEscapes: true},
	}
}

// clickHouseRevisionTable is how ClickHouse keeps the migrator's tables.
//
// The engine is named because ClickHouse gives a table none unless asked, and
// whether an unnamed one is legal at all is the server's
// default_table_engine setting, whose own default is None
// (stokaro/ptah#2234). Which engines a revision table can be is the server's
// judgment, so a configured one is passed through.
//
// partial_hashes is TEXT, never JSON NULL: ClickHouse reads a trailing NULL as
// Nullable(T), 24.x refuses Nullable(JSON) outright, and later servers coerce
// the string null into {} (#950). ClickHouse offers the migrator no
// transaction -- its writer's Commit and Rollback do nothing, which
// TestRevisionCompletionFailure_ClickHouseNoTransactionLive measures -- and a
// row changes only through a mutation, which the migrator waits for.
func clickHouseRevisionTable() descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create: createRevisions,
		CreateAtlas: `CREATE TABLE IF NOT EXISTS {table} (
    version VARCHAR(255) PRIMARY KEY,
    description TEXT NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at TIMESTAMP NOT NULL,
    execution_time BIGINT NOT NULL,
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR(255) NOT NULL,
    partial_hashes TEXT NULL,
    operator_version VARCHAR(255) NOT NULL
){engine}`,
		CreateTags:   createTags,
		CreateLog:    createLog,
		CreateSchema: createSchema,
		EngineClause: " ENGINE = MergeTree",
		FirstRow:     "limit",
		Update: `ALTER TABLE {table}
UPDATE {assignments}
WHERE version = ?
SETTINGS mutations_sync = 1`,
		// A key column cannot be updated, so a respelled row is a copy under
		// the new version, and the old one is deleted once it is written.
		Respell: []string{
			"INSERT INTO {table} SELECT '{file}', * EXCEPT (version) FROM {table} WHERE version = '{recorded}'",
			"ALTER TABLE {table} DELETE WHERE version = '{recorded}' SETTINGS mutations_sync = 1",
		},
		Mutations:      true,
		UpsertTag:      upsertTagOnDuplicateKey,
		VersionCast:    "BIGINT",
		VersionLiteral: "backslash",
		DerivedTableAs: " AS",
		JSONAsText:     true,
	}
}
