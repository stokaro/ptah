package devclean

// White-box testing required: the ClickHouse validator is an internal replay
// boundary whose token-level decisions are not exposed through the public API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lexer"
)

func TestValidateClickHouseReplayStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{
			name:      "empty statement",
			statement: "",
		},
		{
			name:      "local table",
			statement: "CREATE TABLE ptah_dev.events (id UInt64) ENGINE = MergeTree ORDER BY id",
		},
		{
			name:      "embedded local engine",
			statement: "CREATE TABLE ptah_dev.kv (id UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY id",
		},
		{
			name:      "unqualified local insert",
			statement: "INSERT INTO events VALUES (1)",
		},
		{
			name:      "local insert",
			statement: "INSERT INTO ptah_dev.events VALUES (1)",
		},
		{
			name:      "local insert with table keyword",
			statement: "INSERT INTO TABLE ptah_dev.events VALUES (1)",
		},
		{
			name:      "local insert with cte",
			statement: "WITH 1 AS id INSERT INTO ptah_dev.events SELECT id",
		},
		{
			name: "local insert with nested cte",
			statement: "WITH (SELECT number FROM numbers(1)) AS ids " +
				"INSERT INTO TABLE ptah_dev.events SELECT * FROM ids",
		},
		{
			name:      "quoted table named function",
			statement: "INSERT INTO TABLE ptah_dev.`FUNCTION` VALUES (1)",
		},
		{
			name:      "local update",
			statement: "UPDATE ptah_dev.events SET id = 2 WHERE id = 1",
		},
		{
			name:      "local delete",
			statement: "DELETE FROM ptah_dev.events WHERE id = 1",
		},
		{
			name:      "local truncate table",
			statement: "TRUNCATE TABLE ptah_dev.events",
		},
		{
			name:      "local rename table",
			statement: "RENAME TABLE ptah_dev.events TO ptah_dev.events_archive",
		},
		{
			name:      "local rename shorthand",
			statement: "RENAME ptah_dev.events TO ptah_dev.events_archive",
		},
		{
			name:      "local rename dictionary",
			statement: "RENAME DICTIONARY ptah_dev.geo TO ptah_dev.geo_archive",
		},
		{
			name:      "local exchange",
			statement: "EXCHANGE TABLES ptah_dev.events AND ptah_dev.events_archive",
		},
		{
			name:      "local dictionary exchange",
			statement: "EXCHANGE DICTIONARIES ptah_dev.geo AND ptah_dev.geo_archive",
		},
		{
			name:      "local partition move",
			statement: "ALTER TABLE ptah_dev.events MOVE PARTITION 202607 TO TABLE ptah_dev.events_archive",
		},
		{
			name: "local materialized view target",
			statement: "CREATE MATERIALIZED VIEW ptah_dev.events_mv TO ptah_dev.events_archive " +
				"AS SELECT * FROM ptah_dev.events",
		},
		{
			name: "local materialized view without target",
			statement: "CREATE MATERIALIZED VIEW ptah_dev.events_mv ENGINE = Memory " +
				"AS SELECT * FROM ptah_dev.events",
		},
		{
			name:      "ordinary session setting",
			statement: "SET max_threads = 4",
		},
		{
			name:      "column named freeze",
			statement: "ALTER TABLE ptah_dev.events ADD COLUMN freeze String",
		},
		{
			name:      "quoted column named freeze",
			statement: "ALTER TABLE ptah_dev.events ADD COLUMN `FREEZE` String",
		},
		{
			name:      "freeze in string literal",
			statement: "ALTER TABLE ptah_dev.events ADD COLUMN note String DEFAULT 'FREEZE'",
		},
		{
			name:      "rename column to unfreeze",
			statement: "ALTER TABLE ptah_dev.events RENAME COLUMN id TO unfreeze",
		},
		{
			name:      "nested freeze column",
			statement: "ALTER TABLE ptah_dev.events ADD COLUMN metadata Tuple(freeze String)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateClickHouseReplayStatement(
				"ptah_dev",
				clickHouseReplayTokens(test.statement),
			)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateClickHouseReplayStatement_GlobalFailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name: "parallel statement",
			statement: "SELECT 1 PARALLEL WITH " +
				"CREATE FUNCTION leaked AS x -> x",
			wantErr: `clickhouse migration replay rejects PARALLEL WITH .*`,
		},
		{
			name:      "grant",
			statement: "GRANT ALL ON *.* TO migration_user WITH GRANT OPTION",
			wantErr:   `clickhouse migration replay rejects GRANT RBAC change .*`,
		},
		{
			name:      "revoke",
			statement: "REVOKE ALL ON *.* FROM migration_user",
			wantErr:   `clickhouse migration replay rejects REVOKE RBAC change .*`,
		},
		{
			name:      "set default role",
			statement: "SET DEFAULT ROLE ALL TO migration_user",
			wantErr:   `clickhouse migration replay rejects role change .*`,
		},
		{
			name:      "set active role",
			statement: "SET ROLE ALL",
			wantErr:   `clickhouse migration replay rejects role change .*`,
		},
		{
			name:      "function",
			statement: "CREATE FUNCTION normalize AS x -> lower(x)",
			wantErr:   `clickhouse migration replay rejects CREATE FUNCTION .*`,
		},
		{
			name:      "user",
			statement: "CREATE USER migration_user",
			wantErr:   `clickhouse migration replay rejects CREATE USER .*`,
		},
		{
			name:      "named collection",
			statement: "ALTER NAMED COLLECTION remote SET url = 'https://example.test'",
			wantErr:   `clickhouse migration replay rejects ALTER NAMED .*`,
		},
		{
			name:      "workload",
			statement: "DROP WORKLOAD analytics",
			wantErr:   `clickhouse migration replay rejects DROP WORKLOAD .*`,
		},
		{
			name:      "resource",
			statement: "CREATE RESOURCE io_read (READ DISK disk1 FOR INTERVAL 1 SECOND MAX 10M)",
			wantErr:   `clickhouse migration replay rejects CREATE RESOURCE .*`,
		},
		{
			name:      "on cluster",
			statement: "CREATE TABLE ptah_dev.events ON CLUSTER production (id UInt64) ENGINE = Memory",
			wantErr:   `clickhouse migration replay rejects ON CLUSTER .*`,
		},
		{
			name:      "temporary view",
			statement: "CREATE TEMPORARY VIEW events_view AS SELECT 1",
			wantErr:   `clickhouse migration replay rejects TEMP object .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateClickHouseReplayStatement(
				"ptah_dev",
				clickHouseReplayTokens(test.statement),
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestValidateClickHouseReplayStatement_EngineFailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "distributed engine",
			statement: "CREATE TABLE ptah_dev.events (id UInt64) ENGINE = Distributed(cluster, production, events)",
			wantErr:   `clickhouse migration replay rejects DISTRIBUTED table engine .*`,
		},
		{
			name:      "implicit default table engine",
			statement: "CREATE TABLE ptah_dev.events (id UInt64)",
			wantErr:   `clickhouse migration replay rejects implicit/default table engine .*`,
		},
		{
			name:      "table as select with implicit default engine",
			statement: "CREATE TABLE ptah_dev.copied AS SELECT engine FROM ptah_dev.source",
			wantErr:   `clickhouse migration replay rejects implicit/default table engine .*`,
		},
		{
			name:      "external dictionary source",
			statement: "CREATE DICTIONARY ptah_dev.remote (id UInt64) PRIMARY KEY id SOURCE(HTTP(URL 'https://example.test/data')) LAYOUT(FLAT())",
			wantErr:   `clickhouse migration replay rejects HTTP dictionary source .*`,
		},
		{
			name:      "executable dictionary source",
			statement: "CREATE DICTIONARY ptah_dev.remote (id UInt64) PRIMARY KEY id SOURCE(EXECUTABLE(COMMAND 'touch /tmp/escaped')) LAYOUT(FLAT())",
			wantErr:   `clickhouse migration replay rejects EXECUTABLE dictionary source .*`,
		},
		{
			name:      "persistent table snapshot",
			statement: "ALTER TABLE ptah_dev.events FREEZE",
			wantErr:   `clickhouse migration replay rejects persistent table snapshot .*`,
		},
		{
			name:      "persistent table unfreeze",
			statement: "ALTER TABLE ptah_dev.events UNFREEZE WITH NAME 'snapshot'",
			wantErr:   `clickhouse migration replay rejects persistent table snapshot .*`,
		},
		{
			name:      "persistent snapshot as second alter action",
			statement: "ALTER TABLE ptah_dev.events ADD COLUMN note String, FREEZE",
			wantErr:   `clickhouse migration replay rejects persistent table snapshot .*`,
		},
		{
			name:      "remote PostgreSQL engine",
			statement: "CREATE TABLE ptah_dev.events (id UInt64) ENGINE = PostgreSQL('host:5432', 'db', 'events', 'user', 'pass')",
			wantErr:   `clickhouse migration replay rejects POSTGRESQL table engine .*`,
		},
		{
			name:      "replicated engine",
			statement: "CREATE TABLE ptah_dev.events (id UInt64) ENGINE = ReplicatedMergeTree('/tables/events', '{replica}') ORDER BY id",
			wantErr:   `clickhouse migration replay rejects REPLICATEDMERGETREE table engine .*`,
		},
		{
			name:      "unknown engine fails closed",
			statement: "CREATE TABLE ptah_dev.events (id UInt64) ENGINE = FutureRemoteEngine('endpoint')",
			wantErr:   `clickhouse migration replay rejects FUTUREREMOTEENGINE table engine .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateClickHouseReplayStatement(
				"ptah_dev",
				clickHouseReplayTokens(test.statement),
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestValidateClickHouseReplayStatement_LifecycleFailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "attach table",
			statement: "ATTACH TABLE ptah_dev.events",
			wantErr:   `clickhouse migration replay rejects ATTACH .*`,
		},
		{
			name:      "detach table",
			statement: "DETACH TABLE ptah_dev.events PERMANENTLY",
			wantErr:   `clickhouse migration replay rejects DETACH .*`,
		},
		{
			name:      "undrop table",
			statement: "UNDROP TABLE ptah_dev.events",
			wantErr:   `clickhouse migration replay rejects UNDROP .*`,
		},
		{
			name:      "backup",
			statement: "BACKUP DATABASE ptah_dev TO Disk('backups', 'ptah.zip')",
			wantErr:   `clickhouse migration replay rejects BACKUP .*`,
		},
		{
			name:      "restore",
			statement: "RESTORE DATABASE production FROM Disk('backups', 'production.zip')",
			wantErr:   `clickhouse migration replay rejects RESTORE .*`,
		},
		{
			name:      "truncate database",
			statement: "TRUNCATE DATABASE production",
			wantErr:   `clickhouse migration replay rejects database-wide TRUNCATE .*`,
		},
		{
			name:      "truncate all tables",
			statement: "TRUNCATE ALL TABLES FROM production",
			wantErr:   `clickhouse migration replay rejects database-wide TRUNCATE .*`,
		},
		{
			name:      "truncate tables",
			statement: "TRUNCATE TABLES FROM production",
			wantErr:   `clickhouse migration replay rejects database-wide TRUNCATE .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateClickHouseReplayStatement(
				"ptah_dev",
				clickHouseReplayTokens(test.statement),
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestValidateClickHouseReplayStatement_CrossDatabaseFailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "rename destination",
			statement: "RENAME TABLE ptah_dev.events TO production.events",
			wantErr:   `clickhouse migration replay rejects cross-database target "production.events" .*`,
		},
		{
			name:      "rename shorthand destination",
			statement: "RENAME ptah_dev.events TO production.events",
			wantErr:   `clickhouse migration replay rejects cross-database target "production.events" .*`,
		},
		{
			name:      "rename dictionary destination",
			statement: "RENAME DICTIONARY ptah_dev.geo TO production.geo",
			wantErr:   `clickhouse migration replay rejects cross-database target "production.geo" .*`,
		},
		{
			name: "rename second source",
			statement: "RENAME TABLE ptah_dev.events TO ptah_dev.events_archive, " +
				"production.audit TO ptah_dev.audit",
			wantErr: `clickhouse migration replay rejects cross-database target "production.audit" .*`,
		},
		{
			name:      "exchange right target",
			statement: "EXCHANGE TABLES ptah_dev.events AND production.events",
			wantErr:   `clickhouse migration replay rejects cross-database target "production.events" .*`,
		},
		{
			name: "exchange second pair",
			statement: "EXCHANGE TABLES ptah_dev.events AND ptah_dev.events_archive, " +
				"production.audit AND ptah_dev.audit",
			wantErr: `clickhouse migration replay rejects cross-database target "production.audit" .*`,
		},
		{
			name: "move partition destination",
			statement: "ALTER TABLE ptah_dev.events MOVE PARTITION 202607 " +
				"TO TABLE production.events",
			wantErr: `clickhouse migration replay rejects cross-database target "production.events" .*`,
		},
		{
			name: "materialized view destination",
			statement: "CREATE MATERIALIZED VIEW ptah_dev.events_mv TO production.events " +
				"AS SELECT * FROM ptah_dev.events",
			wantErr: `clickhouse migration replay rejects cross-database target "production.events" .*`,
		},
		{
			name: "window view destination",
			statement: "CREATE WINDOW VIEW ptah_dev.events_window TO `production`.`events` " +
				"AS SELECT * FROM ptah_dev.events GROUP BY tumble(now(), INTERVAL 1 MINUTE)",
			wantErr: `clickhouse migration replay rejects cross-database target "production.events" .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateClickHouseReplayStatement(
				"ptah_dev",
				clickHouseReplayTokens(test.statement),
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestValidateClickHouseReplayStatement_InsertFailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "insert with table keyword",
			statement: "INSERT INTO TABLE production.audit VALUES (1)",
			wantErr:   `clickhouse migration replay rejects cross-database target "production.audit" .*`,
		},
		{
			name:      "insert with cte",
			statement: "WITH 1 AS id INSERT INTO production.audit SELECT id",
			wantErr:   `clickhouse migration replay rejects cross-database target "production.audit" .*`,
		},
		{
			name:      "table function",
			statement: "INSERT INTO FUNCTION remote('prod:9000', production.audit) VALUES (1)",
			wantErr:   `clickhouse migration replay rejects table-function write .*`,
		},
		{
			name:      "table function with table keyword",
			statement: "INSERT INTO TABLE FUNCTION file('audit.tsv', TSV, 'id UInt64') VALUES (1)",
			wantErr:   `clickhouse migration replay rejects table-function write .*`,
		},
		{
			name: "table function with cte",
			statement: "WITH 1 AS id INSERT INTO TABLE FUNCTION " +
				"remote('prod:9000', production.audit) SELECT id",
			wantErr: `clickhouse migration replay rejects table-function write .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateClickHouseReplayStatement(
				"ptah_dev",
				clickHouseReplayTokens(test.statement),
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func clickHouseReplayTokens(statement string) []lexer.Token {
	return significantTokens(statement, replayLexerOptions(platform.ClickHouse))
}
