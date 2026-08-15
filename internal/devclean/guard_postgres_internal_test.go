package devclean

// White-box testing required: this file verifies the unexported PostgreSQL-
// family replay policy without expanding the package API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
)

func TestValidatePostgresReplayStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
	}{
		{
			name:      "schema qualified table",
			dialect:   platform.Postgres,
			statement: `CREATE TABLE app.users (id bigint PRIMARY KEY)`,
		},
		{
			name:      "ordinary insert",
			dialect:   platform.Postgres,
			statement: `INSERT INTO app.users (id) VALUES (1)`,
		},
		{
			name:      "ordinary update in cte",
			dialect:   platform.Postgres,
			statement: `WITH changed AS (UPDATE app.users SET id = 2 RETURNING id) SELECT id FROM changed`,
		},
		{
			name:      "system catalog read",
			dialect:   platform.Postgres,
			statement: `SELECT relname FROM pg_catalog.pg_class`,
		},
		{
			name:      "table from system catalog read",
			dialect:   platform.Postgres,
			statement: `CREATE TABLE app.relations AS SELECT relname FROM pg_catalog.pg_class`,
		},
		{
			name:      "schema scoped default privileges",
			dialect:   platform.Postgres,
			statement: `ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO app_reader`,
		},
		{
			name:      "table grant",
			dialect:   platform.Postgres,
			statement: `GRANT SELECT ON TABLE app.users TO app_reader`,
		},
		{
			name:      "table comment",
			dialect:   platform.Postgres,
			statement: `COMMENT ON TABLE app.users IS 'application users'`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tokens := significantTokens(test.statement, replayLexerOptions(test.dialect))
			err := validatePostgresReplayStatement(test.dialect, tokens)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidatePostgresReplayStatement_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		wantErr   string
	}{
		{
			name:      "create in pg catalog",
			dialect:   platform.Postgres,
			statement: `CREATE TABLE pg_catalog.ptah_leak (id integer)`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "nested update in information schema",
			dialect:   platform.Postgres,
			statement: `WITH changed AS (UPDATE information_schema.tables SET table_name = 'x' RETURNING table_name) SELECT table_name FROM changed`,
			wantErr:   `postgres migration replay rejects protected namespace "information_schema" mutation .*`,
		},
		{
			name:      "drop crdb internal schema",
			dialect:   platform.CockroachDB,
			statement: `DROP SCHEMA crdb_internal CASCADE`,
			wantErr:   `cockroachdb migration replay rejects protected namespace "crdb_internal" mutation .*`,
		},
		{
			name:      "comment on pg catalog table",
			dialect:   platform.Postgres,
			statement: `COMMENT ON TABLE pg_catalog.pg_class IS 'changed'`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "grant on protected schema",
			dialect:   platform.Postgres,
			statement: `GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO app_reader`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "grant on second protected schema",
			dialect:   platform.Postgres,
			statement: `GRANT SELECT ON ALL TABLES IN SCHEMA app, pg_catalog TO app_reader`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "drop second protected table",
			dialect:   platform.Postgres,
			statement: `DROP TABLE app.users, pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "explicit pg temp target",
			dialect:   platform.Postgres,
			statement: `CREATE TABLE pg_temp.ptah_leak (id integer)`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_temp" mutation .*`,
		},
		{
			name:      "collation in pg catalog",
			dialect:   platform.Postgres,
			statement: `CREATE COLLATION pg_catalog.ptah_collation FROM "C"`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "operator in pg catalog",
			dialect:   platform.Postgres,
			statement: `CREATE OPERATOR pg_catalog.=== (FUNCTION = app.equals, LEFTARG = integer, RIGHTARG = integer)`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "policy on pg catalog",
			dialect:   platform.Postgres,
			statement: `CREATE POLICY ptah_policy ON pg_catalog.pg_class USING (true)`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "foreign schema import into pg catalog",
			dialect:   platform.Postgres,
			statement: `IMPORT FOREIGN SCHEMA remote FROM SERVER remote_srv INTO pg_catalog`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "extension schema pg catalog",
			dialect:   platform.Postgres,
			statement: `CREATE EXTENSION hstore WITH SCHEMA pg_catalog`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "database grant",
			dialect:   platform.Postgres,
			statement: `GRANT CONNECT ON DATABASE ptah_dev TO PUBLIC`,
			wantErr:   `postgres migration replay rejects GRANT database or global privilege .*`,
		},
		{
			name:      "role membership",
			dialect:   platform.Postgres,
			statement: `GRANT app_owner TO app_user`,
			wantErr:   `postgres migration replay rejects GRANT role membership .*`,
		},
		{
			name:      "quoted role named on",
			dialect:   platform.Postgres,
			statement: `GRANT "on" TO app_user`,
			wantErr:   `postgres migration replay rejects GRANT role membership .*`,
		},
		{
			name:      "global default privileges",
			dialect:   platform.Postgres,
			statement: `ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO PUBLIC`,
			wantErr:   `postgres migration replay rejects ALTER DEFAULT PRIVILEGES without IN SCHEMA .*`,
		},
		{
			name:      "database comment",
			dialect:   platform.Postgres,
			statement: `COMMENT ON DATABASE ptah_dev IS 'persistent'`,
			wantErr:   `postgres migration replay rejects COMMENT ON global metadata .*`,
		},
		{
			name:      "root schema comment",
			dialect:   platform.Postgres,
			statement: `COMMENT ON SCHEMA public IS 'persistent'`,
			wantErr:   `postgres migration replay rejects COMMENT ON global metadata .*`,
		},
		{
			name:      "alter schema owner",
			dialect:   platform.Postgres,
			statement: `ALTER SCHEMA public OWNER TO app_owner`,
			wantErr:   `postgres migration replay rejects ALTER SCHEMA metadata .*`,
		},
		{
			name:      "drop owned",
			dialect:   platform.Postgres,
			statement: `DROP OWNED BY app_owner`,
			wantErr:   `postgres migration replay rejects DROP OWNED .*`,
		},
		{
			name:      "reassign owned",
			dialect:   platform.Postgres,
			statement: `REASSIGN OWNED BY app_owner TO replacement_owner`,
			wantErr:   `postgres migration replay rejects REASSIGN OWNED .*`,
		},
		{
			name:      "set session role",
			dialect:   platform.Postgres,
			statement: `SET SESSION ROLE app_owner`,
			wantErr:   `postgres migration replay rejects SET ROLE .*`,
		},
		{
			name:      "set local role",
			dialect:   platform.Postgres,
			statement: `SET LOCAL ROLE app_owner`,
			wantErr:   `postgres migration replay rejects SET ROLE .*`,
		},
		{
			name:      "set local session authorization",
			dialect:   platform.Postgres,
			statement: `SET LOCAL SESSION AUTHORIZATION app_owner`,
			wantErr:   `postgres migration replay rejects SET SESSION AUTHORIZATION .*`,
		},
		{
			name:      "reset session",
			dialect:   platform.Postgres,
			statement: `RESET ALL`,
			wantErr:   `postgres migration replay rejects RESET session or transaction state .*`,
		},
		{
			name:      "set config function",
			dialect:   platform.Postgres,
			statement: `SELECT set_config('session_replication_role', 'replica', false)`,
			wantErr:   `postgres migration replay rejects cluster control function .*`,
		},
		{
			name:      "select into temp",
			dialect:   platform.Postgres,
			statement: `SELECT 1 INTO TEMP TABLE ptah_leak`,
			wantErr:   `postgres migration replay rejects TEMP object .*`,
		},
		{
			name:      "select into protected namespace",
			dialect:   platform.Postgres,
			statement: `SELECT 1 INTO pg_catalog.ptah_leak`,
			wantErr:   `postgres migration replay rejects protected namespace "pg_catalog" mutation .*`,
		},
		{
			name:      "begin transaction",
			dialect:   platform.Postgres,
			statement: `BEGIN`,
			wantErr:   `postgres migration replay rejects BEGIN session or transaction state .*`,
		},
		{
			name:      "prepare transaction",
			dialect:   platform.Postgres,
			statement: `PREPARE TRANSACTION 'ptah_leak'`,
			wantErr:   `postgres migration replay rejects PREPARE session or transaction state .*`,
		},
		{
			name:      "listen",
			dialect:   platform.Postgres,
			statement: `LISTEN ptah_channel`,
			wantErr:   `postgres migration replay rejects LISTEN session or transaction state .*`,
		},
		{
			name:      "advisory lock",
			dialect:   platform.Postgres,
			statement: `SELECT pg_catalog.pg_advisory_lock(42)`,
			wantErr:   `postgres migration replay rejects session advisory lock .*`,
		},
		{
			name:      "large object",
			dialect:   platform.Postgres,
			statement: `SELECT lo_create(900001)`,
			wantErr:   `postgres migration replay rejects large object operation .*`,
		},
		{
			name:      "replication slot",
			dialect:   platform.Postgres,
			statement: `SELECT * FROM pg_create_logical_replication_slot('ptah_leak', 'test_decoding')`,
			wantErr:   `postgres migration replay rejects replication state operation .*`,
		},
		{
			name:      "dblink external mutation",
			dialect:   platform.Postgres,
			statement: `SELECT dblink_exec('host=remote', 'DELETE FROM accounts')`,
			wantErr:   `postgres migration replay rejects external dblink operation .*`,
		},
		{
			name:      "cockroach cluster setting",
			dialect:   platform.CockroachDB,
			statement: `SET CLUSTER SETTING diagnostics.reporting.enabled = false`,
			wantErr:   `cockroachdb migration replay rejects SET CLUSTER SETTING .*`,
		},
		{
			name:      "cockroach changefeed",
			dialect:   platform.CockroachDB,
			statement: `CREATE CHANGEFEED FOR TABLE public.events INTO 's3://bucket/path'`,
			wantErr:   `cockroachdb migration replay rejects CREATE CHANGEFEED .*`,
		},
		{
			name:      "cockroach external connection",
			dialect:   platform.CockroachDB,
			statement: `CREATE EXTERNAL CONNECTION sink AS 's3://bucket/path'`,
			wantErr:   `cockroachdb migration replay rejects CREATE EXTERNAL CONNECTION .*`,
		},
		{
			name:      "cockroach cluster job",
			dialect:   platform.CockroachDB,
			statement: `PAUSE JOB 123`,
			wantErr:   `cockroachdb migration replay rejects PAUSE cluster job .*`,
		},
		{
			name:      "yugabyte tablegroup",
			dialect:   platform.YugabyteDB,
			statement: `CREATE TABLEGROUP ptah_leak`,
			wantErr:   `yugabytedb migration replay rejects CREATE TABLEGROUP .*`,
		},
		{
			name:      "yugabyte tablegroup grant",
			dialect:   platform.YugabyteDB,
			statement: `GRANT ALL ON TABLEGROUP ptah_leak TO app_user`,
			wantErr:   `yugabytedb migration replay rejects GRANT database or global privilege .*`,
		},
		{
			name:      "existing create role rejection",
			dialect:   platform.Postgres,
			statement: `CREATE ROLE app_user`,
			wantErr:   `postgres migration replay rejects CREATE ROLE .*`,
		},
		{
			name:      "existing external copy rejection",
			dialect:   platform.Postgres,
			statement: `COPY app.users TO PROGRAM 'cat > /tmp/users'`,
			wantErr:   `postgres migration replay rejects external COPY .*`,
		},
		{
			name:      "function definition",
			dialect:   platform.Postgres,
			statement: `CREATE OR REPLACE FUNCTION app.answer() RETURNS integer LANGUAGE SQL AS $$ SELECT 42 $$`,
			wantErr:   `postgres migration replay rejects CREATE routine definition .*`,
		},
		{
			name:      "procedure definition",
			dialect:   platform.Postgres,
			statement: `CREATE PROCEDURE app.refresh() LANGUAGE SQL AS $$ DELETE FROM app.cache $$`,
			wantErr:   `postgres migration replay rejects CREATE routine definition .*`,
		},
		{
			name:      "alter routine",
			dialect:   platform.Postgres,
			statement: `ALTER ROUTINE app.answer() SECURITY DEFINER`,
			wantErr:   `postgres migration replay rejects ALTER routine definition .*`,
		},
		{
			name:      "anonymous block",
			dialect:   platform.Postgres,
			statement: `DO $$ BEGIN RAISE NOTICE 'replay'; END $$`,
			wantErr:   `postgres migration replay rejects DO sublanguage .*`,
		},
		{
			name:      "procedure call",
			dialect:   platform.Postgres,
			statement: `CALL app.refresh_materialized_data()`,
			wantErr:   `postgres migration replay rejects CALL sublanguage .*`,
		},
		{
			name:      "session search path",
			dialect:   platform.Postgres,
			statement: `SET search_path TO app, public`,
			wantErr:   `postgres migration replay rejects SET search_path .*`,
		},
		{
			name:      "explicit session search path",
			dialect:   platform.Postgres,
			statement: `SET SESSION search_path TO app, public`,
			wantErr:   `postgres migration replay rejects SET search_path .*`,
		},
		{
			name:      "foreign table",
			dialect:   platform.Postgres,
			statement: `CREATE FOREIGN TABLE app.remote_users (id bigint) SERVER remote_srv`,
			wantErr:   `postgres migration replay rejects CREATE FOREIGN .*`,
		},
		{
			name:      "foreign schema import",
			dialect:   platform.Postgres,
			statement: `IMPORT FOREIGN SCHEMA remote FROM SERVER remote_srv INTO app`,
			wantErr:   `postgres migration replay rejects IMPORT FOREIGN SCHEMA .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tokens := significantTokens(test.statement, replayLexerOptions(test.dialect))
			err := validatePostgresReplayStatement(test.dialect, tokens)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
