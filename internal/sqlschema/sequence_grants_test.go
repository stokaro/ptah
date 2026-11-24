package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// A grant on a table name that names a declared sequence is a grant on the
// sequence, on a dialect whose GRANT names a sequence as it names a table, so
// it compares with the sequence grant the read reports.
func TestRead_GrantOnADeclaredSequenceName(t *testing.T) {
	tests := []struct {
		name, dialect, sql string
		sequence           string
		privileges         []string
	}{
		{
			name: "no object kind", dialect: platform.Postgres,
			sql:      "CREATE SEQUENCE s1;\nGRANT SELECT ON s1 TO r;\n",
			sequence: "s1", privileges: []string{"SELECT"},
		},
		{
			name: "the TABLE keyword", dialect: platform.Postgres,
			sql:      "CREATE SEQUENCE s1;\nGRANT USAGE ON TABLE s1 TO r;\n",
			sequence: "s1", privileges: []string{"USAGE"},
		},
		{
			name: "a qualified name", dialect: platform.Postgres,
			sql:      "CREATE SCHEMA app;\nCREATE SEQUENCE app.s1;\nGRANT SELECT ON app.s1 TO r;\n",
			sequence: "app.s1", privileges: []string{"SELECT"},
		},
		{
			name: "a GRANT before the CREATE SEQUENCE", dialect: platform.Postgres,
			sql:      "GRANT SELECT ON s1 TO r;\nCREATE SEQUENCE s1;\n",
			sequence: "s1", privileges: []string{"SELECT"},
		},
		{
			name: "a REVOKE in the other spelling", dialect: platform.Postgres,
			sql:      "CREATE SEQUENCE s1;\nGRANT SELECT, UPDATE ON SEQUENCE s1 TO r;\nREVOKE UPDATE ON s1 FROM r;\n",
			sequence: "s1", privileges: []string{"SELECT"},
		},
		{
			name: "the default schema written out", dialect: platform.Postgres,
			sql:      "CREATE SEQUENCE s1;\nGRANT SELECT ON public.s1 TO r;\n",
			sequence: "public.s1", privileges: []string{"SELECT"},
		},
		{
			name: "Oracle, the grant in the schema the sequence leaves to the connection", dialect: platform.Oracle,
			sql:      "CREATE SEQUENCE s1;\nGRANT SELECT ON app.s1 TO r;\n",
			sequence: "app.s1", privileges: []string{"SELECT"},
		},
		{
			name: "Oracle, the sequence in the schema the grant leaves to the connection", dialect: platform.Oracle,
			sql:      "CREATE SEQUENCE app.s1;\nGRANT SELECT ON s1 TO r;\n",
			sequence: "s1", privileges: []string{"SELECT"},
		},
		{
			name: "Oracle, the sequence the grant names as written", dialect: platform.Oracle,
			sql:      "CREATE TABLE s1 (id NUMBER);\nCREATE SEQUENCE app.s1;\nGRANT SELECT ON app.s1 TO r;\n",
			sequence: "app.s1", privileges: []string{"SELECT"},
		},
		{
			name: "Oracle, the sequence the grant names as written beside a table in a schema", dialect: platform.Oracle,
			sql:      "CREATE SEQUENCE s1;\nCREATE TABLE app.s1 (id NUMBER);\nGRANT SELECT ON s1 TO r;\n",
			sequence: "s1", privileges: []string{"SELECT"},
		},
		{
			name: "Oracle, whose GRANT names no kind", dialect: platform.Oracle,
			sql:      "CREATE SEQUENCE app.s1;\nGRANT KEEP SEQUENCE, SELECT ON app.s1 TO r;\n",
			sequence: "app.s1", privileges: []string{"KEEP SEQUENCE", "SELECT"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Grants, qt.HasLen, 1)
			c.Assert(database.Grants[0].OnSequence, qt.Equals, test.sequence)
			c.Assert(database.Grants[0].OnTable, qt.Equals, "")
			c.Assert(database.Grants[0].Privileges, qt.DeepEquals, test.privileges)
		})
	}
}

// A revoke on a declared sequence name is a revoke on the sequence, written
// after the CREATE SEQUENCE and before it.
func TestRead_RevokeOnADeclaredSequenceName(t *testing.T) {
	tests := []struct {
		name, sql string
	}{
		{name: "after the CREATE SEQUENCE", sql: "CREATE SEQUENCE s1;\nREVOKE SELECT ON s1 FROM rv_u;\n"},
		{name: "before the CREATE SEQUENCE", sql: "REVOKE SELECT ON s1 FROM rv_u;\nCREATE SEQUENCE s1;\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(database.RevokedGrants, qt.HasLen, 1)
			c.Assert(database.RevokedGrants[0].OnSequence, qt.Equals, "s1")
			c.Assert(database.RevokedGrants[0].OnTable, qt.Equals, "")
			c.Assert(database.RevokedGrants[0].Privileges, qt.DeepEquals, []string{"SELECT"})
		})
	}
}

// A grant stays on the table name where the name is no declared sequence
// alone, or where the dialect reads a sequence grant as a table grant.
func TestRead_GrantOnATableNameStaysOnTheTable(t *testing.T) {
	tests := []struct {
		name, dialect, sql, table string
	}{
		{name: "no sequence of the name", dialect: platform.Postgres, sql: "GRANT SELECT ON s1 TO r;\n", table: "s1"},
		{
			name: "an Oracle table of the name", dialect: platform.Oracle,
			sql: "CREATE TABLE s1 (id NUMBER);\nCREATE SEQUENCE s1;\nGRANT SELECT ON s1 TO r;\n", table: "s1",
		},
		{
			name: "an Oracle view of the name", dialect: platform.Oracle,
			sql: "CREATE VIEW s1 AS SELECT 1 AS id FROM dual;\nCREATE SEQUENCE s1;\nGRANT SELECT ON s1 TO r;\n", table: "s1",
		},
		{
			name: "a sequence in another schema", dialect: platform.Postgres,
			sql: "CREATE SCHEMA app;\nCREATE SEQUENCE app.s1;\nGRANT SELECT ON s1 TO r;\n", table: "s1",
		},
		{
			name: "a sequence outside the default schema", dialect: platform.Postgres,
			sql: "CREATE SEQUENCE s1;\nGRANT SELECT ON app.s1 TO r;\n", table: "app.s1",
		},
		{
			name: "two Oracle tables the name could mean", dialect: platform.Oracle,
			sql: "CREATE TABLE a.s1 (id NUMBER);\nCREATE TABLE b.s1 (id NUMBER);\nGRANT SELECT ON s1 TO r;\n", table: "s1",
		},
		{name: "columns", dialect: platform.Postgres, sql: "CREATE SEQUENCE s1;\nGRANT SELECT (id) ON s1 TO r;\n", table: "s1"},
		{name: "MariaDB, which keeps a sequence as a table", dialect: platform.MariaDB, sql: "CREATE SEQUENCE s1;\nGRANT SELECT ON s1 TO r;\n", table: "s1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Grants, qt.HasLen, 1)
			c.Assert(database.Grants[0].OnTable, qt.Equals, test.table)
			c.Assert(database.Grants[0].OnSequence, qt.Equals, "")
		})
	}
}

// Where the connection decides the schema of an unqualified name, a grant that
// can reach more than one declared object, a sequence among them, is refused:
// which one it reaches depends on the account that runs the file.
func TestRead_GrantOnASequenceNameTheAccountDecides_FailurePath(t *testing.T) {
	tests := []struct {
		name, sql, wantErr string
	}{
		{
			name:    "two sequences",
			sql:     "CREATE SEQUENCE a.s1;\nCREATE SEQUENCE b.s1;\nGRANT SELECT ON s1 TO r;\n",
			wantErr: `line 3, column 1: GRANT ON s1 can reach the sequence a.s1 or the sequence b.s1, by the schema of the account that runs the file: write the schema in the name`,
		},
		{
			name:    "a sequence and a table",
			sql:     "CREATE SEQUENCE a.s1;\nCREATE TABLE b.s1 (id NUMBER);\nGRANT SELECT ON s1 TO r;\n",
			wantErr: `line 3, column 1: GRANT ON s1 can reach the sequence a.s1 or the table b.s1, by the schema of the account that runs the file: write the schema in the name`,
		},
		{
			name:    "a REVOKE",
			sql:     "CREATE SEQUENCE a.s1;\nCREATE SEQUENCE b.s1;\nREVOKE SELECT ON s1 FROM r;\n",
			wantErr: `line 3, column 1: REVOKE ON s1 can reach the sequence a.s1 or the sequence b.s1, by the schema of the account that runs the file: write the schema in the name`,
		},
		{
			name:    "a GRANT before the sequences",
			sql:     "GRANT SELECT ON s1 TO r;\nCREATE SEQUENCE a.s1;\nCREATE SEQUENCE b.s1;\n",
			wantErr: `GRANT ON s1 can reach the sequence a.s1 or the sequence b.s1, by the schema of the account that runs the file: write the schema in the name`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Oracle)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
