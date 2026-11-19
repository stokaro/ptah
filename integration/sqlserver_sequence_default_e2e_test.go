//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// sqlServerSequenceDefaultSchema declares defaults that read a sequence
// through NEXT VALUE FOR, unqualified, qualified, bracketed and without
// parentheses, beside a string that spells one. SQL Server 2022 stores
// (NEXT VALUE FOR ptah_s) as (NEXT VALUE FOR [ptah_s]),
// next value for dbo.ptah_s as (NEXT VALUE FOR [dbo].[ptah_s]) and a bare
// NEXT VALUE FOR PTAH_S as (NEXT VALUE FOR [PTAH_S]).
const sqlServerSequenceDefaultSchema = "CREATE SEQUENCE ptah_s AS bigint START WITH 1;\n" +
	"CREATE TABLE seqd (\n" +
	"  id int PRIMARY KEY,\n" +
	"  a bigint DEFAULT (NEXT VALUE FOR ptah_s),\n" +
	"  b bigint DEFAULT (next value for dbo.ptah_s),\n" +
	"  c bigint DEFAULT NEXT VALUE FOR PTAH_S,\n" +
	"  d bigint DEFAULT (NEXT VALUE FOR [dbo].[ptah_s]),\n" +
	"  t varchar(30) DEFAULT 'NEXT VALUE FOR ptah_s'\n" +
	");\n"

// TestApplySQLServerSequenceDefaultE2E applies sqlServerSequenceDefaultSchema
// twice through `ptah schema apply`. A row inserted without the columns takes
// the sequence's value, which SQL Server gives every reference to one
// sequence in a row, and the string stays a string. The second apply
// plans nothing: compared as text, every default but t differs from the
// spelling SQL Server stores, and each apply plans it again.
func TestApplySQLServerSequenceDefaultE2E(t *testing.T) {
	c := qt.New(t)
	database := sqlServerIdentityScratchDatabase(c)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(sqlServerSequenceDefaultSchema), 0o600), qt.IsNil)
	apply := []string{"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve"}

	applied := runPtahIn(c, apply...)
	again := runPtahIn(c, apply...)

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	db, err := sql.Open("sqlserver", database)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(c.Context(), "INSERT INTO seqd (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var got string
	err = db.QueryRowContext(c.Context(), "SELECT CONCAT_WS(',', a, b, c, d, t) FROM seqd").Scan(&got)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "1,1,1,1,NEXT VALUE FOR ptah_s")
}

// TestApplySQLServerSequenceNameCaseE2E applies sqlServerSequenceDefaultSchema,
// then a file whose defaults name the same sequence and schema in another
// case. SQL Server stores each reference as written, so the account holds
// NEXT VALUE FOR [ptah_s] and [dbo].[ptah_s], and its default collation,
// SQL_Latin1_General_CP1_CI_AS, reads PTAH_S and DBO as the sequence and the
// schema it holds. The second file plans nothing. Compared as stored, it
// plans every default it spells in another case.
func TestApplySQLServerSequenceNameCaseE2E(t *testing.T) {
	c := qt.New(t)
	database := sqlServerIdentityScratchDatabase(c)
	first := filepath.Join(c.TempDir(), "first.sql")
	c.Assert(os.WriteFile(first, []byte(sqlServerSequenceDefaultSchema), 0o600), qt.IsNil)
	second := filepath.Join(c.TempDir(), "second.sql")
	c.Assert(os.WriteFile(second, []byte("CREATE SEQUENCE ptah_s AS bigint START WITH 1;\n"+
		"CREATE TABLE seqd (\n"+
		"  id int PRIMARY KEY,\n"+
		"  a bigint DEFAULT (NEXT VALUE FOR PTAH_S),\n"+
		"  b bigint DEFAULT (next value for DBO.Ptah_S),\n"+
		"  c bigint DEFAULT NEXT VALUE FOR PTAH_S,\n"+
		"  d bigint DEFAULT (NEXT VALUE FOR [DBO].[PTAH_S]),\n"+
		"  t varchar(30) DEFAULT 'NEXT VALUE FOR ptah_s'\n"+
		");\n"), 0o600), qt.IsNil)

	applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", first, "--auto-approve")
	recased := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", second, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(recased.ExitCode, qt.Equals, 0, historyComment(recased))
	c.Assert(recased.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}
