//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestApplyOracleSequenceDefaultE2E applies defaults that read a sequence
// pseudocolumn, unqualified, qualified, quoted and inside an operator,
// beside a string that spells one. Oracle stores an unqualified reference
// qualified and quoted, as "<SCHEMA>"."PTAH_S"."NEXTVAL", and a qualified one
// as written. A row inserted without the columns takes the sequences'
// values, and the string stays a string; a second apply plans nothing, and a
// file `schema inspect` writes compares clean with the account. Read as
// values, the references render as strings, and Oracle refuses a string for
// a NUMBER column.
func TestApplyOracleSequenceDefaultE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	account, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	schemaPath := writeDeclaredSchema(c, "CREATE SEQUENCE ptah_s;\n"+
		"CREATE SEQUENCE \"Mixed\";\n"+
		"CREATE TABLE seqd (\n"+
		"  id NUMBER DEFAULT ptah_s.NEXTVAL PRIMARY KEY,\n"+
		"  c NUMBER DEFAULT ptah_s.CURRVAL,\n"+
		"  q NUMBER DEFAULT "+account+".ptah_s.nextval,\n"+
		"  r NUMBER DEFAULT \"Mixed\".NEXTVAL,\n"+
		"  u NUMBER DEFAULT ptah_s.NEXTVAL + 1,\n"+
		"  t VARCHAR2(20) DEFAULT 'ptah_s.NEXTVAL'\n"+
		");\n")
	apply := []string{"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve"}

	applied, stderr, err := runCLIProcess(ctx, c.TempDir(), binary, apply...)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, stderr))
	_, err = conn.ExecContext(ctx, "INSERT INTO seqd (t) VALUES (DEFAULT)")
	c.Assert(err, qt.IsNil)
	c.Assert(accountNames(c, ctx, conn, "SELECT id || ',' || c || ',' || q || ',' || r || ',' || u || ',' || t FROM seqd"),
		qt.DeepEquals, []string{"1,1,1,1,2,ptah_s.NEXTVAL"})
	again, stderr, err := runCLIProcess(ctx, c.TempDir(), binary, apply...)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, stderr))
	c.Assert(again, qt.Equals, "Schema is synced, no changes to be made.\n")

	inspected, stderr, err := runCLIProcess(ctx, c.TempDir(), binary, "schema", "inspect", "--db-url", dbURL, "--format", "sql")
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(inspected, qt.Contains, `ID NUMBER DEFAULT "`)
	compared, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "compare", "--db-url", dbURL, "--schema-file", writeDeclaredSchema(c, inspected))
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, stderr))
	c.Assert(compared, qt.Contains, "No schema differences detected")
}
