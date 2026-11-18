//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// oracleNiladicDefaultSchema declares a default with each Oracle function
// that takes no parentheses, and h writes SYSDATE in lower case. Read as a
// value, SYSDATE renders as the string 'SYSDATE', and a current-time keyword
// gains `()`, which Oracle refuses with ORA-03077.
const oracleNiladicDefaultSchema = "CREATE TABLE ptah_niladic_defaults (\n" +
	"  id NUMBER PRIMARY KEY,\n" +
	"  a DATE DEFAULT SYSDATE,\n" +
	"  b TIMESTAMP DEFAULT SYSTIMESTAMP,\n" +
	"  c DATE DEFAULT CURRENT_DATE,\n" +
	"  d TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
	"  e TIMESTAMP DEFAULT LOCALTIMESTAMP,\n" +
	"  f VARCHAR2(128) DEFAULT USER,\n" +
	"  g NUMBER DEFAULT UID,\n" +
	"  h DATE DEFAULT sysdate,\n" +
	"  i VARCHAR2(64) DEFAULT SESSIONTIMEZONE,\n" +
	"  j VARCHAR2(64) DEFAULT DBTIMEZONE,\n" +
	"  k VARCHAR2(128) DEFAULT ORA_INVOKING_USER,\n" +
	"  l NUMBER DEFAULT ORA_INVOKING_USERID\n" +
	");\n"

// TestApplyOracleNiladicDefaultE2E applies the defaults through `ptah schema
// apply`, inserts a row that takes every one, and reads the row back: the
// dates are this century's, the user and its id are the account's that
// inserted it, and the time zones are the session's and the database's. A
// second apply plans nothing, so the comparison reads each default Oracle
// stores as the one the file declares.
func TestApplyOracleNiladicDefaultE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	dbURL := dbtarget.URL(c, dbtarget.Oracle)
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	defer func() {
		c.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
	}()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(oracleNiladicDefaultSchema), 0o600), qt.IsNil)
	apply := []string{"schema", "apply", "--db-url", dbURL, "--schema-file", schema, "--auto-approve"}

	applied, appliedErr, err := runPtahSplitStreams(ctx, apply)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
	again, againErr, err := runPtahSplitStreams(ctx, apply)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, againErr))

	c.Assert(again, qt.Equals, "Schema is synced, no changes to be made.\n")
	_, err = conn.ExecContext(ctx, "INSERT INTO ptah_niladic_defaults (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var got string
	err = conn.QueryRowContext(ctx, "SELECT "+
		"CASE WHEN a > DATE '2000-01-01' AND b > TIMESTAMP '2000-01-01 00:00:00' AND c > DATE '2000-01-01' "+
		"AND d > TIMESTAMP '2000-01-01 00:00:00' AND e > TIMESTAMP '2000-01-01 00:00:00' AND h > DATE '2000-01-01' "+
		"THEN 'dated' ELSE 'undated' END "+
		"|| ',' || CASE WHEN f = USER AND k = USER THEN 'user' ELSE f || '/' || k END "+
		"|| ',' || CASE WHEN g = UID AND l = UID THEN 'uid' ELSE g || '/' || l END "+
		"|| ',' || CASE WHEN i = SESSIONTIMEZONE AND j = DBTIMEZONE THEN 'zones' ELSE i || '/' || j END "+
		"FROM ptah_niladic_defaults").Scan(&got)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "dated,user,uid,zones")
}
