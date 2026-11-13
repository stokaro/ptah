//go:build integration

package atlas_test

import (
	"bytes"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
)

// The PostgreSQL writer writes a role password from its PTAH_SECRET_ variable
// into the statement it sends, and takes it out of the error the server
// answers with. `SET PASSWORD` is MySQL's spelling, so PostgreSQL refuses the
// statement with a syntax error that quotes the literal, password included.
//
// Outside a transaction the revision table keeps the failure, and its `error`
// column records the server's message without Ptah's context. The server's own
// error is below the redaction in the chain, so the column must record the
// redaction's text rather than the error it rewrote.
func TestMigrateApplyRolePasswordErrorRedactedInRevisionLive(t *testing.T) {
	const password = "hunter2zz"
	t.Setenv("PTAH_SECRET_PASSWORD_APP", password)
	c := qt.New(t)
	dir := writeHashedLayoutDirectory(c, "atlas", map[string]string{
		"20240901000000_role.sql": "ALTER ROLE app SET PASSWORD $PTAH_SECRET_PASSWORD_APP;\n",
	})
	dbURL := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), nil)
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply", "--dir", "file://" + dir, "--url", dbURL, "--tx-mode", "none"})

	err = cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `(?s).*syntax error at or near "E'\[secret\]'".*`, qt.Commentf("output:\n%s", out.String()))
	c.Assert(err.Error(), qt.Not(qt.Contains), password)
	c.Assert(out.String(), qt.Not(qt.Contains), password)
	applied, failure := latestRevision(c, db, "20240901000000")
	c.Assert(applied, qt.Equals, 0)
	c.Assert(failure, qt.Not(qt.Contains), password)
	c.Assert(failure, qt.Equals, `ERROR: syntax error at or near "E'[secret]'" (SQLSTATE 42601)`)
}
