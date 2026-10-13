//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// EXCLUDE parsing has to preserve expression boundaries through both catalog
// inspection and the server normalization probe. The second plan must keep the
// constraint itself, including its comment and catalog identity.
func TestExclusionQuotedDelimitersApplyAndReplanE2E(t *testing.T) {
	for _, expression := range []string{
		`replace(t, ')', '(')`,
		`replace(t, '(', '')`,
		`replace(t, $$)$$, '')`,
	} {
		t.Run(expression, func(t *testing.T) {
			c := qt.New(t)
			dev := postgresUnloggedDatabase(c)
			path := filepath.Join(c.TempDir(), "schema.sql")
			ddl := `CREATE TABLE slots (t text, CONSTRAINT slots_ex EXCLUDE USING btree (` + expression + ` WITH =) WHERE (t <> ')') DEFERRABLE INITIALLY DEFERRED); COMMENT ON CONSTRAINT slots_ex ON slots IS 'keep the exclusion';`
			c.Assert(os.WriteFile(path, []byte(ddl), 0o600), qt.IsNil)
			out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--schema-file", path, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			_, err = dev.conn.ExecContext(c.Context(), `COMMENT ON CONSTRAINT slots_ex ON slots IS 'keep the exclusion'`)
			c.Assert(err, qt.IsNil)
			var before, after, comment string
			c.Assert(dev.conn.QueryRowContext(c.Context(), `SELECT oid::text FROM pg_constraint WHERE conrelid = 'slots'::regclass AND conname = 'slots_ex'`).Scan(&before), qt.IsNil)
			out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", path)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schemas are synced")
			c.Assert(dev.conn.QueryRowContext(c.Context(), `SELECT oid::text, obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conrelid = 'slots'::regclass AND conname = 'slots_ex'`).Scan(&after, &comment), qt.IsNil)
			c.Assert(after, qt.Equals, before)
			c.Assert(comment, qt.Equals, "keep the exclusion")
		})
	}
}

func TestExclusionUnrepresentedCatalogClauseRefusesE2E(t *testing.T) {
	c := qt.New(t)
	dev := postgresUnloggedDatabase(c)
	_, err := dev.conn.ExecContext(c.Context(), `CREATE TABLE slots (r int, s text, CONSTRAINT slots_ex EXCLUDE USING btree (r WITH =) INCLUDE (s))`)
	c.Assert(err, qt.IsNil)
	out, err := runPtahNativeWithError("db", "read", "--db-url", dev.url)
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "read EXCLUDE constraint public.slots.slots_ex")
	c.Assert(out, qt.Contains, "unrepresented or unexpected clause INCLUDE")
}
