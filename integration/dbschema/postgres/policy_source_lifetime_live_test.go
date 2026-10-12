//go:build integration

package postgres_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
)

// TestPolicySQLSource_TableLifetime compares a script's catalog state with
// the schema Ptah reads and renders from it. PostgreSQL keeps FORCE while the
// table exists, and DROP TABLE ends that state and every policy comment.
func TestPolicySQLSource_TableLifetime(t *testing.T) {
	tests := []struct{ name, body string }{
		{name: "force and enable", body: `CREATE TABLE %[1]s.docs(id integer); ALTER TABLE %[1]s.docs FORCE ROW LEVEL SECURITY; ALTER TABLE %[1]s.docs ENABLE ROW LEVEL SECURITY; CREATE POLICY p ON %[1]s.docs USING (true); COMMENT ON POLICY p ON %[1]s.docs IS 'kept';`},
		{name: "drop ends force", body: `CREATE TABLE %[1]s.docs(id integer); ALTER TABLE %[1]s.docs FORCE ROW LEVEL SECURITY; DROP TABLE %[1]s.docs; CREATE TABLE %[1]s.docs(id integer); ALTER TABLE %[1]s.docs ENABLE ROW LEVEL SECURITY; CREATE POLICY p ON %[1]s.docs USING (true); COMMENT ON POLICY p ON %[1]s.docs IS 'new';`},
		{name: "drop ends policy comment", body: `CREATE TABLE %[1]s.docs(id integer); ALTER TABLE %[1]s.docs ENABLE ROW LEVEL SECURITY; ALTER TABLE %[1]s.docs FORCE ROW LEVEL SECURITY; CREATE POLICY p ON %[1]s.docs USING (true); COMMENT ON POLICY p ON %[1]s.docs IS 'old'; DROP TABLE %[1]s.docs; CREATE TABLE %[1]s.docs(id integer); ALTER TABLE %[1]s.docs ENABLE ROW LEVEL SECURITY; CREATE POLICY p ON %[1]s.docs USING (true);`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newPolicyRoleFixture(c, dbtarget.PostgreSQL)
			source := fmt.Sprintf(test.body, pgx.Identifier{fixture.schema}.Sanitize())
			for statement := range strings.SplitSeq(strings.TrimSuffix(source, ";"), ";") {
				_, err := fixture.conn.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil)
			}
			expected := readPolicySourceState(c, fixture)
			_, err := fixture.conn.ExecContext(c.Context(), "DROP TABLE "+pgx.Identifier{fixture.schema, "docs"}.Sanitize())
			c.Assert(err, qt.IsNil)

			runtime := builtintest.Runtime()
			desired, _, err := sqlschema.ReadWith(c.Context(), []byte(source), fixture.dialect, nil, sqlschema.Options{Owners: runtime.SQL(), Relations: runtime})
			c.Assert(err, qt.IsNil)
			statements, err := builtin.GetOrderedCreateStatements(&desired, fixture.dialect)
			c.Assert(err, qt.IsNil)
			for _, statement := range statements {
				_, err := fixture.conn.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
			}
			c.Assert(readPolicySourceState(c, fixture), qt.DeepEquals, expected)
		})
	}
}

type policySourceState struct {
	Enabled, Forced bool
	Comment         string
}

func readPolicySourceState(c *qt.C, fixture policyRoleFixture) policySourceState {
	c.Helper()
	var state policySourceState
	c.Assert(fixture.conn.QueryRowContext(c.Context(), `
SELECT c.relrowsecurity, c.relforcerowsecurity, coalesce(obj_description(p.oid, 'pg_policy'), '')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_policy p ON p.polrelid = c.oid
WHERE n.nspname = $1 AND c.relname = 'docs' AND p.polname = 'p'`, fixture.schema).Scan(&state.Enabled, &state.Forced, &state.Comment), qt.IsNil)
	return state
}
