//go:build integration

package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/jackc/pgx/v5"

	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
)

// TestPolicyRoleAlterPreservesIdentityLive applies a role-only change without
// a transaction. The policy's catalog identity must survive every statement,
// which rules out dropping and recreating it even inside a statement batch.
// An independent connection observes each committed step and its comment.
func TestPolicyRoleAlterPreservesIdentityLive(t *testing.T) {
	tests := []struct {
		name      string
		applied   string
		declared  string
		comment   string
		wantSteps int
	}{
		{name: "PUBLIC to a role", applied: "", declared: "TO ROLE_A", comment: "tenant access", wantSteps: 1},
		{name: "a role to PUBLIC", applied: "TO ROLE_A", declared: "", comment: "tenant access", wantSteps: 1},
		{name: "add a role", applied: "TO ROLE_A", declared: "TO ROLE_A, ROLE_B", comment: "tenant access", wantSteps: 1},
		{name: "change a comment with the role", applied: "", declared: "TO ROLE_A", comment: "reader access", wantSteps: 2},
		{name: "clear a comment with the role", applied: "", declared: "TO ROLE_A", wantSteps: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newPolicyRoleFixture(c, dbtarget.PostgreSQL)
			f.apply(c, f.load(c, test.applied))
			_, err := f.conn.ExecContext(c.Context(), "COMMENT ON POLICY docs_tenant ON "+pgx.Identifier{f.schema, "docs"}.Sanitize()+" IS 'tenant access'")
			c.Assert(err, qt.IsNil)
			desired := f.load(c, test.declared)
			objects := must.Must(desired.FeatureObjects.All())
			c.Assert(objects, qt.HasLen, 1)
			objects[0].Value.(*pgpolicy.DesiredPolicy).Comment = test.comment
			desired.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
			statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), must.Must(builtin.New()), f.compare(c, desired), "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, test.wantSteps)
			c.Assert(statements[0], qt.Contains, "ALTER POLICY")
			observer, err := pgx.Connect(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(observer.Close(context.Background()), qt.IsNil) })
			const query = `SELECT p.oid::bigint, COALESCE(obj_description(p.oid, 'pg_policy'), '')
				FROM pg_policy p JOIN pg_class t ON t.oid = p.polrelid
				JOIN pg_namespace n ON n.oid = t.relnamespace
				WHERE n.nspname = $1 AND t.relname = 'docs' AND p.polname = 'docs_tenant'`
			var before, after int64
			var comment string
			c.Assert(observer.QueryRow(c.Context(), query, f.schema).Scan(&before, &comment), qt.IsNil)
			c.Assert(comment, qt.Equals, "tenant access")
			for _, statement := range statements {
				_, err := f.conn.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
				c.Assert(observer.QueryRow(c.Context(), query, f.schema).Scan(&after, &comment), qt.IsNil)
				c.Assert(after, qt.Equals, before)
			}
			c.Assert(comment, qt.Equals, test.comment)
			again, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), must.Must(builtin.New()), f.compare(c, desired), "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(again, qt.HasLen, 0)
		})
	}
}
