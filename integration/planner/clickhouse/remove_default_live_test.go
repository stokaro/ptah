//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
)

// A ClickHouse column whose default goes away, planned against a live server
// and applied the way `ptah schema apply` applies a plan (stokaro/ptah#4030).
//
// A MODIFY COLUMN that names only a type keeps the default the column had, so
// the migration reported success and the default stayed. Only the server's
// system.columns can say whether it went, and only a row inserted without a
// value can say what the column does without it.

// tableWithADefault creates asn with n Int32 DEFAULT 5, one row that states n,
// and one row that takes the default.
func tableWithADefault(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Helper()
	for _, statement := range []string{
		"CREATE TABLE asn (id Int32, n Int32 DEFAULT 5) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO asn VALUES (1, 1)",
		"INSERT INTO asn (id) VALUES (2)",
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// asnDefault reads n's default as system.columns reports it, kind and
// expression, empty when the column has none.
func asnDefault(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var kind, expression string
	c.Assert(conn.QueryRowContext(ctx,
		"SELECT default_kind, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 'asn' AND name = 'n'",
	).Scan(&kind, &expression), qt.IsNil)
	return kind + " " + expression
}

// applyPlan applies every planned statement in order.
func applyPlan(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// The default goes with REMOVE DEFAULT. Afterwards system.columns reports
// none, a row inserted without n takes the type's zero rather than 5, and a
// second plan finds nothing to do.
func TestRemoveDefault_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := setNotNullDatabase(c, ctx)
	tableWithADefault(c, ctx, conn)
	desired := declaredAsn(schemamodel.Field{Type: "INTEGER"})

	plan, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired, Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements(), qt.DeepEquals, []string{
		"ALTER TABLE asn MODIFY COLUMN n REMOVE DEFAULT",
		"ALTER TABLE asn MODIFY COLUMN n Int32",
	})
	applyPlan(c, ctx, conn, plan.Statements())

	c.Assert(asnDefault(c, ctx, conn), qt.Equals, " ")
	_, err = conn.ExecContext(ctx, "INSERT INTO asn (id) VALUES (3)")
	c.Assert(err, qt.IsNil)
	c.Assert(asnState(c, ctx, conn), qt.DeepEquals, []string{"n Int32", "1=1", "2=5", "3=0"})
	again, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired, Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	c.Assert(again.Statements(), qt.HasLen, 0)
}

// The default goes before the type changes, in a statement of its own. In one
// ALTER, 24.10 keeps the default without an error, and a type the old default
// cannot take is refused while the default is still there.
func TestRemoveDefaultWithATypeChange_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := setNotNullDatabase(c, ctx)
	tableWithADefault(c, ctx, conn)
	desired := declaredAsn(schemamodel.Field{Type: "BIGINT"})

	plan, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired, Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements(), qt.DeepEquals, []string{
		"ALTER TABLE asn MODIFY COLUMN n REMOVE DEFAULT",
		"ALTER TABLE asn MODIFY COLUMN n Int64",
	})
	applyPlan(c, ctx, conn, plan.Statements())

	c.Assert(asnDefault(c, ctx, conn), qt.Equals, " ")
	c.Assert(asnState(c, ctx, conn), qt.DeepEquals, []string{"n Int64", "1=1", "2=5"})
	again, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired, Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	c.Assert(again.Statements(), qt.HasLen, 0)
}
