//go:build integration

package schemaclean_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemaclean"
)

// oracleCleanupFixture creates one object of every kind the Oracle writer
// drops: a table, a view, a materialized view, a sequence, a synonym, a
// trigger, an object type and domains, two of them chosen from by a flexible
// domain that sorts after them, and a Ptah revision table, which the reader
// hides by name.
var oracleCleanupFixture = []string{
	"CREATE DOMAIN ocl_m1 AS NUMBER",
	"CREATE DOMAIN ocl_m2 AS NUMBER CHECK (VALUE > 0)",
	"CREATE FLEXIBLE DOMAIN ocl_z_flex (a) CHOOSE DOMAIN USING (k NUMBER) FROM (CASE k WHEN 1 THEN ocl_m1(a) WHEN 2 THEN ocl_m2(a) END)",
	"CREATE OR REPLACE TYPE ocl_point AS OBJECT (x NUMBER)",
	"CREATE TABLE ocl_users (id NUMBER PRIMARY KEY, at ocl_point, a NUMBER, k NUMBER, DOMAIN ocl_z_flex(a) USING (k))",
	"CREATE VIEW ocl_users_v AS SELECT id FROM ocl_users",
	"CREATE MATERIALIZED VIEW ocl_users_mv AS SELECT id FROM ocl_users",
	"CREATE SEQUENCE ocl_seq",
	"CREATE SYNONYM ocl_syn FOR ocl_users",
	"CREATE OR REPLACE TRIGGER ocl_trg BEFORE INSERT ON ocl_users FOR EACH ROW BEGIN NULL; END;",
	`CREATE TABLE "schema_migrations" (version NUMBER PRIMARY KEY)`,
}

// TestInspectNamesEveryObjectApplyDestroys_OracleLive compares the plan
// schemaclean.Inspect produces against the objects that really disappear,
// which is what `ptah db drop-all` reports and then does.
//
// The plan named tables alone on Oracle, and only the tables the reader
// reports: measured on 23.26.3.0.0, the writer dropped every kind in the
// fixture while `ptah db drop-all` said it dropped one object. Ptah's revision
// table is in the census because the reader hides it and the writer drops it.
func TestInspectNamesEveryObjectApplyDestroys_OracleLive(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	conn := newOracleCleanupLiveConnection(c, ctx)
	applyOracleCleanupFixture(c, ctx, conn)

	before := oracleCleanupCensus(c, ctx, conn)
	plan, err := schemaclean.Inspect(ctx, conn)
	c.Assert(err, qt.IsNil)
	planned := plannedObjectKeys(plan)

	c.Assert(schemaclean.Apply(ctx, conn), qt.IsNil)
	after := oracleCleanupCensus(c, ctx, conn)

	c.Assert(destroyedKeys(before, after), qt.DeepEquals, planned)
	c.Assert(planned, qt.DeepEquals, []string{
		"domain|OCL_M1", "domain|OCL_M2", "domain|OCL_Z_FLEX",
		"materialized_view|OCL_USERS_MV",
		"sequence|OCL_SEQ",
		"synonym|OCL_SYN",
		"table|OCL_USERS", "table|schema_migrations",
		"trigger|OCL_TRG",
		"type|OCL_POINT",
		"view|OCL_USERS_V",
	})
}

// TestApplyPlanDropsInTheWritersOrder_OracleLive executes the plan itself, as
// a scoped `schema clean` does, rather than handing the database to the
// writer. The flexible domain sorts after the domains it chooses from, and
// dropping one of those first answers ORA-11502, so the plan has to keep the
// writer's order.
func TestApplyPlanDropsInTheWritersOrder_OracleLive(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	conn := newOracleCleanupLiveConnection(c, ctx)
	applyOracleCleanupFixture(c, ctx, conn)

	plan, err := schemaclean.Inspect(ctx, conn)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Changes, qt.Not(qt.HasLen), 0)

	c.Assert(schemaclean.ApplyPlan(ctx, conn, plan), qt.IsNil)
	c.Assert(oracleCleanupCensus(c, ctx, conn), qt.HasLen, 0)
}

// oracleCleanupCensus enumerates, straight from USER_OBJECTS, every object of
// a kind the writer drops, in the vocabulary of schemaclean.Object.Type. It
// reads the catalog rather than the writer's listing, so the comparison is a
// measurement rather than a restatement.
//
// A materialized view's container table is left out, as the plan's contract
// leaves out every object that disappears with a listed one: it has the
// view's name and goes with the view.
func oracleCleanupCensus(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `
SELECT LOWER(REPLACE(o.object_type, ' ', '_')) || '|' || o.object_name
FROM user_objects o
WHERE o.object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SEQUENCE', 'SYNONYM', 'TRIGGER', 'TYPE', 'DOMAIN')
  AND o.generated = 'N'
  AND o.object_name NOT LIKE 'BIN$%'
  AND NOT (o.object_type = 'TABLE' AND o.object_name IN (SELECT m.mview_name FROM user_mviews m))
ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var census []string
	for rows.Next() {
		var key string
		c.Assert(rows.Scan(&key), qt.IsNil)
		census = append(census, key)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(census)
	return census
}

// applyOracleCleanupFixture empties the account and creates the fixture. The
// cleanup drops every object the account owns, so the test runs on what it
// created alone.
func applyOracleCleanupFixture(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Helper()
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	for _, statement := range oracleCleanupFixture {
		c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	c.Cleanup(func() {
		c.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
	})
}

func newOracleCleanupLiveConnection(c *qt.C, ctx context.Context) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}
