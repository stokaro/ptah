//go:build integration

package schemaclean_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemaclean"
)

// scopedCleanupEngine is a PostgreSQL-family engine whose cleanup runs a
// scoped plan in one transaction, with the statement that drops a throwaway
// database of it.
type scopedCleanupEngine struct {
	name   string
	engine dbtarget.Engine
	drop   string
}

// The engines whose descriptor states a schema-scoped cleanup. CockroachDB and
// YugabyteDB use the PostgreSQL writer, and their catalogs and transactions
// answer differently, so each is its own row.
var (
	scopedPostgreSQL  = scopedCleanupEngine{name: "PostgreSQL", engine: dbtarget.PostgreSQL, drop: "DROP DATABASE IF EXISTS %s WITH (FORCE)"}
	scopedCockroachDB = scopedCleanupEngine{name: "CockroachDB", engine: dbtarget.CockroachDB, drop: "DROP DATABASE IF EXISTS %s CASCADE"}
	scopedYugabyteDB  = scopedCleanupEngine{name: "YugabyteDB", engine: dbtarget.YugabyteDB, drop: "DROP DATABASE IF EXISTS %s"}
)

// TestScopedCleanupRefusesAnUnselectedDependent_PostgresFamilyLive selects a
// table and leaves out the view that reads it. The refusal names the view
// before anything is dropped, and the plan that selects both drops both.
func TestScopedCleanupRefusesAnUnselectedDependent_PostgresFamilyLive(t *testing.T) {
	for _, engine := range []scopedCleanupEngine{scopedPostgreSQL, scopedCockroachDB, scopedYugabyteDB} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := c.Context()
			conn := newScopedCleanupConnection(c, ctx, engine)
			c.Assert(conn.Writer().ExecuteSQL(ctx, `CREATE TABLE dep_t (id INT PRIMARY KEY)`), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(ctx, `CREATE VIEW dep_v AS SELECT id FROM dep_t`), qt.IsNil)
			dialect := conn.Info().Dialect

			err := schemaclean.ApplyPlan(ctx, conn, schemaclean.PlanFromObjects([]schemaclean.Object{
				{Type: schemaclean.ObjectTypeTable, Name: "dep_t"},
			}, dialect))

			c.Assert(err, qt.ErrorMatches, `scoped cleanup would leave dependents behind: view "dep_v" depends on dep_t\. `+
				`Add them to the selection so they are dropped together, or widen it to the whole schema`)
			c.Assert(scopedRelations(c, ctx, conn), qt.DeepEquals, []string{"dep_t", "dep_v"})

			c.Assert(schemaclean.ApplyPlan(ctx, conn, schemaclean.PlanFromObjects([]schemaclean.Object{
				{Type: schemaclean.ObjectTypeTable, Name: "dep_t"},
				{Type: schemaclean.ObjectTypeView, Name: "dep_v"},
			}, dialect)), qt.IsNil)
			c.Assert(scopedRelations(c, ctx, conn), qt.HasLen, 0)
		})
	}
}

// retryFixture is a table, a view reading it and a view reading that view.
// The plan built on it selects both views and carries no view depths, so the
// view the other reads is dropped first and refused.
var retryFixture = []string{
	`CREATE TABLE base_t (id INT PRIMARY KEY)`,
	`CREATE VIEW a_base AS SELECT id FROM base_t`,
	`CREATE VIEW z_child AS SELECT id FROM a_base`,
}

// retryPlan selects the two views of [retryFixture].
func retryPlan(dialect string) schemaclean.Plan {
	return schemaclean.PlanFromObjects([]schemaclean.Object{
		{Type: schemaclean.ObjectTypeView, Name: "a_base"},
		{Type: schemaclean.ObjectTypeView, Name: "z_child"},
	}, dialect)
}

// TestScopedCleanupRetriesASelectedDependencyLive_HappyPath drops the refused view
// under a savepoint, so the transaction survives the refusal, and the retry
// after the dependent view is gone drops it. The table the views read is not
// selected and stays.
func TestScopedCleanupRetriesASelectedDependencyLive_HappyPath(t *testing.T) {
	for _, engine := range []scopedCleanupEngine{scopedPostgreSQL, scopedYugabyteDB} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := c.Context()
			conn := newScopedCleanupConnection(c, ctx, engine)
			for _, statement := range retryFixture {
				c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement: %s", statement))
			}

			err := schemaclean.ApplyPlan(ctx, conn, retryPlan(conn.Info().Dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(scopedRelations(c, ctx, conn), qt.DeepEquals, []string{"base_t"})
		})
	}
}

// TestScopedCleanupRetriesASelectedDependencyLive_FailurePath is the same plan on
// CockroachDB, measured on v26.3.2: the refused DROP VIEW takes the savepoint
// with it, so ROLLBACK TO SAVEPOINT answers SQLSTATE 3B001, the retry cannot
// run, and the whole plan rolls back.
func TestScopedCleanupRetriesASelectedDependencyLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	conn := newScopedCleanupConnection(c, ctx, scopedCockroachDB)
	for _, statement := range retryFixture {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	err := schemaclean.ApplyPlan(ctx, conn, retryPlan(conn.Info().Dialect))

	c.Assert(err, qt.ErrorMatches, `(?s)drop schema object view "a_base": .*\(SQLSTATE 2BP01\).*`+
		`roll back scoped cleanup savepoint: .*savepoint "ptah_scoped_cleanup_object" does not exist \(SQLSTATE 3B001\).*`)
	c.Assert(scopedRelations(c, ctx, conn), qt.DeepEquals, []string{"a_base", "base_t", "z_child"})
}

// newScopedCleanupConnection creates a throwaway database on engine and
// connects to it, so the cleanup's default schema holds only what the test
// creates.
func newScopedCleanupConnection(c *qt.C, ctx context.Context, engine scopedCleanupEngine) *dbschema.DatabaseConnection {
	c.Helper()
	adminURL := dbtarget.URL(c, engine.engine)
	name := fmt.Sprintf("ptah_sc_scoped_%d", time.Now().UnixNano())

	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, withDatabase(c, adminURL, name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		_, dropErr := admin.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf(engine.drop, name))
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	return conn
}

// scopedRelations lists the tables and views of the connection's schema, by
// name.
func scopedRelations(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = current_schema()
		ORDER BY table_name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
