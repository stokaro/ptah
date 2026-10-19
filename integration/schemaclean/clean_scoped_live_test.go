//go:build integration

package schemaclean_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemaclean"
)

// scopedCleanupEngine is a PostgreSQL-family engine that runs scoped cleanup,
// with the statement that drops its throwaway database.
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

// TestScopedCleanupRetriesASelectedDependencyLive_HappyPath drops the refused
// view again after the dependent view is gone. The table the views read is
// not selected and stays. PostgreSQL and YugabyteDB return to a savepoint
// after the refusal. CockroachDB v26.3.2 commits each drop as it runs, so
// the refused drop leaves nothing to return to and is simply sent again.
func TestScopedCleanupRetriesASelectedDependencyLive_HappyPath(t *testing.T) {
	for _, engine := range []scopedCleanupEngine{scopedPostgreSQL, scopedCockroachDB, scopedYugabyteDB} {
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

// refusedFixture is a table nothing reads and an enum that a second table
// uses. The plan built on it selects the free table and the enum and leaves
// the table that uses the enum out. The selection reads no view, so nothing
// refuses it before the first drop: the free table is dropped, and the
// server refuses the enum every time it is asked.
var refusedFixture = []string{
	`CREATE TABLE free_t (id INT PRIMARY KEY)`,
	`CREATE TYPE mood AS ENUM ('calm')`,
	`CREATE TABLE keep_t (id INT PRIMARY KEY, m mood)`,
}

// refusedPlan selects the free table and the enum of [refusedFixture].
func refusedPlan(dialect string) schemaclean.Plan {
	return schemaclean.PlanFromObjects([]schemaclean.Object{
		{Type: schemaclean.ObjectTypeTable, Name: "free_t"},
		{Type: schemaclean.ObjectTypeEnum, Name: "mood"},
	}, dialect)
}

// TestScopedCleanupRefusedDropRollsBackLive_FailurePath ends the plan of
// [refusedFixture] on PostgreSQL, which rolls schema changes back as a unit:
// the transaction rolls back, so the free table dropped before the refusal
// is there again.
func TestScopedCleanupRefusedDropRollsBackLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	conn := newScopedCleanupConnection(c, ctx, scopedPostgreSQL)
	for _, statement := range refusedFixture {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	err := schemaclean.ApplyPlan(ctx, conn, refusedPlan(conn.Info().Dialect))

	c.Assert(err, qt.ErrorMatches, `(?s)drop schema object enum "mood": .*\(SQLSTATE 2BP01\).*`)
	c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*dropped before the failure.*`)
	c.Assert(scopedRelations(c, ctx, conn), qt.DeepEquals, []string{"free_t", "keep_t"})
}

// TestScopedCleanupRefusedDropOnYugabyteDBLive_FailurePath is the plan of
// [refusedFixture] on YugabyteDB, measured on 2026.1.2. The plan runs in one
// transaction, as on PostgreSQL, and the transaction rolls back. The free
// table stays dropped anyway: yb_ddl_transaction_block_enabled defaults to
// off, so the server does not roll back a DROP TABLE run in a transaction
// block. The error does not say so.
func TestScopedCleanupRefusedDropOnYugabyteDBLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	conn := newScopedCleanupConnection(c, ctx, scopedYugabyteDB)
	for _, statement := range refusedFixture {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	err := schemaclean.ApplyPlan(ctx, conn, refusedPlan(conn.Info().Dialect))

	c.Assert(err, qt.ErrorMatches, `(?s)drop schema object enum "mood": .*\(SQLSTATE 2BP01\).*`)
	c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*dropped before the failure.*`)
	c.Assert(scopedRelations(c, ctx, conn), qt.DeepEquals, []string{"keep_t"})
}

// TestScopedCleanupRefusedDropKeepsEarlierDropsLive_FailurePath is the plan
// of [refusedFixture] on CockroachDB, measured on v26.3.2, which commits each
// drop as it runs: the free table stays dropped, and the error says so.
func TestScopedCleanupRefusedDropKeepsEarlierDropsLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	conn := newScopedCleanupConnection(c, ctx, scopedCockroachDB)
	for _, statement := range refusedFixture {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	err := schemaclean.ApplyPlan(ctx, conn, refusedPlan(conn.Info().Dialect))

	c.Assert(err, qt.ErrorMatches, `(?s)drop schema object enum "mood": .*\(SQLSTATE 2BP01\).*`+
		`\nthe server commits each drop as it runs; dropped before the failure: table "free_t"`)
	c.Assert(scopedRelations(c, ctx, conn), qt.DeepEquals, []string{"keep_t"})
}

// TestScopedCleanupValidatesBeforeTheFirstDropLive_FailurePath refuses a
// scoped plan from the caller's validation, which strict compatibility uses
// to read the catalog again. The validation is handed an executor, its
// refusal ends the plan, and nothing is dropped. PostgreSQL and YugabyteDB
// validate inside the transaction; CockroachDB, which opens none, through
// the connection.
func TestScopedCleanupValidatesBeforeTheFirstDropLive_FailurePath(t *testing.T) {
	for _, engine := range []scopedCleanupEngine{scopedPostgreSQL, scopedCockroachDB, scopedYugabyteDB} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := c.Context()
			conn := newScopedCleanupConnection(c, ctx, engine)
			for _, statement := range retryFixture {
				c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("statement: %s", statement))
			}
			refusal := errors.New("schema changed after cleanup confirmation")
			var validatedThrough catalog.SchemaExecutor

			err := schemaclean.ApplyPlanWithOptions(ctx, conn, retryPlan(conn.Info().Dialect), schemaclean.ApplyPlanOptions{
				ValidateBeforeExecute: func(executor catalog.SchemaExecutor) error {
					validatedThrough = executor
					return refusal
				},
			})

			c.Assert(err, qt.ErrorIs, refusal)
			c.Assert(validatedThrough, qt.IsNotNil)
			c.Assert(scopedRelations(c, ctx, conn), qt.DeepEquals, []string{"a_base", "base_t", "z_child"})
		})
	}
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
