//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestPostgresLiveMaterializedViewIndexApplies is the test that cannot be
// written offline.
//
// Both defects it guards are invisible to a fixture comparison and obvious to a
// server. `CREATE UNIQUE INDEX ... ON "MV"` -- the Go struct name -- is
// `relation "MV" does not exist`, and the same statement emitted before the
// view is `relation "mv" does not exist`. Each one is a plan that renders at
// exit 0 and fails on apply (stokaro/ptah#1725).
//
// The last step is why the fix matters beyond the broken statement: a UNIQUE
// index on a materialized view is the precondition REFRESH MATERIALIZED VIEW
// CONCURRENTLY checks, and until the index could name the view there was no way
// to declare it.
func TestPostgresLiveMaterializedViewIndexApplies(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_mvidx_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()
	_, err = conn.ExecContext(ctx, `SET search_path TO "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)

	description := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "MV", Name: "mv", Body: "SELECT id FROM t",
		}},
		Indexes: []goschema.Index{
			{StructName: "MV", Name: "mv_uk", Fields: []string{"id"}, Unique: true},
		},
	}

	// The renderer's own statements, in the renderer's own order. A wrong
	// target or a wrong order fails here rather than being corrected.
	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// The index exists on the VIEW, which is the assertion a fixture cannot make.
	var owner string
	err = conn.QueryRowContext(ctx, `
		SELECT c.relname
		FROM pg_index i
		JOIN pg_class ix ON ix.oid = i.indexrelid
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE ix.relname = 'mv_uk' AND n.nspname = $1`, schemaName).Scan(&owner)
	c.Assert(err, qt.IsNil)
	c.Assert(owner, qt.Equals, "mv")

	// And the precondition it exists for is now satisfied.
	_, err = conn.ExecContext(ctx, `REFRESH MATERIALIZED VIEW CONCURRENTLY "`+schemaName+`"."mv"`)
	c.Assert(err, qt.IsNil)
}
