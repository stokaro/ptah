//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
)

// TestPostgresLiveExactRemovalReleasesGeneratedColumn drives the order against
// PostgreSQL's real dependency catalog. A generated column calls a function
// whose parameter is a view row type. The plan must drop the column and
// function before the view, leaving the table in place.
func TestPostgresLiveExactRemovalReleasesGeneratedColumn(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schema := fmt.Sprintf("ptah_exact_%d", time.Now().UnixNano())
	quoted := `"` + schema + `"`
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA `+quoted)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`)
	}()
	for _, statement := range []string{
		`CREATE VIEW ` + quoted + `."v" AS SELECT 1 AS id`,
		`CREATE FUNCTION ` + quoted + `."f"(r ` + quoted + `."v") RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT 1$$`,
		`CREATE TABLE ` + quoted + `."t"(id integer, g integer GENERATED ALWAYS AS (` + quoted + `."f"(NULL::` + quoted + `."v")) STORED)`,
	} {
		_, err = conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	diff := &difftypes.SchemaDiff{
		ViewsRemoved: difftypes.ViewChanges{{Name: schema + ".v", Body: "SELECT 1 AS id"}},
		FunctionsRemoved: difftypes.FunctionChanges{{
			Function: schemamodel.Function{Name: schema + ".f", Parameters: "r " + schema + ".v",
				Returns: "integer", Language: "sql", Body: "SELECT 1"},
			Signature: new("r " + schema + ".v"),
		}},
		TablesModified: []difftypes.TableDiff{{TableName: schema + ".t", ColumnsRemoved: difftypes.ColumnChanges{{
			Name: "g", GeneratedExpression: schema + ".f(NULL::" + schema + ".v)", GeneratedKind: "STORED",
		}}}},
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, must.Must(builtin.New()), diff, platform.Postgres,
		planext.Options{Capabilities: capability.Postgres18(), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "CASCADE")
	for _, statement := range statements {
		_, err = conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	var count int
	err = conn.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema = $1 AND table_name = 't'`, schema).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)
}

// TestPostgresLiveDropsDomainBeforeItsCheckRoutine confirms the type window
// lands ahead of a routine the removed domain still calls.
func TestPostgresLiveDropsDomainBeforeItsCheckRoutine(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schema := fmt.Sprintf("ptah_domain_%d", time.Now().UnixNano())
	quoted := `"` + schema + `"`
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA `+quoted)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`)
	}()
	for _, statement := range []string{
		`CREATE FUNCTION ` + quoted + `."f"(x integer) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$SELECT x >= 0$$`,
		`CREATE DOMAIN ` + quoted + `."qty" AS integer CHECK (` + quoted + `."f"(VALUE))`,
	} {
		_, err = conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{{
			Subject: pgtype.Ref(pgtype.DomainKind, schema, "qty"),
			Value: &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "integer",
				Check: schema + ".f(VALUE)", Checks: []pgtype.DomainCheck{{Name: "qty_check", Expression: schema + ".f(VALUE)"}}}},
		}},
		FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: schema + ".f", Parameters: "x integer", Returns: "boolean"}, Signature: new("x integer")}},
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, must.Must(builtin.New()), diff, platform.Postgres,
		planext.Options{Capabilities: capability.Postgres18(), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err = conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	var count int
	err = conn.QueryRowContext(ctx, `SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1 AND p.proname = 'f'`, schema).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}
