//go:build integration

package clickhouse_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
)

// A nullable ClickHouse column made NOT NULL, planned against a live server and
// applied the way `ptah schema apply` applies a plan (stokaro/ptah#4020).
//
// Only the engine can say whether the planned statement runs. 26.3 and 26.9
// refuse MODIFY COLUMN from Nullable(Int32) to Int32 unless it names a DEFAULT,
// and the server CI runs is 26.9, so a plan without the DEFAULT fails here at
// the ALTER. The capability set comes from the connection, as it does for the
// command.

// setNotNullDatabase creates a database for one test and drops it afterwards,
// and returns a connection to it. The server is shared, and a plan compares the
// whole database with the declaration, so a table another test left behind
// would be planned for removal.
func setNotNullDatabase(c *qt.C, ctx context.Context) *dbschema.DatabaseConnection {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.ClickHouse)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })

	name := fmt.Sprintf("ptah_set_not_null_%d_%d", os.Getpid(), time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" SYNC")
		c.Check(dropErr, qt.IsNil)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	conn, err := dbschema.ConnectToDatabase(ctx, parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// nullableTableWithANullRow creates asn with a nullable n, and a row whose n is
// NULL beside one whose n is not.
func nullableTableWithANullRow(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Helper()
	for _, statement := range []string{
		"CREATE TABLE asn (id Int32, n Nullable(Int32)) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO asn VALUES (1, NULL), (2, 5)",
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// declaredAsn is the declaration of asn, with n as given.
func declaredAsn(n schemamodel.Field) *schemamodel.Database {
	n.StructName, n.Name = "Asn", "n"
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Asn",
			Name:       "asn",
			Overrides:  map[string]map[string]string{platform.ClickHouse: {"engine": "MergeTree", "order_by": "id"}},
		}},
		Fields: []schemamodel.Field{{StructName: "Asn", Name: "id", Type: "INTEGER", Primary: true}, n},
	}
}

// asnState reads n's type and every row back, as "id=n" with NULL spelled out,
// so one assertion covers the column and the values it holds.
func asnState(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	var columnType string
	c.Assert(conn.QueryRowContext(ctx,
		"SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'asn' AND name = 'n'",
	).Scan(&columnType), qt.IsNil)

	rows, err := conn.QueryContext(ctx, "SELECT concat(toString(id), '=', ifNull(toString(n), 'NULL')) FROM asn ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	state := []string{"n " + columnType}
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		state = append(state, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return state
}

// With a declared default the plan names it, the server accepts the change,
// and the NULL row takes the default. A second plan finds nothing to do, so
// the default the server stored reads back as the one declared.
func TestSetNotNullWithADefault_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := setNotNullDatabase(c, ctx)
	nullableTableWithANullRow(c, ctx, conn)
	desired := declaredAsn(schemamodel.Field{Type: "INTEGER", Default: "7"})

	plan, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements(), qt.DeepEquals, []string{"ALTER TABLE asn MODIFY COLUMN n Int32 DEFAULT '7'"})
	for _, statement := range plan.Statements() {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	c.Assert(asnState(c, ctx, conn), qt.DeepEquals, []string{"n Int32", "1=7", "2=5"})
	again, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	c.Assert(again.Statements(), qt.HasLen, 0)
}

// Without a declared default there is no statement this server takes, so the
// plan is refused before anything runs, and the column and its NULL row stay as
// they were. The statement without a DEFAULT, `MODIFY COLUMN n Int32`, fails at
// the ALTER on this server.
func TestSetNotNullWithoutADefault_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := setNotNullDatabase(c, ctx)
	nullableTableWithANullRow(c, ctx, conn)

	plan, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{
		Desired: declaredAsn(schemamodel.Field{Type: "INTEGER"}),
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*column asn\.n cannot be made NOT NULL here: .*`)
	c.Assert(plan.Statements(), qt.HasLen, 0)
	c.Assert(asnState(c, ctx, conn), qt.DeepEquals, []string{"n Nullable(Int32)", "1=NULL", "2=5"})
}
