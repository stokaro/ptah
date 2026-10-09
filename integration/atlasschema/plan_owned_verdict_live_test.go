//go:build integration

package atlasschema_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/jackc/pgx/v5"

	"ptah.run/core/goschema"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/safety"
)

// A saved plan records the owner's verdict for the statements an owned
// operation renders, not only what their SQL text says. CockroachDB's
// row-level TTL is the owned operation here: `ALTER TABLE ... SET (...)` reads
// as additive, while its owner reports that the policy decides which rows a
// background job deletes. Live because the statement the verdict is attached
// to is the one the plan renders for a real read of the table.
func TestPlanLive_RecordsTheOwnersVerdictForOwnedOperations(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newCockroachPlanConnection(c, ctx,
		"CREATE TABLE public.sessions (id INT8 PRIMARY KEY, expires_at TIMESTAMPTZ)")
	declared := must.Must(goschema.ParseSource("sessions.go", "package entities\n\n"+
		"//ptah:schema:table name=\"sessions\" platform.cockroachdb.ttl_expiration_expression=\"expires_at\"\n"+
		"type Session struct {\n"+
		"\t//ptah:schema:field name=\"id\" type=\"INT8\" primary=\"true\"\n\tID int64\n"+
		"\t//ptah:schema:field name=\"expires_at\" type=\"TIMESTAMPTZ\"\n\tExpiresAt *time.Time\n"+
		"}\n"))

	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
		Desired: &declared,
		Runtime: must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 1, qt.Commentf("%s", plan.SQL()))
	statement := plan.Statements[0]
	c.Assert(strings.ToLower(statement.SQL), qt.Contains, "ttl_expiration_expression")
	c.Assert(statement.Severity, qt.Equals, safety.Warning)
	c.Assert(statement.Reason, qt.Equals,
		"row-level TTL decides which rows a background job deletes; restoring a prior policy cannot recover deleted rows")
	c.Assert(string(statement.Access), qt.Equals, "", qt.Commentf("row-level TTL makes no access claim"))
	// The control: the statement's text alone earns a lower verdict, so the
	// recorded one came from the owner.
	c.Assert(safety.AssessSQL(statement.SQL).Severity, qt.Equals, safety.Safe)
}

// newCockroachPlanConnection connects to a database of the test's own on the
// CockroachDB server, so the plan sees only the fixture's tables. The fixture
// runs through the raw driver, which takes the driver DSN; Ptah connects with
// the engine URL.
func newCockroachPlanConnection(c *qt.C, ctx context.Context, setup ...string) *dbschema.DatabaseConnection {
	c.Helper()
	adminDSN := dbtarget.DriverDSN(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", adminDSN)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })

	name := fmt.Sprintf("ptah_owned_verdict_%d", time.Now().UnixNano())
	ident := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+ident)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.WithoutCancel(ctx), "DROP DATABASE IF EXISTS "+ident+" CASCADE")
		c.Check(dropErr, qt.IsNil)
	})

	fixture, err := sql.Open("pgx", withDatabase(c, adminDSN, name))
	c.Assert(err, qt.IsNil)
	for _, statement := range setup {
		_, err := fixture.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	c.Assert(fixture.Close(), qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, withDatabase(c, dbtarget.URL(c, dbtarget.CockroachDB), name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// withDatabase returns address with its database path replaced by name.
func withDatabase(c *qt.C, address, name string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	parsed.Path, parsed.RawPath = "/"+name, ""
	return parsed.String()
}
