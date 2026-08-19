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
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestPostgresLiveDomainCheckConverges is the test stokaro/ptah#1717 needs and
// no fixture can supply.
//
// A domain's CHECK was never compared, on the sound reasoning that PostgreSQL
// stores a parsed expression and prints it back from the parse tree, so the
// declaration and the read-back are never the same string. What that reasoning
// cost was silence: a changed constraint produced no diff and no diagnostic,
// and `schema apply` reported a synced schema over a database still enforcing
// the old rule.
//
// Every step here is one a server has to answer:
//
//  1. A domain whose declaration the server rewrites beyond recognition --
//     `VALUE IN (...)` is stored as `VALUE = ANY (ARRAY[...])` -- compares as
//     unchanged. A comparison that skipped the normalization would replace this
//     constraint on every run and converge on nothing.
//  2. A changed CHECK is a difference, which is the defect itself.
//  3. The plan applies to a database whose TABLE USES THE DOMAIN. This is the
//     half that makes the comparison worth making: PostgreSQL refuses a
//     non-CASCADE DROP DOMAIN while a column has that type, so the old
//     drop-and-recreate route could not have run here at all.
//  4. The catalog enforces the new rule and no longer enforces the old one,
//     asserted by what the server accepts rather than by what it reports.
//  5. The comparison is empty again afterwards.
func TestPostgresLiveDomainCheckConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_domchk_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()
	_, err = conn.ExecContext(ctx, `SET search_path TO "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)

	// The schema is named on both sides on purpose. The reader qualifies what
	// it reads with the schema it read it from, and the connection's own schema
	// is public here, so a declaration that left it out would be a different
	// object entirely -- one addition and one removal, and none of the steps
	// below would be measuring anything.
	declared := func(check string) *goschema.Database {
		return &goschema.Database{
			Domains: []goschema.Domain{
				{Name: "grade", Schema: schemaName, BaseType: "text", Check: check},
			},
			Tables: []goschema.Table{{StructName: "S", Name: "s", Schema: schemaName}},
			Fields: []goschema.Field{
				{StructName: "S", Name: "id", Type: "INT", Primary: true},
				{StructName: "S", Name: "mark", Type: schemaName + ".grade"},
			},
		}
	}

	const originalCheck = "VALUE IN ('a','b')"
	const replacedCheck = "VALUE IN ('a','b','c')"

	statements, err := renderer.GetOrderedCreateStatements(declared(originalCheck), platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 1. The declaration the server rewrote still compares as unchanged.
	unchanged := compareLiveDomains(c, ctx, conn, declared(originalCheck), schemaName)
	c.Assert(unchanged.DomainsModified, qt.HasLen, 0)

	// 2. A changed CHECK is a difference. It used to be neither reported nor
	//    planned, which is the whole of the issue.
	changed := compareLiveDomains(c, ctx, conn, declared(replacedCheck), schemaName)
	c.Assert(changed.DomainsModified, qt.HasLen, 1)
	c.Assert(changed.DomainsModified[0].Changes["check"], qt.Not(qt.Equals), "")
	c.Assert(changed.DomainsModified[0].CurrentCheckConstraints, qt.HasLen, 1)

	// 3. The plan runs against a database whose column has this domain's type.
	//    A DROP DOMAIN would be refused here; these statements are not that.
	planStatements, err := planner.GenerateSchemaDiffSQLStatements(
		changed, declared(replacedCheck), platform.Postgres,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(planStatements, qt.Not(qt.HasLen), 0)
	for _, statement := range planStatements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 4. The server enforces the new rule and no longer enforces the old one.
	_, err = conn.ExecContext(ctx, `INSERT INTO "`+schemaName+`"."s" (id, mark) VALUES (1, 'c')`)
	c.Assert(err, qt.IsNil, qt.Commentf("the replaced constraint was never added"))
	_, err = conn.ExecContext(ctx, `INSERT INTO "`+schemaName+`"."s" (id, mark) VALUES (2, 'z')`)
	c.Assert(err, qt.IsNotNil, qt.Commentf("the domain enforces nothing at all"))

	// 5. And the comparison is empty again.
	converged := compareLiveDomains(c, ctx, conn, declared(replacedCheck), schemaName)
	c.Assert(converged.DomainsModified, qt.HasLen, 0)
	c.Assert(converged.DomainsAdded, qt.HasLen, 0)
	c.Assert(converged.DomainsRemoved, qt.HasLen, 0)
}

// compareLiveDomains reads the schema and compares it through the connection,
// which is the path that resolves a declaration into the server's own spelling.
func compareLiveDomains(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	declared *goschema.Database,
	schemaName string,
) *difftypes.SchemaDiff {
	c.Helper()
	current, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, current, nil)
	c.Assert(err, qt.IsNil)
	return diff
}
