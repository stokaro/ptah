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

// TestPostgresLiveCompositeAttributeConverges is the composite half of
// stokaro/ptah#1717, and the reason it is worth having is the same as the
// domain half: the path it replaces cannot run at all where it matters.
//
// A composite gaining a field used to be reconciled by dropping the type and
// creating it again, and PostgreSQL refuses to drop a composite a table column
// uses. ALTER TYPE ADD ATTRIBUTE and DROP ATTRIBUTE it accepts there --
// measured on 18.4 -- so the difference between the two paths is not style.
//
// What it does NOT accept there is a change to an attribute's type:
//
//	ALTER TYPE addr ALTER ATTRIBUTE street TYPE varchar(80) [CASCADE];
//	ERROR: cannot alter type "addr" because column "uses_addr.a" uses it
//
// which is why the comparator leaves the delta unset for that case and the
// rebuild keeps it.
func TestPostgresLiveCompositeAttributeConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_comp_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	declared := func(fields ...goschema.CompositeTypeField) *goschema.Database {
		return &goschema.Database{
			CompositeTypes: []goschema.CompositeType{
				{Name: "addr", Schema: schemaName, Fields: fields},
			},
			Tables: []goschema.Table{{StructName: "S", Name: "s", Schema: schemaName}},
			Fields: []goschema.Field{
				{StructName: "S", Name: "id", Type: "INT", Primary: true},
				{StructName: "S", Name: "home", Type: schemaName + ".addr"},
			},
		}
	}
	street := goschema.CompositeTypeField{Name: "street", Type: "text"}
	city := goschema.CompositeTypeField{Name: "city", Type: "text"}
	zip := goschema.CompositeTypeField{Name: "zip", Type: "text"}

	statements, err := renderer.GetOrderedCreateStatements(declared(street, city), platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 1. Unchanged is unchanged.
	c.Assert(compareLiveComposites(c, ctx, conn, declared(street, city), schemaName).CompositeTypesModified, qt.HasLen, 0)

	// 2. An appended field is one modification carrying a field-level delta.
	appended := compareLiveComposites(c, ctx, conn, declared(street, city, zip), schemaName)
	c.Assert(appended.CompositeTypesModified, qt.HasLen, 1)
	c.Assert(appended.CompositeTypesModified[0].AttributesAdded, qt.HasLen, 1)

	// 3. And the plan runs against a database whose column has this type. A
	//    DROP TYPE would be refused here; these statements are not that.
	applyLiveComposite(c, ctx, conn, appended, declared(street, city, zip))
	c.Assert(compareLiveComposites(c, ctx, conn, declared(street, city, zip), schemaName).CompositeTypesModified, qt.HasLen, 0)

	// 4. A removed field takes the same route, in the same conditions.
	removed := compareLiveComposites(c, ctx, conn, declared(street, zip), schemaName)
	c.Assert(removed.CompositeTypesModified, qt.HasLen, 1)
	c.Assert(removed.CompositeTypesModified[0].AttributesRemoved, qt.DeepEquals, []string{"city"})
	applyLiveComposite(c, ctx, conn, removed, declared(street, zip))
	c.Assert(compareLiveComposites(c, ctx, conn, declared(street, zip), schemaName).CompositeTypesModified, qt.HasLen, 0)

	// 5. The type is a working one: a row can be written through it with the
	//    shape the declaration now says it has.
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %q."s" (id, home) VALUES (1, ROW('main', '11111')::%q."addr")`, schemaName, schemaName))
	c.Assert(err, qt.IsNil)
}

// compareLiveComposites reads the schema and compares it through the connection.
func compareLiveComposites(
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

// applyLiveComposite plans the diff and runs every statement it produces.
func applyLiveComposite(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	diff *difftypes.SchemaDiff,
	declared *goschema.Database,
) {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, declared, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}
