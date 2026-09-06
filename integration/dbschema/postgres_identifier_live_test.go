//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// TestPostgresLiveConnection_DefaultSchemaIsTheSelectedSchema pins that a
// PostgreSQL connection reports the schema it resolved to as the schema that
// owns unqualified objects (stokaro/ptah#1991).
//
// The dialect rule answers the constant "public", which is right until a URL
// selects another schema. The catalog reports an object with no schema when it
// is in the schema the read was scoped to, and a desired state written as HCL
// or Go annotations carries that schema explicitly -- so keyed through a
// "public" default, `widget` and `app.widget` are two tables.
//
// It is the PostgreSQL shape of stokaro/ptah#1244, which pinned the same field
// for MySQL. Oracle and SQL Server pin it from the same place.
//
// Live-only: the value comes from the server's own `current_schema()`, and
// asserting it against anything but a real connection would only restate the
// test's own fixture.
func TestPostgresLiveConnection_DefaultSchemaIsTheSelectedSchema(t *testing.T) {
	c := qt.New(t)
	databaseURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	selected := fmt.Sprintf("sel_%d", time.Now().UnixNano())

	setup, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(setup) })
	c.Assert(setup.SchemaWriter().ExecuteSQL(c.Context(),
		fmt.Sprintf("CREATE SCHEMA %s", selected)), qt.IsNil)
	c.Cleanup(func() {
		c.Check(setup.SchemaWriter().ExecuteSQL(context.WithoutCancel(c.Context()),
			fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", selected)), qt.IsNil)
	})

	conn, err := dbschema.ConnectToDatabase(c.Context(), withSelectedSchema(c, databaseURL, selected))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	info := conn.Info()

	c.Assert(info.Schema, qt.Equals, selected)
	// The pairing is the invariant, not either value alone: comparison resolves
	// an absent schema through DefaultSchema, and a connection whose two fields
	// disagree keys its own tables against a schema it is not working in.
	c.Assert(info.IdentifierSemantics.DefaultSchema, qt.Equals, info.Schema)
}

// TestPostgresLiveConnection_ASelectedSchemaComparesEqualToItself is the
// consequence, and the one an operator meets.
//
// A description of the database, compared against the database it came from,
// must plan nothing. Through a URL that selected a schema it planned CREATE
// TABLE for the table that was already there and DROP TABLE for the same table
// under its unqualified name -- measured on PostgreSQL 17 as
// `CREATE TABLE "app"."widget"` beside `DROP TABLE IF EXISTS "widget" CASCADE`,
// failing on 42P07 before the drop ran (stokaro/ptah#1991).
//
// Asserting the field alone would not have caught it: the value is used by the
// comparator, and only a comparison shows what it decides.
func TestPostgresLiveConnection_ASelectedSchemaComparesEqualToItself(t *testing.T) {
	c := qt.New(t)
	databaseURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	selected := fmt.Sprintf("selcmp_%d", time.Now().UnixNano())

	setup, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(setup) })
	c.Assert(setup.SchemaWriter().ExecuteSQL(c.Context(),
		fmt.Sprintf("CREATE SCHEMA %s", selected)), qt.IsNil)
	c.Cleanup(func() {
		c.Check(setup.SchemaWriter().ExecuteSQL(context.WithoutCancel(c.Context()),
			fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", selected)), qt.IsNil)
	})
	c.Assert(setup.SchemaWriter().ExecuteSQL(c.Context(), fmt.Sprintf(
		"CREATE TABLE %s.widget (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL)", selected)), qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(c.Context(), withSelectedSchema(c, databaseURL, selected))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{selected})
	c.Assert(err, qt.IsNil)

	declared := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: selected}},
		Tables:  []schemamodel.Table{{StructName: "W", Name: "widget", Schema: selected}},
		Fields: []schemamodel.Field{
			{StructName: "W", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "W", Name: "tenant", Type: "TEXT"},
		},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(declared, live, conn.Info(), nil)

	c.Assert(err, qt.IsNil)
	// Non-vacuity: the table really is on both sides, so two empty lists cannot
	// pass as agreement.
	c.Assert(live.Tables, qt.HasLen, 1)
	c.Assert(diff.TablesAdded, qt.HasLen, 0, qt.Commentf("%+v", diff.TablesAdded))
	c.Assert(diff.TablesRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.TablesRemoved))
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// withSelectedSchema puts the selection an operator writes on the URL.
func withSelectedSchema(c *qt.C, address, schema string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
