//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
)

// Declared rows against a server that enforces the constraints they cross.
//
// The offline tests measure which statement the plan carries and in which
// order. What they cannot measure is the engine's answer: a foreign key refuses
// a child row before its parent with SQLSTATE 23503, and an ordering test that
// reads the statement text passes whatever the server would have said
// (stokaro/ptah#3252 shipped that way). These drive the same plans into
// PostgreSQL and read the tables back.

// TestDeclaredRowsFollowTheForeignKeyLive plans two reference tables joined by a
// foreign key into an empty database and applies the result.
//
// The child table is named so that it sorts first: a plan that groups its
// statements by table name writes "countries" before "regions" and the
// constraint the same plan created refuses it. The assertion is the engine's,
// not the statement list's.
func TestDeclaredRowsFollowTheForeignKeyLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "fk_order")
	defer done()

	applyDeclaredRows(c, ctx, conn, declaredRowsSchema(
		[]schemamodel.ManagedRow{declaredRegion("emea", "Europe, Middle East and Africa")},
		[]schemamodel.ManagedRow{declaredCountry("CZ", "Czechia", "emea")},
	))

	c.Assert(declaredRowCodes(c, ctx, conn, "regions"), qt.DeepEquals, []string{"emea"})
	c.Assert(declaredRowCodes(c, ctx, conn, "countries"), qt.DeepEquals, []string{"CZ"})
}

// TestDeclaredRowsRemoveChildrenFirstLive is the same constraint from the other
// side: a referenced row may only go once the rows that reference it have gone.
//
// Grouped by table name the DELETE of the parent runs first and the server
// refuses it, so this is the delete direction's engine-level control.
func TestDeclaredRowsRemoveChildrenFirstLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "fk_delete")
	defer done()

	applyDeclaredRows(c, ctx, conn, declaredRowsSchema(
		[]schemamodel.ManagedRow{
			declaredRegion("emea", "Europe, Middle East and Africa"),
			declaredRegion("amer", "Americas"),
		},
		[]schemamodel.ManagedRow{
			declaredCountry("CZ", "Czechia", "emea"),
			declaredCountry("US", "United States", "amer"),
		},
	))

	// The second declaration drops one country and the region it referenced.
	applyDeclaredRows(c, ctx, conn, declaredRowsSchema(
		[]schemamodel.ManagedRow{declaredRegion("emea", "Europe, Middle East and Africa")},
		[]schemamodel.ManagedRow{declaredCountry("CZ", "Czechia", "emea")},
	))

	c.Assert(declaredRowCodes(c, ctx, conn, "regions"), qt.DeepEquals, []string{"emea"})
	c.Assert(declaredRowCodes(c, ctx, conn, "countries"), qt.DeepEquals, []string{"CZ"})
}

// TestDeclaredRowsConvergeLive drives the promise the reference-data page makes:
// a repeated reconciliation over converged rows plans nothing.
//
// Convergence is decided by comparing a declared value with what the driver read
// back, so the engine is the only thing that can answer it. The column types are
// the ones a reference table carries; a type whose read-back cannot pair with
// its declaration makes this plan an UPDATE that runs on every apply forever
// (stokaro/ptah#3261 is the date family, which is why it is absent here).
func TestDeclaredRowsConvergeLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "converge")
	defer done()

	desired := declaredTypedSchema()
	applyDeclaredRows(c, ctx, conn, desired)

	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredPlanSQL(plan), qt.DeepEquals, []string(nil))
}

// TestDeclaredRowsUpdateOneRowLive is the control for the test above: with one
// declared value changed, the plan carries exactly the UPDATE that reaches it
// and the engine applies it.
//
// Without it, a comparison that never pairs anything would satisfy the
// convergence assertion by planning nothing for a change either.
func TestDeclaredRowsUpdateOneRowLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "update_one")
	defer done()

	applyDeclaredRows(c, ctx, conn, declaredTypedSchema())

	changed := declaredTypedSchema()
	changed.ManagedData[0].Rows[0]["label"] = schemamodel.ManagedValue{Tag: "str", Text: "Renamed"}
	schemamodel.Finalize(changed)

	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{Desired: changed})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredPlanSQL(plan), qt.HasLen, 1)

	applyDeclaredRows(c, ctx, conn, changed)
	c.Assert(declaredRowLabel(c, ctx, conn, "one"), qt.Equals, "Renamed")
}

// declaredRowsDatabase opens a scratch database of its own, so a run leaves the
// engine as it found it and two of these tests can share a server.
func declaredRowsDatabase(c *qt.C, ctx context.Context, purpose string) (*dbschema.DatabaseConnection, func()) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_rows_%s_%d", purpose, time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)

	conn, err := dbschema.ConnectToDatabase(ctx, replaceDatabaseName(c, adminURL, name))
	c.Assert(err, qt.IsNil)

	return conn, func() {
		conn.Close()
		dropPostgresFamilyE2EDatabase(c, adminDB, name)
		adminDB.Close()
	}
}

// applyDeclaredRows runs the plan the way `ptah schema apply` does: statement by
// statement, in the order the plan carries them. A statement the engine refuses
// fails the test at the statement, which is the measurement.
func applyDeclaredRows(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, desired *schemamodel.Database) {
	c.Helper()
	plan, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	for _, statement := range plan.Statements() {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

func declaredPlanSQL(plan atlasschema.PlanFile) []string {
	var statements []string
	for _, statement := range plan.Statements {
		statements = append(statements, statement.SQL)
	}
	return statements
}

func declaredRowCodes(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, table string) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `SELECT "code" FROM "`+table+`" ORDER BY "code"`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var codes []string
	for rows.Next() {
		code := ""
		c.Assert(rows.Scan(&code), qt.IsNil)
		codes = append(codes, code)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return codes
}

func declaredRowLabel(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, code string) string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `SELECT "label" FROM "settings" WHERE "code" = '`+code+`'`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	label := ""
	c.Assert(rows.Next(), qt.IsTrue)
	c.Assert(rows.Scan(&label), qt.IsNil)
	c.Assert(rows.Err(), qt.IsNil)
	return label
}

// declaredRowsSchema is the issue's pair: regions, and countries referencing
// them. The child's name sorts first.
func declaredRowsSchema(regions, countries []schemamodel.ManagedRow) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Country", Name: "countries"},
			{StructName: "Region", Name: "regions"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Region", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
			{StructName: "Region", FieldName: "Name", Name: "name", Type: "TEXT"},
			{StructName: "Country", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
			{StructName: "Country", FieldName: "Name", Name: "name", Type: "TEXT"},
			{
				StructName:     "Country",
				FieldName:      "RegionCode",
				Name:           "region_code",
				Type:           "TEXT",
				Foreign:        "regions(code)",
				ForeignKeyName: "fk_countries_region",
			},
		},
		ManagedData: []schemamodel.ManagedData{
			{
				StructName: "Country",
				Table:      "countries",
				Keys:       []string{"code"},
				File:       "countries.yaml",
				Rows:       append(make([]schemamodel.ManagedRow, 0, len(countries)), countries...),
			},
			{
				StructName: "Region",
				Table:      "regions",
				Keys:       []string{"code"},
				File:       "regions.yaml",
				Rows:       append(make([]schemamodel.ManagedRow, 0, len(regions)), regions...),
			},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// declaredTypedSchema is one table carrying the column types a reference table
// holds, each with a declared value.
func declaredTypedSchema() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Setting", Name: "settings"}},
		Fields: []schemamodel.Field{
			{StructName: "Setting", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
			{StructName: "Setting", FieldName: "Label", Name: "label", Type: "TEXT"},
			{StructName: "Setting", FieldName: "Weight", Name: "weight", Type: "INTEGER", Nullable: true},
			{StructName: "Setting", FieldName: "Enabled", Name: "enabled", Type: "BOOLEAN", Nullable: true},
			{StructName: "Setting", FieldName: "Amount", Name: "amount", Type: "NUMERIC(10,2)", Nullable: true},
			{StructName: "Setting", FieldName: "Meta", Name: "meta", Type: "JSONB", Nullable: true},
			{StructName: "Setting", FieldName: "Ref", Name: "ref", Type: "UUID", Nullable: true},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Setting",
			Table:      "settings",
			Keys:       []string{"code"},
			File:       "settings.yaml",
			Rows: []schemamodel.ManagedRow{{
				"code":    {Tag: "str", Text: "one"},
				"label":   {Tag: "str", Text: "Retention window"},
				"weight":  {Tag: "int", Text: "30"},
				"enabled": {Tag: "bool", Text: "true"},
				"amount":  {Tag: "str", Text: "12.50"},
				"meta":    {Tag: "str", Text: `{"a": 1}`},
				"ref":     {Tag: "str", Text: "11111111-2222-3333-4444-555555555555"},
			}},
		}},
	}
	schemamodel.Finalize(db)
	return db
}

func declaredRegion(code, name string) schemamodel.ManagedRow {
	return schemamodel.ManagedRow{
		"code": {Tag: "str", Text: code},
		"name": {Tag: "str", Text: name},
	}
}

func declaredCountry(code, name, region string) schemamodel.ManagedRow {
	return schemamodel.ManagedRow{
		"code":        {Tag: "str", Text: code},
		"name":        {Tag: "str", Text: name},
		"region_code": {Tag: "str", Text: region},
	}
}
