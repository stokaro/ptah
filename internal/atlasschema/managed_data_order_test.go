package atlasschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasschema"
)

// TestPreparePlanFile_DeclaredRowsFollowTheForeignKey is the shape
// stokaro/ptah#3252 reported: two tables joined by a foreign key, each with
// declared rows, planned into an empty database. The child's rows may not be
// written before the parent's, because the same plan creates the constraint
// before either INSERT runs.
//
// "countries" sorts before "regions", so a plan that groups statements by table
// name puts the child first and the database refuses it. The names are that way
// round on purpose: a test whose parent happens to sort first passes without the
// ordering it exists to check.
func TestPreparePlanFile_DeclaredRowsFollowTheForeignKey(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "fk-order.db")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: referenceSchema(
			[]schemamodel.ManagedRow{regionRow("emea", "Europe, Middle East and Africa", 1)},
			[]schemamodel.ManagedRow{countryRow("CZ", "Czechia", "emea")},
		),
	})

	c.Assert(err, qt.IsNil)
	sql := planSQL(plan)
	c.Assert(sql, qt.Contains, `INSERT INTO "regions"`)
	c.Assert(sql, qt.Contains, `INSERT INTO "countries"`)
	c.Assert(
		strings.Index(sql, `INSERT INTO "regions"`) < strings.Index(sql, `INSERT INTO "countries"`),
		qt.IsTrue,
		qt.Commentf("the referenced row has to be written first:\n%s", sql),
	)
}

// TestPreparePlanFile_DeclaredRowsAreAppliedAgainstTheDatabase drives the plan
// through the engine rather than reading it. The assertion above says the
// statements are in the right order; this one says the order is the one
// PostgreSQL and SQLite actually accept, which is the failure the issue
// reported (SQLSTATE 23503).
func TestPreparePlanFile_DeclaredRowsAreAppliedAgainstTheDatabase(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "fk-apply.db")

	applyPlan(c, conn, referenceSchema(
		[]schemamodel.ManagedRow{regionRow("emea", "Europe, Middle East and Africa", 1)},
		[]schemamodel.ManagedRow{countryRow("CZ", "Czechia", "emea")},
	))

	rows, err := conn.QueryContext(context.Background(), `SELECT "code" FROM "countries"`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	codes := make([]string, 0, 1)
	for rows.Next() {
		code := ""
		c.Assert(rows.Scan(&code), qt.IsNil)
		codes = append(codes, code)
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(codes, qt.DeepEquals, []string{"CZ"})
}

// TestPreparePlanFile_DeletedRowsFollowTheForeignKeyBackwards is the mirror the
// issue names: a row that is referenced may only be removed after the rows that
// reference it, so the delete pass reads the dependency order backwards.
func TestPreparePlanFile_DeletedRowsFollowTheForeignKeyBackwards(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "fk-delete.db")
	applyPlan(c, conn, referenceSchema(
		[]schemamodel.ManagedRow{
			regionRow("emea", "Europe, Middle East and Africa", 1),
			regionRow("amer", "Americas", 2),
		},
		[]schemamodel.ManagedRow{
			countryRow("CZ", "Czechia", "emea"),
			countryRow("US", "United States", "amer"),
		},
	))

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: referenceSchema(
			[]schemamodel.ManagedRow{regionRow("emea", "Europe, Middle East and Africa", 1)},
			[]schemamodel.ManagedRow{countryRow("CZ", "Czechia", "emea")},
		),
	})

	c.Assert(err, qt.IsNil)
	sql := planSQL(plan)
	c.Assert(sql, qt.Contains, `DELETE FROM "countries"`)
	c.Assert(sql, qt.Contains, `DELETE FROM "regions"`)
	c.Assert(
		strings.Index(sql, `DELETE FROM "countries"`) < strings.Index(sql, `DELETE FROM "regions"`),
		qt.IsTrue,
		qt.Commentf("the referencing row has to go first:\n%s", sql),
	)
}

// TestPreparePlanFile_AnInsertPrecedesEveryDelete separates the two passes. A
// plan that adds a row to the parent and removes one from the child has to
// write before it removes: grouped by table, the child's DELETE would sit in
// front of the parent's INSERT and a later row could have nothing to reference.
func TestPreparePlanFile_AnInsertPrecedesEveryDelete(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "fk-phases.db")
	applyPlan(c, conn, referenceSchema(
		[]schemamodel.ManagedRow{regionRow("emea", "Europe, Middle East and Africa", 1)},
		[]schemamodel.ManagedRow{countryRow("CZ", "Czechia", "emea")},
	))

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: referenceSchema(
			[]schemamodel.ManagedRow{
				regionRow("emea", "Europe, Middle East and Africa", 1),
				regionRow("amer", "Americas", 2),
			},
			[]schemamodel.ManagedRow{countryRow("US", "United States", "amer")},
		),
	})

	c.Assert(err, qt.IsNil)
	sql := planSQL(plan)
	c.Assert(sql, qt.Contains, `INSERT INTO "regions"`)
	c.Assert(sql, qt.Contains, `DELETE FROM "countries"`)
	c.Assert(
		strings.Index(sql, `INSERT INTO "regions"`) < strings.Index(sql, `DELETE FROM "countries"`),
		qt.IsTrue,
		qt.Commentf("every write runs before every removal:\n%s", sql),
	)
}

// referenceSchema is the issue's two tables: regions, and countries referencing
// them. Both declare rows, and the child's name sorts first.
func referenceSchema(regions, countries []schemamodel.ManagedRow) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Country", Name: "countries"},
			{StructName: "Region", Name: "regions"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Region", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
			{StructName: "Region", FieldName: "Name", Name: "name", Type: "TEXT"},
			{StructName: "Region", FieldName: "Rank", Name: "rank", Type: "INTEGER", Nullable: true},
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

func countryRow(code, name, region string) schemamodel.ManagedRow {
	return schemamodel.ManagedRow{
		"code":        {Tag: "str", Text: code},
		"name":        {Tag: "str", Text: name},
		"region_code": {Tag: "str", Text: region},
	}
}
