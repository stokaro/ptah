package atlasschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/sqliteforeignkeys"
)

// A SQLite plan that rebuilds a table is wrapped in PRAGMA foreign_keys, and
// the apply suspends enforcement on its connection only when the plan starts
// with the disabling pragma and ends with the enabling one. Declared rows
// appended after the enabling pragma end the plan in its place, and the rebuild
// then runs with enforcement on: its DROP fails against a referencing row, or
// cascades into it (stokaro/ptah#3282). The rows belong inside the bracket.
//
// These read the plan. integration/atlasschema applies the same shape and reads
// the tables back.

// TestPlanApply_DeclaredRowsStayInsideASQLiteRebuildBracket is the apply plan.
func TestPlanApply_DeclaredRowsStayInsideASQLiteRebuildBracket(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "rebuild-bracket-apply.db")
	applyPlan(c, conn, rebuildBracketSchema("INTEGER", "CZ"))

	plan, err := atlasschema.PlanApply(context.Background(), conn, atlasschema.ApplyOptions{
		Desired: rebuildBracketSchema("TEXT", "AT", "CZ"),
	})

	c.Assert(err, qt.IsNil)
	statements := plan.Statements()
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `DROP TABLE "regions"`)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `INSERT INTO "countries"`)
	c.Assert(sqliteforeignkeys.Brackets(statements), qt.IsTrue, qt.Commentf("statements: %q", statements))
}

// TestPreparePlanFile_DeclaredRowsStayInsideASQLiteRebuildBracket is the saved
// plan, which records the rows beside the DDL on its own and is applied as the
// statements it records.
func TestPreparePlanFile_DeclaredRowsStayInsideASQLiteRebuildBracket(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "rebuild-bracket-plan.db")
	applyPlan(c, conn, rebuildBracketSchema("INTEGER", "CZ"))

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: rebuildBracketSchema("TEXT", "AT", "CZ"),
	})

	c.Assert(err, qt.IsNil)
	statements := make([]string, 0, len(plan.Statements))
	for _, statement := range plan.Statements {
		statements = append(statements, statement.SQL)
	}
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `DROP TABLE "regions"`)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `INSERT INTO "countries"`)
	c.Assert(sqliteforeignkeys.Brackets(statements), qt.IsTrue, qt.Commentf("statements: %q", statements))
}

// TestPlanApply_DeclaredRowsWithoutARebuildFollowTheSchema is the control: with
// no rebuild there is no bracket, and the rows come after every DDL statement.
func TestPlanApply_DeclaredRowsWithoutARebuildFollowTheSchema(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "rebuild-bracket-none.db")

	plan, err := atlasschema.PlanApply(context.Background(), conn, atlasschema.ApplyOptions{
		Desired: rebuildBracketSchema("INTEGER", "CZ"),
	})

	c.Assert(err, qt.IsNil)
	statements := plan.Statements()
	c.Assert(sqliteforeignkeys.Brackets(statements), qt.IsFalse)
	c.Assert(statements[len(statements)-1], qt.Contains, `INSERT INTO "countries"`)
}

// rebuildBracketSchema is regions, and countries referencing them. Changing the
// type of regions.rank is a change SQLite's ALTER TABLE cannot express, so a
// second plan with a different rankType rebuilds regions under the reference.
func rebuildBracketSchema(rankType string, countries ...string) *schemamodel.Database {
	rows := make([]schemamodel.ManagedRow, 0, len(countries))
	for _, code := range countries {
		rows = append(rows, countryRow(code, "Country "+code, "emea"))
	}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Country", Name: "countries"},
			{StructName: "Region", Name: "regions"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Region", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
			{StructName: "Region", FieldName: "Name", Name: "name", Type: "TEXT"},
			{StructName: "Region", FieldName: "Rank", Name: "rank", Type: rankType, Nullable: true},
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
			{StructName: "Country", Table: "countries", Keys: []string{"code"}, File: "countries.yaml", Rows: rows},
			{
				StructName: "Region",
				Table:      "regions",
				Keys:       []string{"code"},
				File:       "regions.yaml",
				Rows: []schemamodel.ManagedRow{{
					"code": {Tag: "str", Text: "emea"},
					"name": {Tag: "str", Text: "Europe, Middle East and Africa"},
				}},
			},
		},
	}
	schemamodel.Finalize(db)
	return db
}
