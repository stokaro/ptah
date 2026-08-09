package sqlite_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// identityRebuildSchema declares one `notes` table whose schema is spelled as
// given, carrying the column the rebuild must not copy out of the old table.
func identityRebuildSchema(tableSchema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Note", Name: "notes", Schema: tableSchema}},
		Fields: []goschema.Field{
			{StructName: "Note", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Note", Name: "body", Type: "TEXT"},
			{StructName: "Note", Name: "author", Type: "TEXT", Nullable: true},
		},
	}
}

// TestRebuildCarriesAddedColumnsAcrossSchemaSpellings is the SQLite half of the
// same defect, and it is the stokaro/ptah#930 corruption shape.
//
// A table whose constraint change is recorded at schema level reaches the
// rebuild through ConstraintsAddedWithTables, which carries the table name the
// DECLARATION spells. The columns it gains are on TablesModified, which carries
// the name the comparator spells. Matching the two as raw text answered "no
// added columns" whenever the two spelled the schema differently, and the
// rebuild then copied the new column out of the old table -- where SQLite reads
// the unknown double-quoted identifier as a STRING LITERAL and writes the
// column's own name into every row, exiting 0.
func TestRebuildCarriesAddedColumnsAcrossSchemaSpellings(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name             string
		tableSchema      string
		diffTableName    string
		constraintTable  string
		wantSelectSuffix string
	}{
		{
			// Control: both sides already agree.
			name:             "both sides spell the table the same way",
			tableSchema:      "",
			diffTableName:    "notes",
			constraintTable:  "notes",
			wantSelectSuffix: `SELECT "id", "body" FROM "notes"`,
		},
		{
			name:             "the diff qualifies main and the constraint does not",
			tableSchema:      "",
			diffTableName:    "main.notes",
			constraintTable:  "notes",
			wantSelectSuffix: `SELECT "id", "body" FROM "notes"`,
		},
		{
			name:             "the constraint qualifies main and the diff does not",
			tableSchema:      "main",
			diffTableName:    "notes",
			constraintTable:  "main.notes",
			wantSelectSuffix: `SELECT "id", "body" FROM "main"."notes"`,
		},
		{
			// SQLite folds ASCII, so a case difference alone is the same split.
			name:             "the two sides differ only in case",
			tableSchema:      "",
			diffTableName:    "Notes",
			constraintTable:  "notes",
			wantSelectSuffix: `SELECT "id", "body" FROM "notes"`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			diff := &types.SchemaDiff{
				TablesModified: []types.TableDiff{{
					TableName:    test.diffTableName,
					ColumnsAdded: []string{"author"},
				}},
				ConstraintsAdded: []string{"ck_notes_body"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name:            "ck_notes_body",
					TableName:       test.constraintTable,
					Type:            "CHECK",
					CheckExpression: "length(body) > 0",
				}},
			}
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				diff,
				identityRebuildSchema(test.tableSchema),
				"sqlite",
			)
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, test.wantSelectSuffix)
			c.Assert(plan, qt.Not(qt.Contains), `"author"`+" FROM")
		})
	}
}

// TestConstraintOnACreatedTableIsNotAlsoRebuilt covers the other half of the
// same function: a table the diff CREATES already carries its constraints
// inline, so it must not also be rebuilt. Asking that as raw string membership
// answered "not being created" across a spelling difference and the plan both
// created and rebuilt one table.
func TestConstraintOnACreatedTableIsNotAlsoRebuilt(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name            string
		addedTableName  string
		constraintTable string
	}{
		{
			name:            "both sides spell the table the same way",
			addedTableName:  "notes",
			constraintTable: "notes",
		},
		{
			name:            "the addition qualifies main and the constraint does not",
			addedTableName:  "main.notes",
			constraintTable: "notes",
		},
		{
			name:            "the two sides differ only in case",
			addedTableName:  "Notes",
			constraintTable: "notes",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			diff := &types.SchemaDiff{
				TablesAdded:      []string{test.addedTableName},
				ConstraintsAdded: []string{"ck_notes_body"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name:            "ck_notes_body",
					TableName:       test.constraintTable,
					Type:            "CHECK",
					CheckExpression: "length(body) > 0",
				}},
			}
			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, identityRebuildSchema(""), "sqlite")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Not(qt.Contains), "__ptah_rebuild_notes")
			c.Assert(plan, qt.Contains, `CREATE TABLE "notes"`)
		})
	}
}
