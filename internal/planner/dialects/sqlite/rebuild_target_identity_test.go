package sqlite_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestRebuildResolvesTheRetainedTableAcrossSchemaSpellings pins sqlite's
// findTable, which resolves the definition a rebuild is rendered FROM.
//
// The rebuild target's name comes from the diff; the definition comes from the
// declaration. Reverting the function to the raw `QualifiedName() == name` loop
// left the whole suite green: every existing rebuild fixture happens to spell
// both sides the same way. On a mismatch the planner returns "rebuilding table
// %s requires the retained table definition" and the migration produces nothing
// at all -- the column type change the rebuild exists for is silently not
// planned.
func TestRebuildResolvesTheRetainedTableAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name          string
		tableSchema   string
		diffTableName string
	}{
		{
			// Control: both sides already agree.
			name:          "both sides spell the table the same way",
			tableSchema:   "",
			diffTableName: "notes",
		},
		{
			name:          "the diff qualifies main and the declaration does not",
			tableSchema:   "",
			diffTableName: "main.notes",
		},
		{
			name:          "the declaration qualifies main and the diff does not",
			tableSchema:   "main",
			diffTableName: "notes",
		},
		{
			// SQLite folds ASCII, so a case difference alone is the same split.
			name:          "the two sides differ only in case",
			tableSchema:   "",
			diffTableName: "Notes",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName: test.diffTableName,
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName: "body",
					Changes:    map[string]string{"type": "TEXT -> BLOB"},
				}},
			}}}
			declared := identityRebuildSchema(test.tableSchema)
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				declaringTheOnlyTable(diff, declared),
				declared,
				"sqlite",
			)
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, "__ptah_rebuild_notes", qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestRebuiltTableDoesNotAlsoGetItsIndexAndTriggerRecreated pins
// tableRebuilds.target and its contains wrapper.
//
// A rebuild already drops every index and trigger with the old table and
// recreates the desired set from the declaration, so the index and trigger
// phases have to skip a table under rebuild. That set is keyed by the DIFF's
// spelling while an index or trigger ref carries the DECLARATION's, and the raw
// map lookup master used answered "not being rebuilt" whenever the two
// disagreed. Neither `CREATE INDEX` nor `CREATE TRIGGER` carries IF NOT EXISTS
// here, so the second copy does not duplicate quietly -- it aborts the
// migration after the rebuild has already dropped and recreated the table.
//
// Reverting `target` to `r.targets[tableName]` leaves the whole planner and
// migration suite green, which is why this pin is here rather than assumed.
func TestRebuiltTableDoesNotAlsoGetItsIndexAndTriggerRecreated(t *testing.T) {
	tests := []struct {
		name          string
		tableSchema   string
		diffTableName string
		refTableName  string
	}{
		{
			// Control: both sides already agree.
			name:          "both sides spell the table the same way",
			tableSchema:   "",
			diffTableName: "notes",
			refTableName:  "notes",
		},
		{
			name:          "the diff qualifies main and the refs do not",
			tableSchema:   "",
			diffTableName: "main.notes",
			refTableName:  "notes",
		},
		{
			name:          "the refs qualify main and the diff does not",
			tableSchema:   "main",
			diffTableName: "notes",
			refTableName:  "main.notes",
		},
		{
			name:          "the two sides differ only in case",
			tableSchema:   "",
			diffTableName: "Notes",
			refTableName:  "notes",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := identityRebuildSchema(test.tableSchema)
			desired.Indexes = []schemamodel.Index{{
				StructName: "Note",
				Name:       "idx_notes_body",
				Fields:     []string{"body"},
			}}
			desired.Triggers = []schemamodel.Trigger{{
				StructName: "Note",
				Name:       "trg_notes_touch",
				Table:      test.refTableName,
				Timing:     "AFTER",
				Event:      "UPDATE",
				Body:       "SELECT 1;",
				ForEach:    "ROW",
			}}

			diff := &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName: test.diffTableName,
					ColumnsModified: []difftypes.ColumnDiff{{
						ColumnName: "body",
						Changes:    map[string]string{"type": "TEXT -> BLOB"},
					}},
				}},
				IndexesAdded: []difftypes.IndexRef{{
					Name:      "idx_notes_body",
					TableName: test.refTableName,
				}},
				TriggersAdded: []difftypes.TriggerRef{{
					TriggerName: "trg_notes_touch",
					TableName:   test.refTableName,
				}},
			}

			statements, err := planner.GenerateSchemaDiffSQLStatements(declaringTheOnlyTable(diff, desired), desired, "sqlite")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(strings.Count(plan, "CREATE INDEX"), qt.Equals, 1, qt.Commentf("plan:\n%s", plan))
			c.Assert(strings.Count(plan, "CREATE TRIGGER"), qt.Equals, 1, qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestRebuildDoesNotResolveATableInAnotherSchema is the control: a table
// declared under one schema is not the table a diff names under another, and
// rebuilding it would drop and recreate the wrong relation. Refusing is loud --
// the planner returns an error naming the table it could not resolve.
func TestRebuildDoesNotResolveATableInAnotherSchema(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "attached.notes",
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "body",
			Changes:    map[string]string{"type": "TEXT -> BLOB"},
		}},
	}}}
	_, err := planner.GenerateSchemaDiffSQLStatements(diff, identityRebuildSchema("main"), "sqlite")
	c.Assert(err, qt.ErrorMatches, `(?s).*requires its desired definition, and the declaration does not contain it.*`)
}
