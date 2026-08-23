package schemachange_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestTableChangesMatchTheExistingComparator is the differential test for this
// family, and it compares CHANGES rather than statements.
//
// Statements are what the constraint slice compares, because that slice
// renders. This one does not yet: [schemachange.Plan] answers ErrNotRendered
// for a table or a column, so a statement comparison would compare one path's
// output against nothing. What is comparable now is the decision -- which
// objects change and in which direction -- and that is the half a name in a
// slice already carries, so a divergence here is a divergence in the shipping
// comparator's own terms rather than in a spelling.
//
// A difference is either a defect in the new path or a decision the old one
// makes that this one should not; both are worth finding before anything
// depends on the answer (stokaro/ptah#1662).
func TestTableChangesMatchTheExistingComparator(t *testing.T) {
	tests := []struct {
		name        string
		description *goschema.Database
		catalog     *dbschematypes.DBSchema
	}{
		{
			name: "a table the database does not have",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true}),
			catalog: &dbschematypes.DBSchema{},
		},
		{
			name:        "a table the desired schema does not declare",
			description: &goschema.Database{},
			catalog: catalogTable(dbschematypes.DBColumn{
				Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
			}),
		},
		{
			name: "a column the table does not have",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true}),
			catalog: catalogTable(dbschematypes.DBColumn{
				Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
			}),
		},
		{
			name: "a column the desired schema does not declare",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true}),
			catalog: catalogTable(
				dbschematypes.DBColumn{
					Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
				},
				dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"}),
		},
		{
			name: "a column whose type changed",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "code", Type: "varchar(200)", Nullable: true}),
			catalog: catalogTable(dbschematypes.DBColumn{
				Name: "code", DataType: "varchar(50)", IsNullable: "YES",
			}),
		},
		{
			name: "a column whose nullability changed",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: false}),
			catalog: catalogTable(dbschematypes.DBColumn{
				Name: "code", DataType: "text", IsNullable: "YES",
			}),
		},
		{
			// The control the rows above need: a comparator that reported a
			// change for everything would agree with the existing one on all
			// six and be useless.
			name: "an unchanged schema",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true}),
			catalog: catalogTable(
				dbschematypes.DBColumn{
					Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
				},
				dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			profile := postgresProfile()

			existing := existingTableDecisions(
				schemadiff.CompareWithDialect(test.description, test.catalog, profile.Dialect))
			prototype := canonicalTableDecisions(changesFor(c, test.description, test.catalog))

			c.Assert(prototype, qt.DeepEquals, existing)
		})
	}
}

// existingTableDecisions reduces the existing comparator's per-family slices to
// one decision per object, so the two paths are compared on what they decided
// rather than on how each records it.
func existingTableDecisions(diff *difftypes.SchemaDiff) []string {
	decisions := make([]string, 0)
	for _, table := range diff.TablesAdded {
		decisions = append(decisions, fmt.Sprintf("add table %s", qualified(table)))
	}
	for _, table := range diff.TablesRemoved {
		decisions = append(decisions, fmt.Sprintf("remove table %s", qualified(table)))
	}
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsAdded {
			decisions = append(decisions,
				fmt.Sprintf("add column %s.%s", qualified(table.TableName), column))
		}
		for _, column := range table.ColumnsRemoved {
			decisions = append(decisions,
				fmt.Sprintf("remove column %s.%s", qualified(table.TableName), column))
		}
		for _, column := range table.ColumnsModified {
			decisions = append(decisions,
				fmt.Sprintf("modify column %s.%s", qualified(table.TableName), column.ColumnName))
		}
	}
	slices.Sort(decisions)
	return decisions
}

// qualified puts an unqualified table name in the profile's default schema.
//
// The existing comparator spells a table the way the SIDE it came from spelled
// it: a table added from a description that wrote no schema is "widget", and
// the same table removed from a catalog row is "public.widget". Comparing those
// as words reports a difference where both paths decided the same thing about
// one object, so both sides are put in one spelling here -- and a genuine
// schema difference still shows, because this qualifies rather than strips.
func qualified(table string) string {
	return map[bool]string{
		true:  table,
		false: "public." + table,
	}[strings.Contains(table, ".")]
}

// canonicalTableDecisions reduces the canonical changes the same way, dropping
// the families this comparison is not about.
func canonicalTableDecisions(changes []schemachange.Change) []string {
	decisions := make([]string, 0)
	for _, change := range changes {
		decisions = appendTableDecision(decisions, change)
	}
	slices.Sort(decisions)
	return decisions
}

// appendTableDecision keeps the loop above free of the conditionals the
// repository's test style refuses inside a test body.
func appendTableDecision(decisions []string, change schemachange.Change) []string {
	name := map[bool]string{
		true:  change.ID.Schema.Source + "." + change.ID.Name.Source,
		false: change.ID.Schema.Source + "." + change.ID.Parent.Source + "." + change.ID.Name.Source,
	}[string(change.ID.Kind) == "table"]
	keep := map[string]bool{"table": true, "column": true}[string(change.ID.Kind)]
	return map[bool]func() []string{
		true: func() []string {
			return append(decisions, fmt.Sprintf("%s %s %s", change.Operation, change.ID.Kind, name))
		},
		false: func() []string { return decisions },
	}[keep]()
}

// TestTableStatementsMatchTheExistingPlanner is the differential test at the
// level the constraint slice already uses: the SQL both paths render.
//
// A change comparison says the two paths agree about WHICH objects move. This
// says they agree about what is executed, which is the half a plan is judged on
// -- and it found four defects in the NEW path, all of which the change-level
// comparison passed straight over:
//
//   - a defaulted schema written back into DDL an author wrote bare;
//   - a composite primary key missed entirely, because the adapter read the
//     field flag and not the table-level declaration that spells one;
//   - a DROP TABLE without the guard and the CASCADE the shipping path emits;
//   - a DROP COLUMN addressing the catalog's spelling of a table both sides
//     describe.
//
// The existing planner was right about all four.
func TestTableStatementsMatchTheExistingPlanner(t *testing.T) {
	tests := []struct {
		name        string
		description *goschema.Database
		catalog     *dbschematypes.DBSchema
	}{
		{
			name: "creating a table",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true}),
			catalog: &dbschematypes.DBSchema{},
		},
		{
			name: "creating a table with a default",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{
					StructName: "Widget", Name: "code", Type: "text", Nullable: true,
					Default: "unset", DefaultSet: true,
				}),
			catalog: &dbschematypes.DBSchema{},
		},
		{
			name: "creating a table with a default expression",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{
					StructName: "Widget", Name: "seen", Type: "timestamp", Nullable: true,
					DefaultExpr: "now()",
				}),
			catalog: &dbschematypes.DBSchema{},
		},
		{
			name:        "dropping a table",
			description: &goschema.Database{},
			catalog: catalogTable(dbschematypes.DBColumn{
				Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
			}),
		},
		{
			name: "adding a column",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true}),
			catalog: catalogTable(dbschematypes.DBColumn{
				Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
			}),
		},
		{
			name: "dropping a column",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true}),
			catalog: catalogTable(
				dbschematypes.DBColumn{
					Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
				},
				dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"}),
		},
		{
			name: "widening a column",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "code", Type: "varchar(200)", Nullable: true}),
			catalog: catalogTable(dbschematypes.DBColumn{
				Name: "code", DataType: "varchar(50)", IsNullable: "YES",
			}),
		},
		{
			// A column-level UNIQUE. The fact was already in the canonical
			// model and the renderer did not ask for it, so the CREATE built a
			// table without a guarantee its author declared -- measured, by
			// running the two paths over a fixture that carries it.
			name: "creating a table with a unique column",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{
					StructName: "Widget", Name: "code", Type: "text", Nullable: true, Unique: true,
				}),
			catalog: &dbschematypes.DBSchema{},
		},
		{
			// A column-level CHECK, which the canonical Column did not carry at
			// all until the same measurement found it.
			name: "creating a table with a column check",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{
					StructName: "Widget", Name: "code", Type: "text", Nullable: true,
					Check: "length(code) > 0",
				}),
			catalog: &dbschematypes.DBSchema{},
		},
		{
			// A composite key, which the column syntax cannot express: PRIMARY
			// KEY on two columns declares two keys rather than one over both,
			// so it has to become a table-level constraint.
			name: "creating a table with a composite key",
			description: describedTableWithKey(
				[]string{"tenant", "id"},
				goschema.Field{StructName: "Widget", Name: "tenant", Type: "int"},
				goschema.Field{StructName: "Widget", Name: "id", Type: "int"}),
			catalog: &dbschematypes.DBSchema{},
		},
		{
			// The control. A planner that emitted nothing would agree with the
			// existing one on this row and on nothing else; a planner that
			// emitted something here would disagree with it on this row alone.
			name: "an unchanged schema",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true}),
			catalog: catalogTable(
				dbschematypes.DBColumn{
					Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
				},
				dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			profile := postgresProfile()

			existing := executableLines(existingPathStatements(c, test.description, test.catalog, profile))
			prototype := executableLines(plannedStatements(c, test.description, test.catalog, profile))

			c.Assert(prototype, qt.DeepEquals, existing)
		})
	}
}

// plannedStatements renders the canonical path's plan for one input pair.
func plannedStatements(
	c *qt.C,
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
	profile schemastate.Profile,
) []string {
	c.Helper()
	operations, err := schemachange.Plan(changesFor(c, description, catalog), profile)
	c.Assert(err, qt.IsNil)
	return schemachange.Statements(operations)
}

// executableLines keeps the lines a database would run.
//
// Both paths wrap their statements in comments -- the existing planner writes a
// header per table and a warning per destructive change, the renderer writes a
// banner -- and comparing those would be comparing two narrators rather than
// two plans. Everything that is not a comment survives, so a statement one path
// emits and the other does not is still a difference.
func executableLines(statements []string) []string {
	lines := make([]string, 0)
	for _, statement := range statements {
		for line := range strings.SplitSeq(statement, "\n") {
			lines = appendExecutable(lines, strings.TrimSpace(line))
		}
	}
	return lines
}

// appendExecutable keeps [executableLines] free of the conditionals the
// repository's test style refuses inside a test.
func appendExecutable(lines []string, trimmed string) []string {
	executable := trimmed != "" && !strings.HasPrefix(trimmed, "--")
	return map[bool][]string{
		true:  append(lines, normalizeStatement(trimmed)),
		false: lines,
	}[executable]
}

// TestAModificationCarriesWhatItReplaces pins the metadata a rendered
// modification carries beside the new definition.
//
// Most renderers ignore it, which is exactly why a statement comparison against
// PostgreSQL cannot see it: the two paths agree on every byte with the previous
// values present or absent. Two consumers cannot ignore it. Safety analysis
// tells a narrowing change from a widening one by the previous type, and
// Oracle's MODIFY states the WHOLE new column definition, so a cleared default
// has to be spelled DEFAULT NULL or the old one stays and the migration reports
// success (stokaro/ptah#1885).
func TestAModificationCarriesWhatItReplaces(t *testing.T) {
	c := qt.New(t)
	profile := postgresProfile()
	changes := changesFor(c, describedTable(goschema.Field{
		StructName: "Widget", Name: "code", Type: "varchar(200)", Nullable: true,
	}), catalogTable(dbschematypes.DBColumn{
		Name: "code", DataType: "varchar(50)", IsNullable: "NO",
		ColumnDefault: new("'unset'"),
	}))

	operations, err := schemachange.Plan(changes, profile)

	c.Assert(err, qt.IsNil)
	c.Assert(operations, qt.HasLen, 1)
	alter, ok := operations[0].Node.(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alter.Operations, qt.HasLen, 1)
	modify, ok := alter.Operations[0].(*ast.ModifyColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(modify.PreviousType, qt.Equals, "varchar(50)")
	c.Assert(modify.HasPreviousNullable, qt.IsTrue)
	c.Assert(modify.PreviousNullable, qt.IsFalse)
	c.Assert(modify.HasPreviousDefault, qt.IsTrue)
	c.Assert(modify.PreviousDefault, qt.Equals, "'unset'")
	c.Assert(modify.Column.Type, qt.Equals, "varchar(200)")
}
