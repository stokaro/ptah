package generator

// White-box testing required: the projection of a changed table is the
// private boundary between the forward plan and the reverse planner. No
// built-in target predicts a read of its columns, so no public plan shows
// which column observation the projection hands the reverse planner.

import (
	"cmp"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/internal/builtintest"
)

// predictingRuntime is a runtime whose table owner predicts what a read of a
// created table reports for its columns, as an executable provider with the
// columns feature does: a column declared as a type in stored reads back as
// the type stored names.
type predictingRuntime struct {
	Runtime
	stored map[string]string
}

func (r predictingRuntime) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	result, err := r.Runtime.ProjectTableCreations(ctx, request)
	for i, table := range request.Tables {
		result.Tables[i].ColumnsPrepared = true
		for _, field := range table.Declaration.Fields {
			read := cmp.Or(r.stored[field.Type], field.Type)
			result.Tables[i].Columns = append(result.Tables[i].Columns, catalog.Column{
				Name: field.Name, DataType: read, ColumnType: read, IsNullable: map[bool]string{true: "YES", false: "NO"}[field.Nullable],
			})
		}
	}
	return result, err
}

// changedColumnType is a table whose column n the declaration changes from
// INTEGER to BIGINT.
func changedColumnType() difftypes.TableDiff {
	key := schemamodel.Field{StructName: "Item", Name: "id", Type: "INTEGER", Primary: true}
	changed := schemamodel.Field{StructName: "Item", Name: "n", Type: "BIGINT", Nullable: true}
	return difftypes.TableDiff{
		TableName: "items",
		Desired: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}},
			Fields: []schemamodel.Field{key, changed},
		},
		Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items", Columns: []catalog.Column{
			{Name: "id", DataType: "INTEGER", ColumnType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "n", DataType: "INTEGER", ColumnType: "INTEGER", IsNullable: "YES", OrdinalPosition: 2},
		}}},
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Desired: changed}},
	}
}

// A changed column takes what its owner predicts a read reports for it, so
// the reverse planner compares the prior column with a type a read reports.
// The column the change leaves alone keeps its observation.
func TestProjectColumnsTakesAChangedColumnFromItsOwnersPrediction(t *testing.T) {
	c := qt.New(t)
	runtime := predictingRuntime{Runtime: builtintest.Runtime(), stored: map[string]string{"BIGINT": "Int64", "INTEGER": "Int32"}}

	columns, err := projectColumns(t.Context(), changedColumnType(), "sqlite", identifier.ForDialect("sqlite"), runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(columns, qt.HasLen, 2)
	c.Assert(columns[0].DataType, qt.Equals, "INTEGER")
	c.Assert(columns[1].Name, qt.Equals, "n")
	c.Assert(columns[1].DataType, qt.Equals, "Int64")
	c.Assert(columns[1].ColumnType, qt.Equals, "Int64")
	c.Assert(columns[1].IsNullable, qt.Equals, "YES")
	c.Assert(columns[1].OrdinalPosition, qt.Equals, 2)
}

// A target whose owner predicts no columns keeps the conversion of the
// declaration, as every built-in target does.
func TestProjectColumnsConvertsTheDeclarationWithoutAPrediction(t *testing.T) {
	c := qt.New(t)

	columns, err := projectColumns(t.Context(), changedColumnType(), "sqlite", identifier.ForDialect("sqlite"), builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	c.Assert(columns, qt.HasLen, 2)
	c.Assert(columns[1].DataType, qt.Equals, "BIGINT")
}
