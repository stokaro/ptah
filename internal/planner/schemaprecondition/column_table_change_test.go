package schemaprecondition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestRefuseYDBTableSettingChanges_FailurePath refuses a change of a YDB
// column table's storage for a planner that plans none, naming the table and
// the planner: planning nothing would leave the table as it is while the
// comparison kept reporting the difference.
func TestRefuseYDBTableSettingChanges_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
		{TableName: "plain"},
		{TableName: "items", YDBColumnTableChange: &difftypes.YDBColumnTableChange{
			Desired: &ast.YDBColumnTableSpec{HashColumns: []string{"id"}},
		}},
	}}
	err := schemaprecondition.RefuseYDBTableSettingChanges("mysql", diff)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `unsupported feature: column-table changes require a YDB planner; the mysql planner plans none of table "items"`)
}

// TestRefuseYDBTableSettingChanges_HappyPath passes a diff that changes no
// column table, a table modified otherwise included, and no diff at all.
func TestRefuseYDBTableSettingChanges_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "no diff"},
		{name: "an empty diff", diff: &difftypes.SchemaDiff{}},
		{name: "a table modified otherwise", diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
			{TableName: "items", ColumnsRemoved: difftypes.ColumnChanges{{Name: "old"}}},
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprecondition.RefuseYDBTableSettingChanges("mysql", test.diff), qt.IsNil)
		})
	}
}
