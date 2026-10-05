package schemaprecondition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestRefuseYDBTablePartitioningChanges_FailurePath refuses a change of a table's
// YDB settings for a planner that plans none, naming the table and the
// planner: planning nothing would leave the table at the server's defaults
// while the comparison kept reporting the difference.
func TestRefuseYDBTablePartitioningChanges_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
		{TableName: "plain"},
		{TableName: "items", YDBPartitioningChange: &difftypes.YDBTablePartitioningChange{
			Desired: &ast.YDBTablePartitioningSpec{KeyBloomFilter: new(true)},
		}},
	}}
	err := schemaprecondition.RefuseYDBTablePartitioningChanges("mysql", diff)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `unsupported feature: the diff changes the partitioning, read replicas or key bloom `+
		`filter of table "items", which only a YDB plan does; the mysql planner plans none`)
}

// TestRefuseYDBTablePartitioningChanges_HappyPath passes a diff that changes no
// table's settings, a table modified otherwise included, and no diff at all.
func TestRefuseYDBTablePartitioningChanges_HappyPath(t *testing.T) {
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
			c.Assert(schemaprecondition.RefuseYDBTablePartitioningChanges("mysql", test.diff), qt.IsNil)
		})
	}
}
