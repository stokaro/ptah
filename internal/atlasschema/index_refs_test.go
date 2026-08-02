package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestApplyDiffPolicy_PreservesSameNamedIndexOnKeptTable(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{TablesRemoved: []string{"users"}}
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})

	got := atlasschema.ApplyDiffPolicy(diff, atlasschema.DiffPolicy{SkipDropTable: true})

	c.Assert(got.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "users"},
	})
}
