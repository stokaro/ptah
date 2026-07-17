package types_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema/types"
)

func TestDBConstraint_ColumnSlicesFallbackToLegacyFields(t *testing.T) {
	c := qt.New(t)

	legacy := types.DBConstraint{
		ColumnName:    "tenant_id",
		ForeignColumn: new("id"),
	}
	c.Assert(legacy.ColumnNamesOrDefault(), qt.DeepEquals, []string{"tenant_id"})
	c.Assert(legacy.ForeignColumnsOrDefault(), qt.DeepEquals, []string{"id"})

	composite := types.DBConstraint{
		ColumnName:     "tenant_id",
		ColumnNames:    []string{"tenant_id", "owner_id"},
		ForeignColumn:  new("tenant_id"),
		ForeignColumns: []string{"tenant_id", "id"},
	}
	c.Assert(composite.ColumnNamesOrDefault(), qt.DeepEquals, []string{"tenant_id", "owner_id"})
	c.Assert(composite.ForeignColumnsOrDefault(), qt.DeepEquals, []string{"tenant_id", "id"})
}
