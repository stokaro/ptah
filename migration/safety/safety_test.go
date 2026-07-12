package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/safety"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestClassifySchemaDiff_HighestSeverity(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesAdded: []string{"users"},
		TablesModified: []types.TableDiff{
			{
				TableName:      "products",
				ColumnsAdded:   []string{"sku"},
				ColumnsRemoved: []string{"legacy_code"},
			},
		},
		IndexesRemoved: []string{"idx_products_old"},
	}

	findings := safety.ClassifySchemaDiff(diff)

	c.Assert(findings, qt.Contains, safety.Finding{
		Category: "columns_removed",
		Count:    1,
		Severity: safety.Destructive,
	})
	c.Assert(findings, qt.Contains, safety.Finding{
		Category: "indexes_removed",
		Count:    1,
		Severity: safety.Warning,
	})
	c.Assert(safety.Highest(findings), qt.Equals, safety.Destructive)
	c.Assert(safety.HasDestructive(findings), qt.IsTrue)
}

func TestClassifySchemaDiff_AggregatesRepeatedCategories(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{TableName: "users", ColumnsAdded: []string{"email"}},
			{TableName: "posts", ColumnsAdded: []string{"slug", "status"}},
		},
	}

	findings := safety.ClassifySchemaDiff(diff)

	c.Assert(findings, qt.DeepEquals, []safety.Finding{
		{
			Category: "columns_added",
			Count:    3,
			Severity: safety.Warning,
		},
	})
	c.Assert(safety.Highest(findings), qt.Equals, safety.Warning)
}
