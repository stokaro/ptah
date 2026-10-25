package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/deporder"
)

// TestViewLikesForDropForDialect_HappyPath pins the order view-likes are
// dropped in one at a time, without CASCADE: a reader before what it reads, and
// the arrival order wherever no read decides it.
func TestViewLikesForDropForDialect_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		objects []deporder.ViewLike
		want    []string
	}{
		{
			name: "a reader goes before the view it reads",
			objects: []deporder.ViewLike{
				{Name: "item_skus", Body: "SELECT id, sku FROM items"},
				{Name: "item_sku_count", Body: "SELECT sku, count(*) FROM item_skus GROUP BY sku"},
			},
			want: []string{"item_sku_count", "item_skus"},
		},
		{
			name: "a view reading a materialized view goes first",
			objects: []deporder.ViewLike{
				{Name: "totals", Body: "SELECT sum(n) AS n FROM orders", Materialized: true},
				{Name: "report", Body: "SELECT n FROM totals"},
			},
			want: []string{"report", "totals"},
		},
		{
			name: "a chain three deep, read from its middle",
			objects: []deporder.ViewLike{
				{Name: "b", Body: "SELECT x FROM a"},
				{Name: "a", Body: "SELECT x FROM t"},
				{Name: "c", Body: "SELECT x FROM b"},
			},
			want: []string{"c", "b", "a"},
		},
		{
			name: "views that read nothing of each other keep their order",
			objects: []deporder.ViewLike{
				{Name: "first", Body: "SELECT 1 FROM t"},
				{Name: "second", Body: "SELECT 2 FROM u"},
				{Name: "third", Body: "SELECT 3 FROM v"},
			},
			want: []string{"first", "second", "third"},
		},
		{
			name: "a name inside a string literal reads nothing",
			objects: []deporder.ViewLike{
				{Name: "base", Body: "SELECT 1 FROM t"},
				{Name: "label", Body: "SELECT 'base' AS what FROM t"},
			},
			want: []string{"base", "label"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := deporder.ViewLikesForDropForDialect(tt.objects, platform.Postgres)

			c.Assert(viewLikeNames(got), qt.DeepEquals, tt.want)
		})
	}
}
