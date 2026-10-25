package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertMaterializedViews_WritesWithNoData pins how a description keeps a
// materialized view the server holds without rows. The server reports the query
// alone, with no WITH NO DATA clause, so the description writes the clause from
// the read's flag: a rollback that creates the view again from it leaves the
// view empty, as it was. A view the server holds with rows keeps its body.
func TestConvertMaterializedViews_WritesWithNoData(t *testing.T) {
	c := qt.New(t)

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
		MatViews: []catalog.MaterializedView{
			{Name: "order_totals", Body: " SELECT sum(amount) AS total\n   FROM orders;", Unpopulated: true},
			{Name: "order_counts", Body: " SELECT count(*) AS n\n   FROM orders;"},
		},
	}, "postgres", must.Must(builtin.New())))

	c.Assert(converted.MaterializedViews, qt.HasLen, 2)
	c.Assert(converted.MaterializedViews[0].Body, qt.Equals, "SELECT sum(amount) AS total\n   FROM orders\nWITH NO DATA")
	c.Assert(converted.MaterializedViews[1].Body, qt.Equals, " SELECT count(*) AS n\n   FROM orders;")
}
