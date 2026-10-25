package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// rebuildObject is a view or a materialized view, as the database holds it or
// as the declaration does.
type rebuildObject struct {
	name, body   string
	materialized bool
}

// rebuildState is one side of a rebuild comparison: the type of items.sku and
// the view-like objects. Both sides hold the tables orders(id, amount) and
// items(id, sku).
type rebuildState struct {
	skuType string
	objects []rebuildObject
}

// planRebuild compares the declaration against the database in the statement
// style and returns the planned statements.
func planRebuild(c *qt.C, style planext.StatementStyle, live, declared rebuildState) ([]string, error) {
	c.Helper()
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}, {StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "integer", Primary: true},
			{StructName: "Order", Name: "amount", Type: "numeric"},
			{StructName: "Item", Name: "id", Type: "integer", Primary: true},
			{StructName: "Item", Name: "sku", Type: declared.skuType},
		},
	}
	for _, object := range declared.objects {
		if object.materialized {
			desired.MaterializedViews = append(desired.MaterializedViews, schemamodel.MaterializedView{Name: object.name, Body: object.body})
			continue
		}
		desired.Views = append(desired.Views, schemamodel.View{Name: object.name, Body: object.body})
	}

	database := &catalog.Database{Tables: []catalog.Table{
		{Name: "orders", Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "amount", DataType: "numeric", UDTName: "numeric", IsNullable: "NO", OrdinalPosition: 2},
		}},
		{Name: "items", Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "sku", DataType: live.skuType, UDTName: "varchar", IsNullable: "NO", OrdinalPosition: 2},
		}},
	}}
	for _, object := range live.objects {
		if object.materialized {
			database.MatViews = append(database.MatViews, catalog.MaterializedView{Name: object.name, Schema: "public", Body: object.body})
			continue
		}
		database.Views = append(database.Views, catalog.View{Name: object.name, Schema: "public", Body: object.body})
	}

	diff := must.Must(schemadiff.CompareWithDatabaseInfo(c.Context(), desired, database,
		catalog.ServerInfo{Dialect: "postgres", Schema: "public"}, nil, must.Must(builtin.New())))
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(context.Background(), must.Must(builtin.New()), diff, "postgres",
		planext.Options{Capabilities: capability.ForDialect("postgres"), Statements: style})
}

var (
	orderTotals        = rebuildObject{name: "order_totals", body: "SELECT sum(amount) AS total FROM orders", materialized: true}
	orderTotalsChanged = rebuildObject{name: "order_totals", body: "SELECT sum(amount) AS total, count(*) AS n FROM orders", materialized: true}
	orderReport        = rebuildObject{name: "order_report", body: "SELECT total FROM order_totals"}
	orderReportWide    = rebuildObject{name: "order_report", body: "SELECT total, 1 AS one FROM order_totals"}
	orderReportMoved   = rebuildObject{name: "order_report", body: "SELECT 1 AS one, total FROM order_totals"}
	orderTop           = rebuildObject{name: "order_top", body: "SELECT max(total) AS top FROM order_totals", materialized: true}
	orderTopReport     = rebuildObject{name: "order_top_report", body: "SELECT top FROM order_top"}
	orderList          = rebuildObject{name: "order_list", body: "SELECT id, amount FROM orders"}
	orderListMoved     = rebuildObject{name: "order_list", body: "SELECT amount, id FROM orders"}
	orderMix           = rebuildObject{name: "order_mix", body: "SELECT l.id, t.total FROM order_list l, order_totals t"}
	newReport          = rebuildObject{name: "new_report", body: "SELECT total, n FROM order_totals"}
	itemSkus           = rebuildObject{name: "item_skus", body: "SELECT id, sku FROM items"}
	skuCounts          = rebuildObject{name: "sku_counts", body: "SELECT sku, count(*) AS n FROM item_skus GROUP BY sku", materialized: true}
	skuCountsChanged   = rebuildObject{name: "sku_counts", body: "SELECT sku, count(*) AS n, max(id) AS top FROM item_skus GROUP BY sku", materialized: true}
	skuList            = rebuildObject{name: "item_sku_list", body: "SELECT sku FROM item_skus"}
)

const (
	liveSKU     = "character varying"
	declaredSKU = "varchar"
	widenedSKU  = "VARCHAR(128)"
)

// TestViewRebuild_HappyPath pins the plans of the view rebuild in both
// statement styles. A changed materialized view leaves with every view and
// materialized view that reads it, after them, and comes back before them
// (stokaro/ptah#4964); a column type change does the same with the views that
// read the column. Each object the plan drops goes after the objects that read
// it, so the exact style drops without CASCADE and writes no guard, and Ptah's
// own style keeps both.
//
// An added object that reads a rebuilt one is created after it, a removed one
// is only dropped, and a reader that also reads a view the view step drops is
// created after that view is back.
func TestViewRebuild_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		live, declared rebuildState
		annotated      []string
		exact          []string
	}{
		{
			name:     "a view that reads a changed materialized view",
			live:     rebuildState{liveSKU, []rebuildObject{orderTotals, orderReport}},
			declared: rebuildState{declaredSKU, []rebuildObject{orderTotalsChanged, orderReport}},
			annotated: []string{
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createReport,
			},
			exact: []string{`DROP VIEW "order_report"`, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, createReport},
		},
		{
			name:     "a chain of readers through a materialized view",
			live:     rebuildState{liveSKU, []rebuildObject{orderTotals, orderTop, orderTopReport}},
			declared: rebuildState{declaredSKU, []rebuildObject{orderTotalsChanged, orderTop, orderTopReport}},
			annotated: []string{
				`DROP VIEW IF EXISTS "order_top_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_top" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createTop,
				createTopReport,
			},
			exact: []string{
				`DROP VIEW "order_top_report"`,
				`DROP MATERIALIZED VIEW "order_top"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
				createTop,
				createTopReport,
			},
		},
		{
			name:      "a view the plan adds over a changed materialized view",
			live:      rebuildState{liveSKU, []rebuildObject{orderTotals}},
			declared:  rebuildState{declaredSKU, []rebuildObject{orderTotalsChanged, newReport}},
			annotated: []string{`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`, createTotalsChanged, createNewReport},
			exact:     []string{`DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, createNewReport},
		},
		{
			name:     "a reader of a changed materialized view and of a view the view step drops",
			live:     rebuildState{liveSKU, []rebuildObject{orderList, orderTotals, orderMix}},
			declared: rebuildState{declaredSKU, []rebuildObject{orderListMoved, orderTotalsChanged, orderMix}},
			annotated: []string{
				`DROP VIEW IF EXISTS "order_mix" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				`DROP VIEW IF EXISTS "order_list" CASCADE`,
				createListMoved,
				createTotalsChanged,
				createMix,
			},
			exact: []string{
				`DROP VIEW "order_mix"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				`DROP VIEW "order_list"`,
				createListMoved,
				createTotalsChanged,
				createMix,
			},
		},
		{
			name:     "a view the view step drops that reads a changed materialized view",
			live:     rebuildState{liveSKU, []rebuildObject{orderTotals, orderReportWide}},
			declared: rebuildState{declaredSKU, []rebuildObject{orderTotalsChanged, orderReportMoved}},
			annotated: []string{
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createReportMoved,
			},
			exact: []string{`DROP VIEW "order_report"`, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, createReportMoved},
		},
		{
			name:     "a changed materialized view over a view a column type change rebuilds",
			live:     rebuildState{liveSKU, []rebuildObject{itemSkus, skuCounts}},
			declared: rebuildState{widenedSKU, []rebuildObject{itemSkus, skuCountsChanged}},
			annotated: []string{
				`DROP MATERIALIZED VIEW IF EXISTS "sku_counts" CASCADE`,
				`DROP VIEW IF EXISTS "item_skus" CASCADE`,
				"-- Add/modify columns for table: items\n-- Modify column items.sku: type: varchar -> VARCHAR(128)\n" + widenSKU,
				createItemSkus,
				createSkuCountsChanged,
			},
			exact: []string{`DROP MATERIALIZED VIEW "sku_counts"`, `DROP VIEW "item_skus"`, widenSKU, createItemSkus, createSkuCountsChanged},
		},
		{
			name:     "a view the plan adds over a view a column type change rebuilds",
			live:     rebuildState{liveSKU, []rebuildObject{itemSkus}},
			declared: rebuildState{widenedSKU, []rebuildObject{itemSkus, skuList}},
			annotated: []string{
				`DROP VIEW IF EXISTS "item_skus" CASCADE`,
				"-- Add/modify columns for table: items\n-- Modify column items.sku: type: varchar -> VARCHAR(128)\n" + widenSKU,
				createItemSkus,
				createSkuList,
			},
			exact: []string{`DROP VIEW "item_skus"`, widenSKU, createItemSkus, createSkuList},
		},
		{
			name:     "a view the declaration removes that reads a changed materialized view",
			live:     rebuildState{liveSKU, []rebuildObject{orderTotals, orderReport}},
			declared: rebuildState{declaredSKU, []rebuildObject{orderTotalsChanged}},
			annotated: []string{
				`DROP VIEW IF EXISTS "public"."order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
			},
			exact: []string{`DROP VIEW "public"."order_report"`, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged},
		},
		{
			name:      "a changed materialized view nothing reads",
			live:      rebuildState{liveSKU, []rebuildObject{orderTotals}},
			declared:  rebuildState{declaredSKU, []rebuildObject{orderTotalsChanged}},
			annotated: []string{`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`, createTotalsChanged},
			exact:     []string{`DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			annotated, annotatedErr := planRebuild(c, planext.StatementStyleAnnotated, test.live, test.declared)
			exact, exactErr := planRebuild(c, planext.StatementStyleExact, test.live, test.declared)

			c.Assert(annotatedErr, qt.IsNil)
			c.Assert(annotated, qt.DeepEquals, test.annotated)
			c.Assert(exactErr, qt.IsNil)
			c.Assert(exact, qt.DeepEquals, test.exact)
		})
	}
}

const (
	createTotalsChanged    = "CREATE MATERIALIZED VIEW \"order_totals\" AS\nSELECT sum(amount) AS total, count(*) AS n FROM orders"
	createReport           = "CREATE VIEW \"order_report\" AS\nSELECT total FROM order_totals"
	createReportMoved      = "CREATE VIEW \"order_report\" AS\nSELECT 1 AS one, total FROM order_totals"
	createTop              = "CREATE MATERIALIZED VIEW \"order_top\" AS\nSELECT max(total) AS top FROM order_totals"
	createTopReport        = "CREATE VIEW \"order_top_report\" AS\nSELECT top FROM order_top"
	createNewReport        = "CREATE VIEW \"new_report\" AS\nSELECT total, n FROM order_totals"
	createListMoved        = "CREATE VIEW \"order_list\" AS\nSELECT amount, id FROM orders"
	createMix              = "CREATE VIEW \"order_mix\" AS\nSELECT l.id, t.total FROM order_list l, order_totals t"
	createItemSkus         = "CREATE VIEW \"item_skus\" AS\nSELECT id, sku FROM items"
	createSkuCountsChanged = "CREATE MATERIALIZED VIEW \"sku_counts\" AS\nSELECT sku, count(*) AS n, max(id) AS top FROM item_skus GROUP BY sku"
	createSkuList          = "CREATE VIEW \"item_sku_list\" AS\nSELECT sku FROM item_skus"
	widenSKU               = `ALTER TABLE "items" ALTER COLUMN "sku" TYPE VARCHAR(128)`
)
