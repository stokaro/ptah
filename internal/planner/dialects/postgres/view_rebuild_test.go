package postgres_test

import (
	"cmp"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// rebuildObject is a view or a materialized view, as the database holds it or
// as the declaration does, in public unless schema names another schema.
type rebuildObject struct {
	schema, name, body string
	materialized       bool
}

// declaredName is the name a declaration gives object: qualified in a schema
// other than public.
func (o rebuildObject) declaredName() string {
	if o.schema == "" {
		return o.name
	}
	return o.schema + "." + o.name
}

// rebuildState is one side of a rebuild comparison: the type of items.sku, the
// view-like objects, and what may depend on them. Both sides hold the tables
// orders(id, amount) and items(id, sku).
type rebuildState struct {
	skuType  string
	objects  []rebuildObject
	routines []rebuildRoutine
	indexes  []rebuildIndex
	grants   []rebuildGrant
	triggers []rebuildTrigger
	// roles are the roles the schema manages, on both sides.
	roles []string
	// options is, on the database side, the options a view or a materialized
	// view is held with, by name.
	options map[string][]string
}

// rebuildRoutine is a routine of the schema in public. identity is its
// argument list as the database reports it.
type rebuildRoutine struct {
	name, parameters, identity, returns, language, body string
}

// rebuildIndex is an index on a materialized view.
type rebuildIndex struct {
	name, relation, column string
	unique                 bool
}

// rebuildGrant is one privilege. On the declaration side it is declared; on
// the database side the read reports it, implicit when the routine holds the
// default privileges.
type rebuildGrant struct {
	role, privilege, objectType, object, arguments, column string
	implicit                                               bool
}

// rebuildTrigger is an INSTEAD OF INSERT trigger on a view that runs a
// routine of the schema.
type rebuildTrigger struct {
	name, view, function, body string
}

// planRebuild compares the declaration against the database in the statement
// style and returns the planned statements.
func planRebuild(c *qt.C, style planext.StatementStyle, live, declared rebuildState) ([]string, error) {
	c.Helper()
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(context.Background(), must.Must(builtin.New()),
		compareRebuild(c, live, declared), "postgres",
		planext.Options{Capabilities: capability.ForDialect("postgres"), Statements: style})
}

// compareRebuild compares the declaration against the database.
func compareRebuild(c *qt.C, live, declared rebuildState) *difftypes.SchemaDiff {
	c.Helper()
	semantics := identifier.ForDialect("postgres")
	semantics.SearchPath = []string{"public"}
	return must.Must(schemadiff.CompareWithDatabaseInfo(c.Context(), declaredRebuild(declared), liveRebuild(live),
		catalog.ServerInfo{Dialect: "postgres", Schema: "public", IdentifierSemantics: semantics}, nil, must.Must(builtin.New())))
}

// declaredRebuild is the declaration side of a rebuild comparison.
func declaredRebuild(declared rebuildState) *schemamodel.Database {
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
			desired.MaterializedViews = append(desired.MaterializedViews, schemamodel.MaterializedView{Name: object.declaredName(), Body: object.body})
			continue
		}
		desired.Views = append(desired.Views, schemamodel.View{Name: object.declaredName(), Body: object.body})
	}
	for _, routine := range declared.routines {
		desired.Functions = append(desired.Functions, schemamodel.Function{
			Name: routine.name, Parameters: routine.parameters, Returns: routine.returns,
			Language: routine.language, Volatility: "STABLE", Body: routine.body,
		})
	}
	for _, index := range declared.indexes {
		desired.Indexes = append(desired.Indexes, schemamodel.Index{
			Name: index.name, TableName: index.relation, Fields: []string{index.column}, Unique: index.unique,
		})
	}
	for _, grant := range declared.grants {
		declaredGrant := schemamodel.Grant{Role: grant.role, Privileges: []string{grant.privilege}}
		if grant.objectType == "FUNCTION" {
			declaredGrant.OnRoutine, declaredGrant.RoutineKind, declaredGrant.RoutineArguments = grant.object, "FUNCTION", grant.arguments
		} else {
			declaredGrant.OnTable = grant.object
		}
		if grant.column != "" {
			declaredGrant.Columns = []string{grant.column}
		}
		desired.Grants = append(desired.Grants, declaredGrant)
	}
	for _, role := range declared.roles {
		desired.Roles = append(desired.Roles, schemamodel.Role{Name: role, Inherit: true})
	}
	for _, trigger := range declared.triggers {
		desired.Triggers = append(desired.Triggers, schemamodel.Trigger{
			Name: trigger.name, Table: trigger.view, Timing: "INSTEAD OF", Event: "INSERT", ForEach: "ROW",
			ExecuteFunction: trigger.function, Body: trigger.body,
		})
	}
	return desired
}

// liveRebuild is the database side of a rebuild comparison.
func liveRebuild(live rebuildState) *catalog.Database {
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
			database.MatViews = append(database.MatViews, catalog.MaterializedView{
				Name: object.name, Schema: cmp.Or(object.schema, "public"), Body: object.body, Options: live.options[object.name],
			})
			continue
		}
		database.Views = append(database.Views, catalog.View{
			Name: object.name, Schema: cmp.Or(object.schema, "public"), Body: object.body, Options: live.options[object.name],
		})
	}
	for _, routine := range live.routines {
		database.Functions = append(database.Functions, catalog.Function{
			Name: routine.name, Parameters: routine.parameters, IdentityArguments: new(routine.identity),
			Returns: routine.returns, Language: routine.language, Volatility: "STABLE", Body: routine.body,
		})
	}
	for _, index := range live.indexes {
		database.Indexes = append(database.Indexes, catalog.Index{
			Name: index.name, TableName: index.relation, Schema: "public", Columns: []string{index.column}, IsUnique: index.unique,
		})
	}
	for _, grant := range live.grants {
		database.Grants = append(database.Grants, catalog.Grant{
			Role: grant.role, Privilege: grant.privilege, ObjectType: grant.objectType, ObjectName: grant.object,
			Arguments: grant.arguments, Column: grant.column, Implicit: grant.implicit,
		})
	}
	for _, role := range live.roles {
		database.Roles = append(database.Roles, catalog.Role{Name: role, Inherit: true})
	}
	for _, trigger := range live.triggers {
		database.Triggers = append(database.Triggers, catalog.Trigger{
			Name: trigger.name, Table: trigger.view, Timing: "INSTEAD OF", Event: "INSERT", ForEach: "ROW",
			ExecuteFunction: trigger.function, Body: trigger.body,
		})
	}
	return database
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
	bigOrdersReading   = rebuildObject{name: "big_orders", body: "SELECT id, amount FROM orders WHERE amount > (SELECT total / 10 FROM order_totals)"}
	bigOrdersPlain     = rebuildObject{name: "big_orders", body: "SELECT id, amount FROM orders WHERE amount > 100"}
	bigOrdersOfNewList = rebuildObject{name: "big_orders", body: "SELECT id, amount FROM orders WHERE id IN (SELECT id FROM new_list)"}
	newList            = rebuildObject{name: "new_list", body: "SELECT id FROM orders"}
	// reportingSkus is item_skus's namesake in the schema reporting, and
	// reportingSkuList reads it by its qualified name.
	reportingSkus    = rebuildObject{schema: "reporting", name: "item_skus", body: "SELECT id FROM orders"}
	reportingSkuList = rebuildObject{name: "reporting_sku_list", body: "SELECT id FROM reporting.item_skus"}
	// reportingTotals is order_totals's namesake in the schema reporting.
	reportingTotals        = rebuildObject{schema: "reporting", name: "order_totals", body: "SELECT sum(amount) AS total FROM orders", materialized: true}
	reportingTotalsChanged = rebuildObject{schema: "reporting", name: "order_totals",
		body: "SELECT sum(amount) AS total, count(*) AS n FROM orders", materialized: true}
	// bigOrdersWider is bigOrdersReading with one more column, a change the
	// view step makes in place.
	bigOrdersWider = rebuildObject{name: "big_orders",
		body: "SELECT id, amount, 1 AS one FROM orders WHERE amount > (SELECT total / 10 FROM order_totals)"}
	// qualifiedReport reads order_totals by its name qualified with public.
	qualifiedReport = rebuildObject{name: "qualified_report", body: "SELECT total FROM public.order_totals"}
	orderCount      = rebuildRoutine{name: "order_count", returns: "bigint", language: "sql", body: "SELECT n FROM order_totals"}
	// itemRows returns the rows of the table items, whose column type change
	// the server makes with the routine in place.
	itemRows = rebuildRoutine{name: "item_rows", returns: "SETOF items", language: "sql", body: "SELECT * FROM items"}
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
// is only dropped, and a reader that also reads a view the plan cannot replace
// is created after that view is back.
//
// A view the plan replaces in place is dropped only where it has to be. One
// whose definition in the database reads the changed materialized view and
// whose new one does not is replaced before the drop, and keeps its policies
// and privileges; one that starts reading it is replaced after the
// materialized view is back. One whose new definition reads a view the plan
// adds cannot be replaced before the drop, so it is rebuilt. A routine the plan
// adds is created after the objects its body reads are back: PostgreSQL checks
// a SQL body when it creates the routine.
func TestViewRebuild_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		live, declared rebuildState
		annotated      []string
		exact          []string
	}{
		{
			name:     "a view that reads a changed materialized view",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}},
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
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderTop, orderTopReport}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderTop, orderTopReport}},
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
			live:      rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals}},
			declared:  rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, newReport}},
			annotated: []string{`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`, createTotalsChanged, createNewReport},
			exact:     []string{`DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, createNewReport},
		},
		{
			name:     "a reader of a changed materialized view and of a view the plan cannot replace",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderList, orderTotals, orderMix}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderListMoved, orderTotalsChanged, orderMix}},
			annotated: []string{
				`DROP VIEW IF EXISTS "order_mix" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				`DROP VIEW IF EXISTS "order_list" CASCADE`,
				createTotalsChanged,
				createListMoved,
				createMix,
			},
			exact: []string{
				`DROP VIEW "order_mix"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				`DROP VIEW "order_list"`,
				createTotalsChanged,
				createListMoved,
				createMix,
			},
		},
		{
			name:     "a view the view step drops that reads a changed materialized view",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReportWide}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReportMoved}},
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
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{itemSkus, skuCounts}},
			declared: rebuildState{skuType: widenedSKU, objects: []rebuildObject{itemSkus, skuCountsChanged}},
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
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{itemSkus}},
			declared: rebuildState{skuType: widenedSKU, objects: []rebuildObject{itemSkus, skuList}},
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
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged}},
			annotated: []string{
				`DROP VIEW IF EXISTS "public"."order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
			},
			exact: []string{`DROP VIEW "public"."order_report"`, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged},
		},
		{
			name:     "a view that stops reading a changed materialized view",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, bigOrdersReading}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, bigOrdersPlain}},
			annotated: []string{
				replaceBigOrdersPlain,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
			},
			exact: []string{replaceBigOrdersPlain, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged},
		},
		{
			name:     "a view that starts reading a changed materialized view",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, bigOrdersPlain}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, bigOrdersReading}},
			annotated: []string{
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				replaceBigOrdersReading,
			},
			exact: []string{`DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, replaceBigOrdersReading},
		},
		{
			name:     "a view that stops reading a changed materialized view for a view the plan adds",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, bigOrdersReading}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, bigOrdersOfNewList, newList}},
			annotated: []string{
				`DROP VIEW IF EXISTS "big_orders" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createNewList,
				createTotalsChanged,
				createBigOrdersOfNewList,
			},
			exact: []string{
				`DROP VIEW "big_orders"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createNewList,
				createTotalsChanged,
				createBigOrdersOfNewList,
			},
		},
		{
			name:      "a routine the plan adds that reads a column a changed materialized view gains",
			live:      rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals}},
			declared:  rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged}, routines: []rebuildRoutine{orderCount}},
			annotated: []string{`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`, createTotalsChanged, createOrderCount},
			exact:     []string{`DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, createOrderCount},
		},
		{
			name:     "a routine that returns a retyped table's rows stays in place",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{itemSkus}, routines: []rebuildRoutine{itemRows}},
			declared: rebuildState{skuType: widenedSKU, objects: []rebuildObject{itemSkus}, routines: []rebuildRoutine{itemRows}},
			annotated: []string{
				`DROP VIEW IF EXISTS "item_skus" CASCADE`,
				"-- Add/modify columns for table: items\n-- Modify column items.sku: type: varchar -> VARCHAR(128)\n" + widenSKU,
				createItemSkus,
			},
			exact: []string{`DROP VIEW "item_skus"`, widenSKU, createItemSkus},
		},
		{
			name:     "a view that reads a namesake of a rebuilt view in another schema",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{itemSkus, reportingSkus, reportingSkuList}},
			declared: rebuildState{skuType: widenedSKU, objects: []rebuildObject{itemSkus, reportingSkus, reportingSkuList}},
			annotated: []string{
				`DROP VIEW IF EXISTS "item_skus" CASCADE`,
				"-- Add/modify columns for table: items\n-- Modify column items.sku: type: varchar -> VARCHAR(128)\n" + widenSKU,
				createItemSkus,
			},
			exact: []string{`DROP VIEW "item_skus"`, widenSKU, createItemSkus},
		},
		{
			name:     "a view that reads a bare name when a namesake in another schema changes",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, reportingTotals, orderReport}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, reportingTotalsChanged, orderReport}},
			annotated: []string{
				`DROP MATERIALIZED VIEW IF EXISTS "reporting"."order_totals" CASCADE`,
				createReportingTotalsChanged,
			},
			exact: []string{`DROP MATERIALIZED VIEW "reporting"."order_totals"`, createReportingTotalsChanged},
		},
		{
			name:     "a view the plan replaces that reads a bare name when a namesake in another schema changes",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, reportingTotals, bigOrdersReading}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, reportingTotalsChanged, bigOrdersWider}},
			annotated: []string{
				`DROP MATERIALIZED VIEW IF EXISTS "reporting"."order_totals" CASCADE`,
				replaceBigOrdersWider,
				createReportingTotalsChanged,
			},
			exact: []string{`DROP MATERIALIZED VIEW "reporting"."order_totals"`, replaceBigOrdersWider, createReportingTotalsChanged},
		},
		{
			name:     "a view that reads a changed materialized view by its qualified name",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, qualifiedReport}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, qualifiedReport}},
			annotated: []string{
				`DROP VIEW IF EXISTS "qualified_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createQualifiedReport,
			},
			exact: []string{`DROP VIEW "qualified_report"`, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, createQualifiedReport},
		},
		{
			name:      "a changed materialized view nothing reads",
			live:      rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals}},
			declared:  rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged}},
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
	createTotalsChanged     = "CREATE MATERIALIZED VIEW \"order_totals\" AS\nSELECT sum(amount) AS total, count(*) AS n FROM orders"
	createReport            = "CREATE VIEW \"order_report\" AS\nSELECT total FROM order_totals"
	createReportMoved       = "CREATE VIEW \"order_report\" AS\nSELECT 1 AS one, total FROM order_totals"
	createTop               = "CREATE MATERIALIZED VIEW \"order_top\" AS\nSELECT max(total) AS top FROM order_totals"
	createTopReport         = "CREATE VIEW \"order_top_report\" AS\nSELECT top FROM order_top"
	createNewReport         = "CREATE VIEW \"new_report\" AS\nSELECT total, n FROM order_totals"
	createListMoved         = "CREATE VIEW \"order_list\" AS\nSELECT amount, id FROM orders"
	createMix               = "CREATE VIEW \"order_mix\" AS\nSELECT l.id, t.total FROM order_list l, order_totals t"
	createItemSkus          = "CREATE VIEW \"item_skus\" AS\nSELECT id, sku FROM items"
	createSkuCountsChanged  = "CREATE MATERIALIZED VIEW \"sku_counts\" AS\nSELECT sku, count(*) AS n, max(id) AS top FROM item_skus GROUP BY sku"
	createSkuList           = "CREATE VIEW \"item_sku_list\" AS\nSELECT sku FROM item_skus"
	widenSKU                = `ALTER TABLE "items" ALTER COLUMN "sku" TYPE VARCHAR(128)`
	replaceBigOrdersPlain   = "CREATE OR REPLACE VIEW \"big_orders\" AS\nSELECT id, amount FROM orders WHERE amount > 100"
	replaceBigOrdersReading = "CREATE OR REPLACE VIEW \"big_orders\" AS\n" +
		"SELECT id, amount FROM orders WHERE amount > (SELECT total / 10 FROM order_totals)"
	createNewList                = "CREATE VIEW \"new_list\" AS\nSELECT id FROM orders"
	createOrderCount             = "CREATE FUNCTION \"order_count\"() RETURNS bigint AS $$\nSELECT n FROM order_totals\n$$\nLANGUAGE sql STABLE"
	createBigOrdersOfNewList     = "CREATE VIEW \"big_orders\" AS\nSELECT id, amount FROM orders WHERE id IN (SELECT id FROM new_list)"
	createReportingTotalsChanged = "CREATE MATERIALIZED VIEW \"reporting\".\"order_totals\" AS\n" +
		"SELECT sum(amount) AS total, count(*) AS n FROM orders"
	createQualifiedReport = "CREATE VIEW \"qualified_report\" AS\nSELECT total FROM public.order_totals"
	replaceBigOrdersWider = "CREATE OR REPLACE VIEW \"big_orders\" AS\n" +
		"SELECT id, amount, 1 AS one FROM orders WHERE amount > (SELECT total / 10 FROM order_totals)"
)

var (
	// reportRows returns the rows of order_report, so the server records that
	// it depends on the view.
	reportRows = rebuildRoutine{
		name: "report_rows", parameters: "lim integer", identity: "lim integer",
		returns: "SETOF order_report", language: "sql", body: "SELECT * FROM order_report LIMIT lim",
	}
	// reportCount reads order_report in a SQL-standard body, which records the
	// same dependency.
	reportCount = rebuildRoutine{
		name: "report_count", returns: "bigint", language: "sql",
		body: "BEGIN ATOMIC SELECT count(*) FROM order_report; END",
	}
	// reportCountQuoted is reportCount with its body in quotes, which records
	// no dependency.
	reportCountQuoted = rebuildRoutine{name: "report_count", returns: "bigint", language: "sql", body: "SELECT count(*) FROM order_report"}
	// reportInsert is the routine the INSTEAD OF trigger on order_report runs.
	reportInsert = rebuildRoutine{name: "order_report_ins", returns: "trigger", language: "plpgsql", body: "BEGIN RETURN NEW; END"}
	// reportList calls report_rows, so it depends on the routine.
	reportList = rebuildObject{name: "report_list", body: "SELECT total FROM report_rows(10)"}
	// reportRowsOfReport returns order_report's rows, and reportRowsOfAudit,
	// its new declaration, the rows of a view the plan adds.
	reportRowsOfReport = rebuildRoutine{name: "report_rows", returns: "SETOF order_report", language: "sql", body: "SELECT * FROM order_report"}
	reportRowsOfAudit  = rebuildRoutine{name: "report_rows", returns: "SETOF audit_rows", language: "sql", body: "SELECT * FROM audit_rows"}
	auditRows          = rebuildObject{name: "audit_rows", body: "SELECT id FROM orders"}
	publicRowsOfReport = rebuildGrant{role: "PUBLIC", privilege: "EXECUTE", objectType: "FUNCTION", object: "report_rows", implicit: true}
	// rowTotal takes a row of order_report, and rowTotalScaled, its new
	// declaration, one more parameter.
	rowTotal = rebuildRoutine{name: "row_total", parameters: "r order_report", identity: "r order_report",
		returns: "numeric", language: "sql", body: "SELECT r.total"}
	rowTotalScaled = rebuildRoutine{name: "row_total", parameters: "r order_report, factor integer",
		identity: "r order_report, factor integer", returns: "numeric", language: "sql", body: "SELECT r.total * factor"}
	readerRowTotal = rebuildGrant{role: "reader", privilege: "EXECUTE", objectType: "FUNCTION", object: "row_total", arguments: "r order_report"}

	executeReportRows = rebuildGrant{role: "reader", privilege: "EXECUTE", objectType: "FUNCTION", object: "report_rows", arguments: "lim integer"}
	publicReportRows  = rebuildGrant{role: "PUBLIC", privilege: "EXECUTE", objectType: "FUNCTION", object: "report_rows",
		arguments: "lim integer", implicit: true}
	selectReport      = rebuildGrant{role: "reader", privilege: "SELECT", objectType: "TABLE", object: "order_report"}
	selectReportTotal = rebuildGrant{role: "reader", privilege: "SELECT", objectType: "TABLE", object: "order_report", column: "total"}
	analystTotals     = rebuildGrant{role: "analyst", privilege: "SELECT", objectType: "TABLE", object: "order_totals"}
	reporterReport    = rebuildGrant{role: "reporter", privilege: "SELECT", objectType: "TABLE", object: "order_report"}
	// reporterRows names report_rows by its input types alone, as a
	// declaration may.
	reporterRows       = rebuildGrant{role: "reporter", privilege: "EXECUTE", objectType: "FUNCTION", object: "report_rows", arguments: "integer"}
	uniqueTotals       = rebuildIndex{name: "order_totals_total", relation: "order_totals", column: "total", unique: true}
	reportInsteadOfIns = rebuildTrigger{name: "order_report_ins", view: "order_report", function: "order_report_ins"}
	// reportOwnInsert runs a function Ptah writes for it.
	reportOwnInsert = rebuildTrigger{name: "order_report_ins", view: "order_report", body: "RETURN NEW;"}
)

const (
	dropReportRows   = `DROP FUNCTION IF EXISTS "report_rows"(lim integer)`
	createReportRows = "CREATE FUNCTION \"report_rows\"(lim integer) RETURNS SETOF order_report AS $$\n" +
		"SELECT * FROM order_report LIMIT lim\n$$\nLANGUAGE sql STABLE"
	grantReportRows         = `GRANT EXECUTE ON FUNCTION "report_rows"(lim integer) TO "reader"`
	revokeReportRows        = `REVOKE EXECUTE ON FUNCTION "report_rows"(lim integer) FROM PUBLIC`
	createReportCount       = `CREATE FUNCTION "report_count"() RETURNS bigint LANGUAGE sql STABLE BEGIN ATOMIC SELECT count(*) FROM order_report; END`
	revokeReportCount       = `REVOKE EXECUTE ON FUNCTION "report_count"() FROM PUBLIC`
	createReportCountQuoted = "CREATE FUNCTION \"report_count\"() RETURNS bigint AS $$\nSELECT count(*) FROM order_report\n$$\nLANGUAGE sql STABLE"
)

// TestViewRebuild_PutsBackWhatTheDropTook_HappyPath pins what a rebuild puts
// back beside the views and materialized views it drops and creates again
// (stokaro/ptah#4976, stokaro/ptah#4980), in both statement styles.
//
// A routine that returns the rows of a rebuilt view is dropped before the
// view and created after it, and so is a view that calls the routine. It gets
// back the privileges the database held on it; one that PUBLIC could not
// execute has that taken away again, since a new routine is executable by
// PUBLIC. A privilege the plan revokes is not given back first, and one on an
// object the rebuild drops for good is neither given nor revoked: the drop
// took it. A rebuilt materialized view gets its declared index back, and a
// rebuilt view its INSTEAD OF trigger, and both the privileges the database
// held on them, column privileges and those of a role the schema does not
// manage included. A routine with a SQL-standard body records a dependency on
// the view it reads, so it is dropped and created again too, with that body.
func TestViewRebuild_PutsBackWhatTheDropTook_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		live, declared rebuildState
		annotated      []string
		exact          []string
	}{
		{
			name: "a routine that returns the rows of a rebuilt view, with its privileges",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, routines: []rebuildRoutine{reportRows},
				grants: []rebuildGrant{executeReportRows, publicReportRows}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}, routines: []rebuildRoutine{reportRows},
				grants: []rebuildGrant{executeReportRows}},
			annotated: []string{
				dropReportRows,
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createReport,
				createReportRows,
				grantReportRows,
			},
			exact: []string{
				`DROP FUNCTION "report_rows"(lim integer)`,
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
				createReport,
				createReportRows,
				grantReportRows,
			},
		},
		{
			name:     "a routine with a SQL-standard body that reads a rebuilt view",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, routines: []rebuildRoutine{reportCount}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}, routines: []rebuildRoutine{reportCount}},
			annotated: []string{
				`DROP FUNCTION IF EXISTS "report_count"()`,
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createReport,
				createReportCount,
				revokeReportCount,
			},
			exact: []string{
				`DROP FUNCTION "report_count"()`,
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
				createReport,
				createReportCount,
				revokeReportCount,
			},
		},
		{
			name:     "a routine whose SQL-standard body the declaration puts in quotes",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, routines: []rebuildRoutine{reportCount}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}, routines: []rebuildRoutine{reportCountQuoted}},
			annotated: []string{
				`DROP FUNCTION IF EXISTS "report_count"()`,
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createReport,
				createReportCountQuoted,
				revokeReportCount,
			},
			exact: []string{
				`DROP FUNCTION "report_count"()`,
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
				createReport,
				createReportCountQuoted,
				revokeReportCount,
			},
		},
		{
			name:      "a routine PUBLIC could not execute",
			live:      rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, routines: []rebuildRoutine{reportRows}},
			declared:  rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}, routines: []rebuildRoutine{reportRows}},
			annotated: []string{dropReportRows, `DROP VIEW IF EXISTS "order_report" CASCADE`, `DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`, createTotalsChanged, createReport, createReportRows, revokeReportRows},
			exact: []string{`DROP FUNCTION "report_rows"(lim integer)`, `DROP VIEW "order_report"`, `DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged, createReport, createReportRows, revokeReportRows},
		},
		{
			name: "a view that calls a routine that returns the rows of a rebuilt view",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport, reportList}, routines: []rebuildRoutine{reportRows},
				grants: []rebuildGrant{publicReportRows}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport, reportList}, routines: []rebuildRoutine{reportRows}},
			annotated: []string{
				`DROP VIEW IF EXISTS "report_list" CASCADE`,
				dropReportRows,
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createReport,
				createReportRows,
				"CREATE VIEW \"report_list\" AS\nSELECT total FROM report_rows(10)",
			},
			exact: []string{
				`DROP VIEW "report_list"`,
				`DROP FUNCTION "report_rows"(lim integer)`,
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
				createReport,
				createReportRows,
				"CREATE VIEW \"report_list\" AS\nSELECT total FROM report_rows(10)",
			},
		},
		{
			name: "a routine whose result stops being a rebuilt view's rows",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, routines: []rebuildRoutine{reportRowsOfReport},
				grants: []rebuildGrant{publicRowsOfReport}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport, auditRows},
				routines: []rebuildRoutine{reportRowsOfAudit}},
			annotated: []string{
				`DROP FUNCTION IF EXISTS "report_rows"()`,
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createAuditRows,
				createTotalsChanged,
				createReport,
				createRowsOfAudit,
			},
			exact: []string{
				`DROP FUNCTION "report_rows"()`,
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createAuditRows,
				createTotalsChanged,
				createReport,
				createRowsOfAudit,
			},
		},
		{
			name: "a routine whose parameters change, with a privilege on the old ones",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, routines: []rebuildRoutine{rowTotal},
				grants: []rebuildGrant{readerRowTotal}},
			declared:  rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}, routines: []rebuildRoutine{rowTotalScaled}},
			annotated: []string{`DROP FUNCTION IF EXISTS "row_total"(r order_report)`, `DROP VIEW IF EXISTS "order_report" CASCADE`, `DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`, createTotalsChanged, createReport, createRowTotalScaled},
			exact:     []string{`DROP FUNCTION "row_total"(r order_report)`, `DROP VIEW "order_report"`, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, createReport, createRowTotalScaled},
		},
		{
			name: "a trigger whose function Ptah writes, on a rebuilt view",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport},
				triggers: []rebuildTrigger{reportOwnInsert}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport},
				triggers: []rebuildTrigger{reportOwnInsert}},
			annotated: []string{
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createReport,
				replaceOwnInsertFunction,
				replaceOwnInsert,
			},
			exact: []string{
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
				createReport,
				replaceOwnInsertFunction,
				replaceOwnInsert,
			},
		},
		{
			name: "a privilege the plan revokes from a rebuilt view",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, roles: []string{"reporter"},
				grants: []rebuildGrant{reporterReport}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}, roles: []string{"reporter"}},
			annotated: []string{
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createReport,
				`REVOKE SELECT ON TABLE "order_report" FROM "reporter"`,
			},
			exact: []string{
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
				createReport,
				`REVOKE SELECT ON TABLE "order_report" FROM "reporter"`,
			},
		},
		{
			name: "a privilege on a view the rebuild drops for good",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, roles: []string{"reporter"},
				grants: []rebuildGrant{reporterReport}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged}, roles: []string{"reporter"}},
			annotated: []string{
				`DROP VIEW IF EXISTS "public"."order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
			},
			exact: []string{`DROP VIEW "public"."order_report"`, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged},
		},
		{
			name: "a privilege on a routine the rebuild drops for good",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, routines: []rebuildRoutine{reportRows},
				roles: []string{"reporter"}, grants: []rebuildGrant{reporterRows}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}, roles: []string{"reporter"}},
			annotated: []string{
				dropReportRows,
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				createReport,
			},
			exact: []string{
				`DROP FUNCTION "report_rows"(lim integer)`,
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
				createReport,
			},
		},
		{
			name: "an index, a trigger and privileges on rebuilt objects",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, routines: []rebuildRoutine{reportInsert},
				indexes: []rebuildIndex{uniqueTotals}, grants: []rebuildGrant{selectReport, selectReportTotal, analystTotals},
				triggers: []rebuildTrigger{reportInsteadOfIns}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}, routines: []rebuildRoutine{reportInsert},
				indexes: []rebuildIndex{uniqueTotals}, grants: []rebuildGrant{selectReport, selectReportTotal},
				triggers: []rebuildTrigger{reportInsteadOfIns}},
			annotated: []string{
				`DROP VIEW IF EXISTS "order_report" CASCADE`,
				`DROP MATERIALIZED VIEW IF EXISTS "order_totals" CASCADE`,
				createTotalsChanged,
				`CREATE UNIQUE INDEX IF NOT EXISTS "order_totals_total" ON "order_totals" ("total")`,
				`GRANT SELECT ON TABLE "order_totals" TO "analyst"`,
				createReport,
				createReportTrigger,
				`GRANT SELECT ON TABLE "order_report" TO "reader"`,
				`GRANT SELECT ("total") ON TABLE "order_report" TO "reader"`,
			},
			exact: []string{
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
				`CREATE UNIQUE INDEX "order_totals_total" ON "order_totals" ("total")`,
				`GRANT SELECT ON TABLE "order_totals" TO "analyst"`,
				createReport,
				createReportTrigger,
				`GRANT SELECT ON TABLE "order_report" TO "reader"`,
				`GRANT SELECT ("total") ON TABLE "order_report" TO "reader"`,
			},
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
	createReportTrigger = `CREATE TRIGGER "order_report_ins" INSTEAD OF INSERT ON "order_report" FOR EACH ROW ` +
		`EXECUTE FUNCTION "order_report_ins"()`
	createAuditRows = "CREATE VIEW \"audit_rows\" AS\nSELECT id FROM orders"
	// The drop of the view leaves the function Ptah wrote for its trigger, so
	// the trigger writes it with CREATE OR REPLACE.
	replaceOwnInsertFunction = "CREATE OR REPLACE FUNCTION \"ptah_trigger_order__report_order__report__ins\"()\n" +
		"RETURNS trigger AS $$\nBEGIN\nRETURN NEW;\nEND;\n$$ LANGUAGE plpgsql"
	replaceOwnInsert = `CREATE OR REPLACE TRIGGER "order_report_ins" INSTEAD OF INSERT ON "order_report" FOR EACH ROW ` +
		`EXECUTE FUNCTION "ptah_trigger_order__report_order__report__ins"()`
	createRowsOfAudit    = "CREATE FUNCTION \"report_rows\"() RETURNS SETOF audit_rows AS $$\nSELECT * FROM audit_rows\n$$\nLANGUAGE sql STABLE"
	createRowTotalScaled = "CREATE FUNCTION \"row_total\"(r order_report, factor integer) RETURNS numeric AS $$\n" +
		"SELECT r.total * factor\n$$\nLANGUAGE sql STABLE"
)

// TestViewRebuild_KeepsTheOptionsTheDatabaseHeld_HappyPath pins the options a
// view or a materialized view keeps through a plan that writes it again.
// PostgreSQL's CREATE OR REPLACE VIEW sets the options it states and clears
// the others, and a view created again starts with none, so a
// security_invoker view would run with its owner's privileges. Ptah declares
// no option, and every statement that writes a view the database holds
// states the ones it holds.
func TestViewRebuild_KeepsTheOptionsTheDatabaseHeld_HappyPath(t *testing.T) {
	invoker := map[string][]string{"order_report": {"security_invoker=true", "security_barrier=on"}, "order_totals": {"fillfactor=70"}}
	tests := []struct {
		name           string
		live, declared rebuildState
		want           []string
	}{
		{
			name:     "a view replaced in place",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, options: invoker},
			declared: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReportWide}},
			want: []string{"CREATE OR REPLACE VIEW \"order_report\" WITH (security_invoker='true', security_barrier='on') AS\n" +
				"SELECT total, 1 AS one FROM order_totals"},
		},
		{
			name:     "a view and a materialized view created again",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, options: invoker},
			declared: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}},
			want: []string{
				`DROP VIEW "order_report"`,
				`DROP MATERIALIZED VIEW "order_totals"`,
				"CREATE MATERIALIZED VIEW \"order_totals\" WITH (fillfactor='70') AS\nSELECT sum(amount) AS total, count(*) AS n FROM orders",
				"CREATE VIEW \"order_report\" WITH (security_invoker='true', security_barrier='on') AS\nSELECT total FROM order_totals",
			},
		},
		{
			name:     "a view replaced before the drops",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, bigOrdersReading}, options: map[string][]string{"big_orders": {"security_invoker=true"}}},
			declared: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotalsChanged, bigOrdersPlain}},
			want: []string{
				"CREATE OR REPLACE VIEW \"big_orders\" WITH (security_invoker='true') AS\nSELECT id, amount FROM orders WHERE amount > 100",
				`DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planRebuild(c, planext.StatementStyleExact, test.live, test.declared)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// TestViewRebuild_FailurePath pins the rebuild the plan refuses rather than
// drop something it cannot create again: a diff built by hand that changes a
// materialized view and carries no declaration of it would drop the view and
// create nothing.
func TestViewRebuild_FailurePath(t *testing.T) {
	t.Run("a materialized view change without its declaration", func(t *testing.T) {
		c := qt.New(t)
		diff := &difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{{
			ViewName: "order_totals",
			Changes:  map[string]string{"body": "a -> b"},
			Desired:  schemamodel.MaterializedView{Name: "order_totals", Body: "SELECT 2"},
		}}}

		statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(context.Background(), must.Must(builtin.New()),
			diff, "postgres", planext.Options{Capabilities: capability.ForDialect("postgres")})

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, `invalid schema diff: the plan drops materialized view order_totals and creates it again, `+
			`and the diff carries no declaration of it`)
		c.Assert(statements, qt.IsNil)
	})
}
