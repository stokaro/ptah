package planeffect_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/planeffect"
)

// view is a view the database reports in the public schema.
func view(name string, options ...string) catalog.View {
	return catalog.View{Schema: "public", Name: name, Body: "SELECT 1", Options: options}
}

func target(views ...catalog.View) planeffect.Target {
	return planeffect.Target{
		Current:       &catalog.Database{Views: views},
		DefaultSchema: "public",
		Semantics:     identifier.ForDialect("postgres"),
		ViewOptions:   true,
	}
}

// security lists both security options of an effect, barrier first, as an
// effect sorts them.
func security(barrier, invoker planeffect.Property) []planeffect.Property {
	barrier.Name, invoker.Name = "security_barrier", "security_invoker"
	return []planeffect.Property{barrier, invoker}
}

func kept(value bool) planeffect.Property {
	return planeffect.Property{Before: value, After: value}
}

func moved(before, after bool) planeffect.Property {
	return planeffect.Property{Before: before, After: after, Changed: true}
}

func created(value bool) planeffect.Property {
	return planeffect.Property{After: value, Changed: true}
}

func dropped(value bool) planeffect.Property {
	return planeffect.Property{Before: value, Changed: true}
}

var orderTotals = planeffect.Object{Kind: planeffect.KindView, Schema: "public", Name: "order_totals"}

// withoutBeforePlan is effects with no BeforePlan. The tests of what each
// statement changes leave it to the tests of BeforePlan, which pin it for each
// path a view takes through a plan.
func withoutBeforePlan(effects [][]planeffect.Effect) [][]planeffect.Effect {
	for _, statement := range effects {
		for i := range statement {
			statement[i].BeforePlan = nil
		}
	}
	return effects
}

func effect(action planeffect.Action, properties []planeffect.Property) []planeffect.Effect {
	return []planeffect.Effect{{Version: planeffect.Version, Object: orderTotals, Action: action, Properties: properties}}
}

// TestDescribe_ViewSecurity pins the value of each security option before and
// after the statements Ptah plans for a view. A statement that restates an
// option says it is unchanged; one that moves it gives both values.
func TestDescribe_ViewSecurity(t *testing.T) {
	replace := func(options ...string) *ast.CreateViewNode {
		return &ast.CreateViewNode{Name: "order_totals", Body: "SELECT 2", Replace: true, Options: options}
	}
	tests := []struct {
		name   string
		target planeffect.Target
		nodes  []ast.Node
		units  []int
		want   [][]planeffect.Effect
	}{
		{
			name:   "a body change restates security_invoker",
			target: target(view("order_totals", "security_invoker=true")),
			nodes:  []ast.Node{replace("security_invoker=true")},
			units:  []int{0},
			want:   [][]planeffect.Effect{effect(planeffect.ActionReplace, security(kept(false), kept(true)))},
		},
		{
			name:   "the server stores another spelling of the value",
			target: target(view("order_totals", "security_invoker=on", "security_barrier=1")),
			nodes:  []ast.Node{replace("security_invoker=true", "security_barrier=true")},
			units:  []int{0},
			want:   [][]planeffect.Effect{effect(planeffect.ActionReplace, security(kept(true), kept(true)))},
		},
		{
			name:   "a replace turns security_invoker on",
			target: target(view("order_totals")),
			nodes:  []ast.Node{replace("security_invoker=true")},
			units:  []int{0},
			want:   [][]planeffect.Effect{effect(planeffect.ActionReplace, security(kept(false), moved(false, true)))},
		},
		{
			name:   "a replace clears an option it does not state",
			target: target(view("order_totals", "security_invoker=true")),
			nodes:  []ast.Node{replace()},
			units:  []int{0},
			want:   [][]planeffect.Effect{effect(planeffect.ActionReplace, security(kept(false), moved(true, false)))},
		},
		{
			name:   "ALTER VIEW SET turns security_invoker on",
			target: target(view("order_totals", "security_invoker=false")),
			nodes:  []ast.Node{ast.NewAlterViewOptions("order_totals", false).SetOptions([]string{"security_invoker=true"})},
			units:  []int{0},
			want:   [][]planeffect.Effect{effect(planeffect.ActionAlter, security(kept(false), moved(false, true)))},
		},
		{
			name:   "ALTER VIEW RESET turns security_invoker off",
			target: target(view("order_totals", "security_invoker=true")),
			nodes:  []ast.Node{ast.NewAlterViewOptions("order_totals", false).ResetOptions([]string{"security_invoker"})},
			units:  []int{0},
			want:   [][]planeffect.Effect{effect(planeffect.ActionAlter, security(kept(false), moved(true, false)))},
		},
		{
			name:   "SET and RESET are two statements, each from where the one before left the view",
			target: target(view("order_totals", "security_invoker=false", "security_barrier=true")),
			nodes: []ast.Node{ast.NewAlterViewOptions("order_totals", false).
				SetOptions([]string{"security_invoker=true"}).ResetOptions([]string{"security_barrier"})},
			units: []int{0, 0},
			want: [][]planeffect.Effect{
				effect(planeffect.ActionAlter, security(kept(true), moved(false, true))),
				effect(planeffect.ActionAlter, security(moved(true, false), kept(true))),
			},
		},
		{
			name:   "a view the plan drops and creates again",
			target: target(view("order_totals", "security_invoker=true")),
			nodes: []ast.Node{
				ast.NewDropView("order_totals"),
				&ast.CreateViewNode{Name: "order_totals", Body: "SELECT 2", Options: []string{"security_invoker=false"}},
			},
			units: []int{0, 1},
			want: [][]planeffect.Effect{
				effect(planeffect.ActionDrop, security(dropped(false), dropped(true))),
				effect(planeffect.ActionCreate, security(created(false), created(false))),
			},
		},
		{
			name:   "a replace after the plan dropped the view creates it",
			target: target(view("order_totals", "security_invoker=true")),
			nodes:  []ast.Node{ast.NewDropView("order_totals"), replace("security_invoker=true")},
			units:  []int{0, 1},
			want: [][]planeffect.Effect{
				effect(planeffect.ActionDrop, security(dropped(false), dropped(true))),
				effect(planeffect.ActionReplace, security(created(false), created(true))),
			},
		},
		{
			name:   "the comment written after the view has no effect",
			target: target(view("order_totals", "security_invoker=true")),
			nodes: []ast.Node{&ast.CreateViewNode{Name: "order_totals", Body: "SELECT 2", Replace: true,
				Options: []string{"security_invoker=true"}, Comment: "totals"}},
			units: []int{0, 0},
			want:  [][]planeffect.Effect{effect(planeffect.ActionReplace, security(kept(false), kept(true))), nil},
		},
		{
			name:   "a statement about something else has no effect",
			target: target(view("order_totals", "security_invoker=true")),
			nodes:  []ast.Node{ast.NewRawSQL("SELECT 1;"), replace("security_invoker=true")},
			units:  []int{0, 1},
			want:   [][]planeffect.Effect{nil, effect(planeffect.ActionReplace, security(kept(false), kept(true)))},
		},
		{
			name:   "a qualified name finds the view the database reports",
			target: target(view("order_totals", "security_invoker=true")),
			nodes: []ast.Node{&ast.CreateViewNode{Name: "public.order_totals", Body: "SELECT 2", Replace: true,
				Options: []string{"security_invoker=true"}}},
			units: []int{0},
			want:  [][]planeffect.Effect{effect(planeffect.ActionReplace, security(kept(false), kept(true)))},
		},
		{
			name:   "a value the server could not read as a boolean leaves the option out",
			target: target(view("order_totals", "security_invoker=maybe")),
			nodes:  []ast.Node{replace("security_invoker=true")},
			units:  []int{0},
			want: [][]planeffect.Effect{effect(planeffect.ActionReplace, []planeffect.Property{
				{Name: "security_barrier", Before: false, After: false},
			})},
		},
		{
			name:   "an ALTER after a drop that cascades still finds the view's options",
			target: target(view("order_totals", "security_invoker=false")),
			nodes: []ast.Node{
				&ast.DropTableNode{Name: "orders", Cascade: true},
				ast.NewAlterViewOptions("order_totals", false).SetOptions([]string{"security_invoker=true"}),
			},
			units: []int{0, 1},
			want:  [][]planeffect.Effect{nil, effect(planeffect.ActionAlter, security(kept(false), moved(false, true)))},
		},
		{
			name:   "a replace of a view the database does not report, then another",
			target: target(),
			nodes:  []ast.Node{replace("security_invoker=true"), replace("security_invoker=false")},
			units:  []int{0, 1},
			want:   [][]planeffect.Effect{nil, effect(planeffect.ActionReplace, security(kept(false), moved(true, false)))},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planeffect.Describe(test.target, test.nodes, test.units)

			c.Assert(withoutBeforePlan(got), qt.DeepEquals, test.want)
		})
	}
}

// TestDescribe_NewView pins the schema of a view the plan creates: the one its
// name states, and the default schema for a name that states none.
func TestDescribe_NewView(t *testing.T) {
	tests := []struct {
		name string
		view string
		want planeffect.Object
	}{
		{name: "a qualified name", view: "analytics.daily", want: planeffect.Object{Kind: planeffect.KindView, Schema: "analytics", Name: "daily"}},
		{name: "an unqualified name", view: "daily", want: planeffect.Object{Kind: planeffect.KindView, Schema: "public", Name: "daily"}},
		{name: "a quoted name", view: `"Reports"."Daily"`, want: planeffect.Object{Kind: planeffect.KindView, Schema: "Reports", Name: "Daily"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planeffect.Describe(target(), []ast.Node{
				&ast.CreateViewNode{Name: test.view, Body: "SELECT 1", Options: []string{"security_barrier"}},
			}, []int{0})

			c.Assert(got, qt.DeepEquals, [][]planeffect.Effect{{{
				Version: planeffect.Version, Object: test.want, Action: planeffect.ActionCreate,
				Properties: security(created(true), created(false)),
				BeforePlan: &planeffect.Held{},
			}}})
		})
	}
}

// TestDescribe_UnqualifiedNameIsTheConnectionSchemas pins what an unqualified
// view name refers to: the view in the schema the connection resolves it in,
// as the server reads the statement, and never a view of the same name in
// another schema. Reading the other view's options would mark an option the
// statement changes as unchanged.
func TestDescribe_UnqualifiedNameIsTheConnectionSchemas(t *testing.T) {
	inPublic := catalog.View{Schema: "public", Name: "v", Body: "SELECT 1", Options: []string{"security_invoker=true"}}
	inApp := catalog.View{Schema: "app", Name: "v", Body: "SELECT 1"}
	connectedToApp := func(views ...catalog.View) planeffect.Target {
		described := target(views...)
		described.DefaultSchema = "app"
		return described
	}
	replace := func(name string) *ast.CreateViewNode {
		return &ast.CreateViewNode{Name: name, Body: "SELECT 2", Replace: true, Options: []string{"security_invoker=true"}}
	}
	tests := []struct {
		name   string
		target planeffect.Target
		node   ast.Node
		want   [][]planeffect.Effect
	}{
		{
			name:   "the connection's view, beside one of the same name in public",
			target: connectedToApp(inPublic, inApp),
			node:   replace("v"),
			want: [][]planeffect.Effect{{{
				Version: planeffect.Version, Action: planeffect.ActionReplace,
				Object:     planeffect.Object{Kind: planeffect.KindView, Schema: "app", Name: "v"},
				Properties: security(kept(false), moved(false, true)),
			}}},
		},
		{
			name:   "a name that states public",
			target: connectedToApp(inPublic, inApp),
			node:   replace("public.v"),
			want: [][]planeffect.Effect{{{
				Version: planeffect.Version, Action: planeffect.ActionReplace,
				Object:     planeffect.Object{Kind: planeffect.KindView, Schema: "public", Name: "v"},
				Properties: security(kept(false), kept(true)),
			}}},
		},
		// The database the plan read holds no app.v, so what the replace finds
		// there is unknown, and it carries no effect rather than public.v's.
		{name: "a view only public holds", target: connectedToApp(inPublic), node: replace("v")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planeffect.Describe(test.target, []ast.Node{test.node}, []int{0})

			c.Assert(withoutBeforePlan(got), qt.DeepEquals, test.want)
		})
	}
}

// TestDescribe_Undescribed pins the statements that carry no effect: the
// description says nothing it cannot establish, and a consumer reads the SQL of
// such a statement instead.
func TestDescribe_Undescribed(t *testing.T) {
	invoker := view("order_totals", "security_invoker=true")
	replace := &ast.CreateViewNode{Name: "order_totals", Body: "SELECT 2", Replace: true, Options: []string{"security_invoker=true"}}
	withoutOptions := target(invoker)
	withoutOptions.ViewOptions = false
	tests := []struct {
		name   string
		target planeffect.Target
		nodes  []ast.Node
		units  []int
		want   [][]planeffect.Effect
	}{
		{name: "a target that keeps no view options", target: withoutOptions, nodes: []ast.Node{replace}, units: []int{0}},
		{name: "no statement", target: target(invoker), nodes: []ast.Node{replace}},
		{name: "a replace of a view the database does not report", target: target(), nodes: []ast.Node{replace}, units: []int{0}},
		{
			name:   "a replace after a drop that cascades",
			target: target(invoker),
			nodes:  []ast.Node{&ast.DropTableNode{Name: "orders", Cascade: true}, replace},
			units:  []int{0, 1},
		},
		{
			name:   "a DROP VIEW IF EXISTS after a drop that cascades",
			target: target(invoker),
			nodes:  []ast.Node{&ast.DropTypeNode{Name: "status", Cascade: true}, ast.NewDropView("order_totals").SetIfExists()},
			units:  []int{0, 1},
		},
		{
			name:   "a replace after another view's drop that cascades",
			target: target(invoker, view("orders_open")),
			nodes:  []ast.Node{ast.NewDropView("orders_open").SetCascade(), replace},
			units:  []int{0, 1},
			want: [][]planeffect.Effect{{{
				Version: planeffect.Version, Action: planeffect.ActionDrop,
				Object:     planeffect.Object{Kind: planeffect.KindView, Schema: "public", Name: "orders_open"},
				Properties: security(dropped(false), dropped(false)),
			}}, nil},
		},
		{
			name:   "a replace after a column drop that cascades",
			target: target(invoker),
			nodes: []ast.Node{
				&ast.AlterTableNode{Name: "orders", Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "total", Cascade: true}}},
				replace,
			},
			units: []int{0, 1},
		},
		// The description reads the Cascade flag of any node rather than a list
		// of node types, so the drops below, which no list named, also leave the
		// view in a state it cannot report.
		{
			name:   "a replace after a sequence drop that cascades",
			target: target(invoker),
			nodes:  []ast.Node{&ast.DropSequenceNode{Name: "orders_seq", Cascade: true}, replace},
			units:  []int{0, 1},
		},
		{
			name:   "a replace after an index drop that cascades",
			target: target(invoker),
			nodes:  []ast.Node{&ast.DropIndexNode{Name: "orders_total_idx", Cascade: true}, replace},
			units:  []int{0, 1},
		},
		{
			name:   "a replace after a trigger drop that cascades",
			target: target(invoker),
			nodes:  []ast.Node{&ast.DropTriggerNode{Name: "orders_audit", Table: "orders", Cascade: true}, replace},
			units:  []int{0, 1},
		},
		{
			name:   "a materialized view's storage parameters",
			target: target(invoker),
			nodes:  []ast.Node{ast.NewAlterViewOptions("order_totals", true).SetOptions([]string{"fillfactor=70"})},
			units:  []int{0},
		},
		{
			name:   "a node that rendered more statements than it writes, and what follows it",
			target: target(invoker),
			nodes:  []ast.Node{replace, ast.NewAlterViewOptions("order_totals", false).ResetOptions([]string{"security_invoker"})},
			units:  []int{0, 0, 1},
		},
		{
			name:   "an ALTER of a view the plan dropped",
			target: target(invoker),
			nodes:  []ast.Node{ast.NewDropView("order_totals"), ast.NewAlterViewOptions("order_totals", false).ResetOptions([]string{"security_invoker"})},
			units:  []int{0, 1},
			want:   [][]planeffect.Effect{effect(planeffect.ActionDrop, security(dropped(false), dropped(true))), nil},
		},
		{name: "a statement of no node", target: target(invoker), nodes: []ast.Node{replace}, units: []int{1}},
		{
			name:   "a name that is not a view's",
			target: target(invoker),
			nodes:  []ast.Node{&ast.CreateViewNode{Name: "a.b.c", Body: "SELECT 1"}},
			units:  []int{0},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planeffect.Describe(test.target, test.nodes, test.units)

			c.Assert(withoutBeforePlan(got), qt.DeepEquals, test.want)
		})
	}
}

// TestEffect_JSON pins the shape a plan and the documents record: a value that
// does not exist is absent, and changed is always written, because false is
// what says a statement restates an option.
func TestEffect_JSON(t *testing.T) {
	tests := []struct {
		name   string
		effect planeffect.Effect
		want   string
	}{
		{
			name: "an option restated",
			effect: planeffect.Effect{Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionReplace,
				Properties: []planeffect.Property{{Name: "security_invoker", Before: true, After: true}}},
			want: `{"version":1,"object":{"kind":"view","schema":"public","name":"order_totals"},"action":"replace",` +
				`"properties":[{"name":"security_invoker","before":true,"after":true,"changed":false}]}`,
		},
		{
			name: "a view created",
			effect: planeffect.Effect{Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionCreate,
				Properties: []planeffect.Property{{Name: "security_invoker", After: false, Changed: true}}},
			want: `{"version":1,"object":{"kind":"view","schema":"public","name":"order_totals"},"action":"create",` +
				`"properties":[{"name":"security_invoker","after":false,"changed":true}]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := json.Marshal(test.effect)

			c.Assert(err, qt.IsNil)
			c.Assert(string(got), qt.Equals, test.want)
		})
	}
}

// mysqlTarget is a MySQL database that reports the views given in the
// database app.
func mysqlTarget(views ...catalog.View) planeffect.Target {
	return planeffect.Target{
		Current:         &catalog.Database{Views: views},
		DefaultSchema:   "app",
		Semantics:       identifier.ForDialect("mysql"),
		ViewSQLSecurity: true,
	}
}

// mysqlView is a view the database app reports, with the SQL SECURITY mode
// given.
func mysqlView(name, security string) catalog.View {
	return catalog.View{Schema: "app", Name: name, Body: "SELECT 1", Security: security}
}

var appOrderTotals = planeffect.Object{Kind: planeffect.KindView, Schema: "app", Name: "order_totals"}

// sqlSecurity is the effect of one statement on app.order_totals that reports
// its SQL SECURITY mode alone.
func sqlSecurity(action planeffect.Action, property planeffect.Property) []planeffect.Effect {
	property.Name = planeffect.SQLSecurity
	return []planeffect.Effect{{Version: planeffect.Version, Object: appOrderTotals, Action: action, Properties: []planeffect.Property{property}}}
}

// TestDescribe_ViewSQLSecurity pins the SQL SECURITY mode a MySQL or MariaDB
// view holds before and after each statement Ptah plans for it. A statement
// that names no mode gives the view the server's DEFINER, which is a change for
// a view held as INVOKER.
func TestDescribe_ViewSQLSecurity(t *testing.T) {
	replace := func(security string) *ast.CreateViewNode {
		return &ast.CreateViewNode{Name: "order_totals", Body: "SELECT 2", Replace: true, Security: security}
	}
	tests := []struct {
		name   string
		target planeffect.Target
		nodes  []ast.Node
		units  []int
		want   [][]planeffect.Effect
	}{
		{
			name:   "a replace that restates INVOKER",
			target: mysqlTarget(mysqlView("order_totals", "INVOKER")),
			nodes:  []ast.Node{replace("INVOKER")},
			units:  []int{0},
			want: [][]planeffect.Effect{sqlSecurity(planeffect.ActionReplace,
				planeffect.Property{Before: "INVOKER", After: "INVOKER"})},
		},
		{
			name:   "a replace that names no mode gives an INVOKER view the server's DEFINER",
			target: mysqlTarget(mysqlView("order_totals", "INVOKER")),
			nodes:  []ast.Node{replace("")},
			units:  []int{0},
			want: [][]planeffect.Effect{sqlSecurity(planeffect.ActionReplace,
				planeffect.Property{Before: "INVOKER", After: "DEFINER", Changed: true})},
		},
		{
			name:   "a replace that names no mode keeps a DEFINER view's mode",
			target: mysqlTarget(mysqlView("order_totals", "DEFINER")),
			nodes:  []ast.Node{replace("")},
			units:  []int{0},
			want: [][]planeffect.Effect{sqlSecurity(planeffect.ActionReplace,
				planeffect.Property{Before: "DEFINER", After: "DEFINER"})},
		},
		{
			name:   "a replace that turns INVOKER on, spelled in lower case",
			target: mysqlTarget(mysqlView("order_totals", "DEFINER")),
			nodes:  []ast.Node{replace("invoker")},
			units:  []int{0},
			want: [][]planeffect.Effect{sqlSecurity(planeffect.ActionReplace,
				planeffect.Property{Before: "DEFINER", After: "INVOKER", Changed: true})},
		},
		{
			name:   "a new view",
			target: mysqlTarget(),
			nodes:  []ast.Node{&ast.CreateViewNode{Name: "order_totals", Body: "SELECT 1", Comment: "totals"}},
			units:  []int{0},
			want: [][]planeffect.Effect{sqlSecurity(planeffect.ActionCreate,
				planeffect.Property{After: "DEFINER", Changed: true})},
		},
		{
			name:   "a drop",
			target: mysqlTarget(mysqlView("order_totals", "INVOKER")),
			nodes:  []ast.Node{ast.NewDropView("order_totals").SetIfExists()},
			units:  []int{0},
			want: [][]planeffect.Effect{sqlSecurity(planeffect.ActionDrop,
				planeffect.Property{Before: "INVOKER", Changed: true})},
		},
		{
			name:   "a mode the catalog did not report leaves the property out",
			target: mysqlTarget(mysqlView("order_totals", "")),
			nodes:  []ast.Node{replace("INVOKER")},
			units:  []int{0},
			want: [][]planeffect.Effect{{{
				Version: planeffect.Version, Object: appOrderTotals, Action: planeffect.ActionReplace,
			}}},
		},
		{
			name: "a target with both properties reports them by name",
			target: planeffect.Target{
				Current:         &catalog.Database{Views: []catalog.View{{Schema: "app", Name: "order_totals", Security: "INVOKER"}}},
				DefaultSchema:   "app",
				Semantics:       identifier.ForDialect("mysql"),
				ViewOptions:     true,
				ViewSQLSecurity: true,
			},
			nodes: []ast.Node{replace("INVOKER")},
			units: []int{0},
			want: [][]planeffect.Effect{{{
				Version: planeffect.Version, Object: appOrderTotals, Action: planeffect.ActionReplace,
				Properties: []planeffect.Property{
					{Name: "security_barrier", Before: false, After: false},
					{Name: "security_invoker", Before: false, After: false},
					{Name: planeffect.SQLSecurity, Before: "INVOKER", After: "INVOKER"},
				},
			}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planeffect.Describe(test.target, test.nodes, test.units)

			c.Assert(withoutBeforePlan(got), qt.DeepEquals, test.want)
		})
	}
}

// TestEffect_JSONSQLSecurity pins the shape of a MySQL view's SQL SECURITY in a
// plan and the documents: the mode is a string, as the catalog spells it.
func TestEffect_JSONSQLSecurity(t *testing.T) {
	c := qt.New(t)
	effect := planeffect.Effect{Version: planeffect.Version, Object: appOrderTotals, Action: planeffect.ActionReplace,
		Properties: []planeffect.Property{{Name: planeffect.SQLSecurity, Before: "INVOKER", After: "INVOKER"}}}

	got, err := json.Marshal(effect)

	c.Assert(err, qt.IsNil)
	c.Assert(string(got), qt.Equals, `{"version":1,"object":{"kind":"view","schema":"app","name":"order_totals"},"action":"replace",`+
		`"properties":[{"name":"sql_security","before":"INVOKER","after":"INVOKER","changed":false}]}`)
}

// TestEffect_JSONRoundTrip pins that a plan file read back carries each
// property's value with its own type, so a document that lists a saved plan's
// effects writes the ones the plan recorded.
func TestEffect_JSONRoundTrip(t *testing.T) {
	c := qt.New(t)
	saved := []planeffect.Effect{{Version: planeffect.Version, Object: appOrderTotals, Action: planeffect.ActionReplace,
		Properties: []planeffect.Property{
			{Name: "security_invoker", Before: true, After: false, Changed: true},
			{Name: planeffect.SQLSecurity, Before: "INVOKER", After: "DEFINER", Changed: true},
		},
		BeforePlan: &planeffect.Held{Exists: true, Properties: []planeffect.HeldProperty{
			{Name: "security_invoker", Value: true}, {Name: planeffect.SQLSecurity, Value: "INVOKER"},
		}},
	}}
	encoded, err := json.Marshal(saved)
	c.Assert(err, qt.IsNil)

	var got []planeffect.Effect
	err = json.Unmarshal(encoded, &got)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, saved)
}

// TestDescribe_BeforePlan pins what each effect records about its view before
// the plan: whether the database held it when the plan started, and what it
// held, whatever the statements ahead of the effect did to it. A create after
// a drop that recorded no effect, such as one a cascade left uncertain, is a
// view the plan makes again when the view existed before the plan, and a new
// view when it did not.
func TestDescribe_BeforePlan(t *testing.T) {
	invoker := view("order_totals", "security_invoker=true")
	invokerBeforePlan := &planeffect.Held{Exists: true, Properties: []planeffect.HeldProperty{
		{Name: "security_barrier", Value: false}, {Name: "security_invoker", Value: true},
	}}
	create := &ast.CreateViewNode{Name: "order_totals", Body: "SELECT 2"}
	tests := []struct {
		name   string
		target planeffect.Target
		nodes  []ast.Node
		units  []int
		want   [][]planeffect.Effect
	}{
		{
			name:   "a replace of a view the database holds",
			target: target(invoker),
			nodes:  []ast.Node{&ast.CreateViewNode{Name: "order_totals", Body: "SELECT 2", Replace: true}},
			units:  []int{0},
			want: [][]planeffect.Effect{{{
				Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionReplace,
				Properties: security(kept(false), moved(true, false)), BeforePlan: invokerBeforePlan,
			}}},
		},
		{
			name:   "a create after a drop that cascades and a drop that records no effect",
			target: target(invoker),
			nodes: []ast.Node{
				&ast.DropTableNode{Name: "orders", Cascade: true},
				ast.NewDropView("order_totals").SetIfExists(),
				create,
			},
			units: []int{0, 1, 2},
			want: [][]planeffect.Effect{nil, nil, {{
				Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionCreate,
				Properties: security(created(false), created(false)), BeforePlan: invokerBeforePlan,
			}}},
		},
		{
			name:   "a create after a drop that records the view",
			target: target(invoker),
			nodes:  []ast.Node{ast.NewDropView("order_totals"), create},
			units:  []int{0, 1},
			want: [][]planeffect.Effect{
				{{
					Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionDrop,
					Properties: security(dropped(false), dropped(true)), BeforePlan: invokerBeforePlan,
				}},
				{{
					Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionCreate,
					Properties: security(created(false), created(false)), BeforePlan: invokerBeforePlan,
				}},
			},
		},
		{
			name:   "a new view, and a replace of it",
			target: target(),
			nodes:  []ast.Node{create, &ast.CreateViewNode{Name: "order_totals", Body: "SELECT 3", Replace: true}},
			units:  []int{0, 1},
			want: [][]planeffect.Effect{
				{{
					Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionCreate,
					Properties: security(created(false), created(false)), BeforePlan: &planeffect.Held{},
				}},
				{{
					Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionReplace,
					Properties: security(kept(false), kept(false)), BeforePlan: &planeffect.Held{},
				}},
			},
		},
		{
			name: "a plan that read no catalog",
			target: planeffect.Target{
				DefaultSchema: "public",
				Semantics:     identifier.ForDialect("postgres"),
				ViewOptions:   true,
			},
			nodes: []ast.Node{create},
			units: []int{0},
			want: [][]planeffect.Effect{{{
				Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionCreate,
				Properties: security(created(false), created(false)),
			}}},
		},
		{
			name:   "a MySQL view's mode",
			target: mysqlTarget(mysqlView("order_totals", "INVOKER")),
			nodes:  []ast.Node{ast.NewDropView("order_totals").SetIfExists(), create},
			units:  []int{0, 1},
			want: [][]planeffect.Effect{
				{{
					Version: planeffect.Version, Object: appOrderTotals, Action: planeffect.ActionDrop,
					Properties: []planeffect.Property{{Name: planeffect.SQLSecurity, Before: "INVOKER", Changed: true}},
					BeforePlan: &planeffect.Held{Exists: true, Properties: []planeffect.HeldProperty{{Name: planeffect.SQLSecurity, Value: "INVOKER"}}},
				}},
				{{
					Version: planeffect.Version, Object: appOrderTotals, Action: planeffect.ActionCreate,
					Properties: []planeffect.Property{{Name: planeffect.SQLSecurity, After: "DEFINER", Changed: true}},
					BeforePlan: &planeffect.Held{Exists: true, Properties: []planeffect.HeldProperty{{Name: planeffect.SQLSecurity, Value: "INVOKER"}}},
				}},
			},
		},
		{
			name:   "a value the server could not read leaves the property out",
			target: mysqlTarget(mysqlView("order_totals", "")),
			nodes:  []ast.Node{ast.NewDropView("order_totals")},
			units:  []int{0},
			want: [][]planeffect.Effect{{{
				Version: planeffect.Version, Object: appOrderTotals, Action: planeffect.ActionDrop,
				BeforePlan: &planeffect.Held{Exists: true},
			}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planeffect.Describe(test.target, test.nodes, test.units)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestEffect_JSONBeforePlan pins the shape of before_plan: exists is always
// written, because false is what says a view is new, and a view that did not
// exist lists no properties.
func TestEffect_JSONBeforePlan(t *testing.T) {
	tests := []struct {
		name       string
		beforePlan *planeffect.Held
		want       string
	}{
		{
			name: "a view the plan makes again",
			beforePlan: &planeffect.Held{Exists: true, Properties: []planeffect.HeldProperty{
				{Name: "security_invoker", Value: true},
			}},
			want: `"before_plan":{"exists":true,"properties":[{"name":"security_invoker","value":true}]}}`,
		},
		{name: "a new view", beforePlan: &planeffect.Held{}, want: `"before_plan":{"exists":false}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effect := planeffect.Effect{Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionCreate,
				Properties: []planeffect.Property{{Name: "security_invoker", After: false, Changed: true}}, BeforePlan: test.beforePlan}

			got, err := json.Marshal(effect)

			c.Assert(err, qt.IsNil)
			c.Assert(string(got), qt.Equals, `{"version":1,"object":{"kind":"view","schema":"public","name":"order_totals"},"action":"create",`+
				`"properties":[{"name":"security_invoker","after":false,"changed":true}],`+test.want)
		})
	}
}
