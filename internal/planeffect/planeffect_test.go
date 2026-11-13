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
	return planeffect.Property{Before: new(value), After: new(value)}
}

func moved(before, after bool) planeffect.Property {
	return planeffect.Property{Before: new(before), After: new(after), Changed: true}
}

func created(value bool) planeffect.Property {
	return planeffect.Property{After: new(value), Changed: true}
}

func dropped(value bool) planeffect.Property {
	return planeffect.Property{Before: new(value), Changed: true}
}

var orderTotals = planeffect.Object{Kind: planeffect.KindView, Schema: "public", Name: "order_totals"}

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
				{Name: "security_barrier", Before: new(false), After: new(false)},
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

			c.Assert(got, qt.DeepEquals, test.want)
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

			c.Assert(got, qt.DeepEquals, test.want)
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

			c.Assert(got, qt.DeepEquals, test.want)
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
				Properties: []planeffect.Property{{Name: "security_invoker", Before: new(true), After: new(true)}}},
			want: `{"version":1,"object":{"kind":"view","schema":"public","name":"order_totals"},"action":"replace",` +
				`"properties":[{"name":"security_invoker","before":true,"after":true,"changed":false}]}`,
		},
		{
			name: "a view created",
			effect: planeffect.Effect{Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionCreate,
				Properties: []planeffect.Property{{Name: "security_invoker", After: new(false), Changed: true}}},
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
