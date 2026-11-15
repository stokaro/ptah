package planeffect_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/planeffect"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

const pipelineTable = "CREATE TABLE orders (id integer PRIMARY KEY, total integer NOT NULL);\n"

// statementEffect is one statement of a plan the PostgreSQL planner wrote, and
// what the description says it does to the view.
type statementEffect struct {
	SQL        string
	Action     planeffect.Action
	Properties []planeffect.Property
}

// describePlan plans held into declared, two SQL schemas, as `schema plan`
// does against PostgreSQL, and describes each statement of the plan.
func describePlan(c *qt.C, held, declared string) []statementEffect {
	c.Helper()
	runtime := must.Must(builtin.New())
	current, _, err := sqlschema.Read([]byte(pipelineTable+held), platform.Postgres)
	c.Assert(err, qt.IsNil)
	desired, _, err := sqlschema.Read([]byte(pipelineTable+declared), platform.Postgres)
	c.Assert(err, qt.IsNil)
	database, err := goschematodb.ToDBSchema(c.Context(), &current, platform.Postgres, runtime)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(c.Context(), &desired, database, platform.Postgres, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := planner.GenerateSchemaDiffRenderedPlan(c.Context(), runtime, diff, platform.Postgres,
		planext.Options{Capabilities: capability.Postgres17()})
	c.Assert(err, qt.IsNil)
	units, err := plan.Units()
	c.Assert(err, qt.IsNil)
	nodes := make([]int, len(units))
	for i, unit := range units {
		nodes[i] = unit.Node
	}

	described := planeffect.Describe(planeffect.Target{
		Current:       database,
		DefaultSchema: "public",
		Semantics:     diff.EffectiveIdentifierSemantics(platform.Postgres),
		ViewOptions:   true,
	}, plan.Request.Nodes, nodes)

	c.Assert(described, qt.HasLen, len(units))
	got := make([]statementEffect, len(units))
	for i, unit := range units {
		c.Assert(described[i], qt.HasLen, 1, qt.Commentf("statement %d: %s", i, unit.Fragment.SQL))
		c.Assert(described[i][0].Object, qt.Equals, orderTotals)
		got[i] = statementEffect{SQL: unit.Fragment.SQL, Action: described[i][0].Action, Properties: described[i][0].Properties}
	}
	return got
}

// orderTotalsView is the view order_totals with the options and body given.
func orderTotalsView(options, body string) string {
	return "CREATE VIEW order_totals WITH (" + options + ") AS " + body + ";\n"
}

// TestDescribe_PostgreSQLPlans runs the PostgreSQL planner and renderer over
// each path a view change takes, and pins the description of every statement
// they write. It is what holds the description to the statements a node
// renders: a renderer that wrote them in another number or order would move
// the values onto the wrong statement here.
func TestDescribe_PostgreSQLPlans(t *testing.T) {
	const (
		positive = "SELECT id, total FROM orders WHERE total > 0"
		appended = "SELECT id, total, total * 2 AS doubled FROM orders"
		narrowed = "SELECT id FROM orders"
	)
	held := orderTotalsView("security_invoker = true", "SELECT id, total FROM orders")
	tests := []struct {
		name     string
		held     string
		declared string
		want     []statementEffect
	}{
		{
			name:     "a body change keeps security_invoker",
			held:     held,
			declared: orderTotalsView("security_invoker = true", positive),
			want: []statementEffect{{
				SQL:    "CREATE OR REPLACE VIEW \"order_totals\" WITH (security_invoker=true) AS\n" + positive,
				Action: planeffect.ActionReplace, Properties: security(kept(false), kept(true)),
			}},
		},
		{
			name:     "an option change alone",
			held:     held,
			declared: orderTotalsView("security_invoker = false", "SELECT id, total FROM orders"),
			want: []statementEffect{{
				SQL:    "ALTER VIEW \"order_totals\" SET (security_invoker=false)",
				Action: planeffect.ActionAlter, Properties: security(kept(false), moved(true, false)),
			}},
		},
		{
			name:     "an option set and one reset",
			held:     orderTotalsView("security_invoker = false", "SELECT id, total FROM orders"),
			declared: orderTotalsView("security_barrier = true", "SELECT id, total FROM orders"),
			want: []statementEffect{
				{
					SQL:    "ALTER VIEW \"order_totals\" SET (security_barrier=true)",
					Action: planeffect.ActionAlter, Properties: security(moved(false, true), kept(false)),
				},
				{
					SQL:    "ALTER VIEW \"order_totals\" RESET (security_invoker)",
					Action: planeffect.ActionAlter, Properties: security(kept(true), kept(false)),
				},
			},
		},
		{
			name:     "a new column and an option change in one replace",
			held:     held,
			declared: orderTotalsView("security_invoker = false", appended),
			want: []statementEffect{{
				SQL:    "CREATE OR REPLACE VIEW \"order_totals\" WITH (security_invoker=false) AS\n" + appended,
				Action: planeffect.ActionReplace, Properties: security(kept(false), moved(true, false)),
			}},
		},
		{
			name:     "a lost column drops the view and creates it again",
			held:     held,
			declared: orderTotalsView("security_invoker = true", narrowed),
			want: []statementEffect{
				{SQL: "DROP VIEW IF EXISTS \"order_totals\" CASCADE", Action: planeffect.ActionDrop, Properties: security(dropped(false), dropped(true))},
				{
					SQL:    "CREATE VIEW \"order_totals\" WITH (security_invoker=true) AS\n" + narrowed,
					Action: planeffect.ActionCreate, Properties: security(created(false), created(true)),
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := describePlan(c, test.held, test.declared)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

const mysqlPipelineTable = "CREATE TABLE orders (id int PRIMARY KEY, total int NOT NULL);\n"

// describeMySQLPlan plans held into declared, two MySQL SQL schemas, as
// `schema plan` does against MySQL, and describes each statement of the plan.
func describeMySQLPlan(c *qt.C, held, declared string) []statementEffect {
	c.Helper()
	runtime := must.Must(builtin.New())
	current, _, err := sqlschema.Read([]byte(mysqlPipelineTable+held), platform.MySQL)
	c.Assert(err, qt.IsNil)
	desired, _, err := sqlschema.Read([]byte(mysqlPipelineTable+declared), platform.MySQL)
	c.Assert(err, qt.IsNil)
	database, err := goschematodb.ToDBSchema(c.Context(), &current, platform.MySQL, runtime)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(c.Context(), &desired, database, platform.MySQL, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := planner.GenerateSchemaDiffRenderedPlan(c.Context(), runtime, diff, platform.MySQL,
		planext.Options{Capabilities: capability.MySQL84()})
	c.Assert(err, qt.IsNil)
	units, err := plan.Units()
	c.Assert(err, qt.IsNil)
	nodes := make([]int, len(units))
	for i, unit := range units {
		nodes[i] = unit.Node
	}

	described := planeffect.Describe(planeffect.Target{
		Current:         database,
		Semantics:       diff.EffectiveIdentifierSemantics(platform.MySQL),
		ViewSQLSecurity: true,
	}, plan.Request.Nodes, nodes)

	c.Assert(described, qt.HasLen, len(units))
	got := make([]statementEffect, len(units))
	for i, unit := range units {
		c.Assert(described[i], qt.HasLen, 1, qt.Commentf("statement %d: %s", i, unit.Fragment.SQL))
		c.Assert(described[i][0].Object, qt.Equals, planeffect.Object{Kind: planeffect.KindView, Name: "order_totals"})
		got[i] = statementEffect{SQL: unit.Fragment.SQL, Action: described[i][0].Action, Properties: described[i][0].Properties}
	}
	return got
}

// mysqlOrderTotalsView is the MySQL view order_totals with the SQL SECURITY
// clause and body given; an empty clause states no mode.
func mysqlOrderTotalsView(security, body string) string {
	create := "CREATE "
	if security != "" {
		create += security + " "
	}
	return create + "VIEW order_totals AS " + body + ";\n"
}

// TestDescribe_MySQLPlans runs the MySQL planner and renderer over a view
// change, and pins the statement it writes and the description of it. A
// declared mode is written and described as declared, and a declaration that
// names no mode over a view held as DEFINER writes no clause and describes the
// server default.
func TestDescribe_MySQLPlans(t *testing.T) {
	const (
		held     = "SELECT id, total FROM orders"
		positive = "SELECT id, total FROM orders WHERE total > 0"
	)
	tests := []struct {
		name     string
		held     string
		declared string
		want     []statementEffect
	}{
		{
			name:     "a body change keeps a declared INVOKER",
			held:     mysqlOrderTotalsView("SQL SECURITY INVOKER", held),
			declared: mysqlOrderTotalsView("SQL SECURITY INVOKER", positive),
			want: []statementEffect{{
				SQL:    "CREATE OR REPLACE SQL SECURITY INVOKER VIEW `order_totals` AS\n" + positive,
				Action: planeffect.ActionReplace,
				Properties: []planeffect.Property{
					{Name: planeffect.SQLSecurity, Before: "INVOKER", After: "INVOKER"},
				},
			}},
		},
		{
			name:     "a body change to a DEFINER view that names no mode",
			held:     mysqlOrderTotalsView("SQL SECURITY DEFINER", held),
			declared: mysqlOrderTotalsView("", positive),
			want: []statementEffect{{
				SQL:    "CREATE OR REPLACE VIEW `order_totals` AS\n" + positive,
				Action: planeffect.ActionReplace,
				Properties: []planeffect.Property{
					{Name: planeffect.SQLSecurity, Before: "DEFINER", After: "DEFINER"},
				},
			}},
		},
		{
			name:     "a declared DEFINER replaces INVOKER",
			held:     mysqlOrderTotalsView("SQL SECURITY INVOKER", held),
			declared: mysqlOrderTotalsView("SQL SECURITY DEFINER", held),
			want: []statementEffect{{
				SQL:    "CREATE OR REPLACE SQL SECURITY DEFINER VIEW `order_totals` AS\n" + held,
				Action: planeffect.ActionReplace,
				Properties: []planeffect.Property{
					{Name: planeffect.SQLSecurity, Before: "INVOKER", After: "DEFINER", Changed: true},
				},
			}},
		},
		{
			name:     "a new view takes the server's DEFINER",
			held:     "",
			declared: mysqlOrderTotalsView("", held),
			want: []statementEffect{{
				SQL:        "CREATE VIEW `order_totals` AS\n" + held,
				Action:     planeffect.ActionCreate,
				Properties: []planeffect.Property{{Name: planeffect.SQLSecurity, After: "DEFINER", Changed: true}},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := describeMySQLPlan(c, test.held, test.declared)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
