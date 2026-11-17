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
// does against PostgreSQL, and describes each statement of the plan. Each
// statement has to carry one effect, on order_totals.
func describePlan(c *qt.C, held, declared string) []statementEffect {
	c.Helper()
	statements, described := planPostgres(c, held, declared)
	c.Assert(described, qt.HasLen, len(statements))
	got := make([]statementEffect, len(statements))
	for i, statement := range statements {
		c.Assert(described[i], qt.HasLen, 1, qt.Commentf("statement %d: %s", i, statement))
		c.Assert(described[i][0].Object, qt.Equals, orderTotals)
		got[i] = statementEffect{SQL: statement, Action: described[i][0].Action, Properties: described[i][0].Properties}
	}
	return got
}

// planPostgres plans held into declared, two SQL schemas, as `schema plan`
// does against PostgreSQL, and returns each statement of the plan and its
// description.
func planPostgres(c *qt.C, held, declared string) ([]string, [][]planeffect.Effect) {
	c.Helper()
	return planOn(c, platform.Postgres, capability.Postgres17(), held, declared)
}

func planOn(c *qt.C, dialect string, capabilities capability.Capabilities, held, declared string) ([]string, [][]planeffect.Effect) {
	c.Helper()
	runtime := must.Must(builtin.New())
	current, _, err := sqlschema.Read([]byte(pipelineTable+held), dialect)
	c.Assert(err, qt.IsNil)
	desired, _, err := sqlschema.Read([]byte(pipelineTable+declared), dialect)
	c.Assert(err, qt.IsNil)
	database, err := goschematodb.ToDBSchema(c.Context(), &current, dialect, runtime)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(c.Context(), &desired, database, dialect, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := planner.GenerateSchemaDiffRenderedPlan(c.Context(), runtime, diff, dialect,
		planext.Options{Capabilities: capabilities})
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
		Semantics:     diff.EffectiveIdentifierSemantics(dialect),
		ViewOptions:   true,
	}, plan.Request.Nodes, nodes)
	statements := make([]string, len(units))
	for i, unit := range units {
		statements[i] = unit.Fragment.SQL
	}
	return statements, described
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

// TestDescribe_PostgreSQLPlanRecordsTheViewsItTakes plans a column lost from
// two views, one reading the other, on PostgreSQL, in the planner's default
// statement style. The plan drops the reader with CASCADE first, so the
// description cannot say the second drop, IF EXISTS, finds its view, and that
// drop records no effect. The CREATE VIEW after it records what the view held
// before the plan, which is what tells it from a new view. The exact style the
// CLI plans in drops both views without CASCADE, and describes both drops.
func TestDescribe_PostgreSQLPlanRecordsTheViewsItTakes(t *testing.T) {
	c := qt.New(t)
	held := "CREATE VIEW order_totals WITH (security_invoker = true) AS SELECT id, total FROM orders;\n" +
		"CREATE VIEW order_ids WITH (security_invoker = true) AS SELECT id, total FROM order_totals;\n"
	declared := "CREATE VIEW order_totals WITH (security_invoker = true) AS SELECT id FROM orders;\n" +
		"CREATE VIEW order_ids WITH (security_invoker = true) AS SELECT id FROM order_totals;\n"
	orderIDs := planeffect.Object{Kind: planeffect.KindView, Schema: "public", Name: "order_ids"}
	invoker := &planeffect.Held{Exists: true, Properties: []planeffect.HeldProperty{
		{Name: "security_barrier", Value: false}, {Name: "security_invoker", Value: true},
	}}

	statements, got := planPostgres(c, held, declared)

	c.Assert(statements, qt.DeepEquals, []string{
		`DROP VIEW IF EXISTS "order_ids" CASCADE`,
		`DROP VIEW IF EXISTS "order_totals" CASCADE`,
		"CREATE VIEW \"order_totals\" WITH (security_invoker=true) AS\nSELECT id FROM orders",
		"CREATE VIEW \"order_ids\" WITH (security_invoker=true) AS\nSELECT id FROM order_totals",
	})
	c.Assert(got, qt.DeepEquals, [][]planeffect.Effect{
		{{
			Version: planeffect.Version, Object: orderIDs, Action: planeffect.ActionDrop,
			Properties: security(dropped(false), dropped(true)), BeforePlan: invoker,
		}},
		nil,
		{{
			Version: planeffect.Version, Object: orderTotals, Action: planeffect.ActionCreate,
			Properties: security(created(false), created(true)), BeforePlan: invoker,
		}},
		{{
			Version: planeffect.Version, Object: orderIDs, Action: planeffect.ActionCreate,
			Properties: security(created(false), created(true)), BeforePlan: invoker,
		}},
	})
}

// TestDescribe_RebuildGivingTheViewAnOwner pins the effect of a rebuild whose
// CREATE VIEW the planner follows with a comment and an ALTER VIEW ... OWNER
// TO: the CREATE carries the view's options, and the statements after it,
// which change no option, carry none. CockroachDB 26 cannot hold a view
// comment and writes it as a comment line in front of the OWNER TO, so the
// node renders fewer statements than it could.
func TestDescribe_RebuildGivingTheViewAnOwner(t *testing.T) {
	createView := "CREATE VIEW \"order_totals\" WITH (security_invoker=true) AS\nSELECT id FROM orders"
	ownerTo := `ALTER VIEW "order_totals" OWNER TO "reporting"`
	tests := []struct {
		name         string
		dialect      string
		capabilities capability.Capabilities
		after        []string
	}{
		{
			name:         "PostgreSQL 17",
			dialect:      platform.Postgres,
			capabilities: capability.Postgres17(),
			after:        []string{`COMMENT ON VIEW "order_totals" IS 'totals'`, ownerTo},
		},
		{
			name:         "CockroachDB 26.3",
			dialect:      platform.CockroachDB,
			capabilities: capability.CockroachDB263(),
			after:        []string{`COMMENT ON VIEW "order_totals" IS 'totals'`, ownerTo},
		},
		{
			name:         "CockroachDB 26, which writes the comment as a comment line",
			dialect:      platform.CockroachDB,
			capabilities: capability.CockroachDB26(),
			after:        []string{"-- COCKROACHDB: view comment order_totals is not supported by this target; skipped.\n" + ownerTo},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, described := planOn(c, test.dialect, test.capabilities,
				orderTotalsView("security_invoker=true", "SELECT id, total FROM orders"),
				orderTotalsView("security_invoker=true", "SELECT id FROM orders")+
					"COMMENT ON VIEW order_totals IS 'totals';\nALTER VIEW order_totals OWNER TO reporting;\n")

			c.Assert(statements, qt.DeepEquals, append([]string{`DROP VIEW IF EXISTS "order_totals" CASCADE`, createView}, test.after...))
			c.Assert(described, qt.HasLen, len(statements))
			c.Assert(described[1], qt.HasLen, 1)
			c.Assert(described[1][0].Object, qt.Equals, orderTotals)
			c.Assert(described[1][0].Action, qt.Equals, planeffect.ActionCreate)
			c.Assert(described[1][0].Properties, qt.DeepEquals, []planeffect.Property{
				{Name: "security_barrier", After: false, Changed: true},
				{Name: "security_invoker", After: true, Changed: true},
			})
			c.Assert(described[2:], qt.DeepEquals, make([][]planeffect.Effect, len(test.after)))
		})
	}
}
