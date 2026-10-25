//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// viewRebuildStyles is both statement styles a plan is spelled in, each with
// the refusal it writes for a lost policy, as a pattern of the dropped
// relation and the policy.
var viewRebuildStyles = []struct {
	name    string
	style   planext.StatementStyle
	refusal string
}{
	{name: "Ptah's own style", style: planext.StatementStyleAnnotated,
		refusal: `(?s).*drops %[1]s with CASCADE, which also drops %[2]s, whose definition reads it.*`},
	{name: "the exact style", style: planext.StatementStyleExact,
		refusal: `(?s).*drops %[1]s, which %[2]s reads, and PostgreSQL refuses the drop while it does.*`},
}

// viewRebuildTarget is a scratch PostgreSQL database built from schema.
func viewRebuildTarget(c *qt.C, schema string) (string, *sql.DB) {
	c.Helper()
	url := postgresScratchDevURL(c, "ptah_view_rebuild")
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(c.Context(), schema)
	c.Assert(err, qt.IsNil)
	return url, db
}

// diffViewRebuild compares the SQL schema declared against the database at
// url.
func diffViewRebuild(c *qt.C, url, declared string) *difftypes.SchemaDiff {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(declared), 0o600), qt.IsNil)
	runtime := builtintest.Runtime()
	desired, err := schemafile.LoadPath(c.Context(), path, schemafile.Options{
		YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL(), Dialect: platform.Postgres,
	})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	return must.Must(schemadiff.CompareWithDialect(c.Context(), desired, live, platform.Postgres, must.Must(builtin.New())))
}

// planRebuildLive plans the change from the database at url to declared in the
// statement style.
func planRebuildLive(c *qt.C, url, declared string, style planext.StatementStyle) ([]string, error) {
	c.Helper()
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()),
		diffViewRebuild(c, url, declared), platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: style})
}

// viewLikeRelations names the views and materialized views of the public
// schema, each as kind/name, sorted.
func viewLikeRelations(c *qt.C, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `SELECT CASE c.relkind WHEN 'v' THEN 'view' ELSE 'matview' END || '/' || c.relname
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm') ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// publicPolicies names the row-level security policies of the public schema,
// each as table/policy, sorted.
func publicPolicies(c *qt.C, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `SELECT tablename || '/' || policyname FROM pg_policies
WHERE schemaname = 'public' ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

const (
	orderTotalsSchema = `CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total FROM orders;
`
	orderTotalsChangedSchema = `CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total, count(*) AS n FROM orders;
`
	orderReport   = "CREATE VIEW order_report AS SELECT total FROM order_totals;\n"
	itemSkusTable = `CREATE TABLE items (id integer PRIMARY KEY, sku varchar);
CREATE VIEW item_skus AS SELECT id, sku FROM items;
`
	itemSkusWidened = `CREATE TABLE items (id integer PRIMARY KEY, sku varchar(128));
CREATE VIEW item_skus AS SELECT id, sku FROM items;
`
)

// TestViewRebuildLive runs the plans of a view rebuild on PostgreSQL in both
// statement styles. The first row is stokaro/ptah#4964 as filed: changing a
// materialized view drops the view that reads it, and the plan creates that
// view again. The others put the rebuild next to the steps it has to be
// ordered with, where the server refuses a plan whose order is wrong: an
// added view and a SQL function over the changed materialized view, a reader
// that also reads a view the view step drops, a column type change under a
// view a changed materialized view reads, and an added view over a view a
// column type change rebuilds.
//
// Each plan runs on the database, which then holds every declared relation,
// and a second comparison finds nothing left to do.
func TestViewRebuildLive(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		declared string
		want     []string
	}{
		{
			name:     "a view that reads a changed materialized view",
			live:     orderTotalsSchema + orderReport,
			declared: orderTotalsChangedSchema + orderReport,
			want:     []string{"matview/order_totals", "view/order_report"},
		},
		{
			name: "a view and a function the plan adds over a changed materialized view",
			live: orderTotalsSchema,
			declared: orderTotalsChangedSchema + `CREATE VIEW new_report AS SELECT total, n FROM order_totals;
CREATE FUNCTION report_rows() RETURNS SETOF new_report LANGUAGE sql STABLE AS $$ SELECT * FROM new_report $$;
`,
			want: []string{"matview/order_totals", "view/new_report"},
		},
		{
			name: "a reader of a changed materialized view and of a view the view step drops",
			live: orderTotalsSchema + `CREATE VIEW order_list AS SELECT id, amount FROM orders;
CREATE VIEW order_mix AS SELECT l.id, t.total FROM order_list l, order_totals t;
`,
			declared: orderTotalsChangedSchema + `CREATE VIEW order_list AS SELECT amount, id FROM orders;
CREATE VIEW order_mix AS SELECT l.id, t.total FROM order_list l, order_totals t;
`,
			want: []string{"matview/order_totals", "view/order_list", "view/order_mix"},
		},
		{
			name:     "a changed materialized view over a view a column type change rebuilds",
			live:     itemSkusTable + "CREATE MATERIALIZED VIEW sku_counts AS SELECT sku, count(*) AS n FROM item_skus GROUP BY sku;\n",
			declared: itemSkusWidened + "CREATE MATERIALIZED VIEW sku_counts AS SELECT sku, count(*) AS n, max(id) AS top FROM item_skus GROUP BY sku;\n",
			want:     []string{"matview/sku_counts", "view/item_skus"},
		},
		{
			name:     "a view the plan adds over a view a column type change rebuilds",
			live:     itemSkusTable,
			declared: itemSkusWidened + "CREATE VIEW item_sku_list AS SELECT sku FROM item_skus;\n",
			want:     []string{"view/item_sku_list", "view/item_skus"},
		},
	}
	for _, test := range tests {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db := viewRebuildTarget(c, test.live)

				statements, err := planRebuildLive(c, url, test.declared, style.style)
				c.Assert(err, qt.IsNil)
				c.Logf("plan:\n%s", strings.Join(statements, ";\n"))
				for _, statement := range statements {
					_, err := db.ExecContext(c.Context(), statement)
					c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
				}

				c.Assert(viewLikeRelations(c, db), qt.DeepEquals, test.want)
				again := diffViewRebuild(c, url, test.declared)
				c.Assert(again.HasChanges(), qt.IsFalse, qt.Commentf("%+v", again))
			})
		}
	}
}

// TestViewRebuildRefusesALostPolicyLive plans the two rebuilds whose drop would
// take a row-level security policy on PostgreSQL, in both statement styles.
// The first row is stokaro/ptah#4965 as filed: a column type change under the
// view a policy reads. The second is the policy half of stokaro/ptah#4964: a
// materialized view change under the view a policy reads. Ptah's own style
// would drop the view with CASCADE and lose the policy, and the exact style
// would drop it without CASCADE, which the server refuses; both refuse the
// plan and name the policy, and the database keeps it.
func TestViewRebuildRefusesALostPolicyLive(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		declared string
		relation string
		policy   string
		want     []string
	}{
		{
			name: "a column type change under a view a policy reads",
			live: `CREATE TABLE tenants (id integer PRIMARY KEY, active boolean NOT NULL);
CREATE VIEW active_tenants AS SELECT id FROM tenants WHERE active;
CREATE TABLE orders (id integer PRIMARY KEY, tenant_id integer NOT NULL);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON orders USING (tenant_id IN (SELECT id FROM active_tenants));
`,
			declared: `CREATE TABLE tenants (id bigint PRIMARY KEY, active boolean NOT NULL);
CREATE VIEW active_tenants AS SELECT id FROM tenants WHERE active;
CREATE TABLE orders (id integer PRIMARY KEY, tenant_id integer NOT NULL);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON orders USING (tenant_id IN (SELECT id FROM active_tenants));
`,
			relation: "active_tenants",
			policy:   `ptah\.run/pgpolicy/policy public\.orders\.tenant_visible`,
			want:     []string{"orders/tenant_visible"},
		},
		{
			name: "a materialized view change under a view a policy reads",
			live: orderTotalsSchema + orderReport + `CREATE TABLE audit (id integer PRIMARY KEY);
ALTER TABLE audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON audit USING (EXISTS (SELECT 1 FROM order_report));
`,
			declared: orderTotalsChangedSchema + orderReport + `CREATE TABLE audit (id integer PRIMARY KEY);
ALTER TABLE audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON audit USING (EXISTS (SELECT 1 FROM order_report));
`,
			relation: "order_report",
			policy:   `ptah\.run/pgpolicy/policy public\.audit\.audit_visible`,
			want:     []string{"audit/audit_visible"},
		},
	}
	for _, test := range tests {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db := viewRebuildTarget(c, test.live)

				statements, err := planRebuildLive(c, url, test.declared, style.style)

				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches, fmt.Sprintf(style.refusal, test.relation, test.policy))
				c.Assert(statements, qt.IsNil)
				c.Assert(publicPolicies(c, db), qt.DeepEquals, test.want)
			})
		}
	}
}
