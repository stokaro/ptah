//go:build integration

package integration_test

import (
	"database/sql"
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

// relationMatchingSchemas is every schema these tests read: the default one
// and the one that holds a relation of the same name as one in it.
var relationMatchingSchemas = []string{"public", "reporting"}

// relationMatchingTarget is a scratch PostgreSQL database built from schema.
func relationMatchingTarget(c *qt.C, schema string) (string, *sql.DB) {
	c.Helper()
	url := postgresScratchDevURL(c, "ptah_relation_matching")
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(c.Context(), schema)
	c.Assert(err, qt.IsNil)
	return url, db
}

// diffRelationMatching compares the SQL schema declared against both schemas
// of the database at url over a connection, as ptah schema apply does, so the
// diff carries the server's default schema.
func diffRelationMatching(c *qt.C, url, declared string) *difftypes.SchemaDiff {
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
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, relationMatchingSchemas)
	c.Assert(err, qt.IsNil)
	return must.Must(schemadiff.CompareWithDatabase(c.Context(), conn, desired, live, nil, must.Must(builtin.New())))
}

// planRelationMatching plans the change from the database at url to declared
// in the statement style.
func planRelationMatching(c *qt.C, url, declared string, style planext.StatementStyle) ([]string, error) {
	c.Helper()
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()),
		diffRelationMatching(c, url, declared), platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: style})
}

// applyRelationMatching runs statements on db one at a time.
func applyRelationMatching(c *qt.C, db *sql.DB, statements []string) {
	c.Helper()
	c.Logf("plan:\n%s", strings.Join(statements, ";\n"))
	for _, statement := range statements {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// relationMatchingState names the views, the materialized views and the
// policies of both schemas, sorted: a view as view/schema.name, a materialized
// view as matview/schema.name with populated or empty, and a policy as
// policy/schema.table.name.
func relationMatchingState(c *qt.C, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `SELECT CASE c.relkind WHEN 'v' THEN 'view/' || n.nspname || '.' || c.relname
	ELSE 'matview/' || n.nspname || '.' || c.relname || CASE WHEN c.relispopulated THEN ' populated' ELSE ' empty' END END
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('public', 'reporting') AND c.relkind IN ('v', 'm')
UNION ALL
SELECT 'policy/' || schemaname || '.' || tablename || '.' || policyname FROM pg_policies
WHERE schemaname IN ('public', 'reporting')
ORDER BY 1`)
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
	reportingOrders = `CREATE SCHEMA reporting;
CREATE TABLE public.orders (id integer PRIMARY KEY, amount numeric NOT NULL);
`
	reportingTotals        = "CREATE MATERIALIZED VIEW reporting.order_totals AS SELECT sum(amount) AS total FROM public.orders;\n"
	reportingTotalsChanged = "CREATE MATERIALIZED VIEW reporting.order_totals AS SELECT sum(amount) AS total, count(*) AS n FROM public.orders;\n"
	auditTable             = `CREATE TABLE public.audit (id integer PRIMARY KEY);
ALTER TABLE public.audit ENABLE ROW LEVEL SECURITY;
`
)

// TestViewRebuildReadsEachSpellingOfAReaderLive runs, in both statement
// styles, rebuilds whose reader names the changed relation in a spelling a
// match of the whole name as text misses. A reader the plan does not find is
// dropped by the CASCADE in Ptah's own style, and refused with 2BP01 by the
// server in the exact style, or blocks the column type change with 0A000
// (stokaro/ptah#5004).
//
// Each plan runs on the database, which then holds every declared relation,
// and a second comparison finds nothing left to do.
func TestViewRebuildReadsEachSpellingOfAReaderLive(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		declared string
		want     []string
	}{
		{
			name:     "a reader that quotes both parts of the changed materialized view",
			live:     reportingOrders + reportingTotals + `CREATE VIEW reporting.order_report AS SELECT total FROM "reporting"."order_totals";` + "\n",
			declared: reportingOrders + reportingTotalsChanged + `CREATE VIEW reporting.order_report AS SELECT total FROM "reporting"."order_totals";` + "\n",
			want:     []string{"matview/reporting.order_totals populated", "view/reporting.order_report"},
		},
		{
			name:     "a reader that quotes only the schema",
			live:     reportingOrders + reportingTotals + `CREATE VIEW reporting.order_report AS SELECT total FROM "reporting".order_totals;` + "\n",
			declared: reportingOrders + reportingTotalsChanged + `CREATE VIEW reporting.order_report AS SELECT total FROM "reporting".order_totals;` + "\n",
			want:     []string{"matview/reporting.order_totals populated", "view/reporting.order_report"},
		},
		{
			name: "a bare reader of a table declared with its schema",
			live: `CREATE TABLE public.items (id integer PRIMARY KEY, sku varchar(64));
CREATE VIEW item_skus AS SELECT id, sku FROM items;
`,
			declared: `CREATE TABLE public.items (id integer PRIMARY KEY, sku varchar(128));
CREATE VIEW item_skus AS SELECT id, sku FROM items;
`,
			want: []string{"view/public.item_skus"},
		},
	}
	for _, test := range tests {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db := relationMatchingTarget(c, test.live)

				statements, err := planRelationMatching(c, url, test.declared, style.style)
				c.Assert(err, qt.IsNil)
				applyRelationMatching(c, db, statements)

				c.Assert(relationMatchingState(c, db), qt.DeepEquals, test.want)
				again := diffRelationMatching(c, url, test.declared)
				c.Assert(again.HasChanges(), qt.IsFalse, qt.Commentf("%+v", again))
			})
		}
	}
}

// TestViewRebuildRefusesAPolicyOnAQuotedReaderLive is the policy half of the
// test above: the rebuild finds the reader of the changed materialized view,
// and a policy reads that reader, so the plan is refused in both styles and
// the database keeps the policy. A reader the plan does not find is dropped by
// the CASCADE, and the policy with it (stokaro/ptah#5004).
func TestViewRebuildRefusesAPolicyOnAQuotedReaderLive(t *testing.T) {
	report := `CREATE VIEW reporting.order_report AS SELECT total FROM "reporting"."order_totals";
CREATE POLICY audit_visible ON public.audit USING (EXISTS (SELECT 1 FROM reporting.order_report));
`
	for _, style := range viewRebuildStyles {
		t.Run(style.name, func(t *testing.T) {
			c := qt.New(t)
			url, db := relationMatchingTarget(c, reportingOrders+reportingTotals+auditTable+report)

			statements, err := planRelationMatching(c, url, reportingOrders+reportingTotalsChanged+auditTable+report, style.style)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*reporting\.order_report.*public\.audit\.audit_visible.*`)
			c.Assert(statements, qt.IsNil)
			c.Assert(relationMatchingState(c, db), qt.DeepEquals, []string{
				"matview/reporting.order_totals populated", "policy/public.audit.audit_visible", "view/reporting.order_report",
			})
		})
	}
}

// TestViewRebuildKeepsSchemasApartLive runs, in both statement styles, a
// column type change that rebuilds a view while a policy reads a view of the
// same name in another schema. The policy reads nothing the plan drops, so the
// plan applies, the database keeps the policy, and a second comparison finds
// nothing left to do. Read as the rebuilt view, the policy's relation refuses
// the plan (stokaro/ptah#5005).
func TestViewRebuildKeepsSchemasApartLive(t *testing.T) {
	const (
		publicItems = `CREATE TABLE public.items (id integer PRIMARY KEY, sku varchar(64));
`
		publicItemsWidened = `CREATE TABLE public.items (id integer PRIMARY KEY, sku varchar(128));
`
		reportingSkus = `CREATE SCHEMA reporting;
CREATE TABLE reporting.skus (id integer PRIMARY KEY, sku varchar(64));
`
		reportingSkusWidened = `CREATE SCHEMA reporting;
CREATE TABLE reporting.skus (id integer PRIMARY KEY, sku varchar(128));
`
	)
	tests := []struct {
		name     string
		live     string
		declared string
	}{
		{
			name: "the rebuilt view declared with its schema",
			live: reportingSkus + publicItems + `CREATE VIEW public.item_skus AS SELECT id, sku FROM public.items;
CREATE VIEW reporting.item_skus AS SELECT id FROM reporting.skus;
` + auditTable + "CREATE POLICY audit_visible ON public.audit USING (id IN (SELECT id FROM reporting.item_skus));\n",
			declared: reportingSkus + publicItemsWidened + `CREATE VIEW public.item_skus AS SELECT id, sku FROM public.items;
CREATE VIEW reporting.item_skus AS SELECT id FROM reporting.skus;
` + auditTable + "CREATE POLICY audit_visible ON public.audit USING (id IN (SELECT id FROM reporting.item_skus));\n",
		},
		{
			name: "the rebuilt view declared without a schema",
			live: reportingSkus + publicItems + `CREATE VIEW item_skus AS SELECT id, sku FROM items;
CREATE VIEW reporting.item_skus AS SELECT id FROM reporting.skus;
` + auditTable + "CREATE POLICY audit_visible ON public.audit USING (id IN (SELECT id FROM reporting.item_skus));\n",
			declared: reportingSkus + publicItemsWidened + `CREATE VIEW item_skus AS SELECT id, sku FROM items;
CREATE VIEW reporting.item_skus AS SELECT id FROM reporting.skus;
` + auditTable + "CREATE POLICY audit_visible ON public.audit USING (id IN (SELECT id FROM reporting.item_skus));\n",
		},
		{
			name: "the rebuilt view outside the default schema and a policy on the default one",
			live: reportingSkus + publicItems + `CREATE VIEW item_skus AS SELECT id FROM items;
CREATE VIEW reporting.item_skus AS SELECT id, sku FROM reporting.skus;
` + auditTable + "CREATE POLICY audit_visible ON public.audit USING (id IN (SELECT id FROM item_skus));\n",
			declared: reportingSkusWidened + publicItems + `CREATE VIEW item_skus AS SELECT id FROM items;
CREATE VIEW reporting.item_skus AS SELECT id, sku FROM reporting.skus;
` + auditTable + "CREATE POLICY audit_visible ON public.audit USING (id IN (SELECT id FROM item_skus));\n",
		},
	}
	for _, test := range tests {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db := relationMatchingTarget(c, test.live)

				statements, err := planRelationMatching(c, url, test.declared, style.style)
				c.Assert(err, qt.IsNil)
				applyRelationMatching(c, db, statements)

				c.Assert(relationMatchingState(c, db), qt.DeepEquals, []string{
					"policy/public.audit.audit_visible", "view/public.item_skus", "view/reporting.item_skus",
				})
				again := diffRelationMatching(c, url, test.declared)
				c.Assert(again.HasChanges(), qt.IsFalse, qt.Commentf("%+v", again))
			})
		}
	}
}

// TestMaterializedViewWithNoDataConvergesLive pins a materialized view
// declared WITH NO DATA, which the server reports without the clause. An
// unchanged declaration compares equal; a plan that only adds a table plans
// that table and is not refused over the policy that reads the view's reader;
// and a change under the view creates it again empty, as declared. Compared
// with the clause, every plan rebuilds the view and its readers and refuses
// over the policy (stokaro/ptah#5006).
func TestMaterializedViewWithNoDataConvergesLive(t *testing.T) {
	const (
		orders   = "CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);\n"
		snapshot = `CREATE MATERIALIZED VIEW order_snapshot AS SELECT sum(amount) AS total FROM orders WITH NO DATA;
CREATE VIEW snap_report AS SELECT total FROM order_snapshot;
`
		audit = `CREATE TABLE audit (id integer PRIMARY KEY);
ALTER TABLE audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON audit USING (EXISTS (SELECT 1 FROM snap_report));
`
	)
	tests := []struct {
		name     string
		live     string
		declared string
		want     []string
	}{
		{
			name:     "an unrelated table added beside a policy on the view's reader",
			live:     orders + snapshot + audit,
			declared: orders + snapshot + audit + "CREATE TABLE unrelated (id integer PRIMARY KEY);\n",
			want: []string{
				"matview/public.order_snapshot empty", "policy/public.audit.audit_visible", "view/public.snap_report",
			},
		},
		{
			name: "a change under the view",
			live: orders + `CREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total FROM orders;
CREATE MATERIALIZED VIEW order_snapshot AS SELECT total FROM order_totals WITH NO DATA;
`,
			declared: orders + `CREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total, count(*) AS n FROM orders;
CREATE MATERIALIZED VIEW order_snapshot AS SELECT total FROM order_totals WITH NO DATA;
`,
			want: []string{"matview/public.order_snapshot empty", "matview/public.order_totals populated"},
		},
	}
	for _, test := range tests {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db := relationMatchingTarget(c, test.live)

				statements, err := planRelationMatching(c, url, test.declared, style.style)
				c.Assert(err, qt.IsNil)
				applyRelationMatching(c, db, statements)

				c.Assert(relationMatchingState(c, db), qt.DeepEquals, test.want)
				again := diffRelationMatching(c, url, test.declared)
				c.Assert(again.HasChanges(), qt.IsFalse, qt.Commentf("%+v", again))
			})
		}
	}
}

// TestMaterializedViewWithNoDataComparesEqualLive is the comparison alone: a
// database built from a declaration holding a materialized view WITH NO DATA
// holds what it declares. The second row is a body only the server's spelling
// matches, a function in FROM, which PostgreSQL prints with a column alias
// list (stokaro/ptah#4057): the server is asked to spell the query, since a
// plain view refuses WITH NO DATA.
func TestMaterializedViewWithNoDataComparesEqualLive(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{
			name: "a view over the materialized view",
			schema: `CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total FROM orders;
CREATE MATERIALIZED VIEW order_snapshot AS SELECT total FROM order_totals WITH NO DATA;
CREATE VIEW snap_report AS SELECT total FROM order_snapshot;
`,
		},
		{
			name:   "a function in FROM",
			schema: "CREATE MATERIALIZED VIEW day_numbers AS SELECT n FROM generate_series(1, 7) AS n WITH NO DATA;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, _ := relationMatchingTarget(c, test.schema)

			diff := diffRelationMatching(c, url, test.schema)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
		})
	}
}
