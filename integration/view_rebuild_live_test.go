//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// viewRebuildStyles is both statement styles a plan is spelled in.
var viewRebuildStyles = []struct {
	name  string
	style planext.StatementStyle
}{
	{name: "Ptah's own style", style: planext.StatementStyleAnnotated},
	{name: "the exact style", style: planext.StatementStyleExact},
}

// viewRebuildTarget is a scratch PostgreSQL database built from schema, and a
// role of the test's own, which the schema names as {role} and which is
// created before it.
func viewRebuildTarget(c *qt.C, schema string) (url string, db *sql.DB, role string) {
	c.Helper()
	role = scratchRole(dbtarget.PostgreSQL)(c)
	url = postgresScratchDevURL(c, "ptah_view_rebuild")
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(c.Context(), "CREATE ROLE "+role+";\n"+withRole(schema, role))
	c.Assert(err, qt.IsNil)
	return url, db, role
}

// withRole is schema with role in place of {role}.
func withRole(schema, role string) string {
	return strings.ReplaceAll(schema, "{role}", role)
}

// loadViewRebuildSchema reads the SQL schema declared.
func loadViewRebuildSchema(c *qt.C, declared string) *schemamodel.Database {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(declared), 0o600), qt.IsNil)
	runtime := builtintest.Runtime()
	desired, err := schemafile.LoadPath(c.Context(), path, schemafile.Options{
		YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL(), Dialect: platform.Postgres,
	})
	c.Assert(err, qt.IsNil)
	return desired
}

// diffViewRebuild compares the SQL schema declared against the database at
// url, and returns the diff with the two sides it compared.
func diffViewRebuild(c *qt.C, url, declared string) (*difftypes.SchemaDiff, *schemamodel.Database, *catalog.Database) {
	c.Helper()
	desired := loadViewRebuildSchema(c, declared)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public", "reporting"})
	c.Assert(err, qt.IsNil)
	// Through the connection, as the CLI compares: a policy's expression is
	// compared the way the server prints it back.
	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, desired, live, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return diff, desired, live
}

// rebuildPending is what a comparison of the database at url against
// declared still finds to do.
func rebuildPending(c *qt.C, url, declared string) *difftypes.SchemaDiff {
	c.Helper()
	diff, _, _ := diffViewRebuild(c, url, declared)
	return diff
}

// planRebuildLive plans the change from the database at url to declared in the
// statement style.
func planRebuildLive(c *qt.C, url, declared string, style planext.StatementStyle) ([]string, error) {
	c.Helper()
	diff, _, _ := diffViewRebuild(c, url, declared)
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: style})
}

// planRebuildBothWays plans the change from the database at url to declared,
// and its rollback, in the statement style, the way `ptah migrations
// generate` does.
func planRebuildBothWays(c *qt.C, url, declared string, style planext.StatementStyle) (up, down []string) {
	c.Helper()
	diff, desired, live := diffViewRebuild(c, url, declared)
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: live,
		Dialect:       platform.Postgres,
		Policy: generator.BidirectionalPlanPolicy{
			Create:     generator.ConcurrentIndexDisabled,
			Drop:       generator.ConcurrentIndexDisabled,
			Statements: style,
		},
		Runtime: must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)
	options := planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: style}
	up, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()),
		plan.Forward.Diff, platform.Postgres, options)
	c.Assert(err, qt.IsNil)
	down, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()),
		plan.Reverse.Diff, platform.Postgres, options)
	c.Assert(err, qt.IsNil)
	return up, down
}

// runRebuildStatements runs statements on db in one transaction, as a
// migration runs them, and names the one the server refuses.
func runRebuildStatements(c *qt.C, db *sql.DB, statements []string) {
	c.Helper()
	tx, err := db.BeginTx(c.Context(), nil)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err := tx.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s\nplan:\n%s", statement, strings.Join(statements, ";\n")))
	}
	c.Assert(tx.Commit(), qt.IsNil)
}

// queryStrings runs query on db and returns its one text column, row by row.
func queryStrings(c *qt.C, db *sql.DB, query string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), query)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", query))
	defer rows.Close()
	values := make([]string, 0)
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}

// heldRebuildQueries read back what a rebuild has to keep: each view and
// materialized view with its options, privileges and comment, whether a
// materialized view holds rows, each column privilege on one, each routine
// with the properties and privileges callers see, each index on a
// materialized view, each trigger, and each policy. An object outside public
// is named with its schema.
//
// A privilege list leaves out the owner's own entry, which PostgreSQL 17 and
// later spell with MAINTAIN and earlier releases without.
var heldRebuildQueries = []string{
	`SELECT 'relation ' || ` + rebuildName + ` || ' options=' || COALESCE(array_to_string(c.reloptions, ','), '') ||
	' acl=' || ` + grantedACL("c.relacl", "c.relowner") + ` || ' comment=' || COALESCE(obj_description(c.oid, 'pg_class'), '')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('public', 'reporting') AND c.relkind IN ('v', 'm') ORDER BY 1`,
	`SELECT 'populated ' || ` + rebuildName + ` || '=' || c.relispopulated::text
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('public', 'reporting') AND c.relkind = 'm' ORDER BY 1`,
	`SELECT 'column ' || ` + rebuildName + ` || '.' || a.attname || ' acl=' || ` + grantedACL("a.attacl", "c.relowner") + `
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('public', 'reporting') AND c.relkind IN ('v', 'm') AND a.attacl IS NOT NULL ORDER BY 1`,
	`SELECT 'routine ' || p.oid::regprocedure::text || ' strict=' || p.proisstrict::text || ' parallel=' || p.proparallel::text ||
	' definer=' || p.prosecdef::text || ' config=' || COALESCE(array_to_string(p.proconfig, ','), '') ||
	' acl=' || CASE WHEN p.proacl IS NULL THEN 'default' ELSE ` + grantedACL("p.proacl", "p.proowner") + ` END ||
	' comment=' || COALESCE(obj_description(p.oid, 'pg_proc'), '')
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname IN ('public', 'reporting') ORDER BY 1`,
	`SELECT 'index ' || i.indexrelid::regclass::text || ' on ' || i.indrelid::regclass::text || ' unique=' || i.indisunique::text ||
	' comment=' || COALESCE(obj_description(i.indexrelid, 'pg_class'), '')
FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('public', 'reporting') AND c.relkind = 'm' ORDER BY 1`,
	`SELECT 'trigger ' || t.tgname || ' on ' || t.tgrelid::regclass::text FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('public', 'reporting') AND NOT t.tgisinternal ORDER BY 1`,
	`SELECT 'policy ' || CASE WHEN schemaname = 'public' THEN '' ELSE schemaname || '.' END || tablename || '.' || policyname
FROM pg_policies WHERE schemaname IN ('public', 'reporting') ORDER BY 1`,
}

// rebuildName is the name of the relation c in namespace n, with its schema
// outside public.
const rebuildName = `CASE WHEN n.nspname = 'public' THEN c.relname ELSE n.nspname || '.' || c.relname END`

// grantedACL is the SQL that lists the entries of the privilege list acl, other
// than the one of the owner, sorted. The owner granted each of them, and is
// written {owner}: it is the role the test connects as, which differs from one
// server to the next.
func grantedACL(acl, owner string) string {
	return `COALESCE((SELECT string_agg(regexp_replace(entry::text, '/' || pg_get_userbyid(` + owner + `) || '$', '/{owner}'),
	',' ORDER BY entry::text) FROM unnest(` + acl + `) entry
	WHERE entry::text NOT LIKE pg_get_userbyid(` + owner + `) || '=%'), '')`
}

// heldRebuildState is what [heldRebuildQueries] read back, with {role} in
// place of the test's role, whose name changes from run to run.
func heldRebuildState(c *qt.C, db *sql.DB, role string) []string {
	c.Helper()
	held := make([]string, 0)
	for _, query := range heldRebuildQueries {
		for _, row := range queryStrings(c, db, query) {
			held = append(held, strings.ReplaceAll(row, role, "{role}"))
		}
	}
	return held
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
	// bigOrdersReading reads order_totals and bigOrdersPlain does not. Both
	// have the same columns, so either replaces the other in place.
	bigOrdersReading = "CREATE VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > (SELECT total / 10 FROM order_totals);\n"
	bigOrdersPlain   = "CREATE VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > 100;\n"
	// auditOfBigOrders is a table whose policy reads big_orders.
	auditOfBigOrders = `CREATE TABLE audit (id integer PRIMARY KEY, order_id integer);
ALTER TABLE audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON audit USING (order_id IN (SELECT id FROM big_orders));
`
	// reportRowsSchema returns the rows of order_report, with properties and
	// privileges a rebuild has to keep.
	reportRowsSchema = `CREATE FUNCTION report_rows(lim integer) RETURNS SETOF order_report LANGUAGE sql STABLE STRICT PARALLEL SAFE
SECURITY DEFINER SET search_path = public AS $$ SELECT * FROM order_report LIMIT lim $$;
COMMENT ON FUNCTION report_rows(integer) IS 'report rows';
REVOKE EXECUTE ON FUNCTION report_rows(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION report_rows(integer) TO {role};
`
	// reportTotal reads order_report in a SQL-standard body, so the server
	// records that it depends on the view.
	reportTotal = "CREATE FUNCTION report_total() RETURNS numeric LANGUAGE sql STABLE RETURN (SELECT total FROM order_report);\n"
	// reportListSchema calls report_rows, so it depends on the routine.
	reportListSchema = "CREATE VIEW report_list AS SELECT total FROM report_rows(10);\n"
	// reportInstead is an INSTEAD OF trigger on order_report.
	reportInstead = `CREATE FUNCTION order_report_ins() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER order_report_ins INSTEAD OF INSERT ON order_report FOR EACH ROW EXECUTE FUNCTION order_report_ins();
`
	// totalsIndexed is a unique index on order_totals, which REFRESH
	// MATERIALIZED VIEW CONCURRENTLY needs, and privileges and comments on
	// both views.
	totalsIndexed = `CREATE UNIQUE INDEX order_totals_total ON order_totals (total);
COMMENT ON INDEX order_totals_total IS 'one row';
GRANT SELECT ON order_totals TO {role};
GRANT SELECT ON order_report TO {role};
GRANT SELECT (total) ON order_report TO {role};
COMMENT ON VIEW order_report IS 'report';
`
)

// reportingSkus is a view public.item_skus over public.items, whose sku column
// is skuType, a view of the same name in reporting over reporting.skus, and a
// policy that reads read.
func reportingSkus(skuType, read string) string {
	return `CREATE SCHEMA reporting;
CREATE TABLE items (id integer PRIMARY KEY, sku ` + skuType + `);
CREATE VIEW item_skus AS SELECT id, sku FROM items;
CREATE TABLE reporting.skus (id integer PRIMARY KEY);
CREATE VIEW reporting.item_skus AS SELECT id FROM reporting.skus;
CREATE TABLE audit (id integer PRIMARY KEY);
ALTER TABLE audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON audit USING (id IN (SELECT id FROM ` + read + `));
`
}

// mirroredSkus is [reportingSkus] the other way round: the column whose type
// is skuType is reporting.skus.sku, which reporting.item_skus reads. A view of
// public reads public.item_skus by its bare name, and the policy reads that
// view: a reader matched in any schema would join the rebuild, and the policy
// would be refused.
func mirroredSkus(skuType string) string {
	return `CREATE SCHEMA reporting;
CREATE TABLE items (id integer PRIMARY KEY);
CREATE VIEW item_skus AS SELECT id FROM items;
CREATE VIEW item_sku_list AS SELECT id FROM item_skus;
CREATE TABLE reporting.skus (id integer PRIMARY KEY, sku ` + skuType + `);
CREATE VIEW reporting.item_skus AS SELECT id, sku FROM reporting.skus;
CREATE TABLE audit (id integer PRIMARY KEY);
ALTER TABLE audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON audit USING (id IN (SELECT id FROM item_sku_list));
`
}

// viewRebuildCase is one change a view rebuild plans.
type viewRebuildCase struct {
	name string
	// live is the schema the database holds, and the declaration the
	// rollback returns to. setup runs after it and declares nothing: a
	// view's options, or a privilege of a role the schema does not manage.
	// {role} in either stands for a role of the test's own.
	live, setup string
	declared    string
	// want and held are the views and materialized views the database holds
	// after the change and after its rollback.
	want, held []string
	// after is what [heldRebuildState] reads back after the change.
	after []string
}

// viewRebuildCases are the plans a rebuild has to make without losing
// anything. The first row is stokaro/ptah#4964 as filed. The next ones put the
// rebuild next to the steps it has to be ordered with: an added view and a SQL
// function over the changed materialized view, a reader that also reads a view
// the plan cannot replace, a column type change under a view a changed
// materialized view reads, and an added view over a view a column type change
// rebuilds. The rest keep what the drops take: a view replaced in place that
// stops or starts reading the changed materialized view, with a policy that
// reads it; a routine that returns a rebuilt view's rows, kept, changed or
// added, with its properties and privileges, and a view that calls it; the
// index, trigger, privileges, comments and options of rebuilt objects; a view
// whose namesake in another schema stays out of the rebuild; and a
// materialized view declared WITH NO DATA, which stays empty both ways.
var viewRebuildCases = []viewRebuildCase{
	{
		name:     "a view that reads a changed materialized view",
		live:     orderTotalsSchema + orderReport,
		declared: orderTotalsChangedSchema + orderReport,
		want:     []string{"matview/order_totals", "view/order_report"},
		held:     []string{"matview/order_totals", "view/order_report"},
		after: []string{
			"relation order_report options= acl= comment=",
			"relation order_totals options= acl= comment=",
			"populated order_totals=true",
		},
	},
	{
		name: "a view and a function the plan adds over a changed materialized view",
		live: orderTotalsSchema,
		declared: orderTotalsChangedSchema + `CREATE VIEW new_report AS SELECT total, n FROM order_totals;
CREATE FUNCTION report_rows() RETURNS SETOF new_report LANGUAGE sql STABLE AS $$ SELECT * FROM new_report $$;
`,
		want: []string{"matview/order_totals", "view/new_report"},
		held: []string{"matview/order_totals"},
		after: []string{
			"relation new_report options= acl= comment=",
			"relation order_totals options= acl= comment=",
			"populated order_totals=true",
			"routine report_rows() strict=false parallel=u definer=false config= acl=default comment=",
		},
	},
	{
		name: "a reader of a changed materialized view and of a view the plan cannot replace",
		live: orderTotalsSchema + `CREATE VIEW order_list AS SELECT id, amount FROM orders;
CREATE VIEW order_mix AS SELECT l.id, t.total FROM order_list l, order_totals t;
`,
		declared: orderTotalsChangedSchema + `CREATE VIEW order_list AS SELECT amount, id FROM orders;
CREATE VIEW order_mix AS SELECT l.id, t.total FROM order_list l, order_totals t;
`,
		want: []string{"matview/order_totals", "view/order_list", "view/order_mix"},
		held: []string{"matview/order_totals", "view/order_list", "view/order_mix"},
		after: []string{
			"relation order_list options= acl= comment=",
			"relation order_mix options= acl= comment=",
			"relation order_totals options= acl= comment=",
			"populated order_totals=true",
		},
	},
	{
		name:     "a changed materialized view over a view a column type change rebuilds",
		live:     itemSkusTable + "CREATE MATERIALIZED VIEW sku_counts AS SELECT sku, count(*) AS n FROM item_skus GROUP BY sku;\n",
		declared: itemSkusWidened + "CREATE MATERIALIZED VIEW sku_counts AS SELECT sku, count(*) AS n, max(id) AS top FROM item_skus GROUP BY sku;\n",
		want:     []string{"matview/sku_counts", "view/item_skus"},
		held:     []string{"matview/sku_counts", "view/item_skus"},
		after: []string{
			"relation item_skus options= acl= comment=",
			"relation sku_counts options= acl= comment=",
			"populated sku_counts=true",
		},
	},
	{
		name:     "a view the plan adds over a view a column type change rebuilds",
		live:     itemSkusTable,
		declared: itemSkusWidened + "CREATE VIEW item_sku_list AS SELECT sku FROM item_skus;\n",
		want:     []string{"view/item_sku_list", "view/item_skus"},
		held:     []string{"view/item_skus"},
		after: []string{
			"relation item_sku_list options= acl= comment=",
			"relation item_skus options= acl= comment=",
		},
	},
	{
		name:     "a view replaced in place that stops reading a changed materialized view, with a policy that reads it",
		live:     orderTotalsSchema + bigOrdersReading + auditOfBigOrders,
		setup:    "ALTER VIEW big_orders SET (security_invoker = true);\nGRANT SELECT ON big_orders TO {role};\n",
		declared: orderTotalsChangedSchema + bigOrdersPlain + auditOfBigOrders,
		want:     []string{"matview/order_totals", "view/big_orders"},
		held:     []string{"matview/order_totals", "view/big_orders"},
		after: []string{
			"relation big_orders options=security_invoker=true acl={role}=r/{owner} comment=",
			"relation order_totals options= acl= comment=",
			"populated order_totals=true",
			"policy audit.audit_visible",
		},
	},
	{
		name:     "a view replaced in place that starts reading a changed materialized view, with a policy that reads it",
		live:     orderTotalsSchema + bigOrdersPlain + auditOfBigOrders,
		setup:    "ALTER VIEW big_orders SET (security_barrier = true);\n",
		declared: orderTotalsChangedSchema + bigOrdersReading + auditOfBigOrders,
		want:     []string{"matview/order_totals", "view/big_orders"},
		held:     []string{"matview/order_totals", "view/big_orders"},
		after: []string{
			"relation big_orders options=security_barrier=true acl= comment=",
			"relation order_totals options= acl= comment=",
			"populated order_totals=true",
			"policy audit.audit_visible",
		},
	},
	{
		name:     "a routine that returns a rebuilt view's rows, and a view that calls it",
		live:     orderTotalsSchema + orderReport + reportRowsSchema + reportListSchema,
		declared: orderTotalsChangedSchema + orderReport + reportRowsSchema + reportListSchema,
		want:     []string{"matview/order_totals", "view/order_report", "view/report_list"},
		held:     []string{"matview/order_totals", "view/order_report", "view/report_list"},
		after: []string{
			"relation order_report options= acl= comment=",
			"relation order_totals options= acl= comment=",
			"relation report_list options= acl= comment=",
			"populated order_totals=true",
			"routine report_rows(integer) strict=true parallel=s definer=true config=search_path=public acl={role}=X/{owner} comment=report rows",
		},
	},
	{
		name: "a changed routine that returns a rebuilt view's rows",
		live: orderTotalsSchema + orderReport + reportRowsSchema,
		declared: orderTotalsChangedSchema + orderReport +
			strings.Replace(reportRowsSchema, "LIMIT lim $$", "ORDER BY total LIMIT lim $$", 1),
		want: []string{"matview/order_totals", "view/order_report"},
		held: []string{"matview/order_totals", "view/order_report"},
		after: []string{
			"relation order_report options= acl= comment=",
			"relation order_totals options= acl= comment=",
			"populated order_totals=true",
			"routine report_rows(integer) strict=true parallel=s definer=true config=search_path=public acl={role}=X/{owner} comment=report rows",
		},
	},
	{
		name:     "a routine the plan adds that returns a rebuilt view's rows",
		live:     orderTotalsSchema + orderReport,
		declared: orderTotalsChangedSchema + orderReport + reportRowsSchema,
		want:     []string{"matview/order_totals", "view/order_report"},
		held:     []string{"matview/order_totals", "view/order_report"},
		after: []string{
			"relation order_report options= acl= comment=",
			"relation order_totals options= acl= comment=",
			"populated order_totals=true",
			"routine report_rows(integer) strict=true parallel=s definer=true config=search_path=public acl={role}=X/{owner} comment=report rows",
		},
	},
	{
		name:     "an index, a trigger, privileges, comments and options of rebuilt objects",
		live:     orderTotalsSchema + orderReport + reportInstead + totalsIndexed,
		setup:    "ALTER VIEW order_report SET (security_invoker = true);\nALTER MATERIALIZED VIEW order_totals SET (fillfactor = 70);\n",
		declared: orderTotalsChangedSchema + orderReport + reportInstead + totalsIndexed,
		want:     []string{"matview/order_totals", "view/order_report"},
		held:     []string{"matview/order_totals", "view/order_report"},
		after: []string{
			"relation order_report options=security_invoker=true acl={role}=r/{owner} comment=report",
			"relation order_totals options=fillfactor=70 acl={role}=r/{owner} comment=",
			"populated order_totals=true",
			"column order_report.total acl={role}=r/{owner}",
			"routine order_report_ins() strict=false parallel=u definer=false config= acl=default comment=",
			"index order_totals_total on order_totals unique=true comment=one row",
			"trigger order_report_ins on order_report",
		},
	},
	{
		name:     "a column type change under a view whose namesake in another schema a policy reads",
		live:     reportingSkus("varchar(64)", "reporting.item_skus"),
		declared: reportingSkus("varchar(128)", "reporting.item_skus"),
		want:     []string{"view/item_skus", "view/reporting.item_skus"},
		held:     []string{"view/item_skus", "view/reporting.item_skus"},
		after: []string{
			"relation item_skus options= acl= comment=",
			"relation reporting.item_skus options= acl= comment=",
			"policy audit.audit_visible",
		},
	},
	{
		name:     "a column type change under a view of another schema whose namesake a policy reads",
		live:     mirroredSkus("varchar(64)"),
		declared: mirroredSkus("varchar(128)"),
		want:     []string{"view/item_sku_list", "view/item_skus", "view/reporting.item_skus"},
		held:     []string{"view/item_sku_list", "view/item_skus", "view/reporting.item_skus"},
		after: []string{
			"relation item_sku_list options= acl= comment=",
			"relation item_skus options= acl= comment=",
			"relation reporting.item_skus options= acl= comment=",
			"policy audit.audit_visible",
		},
	},
	{
		name: "a materialized view declared WITH NO DATA that changes",
		live: `CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total FROM orders WITH NO DATA;
` + orderReport,
		declared: `CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total, count(*) AS n FROM orders WITH NO DATA;
` + orderReport,
		want: []string{"matview/order_totals", "view/order_report"},
		held: []string{"matview/order_totals", "view/order_report"},
		after: []string{
			"relation order_report options= acl= comment=",
			"relation order_totals options= acl= comment=",
			"populated order_totals=false",
		},
	},
	{
		name:     "an INSTEAD OF trigger on a view a column type change rebuilds",
		live:     itemSkusTable + strings.ReplaceAll(reportInstead, "order_report", "item_skus"),
		setup:    "ALTER VIEW item_skus SET (security_barrier = true);\n",
		declared: itemSkusWidened + strings.ReplaceAll(reportInstead, "order_report", "item_skus"),
		want:     []string{"view/item_skus"},
		held:     []string{"view/item_skus"},
		after: []string{
			"relation item_skus options=security_barrier=true acl= comment=",
			"routine item_skus_ins() strict=false parallel=u definer=false config= acl=default comment=",
			"trigger item_skus_ins on item_skus",
		},
	},
}

// TestViewRebuildLive runs the plans of a view rebuild on PostgreSQL in both
// statement styles: up, down and up again, each in one transaction. After each
// step the database holds every declared relation, and a comparison finds
// nothing left to do, against the declaration after an up and against the
// schema the database held before it after the down.
//
// What a drop takes and a comparison does not see is read back too: options,
// privileges, routine properties, indexes, triggers, policies and comments.
// After the change it is the row's after; after the rollback it is what the
// database held before the change, and after the second up it is the row's
// after again.
func TestViewRebuildLive(t *testing.T) {
	for _, test := range viewRebuildCases {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db, role := viewRebuildTarget(c, test.live+test.setup)
				before := heldRebuildState(c, db, role)
				declared, live := withRole(test.declared, role), withRole(test.live, role)

				up, down := planRebuildBothWays(c, url, declared, style.style)
				c.Logf("up:\n%s\ndown:\n%s", strings.Join(up, ";\n"), strings.Join(down, ";\n"))

				runRebuildStatements(c, db, up)
				c.Assert(viewLikeRelations(c, db), qt.DeepEquals, test.want)
				c.Assert(heldRebuildState(c, db, role), qt.DeepEquals, test.after)
				c.Assert(rebuildPending(c, url, declared).HasChanges(), qt.IsFalse)
				runRebuildStatements(c, db, down)
				c.Assert(viewLikeRelations(c, db), qt.DeepEquals, test.held)
				c.Assert(heldRebuildState(c, db, role), qt.DeepEquals, before)
				c.Assert(rebuildPending(c, url, live).HasChanges(), qt.IsFalse)
				runRebuildStatements(c, db, up)
				c.Assert(heldRebuildState(c, db, role), qt.DeepEquals, test.after)
				c.Assert(rebuildPending(c, url, declared).HasChanges(), qt.IsFalse)
			})
		}
	}
}

// viewRebuildForwardCases are changes whose rollback is not the rebuild's to
// get right. The rollback of a column change on a table the schema names with
// its schema leaves the column as it is (stokaro/ptah#5028), and a schema file
// that declares a RETURN body loses the keyword, so the database the rollback
// returns to never matches it (stokaro/ptah#5038). The server records what a
// SQL-standard body reads, so the routine is dropped before the view.
var viewRebuildForwardCases = []viewRebuildCase{
	{
		name:     "a routine with a SQL-standard body that reads a rebuilt view",
		live:     orderTotalsSchema + orderReport + reportTotal,
		declared: orderTotalsChangedSchema + orderReport + reportTotal,
		want:     []string{"matview/order_totals", "view/order_report"},
		after: []string{
			"relation order_report options= acl= comment=",
			"relation order_totals options= acl= comment=",
			"populated order_totals=true",
			"routine report_total() strict=false parallel=u definer=false config= acl=default comment=",
		},
	},
	{
		name:     "a column type change of a table named with its schema under a view that names it without",
		live:     "CREATE TABLE public.items (id integer PRIMARY KEY, sku varchar(64));\nCREATE VIEW item_skus AS SELECT id, sku FROM items;\n",
		declared: "CREATE TABLE public.items (id integer PRIMARY KEY, sku varchar(128));\nCREATE VIEW item_skus AS SELECT id, sku FROM items;\n",
		want:     []string{"view/item_skus"},
		after: []string{
			"relation item_skus options= acl= comment=",
		},
	},
}

// TestViewRebuildForwardLive applies each of [viewRebuildForwardCases] in both
// statement styles, in one transaction, and requires what [TestViewRebuildLive]
// requires after an up: the declared relations, the row's after, and an empty
// comparison against the declaration. A view that reads a table the schema
// names with its schema is found by the rebuild, so the column type change
// does not stop at the view, and neither does a materialized view change at a
// routine whose SQL-standard body reads a view over it.
func TestViewRebuildForwardLive(t *testing.T) {
	for _, test := range viewRebuildForwardCases {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db, role := viewRebuildTarget(c, test.live+test.setup)
				declared := withRole(test.declared, role)

				up, _ := planRebuildBothWays(c, url, declared, style.style)
				c.Logf("up:\n%s", strings.Join(up, ";\n"))

				runRebuildStatements(c, db, up)
				c.Assert(viewLikeRelations(c, db), qt.DeepEquals, test.want)
				c.Assert(heldRebuildState(c, db, role), qt.DeepEquals, test.after)
				c.Assert(rebuildPending(c, url, declared).HasChanges(), qt.IsFalse)
			})
		}
	}
}

// viewLikeRelations names the views and materialized views of the public and
// reporting schemas, each as kind/name, sorted.
func viewLikeRelations(c *qt.C, db *sql.DB) []string {
	c.Helper()
	return queryStrings(c, db, `SELECT CASE c.relkind WHEN 'v' THEN 'view' ELSE 'matview' END || '/' || `+rebuildName+`
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('public', 'reporting') AND c.relkind IN ('v', 'm') ORDER BY 1`)
}

// TestViewRebuildKeepsAConcurrentRefreshLive pins the reason a rebuilt
// materialized view gets its unique index back in the same migration:
// `REFRESH MATERIALIZED VIEW CONCURRENTLY` needs one, and answers `cannot
// refresh materialized view "public.order_totals" concurrently` without it.
// After the change and after its rollback, the refresh runs.
func TestViewRebuildKeepsAConcurrentRefreshLive(t *testing.T) {
	const index = "CREATE UNIQUE INDEX order_totals_total ON order_totals (total);\n"
	for _, style := range viewRebuildStyles {
		t.Run(style.name, func(t *testing.T) {
			c := qt.New(t)
			url, db, _ := viewRebuildTarget(c, orderTotalsSchema+index)
			up, down := planRebuildBothWays(c, url, orderTotalsChangedSchema+index, style.style)

			runRebuildStatements(c, db, up)
			_, refreshed := db.ExecContext(c.Context(), "REFRESH MATERIALIZED VIEW CONCURRENTLY order_totals")
			runRebuildStatements(c, db, down)
			_, refreshedBack := db.ExecContext(c.Context(), "REFRESH MATERIALIZED VIEW CONCURRENTLY order_totals")

			c.Assert(refreshed, qt.IsNil)
			c.Assert(refreshedBack, qt.IsNil)
		})
	}
}

// TestViewRebuildRefusesALostPolicyLive plans the rebuilds whose drop would
// take a row-level security policy on PostgreSQL, in both statement styles.
// The first row is stokaro/ptah#4965 as filed: a column type change under the
// view a policy reads. The second is the policy half of stokaro/ptah#4964: a
// materialized view change under the view a policy reads. The third is a
// policy that calls a routine returning the rows of such a view
// (stokaro/ptah#4980).
//
// Each plan is refused, names the policy and the change, and leaves the
// database as it was. The refusal answers what the server would do: the drop
// the plan would have to write, run here in a transaction that is rolled
// back, is refused without CASCADE (SQLSTATE 2BP01, naming the policy), and
// with CASCADE it takes the policy with it.
func TestViewRebuildRefusesALostPolicyLive(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		declared string
		drop     string
		refusal  string
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
			drop: "DROP VIEW active_tenants",
			refusal: "the plan drops view active_tenants and creates it again, for the type change of tenants.id; " +
				"ptah.run/pgpolicy/policy public.orders.tenant_visible reads it",
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
			drop: "DROP VIEW order_report",
			refusal: "the plan drops view order_report and creates it again, for the change to materialized view order_totals; " +
				"ptah.run/pgpolicy/policy public.audit.audit_visible reads it",
		},
		{
			name:     "a policy that calls a routine returning a rebuilt view's rows",
			live:     orderTotalsSchema + orderReport + reportRowsCall,
			declared: orderTotalsChangedSchema + orderReport + reportRowsCall,
			drop:     "DROP FUNCTION report_rows()",
			refusal: "the plan drops routine report_rows and creates it again, for the change to materialized view order_totals; " +
				"ptah.run/pgpolicy/policy public.audit.audit_visible calls it, and PostgreSQL refuses the drop while it does",
		},
	}
	for _, test := range tests {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db, role := viewRebuildTarget(c, test.live)
				before := heldRebuildState(c, db, role)

				statements, err := planRebuildLive(c, url, test.declared, style.style)

				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches, `(?s)unsupported feature: `+regexp.QuoteMeta(test.refusal)+`.*`)
				c.Assert(statements, qt.IsNil)
				c.Assert(heldRebuildState(c, db, role), qt.DeepEquals, before)
				kept, refused := tryRebuildDrop(c, db, test.drop)
				var dependent *pgconn.PgError
				c.Assert(refused, qt.ErrorAs, &dependent)
				c.Assert(dependent.Code, qt.Equals, "2BP01")
				c.Assert(dependent.Detail, qt.Matches, `(?s).*policy \w+ on table \w+ depends on .*`)
				c.Assert(kept, qt.HasLen, 0)
			})
		}
	}
}

// reportRowsCall is a routine that returns the rows of order_report and a
// policy that calls it.
const reportRowsCall = `CREATE FUNCTION report_rows() RETURNS SETOF order_report LANGUAGE sql STABLE AS $$ SELECT * FROM order_report $$;
CREATE TABLE audit (id integer PRIMARY KEY);
ALTER TABLE audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON audit USING (EXISTS (SELECT 1 FROM report_rows()));
`

// tryRebuildDrop runs drop on db without CASCADE, and then with it, each in a
// transaction it rolls back. It returns the policies the second leaves and the
// error the first answers.
func tryRebuildDrop(c *qt.C, db *sql.DB, drop string) (policies []string, refused error) {
	c.Helper()
	tx, err := db.BeginTx(c.Context(), nil)
	c.Assert(err, qt.IsNil)
	_, refused = tx.ExecContext(c.Context(), drop)
	c.Assert(tx.Rollback(), qt.IsNil)
	tx, err = db.BeginTx(c.Context(), nil)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(tx.Rollback(), qt.IsNil) }()
	_, err = tx.ExecContext(c.Context(), drop+" CASCADE")
	c.Assert(err, qt.IsNil)
	rows, err := tx.QueryContext(c.Context(), "SELECT policyname FROM pg_policies WHERE schemaname = 'public'")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	for rows.Next() {
		var policy string
		c.Assert(rows.Scan(&policy), qt.IsNil)
		policies = append(policies, policy)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return policies, refused
}
