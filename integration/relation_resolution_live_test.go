//go:build integration

package integration_test

import (
	"database/sql"
	"net/url"
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

// resolutionTarget is a scratch PostgreSQL database built from schema and
// reached through a URL carrying searchPath, empty for none. The returned
// handle uses the same URL, so a statement that names no schema runs where
// Ptah's session would run it.
func resolutionTarget(c *qt.C, schema, searchPath string) (string, *sql.DB) {
	c.Helper()
	address := searchPathURL(c, postgresScratchDevURL(c, "ptah_relation_resolution"), "")
	db, err := sql.Open("pgx", address)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), schema)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)

	address = searchPathURL(c, address, searchPath)
	db, err = sql.Open("pgx", address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return address, db
}

// searchPathURL is rawURL with the search_path query parameter set to
// searchPath, or removed for an empty one.
func searchPathURL(c *qt.C, rawURL, searchPath string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Del("search_path")
	if searchPath != "" {
		query.Set("search_path", searchPath)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// diffResolution compares the SQL schema declared against the schemas of the
// database at address over a connection, as ptah schema apply does, so the diff
// carries the session's default schema and search path.
func diffResolution(c *qt.C, address, declared string, schemas []string) *difftypes.SchemaDiff {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(declared), 0o600), qt.IsNil)
	runtime := builtintest.Runtime()
	desired, err := schemafile.LoadPath(c.Context(), path, schemafile.Options{
		YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL(), Dialect: platform.Postgres,
	})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, schemas)
	c.Assert(err, qt.IsNil)
	return must.Must(schemadiff.CompareWithDatabase(c.Context(), conn, desired, live, nil, must.Must(builtin.New())))
}

// planResolution plans the change from the database at address to declared in the
// statement style.
func planResolution(c *qt.C, address, declared string, schemas []string, style planext.StatementStyle) ([]string, error) {
	c.Helper()
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()),
		diffResolution(c, address, declared, schemas), platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: style})
}

// relationACL is the access privileges of the relation named qualified, as
// pg_class.relacl prints them, or <none> for a relation with none granted.
func relationACL(c *qt.C, db *sql.DB, qualified string) string {
	c.Helper()
	var acl string
	c.Assert(db.QueryRowContext(c.Context(),
		`SELECT COALESCE(relacl::text, '<none>') FROM pg_class WHERE oid = $1::regclass`, qualified).Scan(&acl), qt.IsNil)
	return acl
}

// schemaViews names the views of schema, sorted.
func schemaViews(c *qt.C, db *sql.DB, schema string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `SELECT table_name FROM information_schema.views WHERE table_schema = $1 ORDER BY 1`, schema)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var views []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		views = append(views, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return views
}

// TestConnectionReportsTheSearchPathLive pins the search path a connection
// reports beside its default schema: every schema a name written without one
// resolves in, which is what a policy's expression is printed back against.
// A schema named after the role is on the default path, `"$user", public`, and
// is the default schema when it exists; `$user` in a row stands for the role.
func TestConnectionReportsTheSearchPathLive(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		searchPath string
		want       []string
	}{
		{
			name: "a path naming two schemas", schema: "CREATE SCHEMA reporting;", searchPath: "reporting,public",
			want: []string{"reporting", "public"},
		},
		{
			name: "a path naming a schema that does not exist", schema: "CREATE SCHEMA reporting;", searchPath: "nosuch,public",
			want: []string{"public"},
		},
		{name: "no path on the URL", schema: "CREATE SCHEMA reporting;", want: []string{"public"}},
		{
			name:   "no path on the URL and a schema named after the role",
			schema: `DO $$ BEGIN EXECUTE format('CREATE SCHEMA %I', current_user); END $$;`,
			want:   []string{"$user", "public"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			address, db := resolutionTarget(c, test.schema, test.searchPath)
			var role string
			c.Assert(db.QueryRowContext(c.Context(), "SELECT current_user").Scan(&role), qt.IsNil)
			want := make([]string, 0, len(test.want))
			for _, schema := range test.want {
				want = append(want, strings.ReplaceAll(schema, "$user", role))
			}

			conn, err := dbschema.ConnectToDatabase(c.Context(), address)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			c.Assert(conn.Info().IdentifierSemantics.SearchPath, qt.DeepEquals, want)
			c.Assert(conn.Info().IdentifierSemantics.DefaultSchema, qt.Equals, want[0])
		})
	}
}

const (
	namesakeTables = `CREATE SCHEMA reporting;
CREATE TABLE public.item_skus (id integer PRIMARY KEY, sku varchar(64));
CREATE VIEW public.sku_list AS SELECT id, sku FROM item_skus;
`
	namesakeReporting        = "CREATE TABLE reporting.item_skus (id integer PRIMARY KEY, sku varchar(64));\nCREATE VIEW reporting.sku_report AS SELECT id, sku FROM reporting.item_skus;\n"
	namesakeReportingWidened = "CREATE TABLE reporting.item_skus (id integer PRIMARY KEY, sku varchar(128));\nCREATE VIEW reporting.sku_report AS SELECT id, sku FROM reporting.item_skus;\n"
	namesakeDocs             = `CREATE TABLE public.docs (id integer PRIMARY KEY, sku_id integer);
ALTER TABLE public.docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_sku ON public.docs USING (EXISTS (SELECT 1 FROM sku_list s WHERE s.id = sku_id));
`
	namesakeTotals = `CREATE SCHEMA reporting;
CREATE TABLE public.orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW public.order_totals AS SELECT sum(amount) AS total FROM public.orders;
CREATE VIEW public.order_report AS SELECT total FROM order_totals;
CREATE TABLE public.audit (id integer PRIMARY KEY);
ALTER TABLE public.audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON public.audit USING (EXISTS (SELECT 1 FROM order_report));
`
)

// TestViewRebuildLeavesANamesakeReaderLive runs, in both statement styles, a
// rebuild under reporting while public.sku_list, or public.order_report,
// reads a relation of the same name in public by its bare name, and a policy
// reads that view. The view reads nothing the plan drops, so the plan applies,
// leaves it and the policy alone, and a second comparison finds nothing left
// to do. Read as the rebuilt relation, the bare name rebuilds the view, and
// the policy on it refuses the plan.
func TestViewRebuildLeavesANamesakeReaderLive(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		declared string
		want     []string
	}{
		{
			name:     "a column type change under reporting.sku_report",
			live:     namesakeTables + namesakeReporting + namesakeDocs,
			declared: namesakeTables + namesakeReportingWidened + namesakeDocs,
			want:     []string{"policy/public.docs.docs_sku", "view/public.sku_list", "view/reporting.sku_report"},
		},
		{
			name:     "a change to reporting.order_totals",
			live:     namesakeTotals + "CREATE MATERIALIZED VIEW reporting.order_totals AS SELECT count(*) AS n FROM public.orders;\n",
			declared: namesakeTotals + "CREATE MATERIALIZED VIEW reporting.order_totals AS SELECT count(*) AS n, max(id) AS last_id FROM public.orders;\n",
			want: []string{
				"matview/public.order_totals populated", "matview/reporting.order_totals populated",
				"policy/public.audit.audit_visible", "view/public.order_report",
			},
		},
	}
	for _, test := range tests {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				address, db := resolutionTarget(c, test.live, "")

				statements, err := planResolution(c, address, test.declared, relationMatchingSchemas, style.style)
				c.Assert(err, qt.IsNil)
				applyRelationMatching(c, db, statements)

				c.Assert(relationMatchingState(c, db), qt.DeepEquals, test.want)
				again := diffResolution(c, address, test.declared, relationMatchingSchemas)
				c.Assert(again.HasChanges(), qt.IsFalse, qt.Commentf("%+v", again))
			})
		}
	}
}

// TestViewRebuildKeepsANamesakeReadersGrantLive is the same rebuild with a
// grant on the view that reads the namesake and no policy: the view is not
// dropped, so it keeps the grant the schema does not declare. Rebuilt, it
// comes back without it.
func TestViewRebuildKeepsANamesakeReadersGrantLive(t *testing.T) {
	for _, style := range viewRebuildStyles {
		t.Run(style.name, func(t *testing.T) {
			c := qt.New(t)
			address, db := resolutionTarget(c, namesakeTables+namesakeReporting+"GRANT SELECT ON public.sku_list TO PUBLIC;\n", "")
			granted := relationACL(c, db, "public.sku_list")

			statements, err := planResolution(c, address, namesakeTables+namesakeReportingWidened, relationMatchingSchemas, style.style)
			c.Assert(err, qt.IsNil)
			applyRelationMatching(c, db, statements)

			c.Assert(granted, qt.Matches, `.*=r/.*`)
			c.Assert(relationACL(c, db, "public.sku_list"), qt.Equals, granted)
		})
	}
}

// TestViewRebuildRefusesAPolicyAlongTheSearchPathLive pins a policy that reads
// a changed materialized view through the search path. Under
// `search_path=reporting,public`, or under the default `"$user", public` with a
// schema named after the role, the default schema is not public, and
// PostgreSQL prints the policy's public.order_totals as `order_totals`. Read in
// the default schema alone, the policy reads nothing the plan drops: the
// CASCADE takes it in Ptah's own style, and the exact style's drop fails. Both
// styles refuse the plan, and the database keeps the policy.
func TestViewRebuildRefusesAPolicyAlongTheSearchPathLive(t *testing.T) {
	const (
		totals        = "CREATE MATERIALIZED VIEW public.order_totals AS SELECT sum(amount) AS total FROM public.orders;\n"
		totalsChanged = "CREATE MATERIALIZED VIEW public.order_totals AS SELECT sum(amount) AS total, count(*) AS n FROM public.orders;\n"
		audit         = `CREATE TABLE reporting.audit (id integer PRIMARY KEY);
ALTER TABLE reporting.audit ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_visible ON reporting.audit USING (EXISTS (SELECT 1 FROM order_totals));
`
		tables = "CREATE SCHEMA reporting;\nCREATE TABLE public.orders (id integer PRIMARY KEY, amount numeric NOT NULL);\n"
	)
	tests := []struct {
		name       string
		setup      string
		searchPath string
	}{
		{name: "a path naming two schemas", searchPath: "reporting,public"},
		{name: "the default path and a schema named after the role", setup: `DO $$ BEGIN EXECUTE format('CREATE SCHEMA %I', current_user); END $$;` + "\n"},
	}
	for _, test := range tests {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				address, db := resolutionTarget(c, tables+test.setup+totals+audit, test.searchPath)
				var defaultSchema string
				c.Assert(db.QueryRowContext(c.Context(), "SELECT current_schema()").Scan(&defaultSchema), qt.IsNil)
				schemas := []string{"public", "reporting", defaultSchema}

				statements, err := planResolution(c, address, tables+totalsChanged+audit, schemas, style.style)

				c.Assert(defaultSchema, qt.Not(qt.Equals), "public")
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches, `(?s).*public\.order_totals.*reporting\.audit\.audit_visible.*`)
				c.Assert(statements, qt.IsNil)
				c.Assert(relationMatchingState(c, db), qt.DeepEquals, []string{
					"matview/public.order_totals populated", "policy/reporting.audit.audit_visible",
				})
			})
		}
	}
}

// TestCreationOrderFollowsTheSessionSchemaLive pins the creation order under
// `search_path=app`: a view that reads app.zbase is created after the zbase
// declared without a schema, which the session creates in app. Ordered by the
// dialect's default schema, public, the reader is created first and fails with
// `relation "app.zbase" does not exist`.
func TestCreationOrderFollowsTheSessionSchemaLive(t *testing.T) {
	declared := `CREATE VIEW asummary AS SELECT id FROM app.zbase;
CREATE VIEW zbase AS SELECT 1 AS id;
`
	for _, style := range viewRebuildStyles {
		t.Run(style.name, func(t *testing.T) {
			c := qt.New(t)
			address, db := resolutionTarget(c, "CREATE SCHEMA app;", "app")

			statements, err := planResolution(c, address, declared, []string{"app"}, style.style)
			c.Assert(err, qt.IsNil)
			applyRelationMatching(c, db, statements)

			c.Assert(schemaViews(c, db, "app"), qt.DeepEquals, []string{"asummary", "zbase"})
			again := diffResolution(c, address, declared, []string{"app"})
			c.Assert(again.HasChanges(), qt.IsFalse, qt.Commentf("%+v", again))
		})
	}
}
