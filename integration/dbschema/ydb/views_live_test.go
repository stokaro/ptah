//go:build integration

package ydb_test

import (
	"context"
	"net/url"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/cli/introspect"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/lint"
)

// The directories the view tests write into: a view at the top of one, and a
// view in a directory below it, each over a table beside it.
const (
	viewsSchema        = "ptah_ydb_views"
	viewsArchiveSchema = "ptah_ydb_views/archive"
)

var viewsSchemas = []string{viewsSchema, viewsArchiveSchema}

// viewsDeclaration is two tables and three views: one whose query is written
// with comments, odd spacing and a trailing semicolon, one over it, declared
// first so only the plan's order puts it after the view it reads, and one in
// a directory below.
func viewsDeclaration(activeQuery string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Item", Name: "items", Schema: viewsSchema},
			{StructName: "Old", Name: "old", Schema: viewsArchiveSchema},
		},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Item", Name: "label", Type: "TEXT", Nullable: true},
			{StructName: "Item", Name: "qty", Type: "INTEGER", Nullable: true},
			{StructName: "Old", Name: "id", Type: "BIGINT", Primary: true},
		},
		Views: []schemamodel.View{
			{Name: viewsSchema + ".active_count", Body: "SELECT COUNT(*) AS n FROM `ptah_ydb_views/active`"},
			{Name: viewsSchema + ".active", Body: activeQuery},
			{Name: viewsArchiveSchema + ".old_ids", Body: "SELECT id FROM `ptah_ydb_views/archive/old`"},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// activeQuery is the active view's query as an author might write it.
const activeQuery = "select   id,\n  `label` -- the name shown\nFROM `ptah_ydb_views/items`\nWHERE qty>0;"

// dropViewsAndTables drops every view and then every table in the directories
// a test owns.
func dropViewsAndTables(c *qt.C, conn *dbschema.DatabaseConnection, schemas []string) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(context.Background(), conn, schemas)
	c.Assert(err, qt.IsNil)
	for _, view := range live.Views {
		statement := "DROP VIEW " + sqlident.Quote("ydb", view.Schema+"/"+view.Name)
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), statement), qt.IsNil)
	}
	dropTables(c, conn, schemas)
}

// viewNames lists the views a read describes as directory|name.
func viewNames(live *catalog.Database) []string {
	names := make([]string, 0, len(live.Views))
	for _, view := range live.Views {
		names = append(names, view.Schema+"|"+view.Name)
	}
	slices.Sort(names)
	return names
}

// viewNamed is the view a read describes in schema under name.
func viewNamed(c *qt.C, live *catalog.Database, schema, name string) catalog.View {
	c.Helper()
	index := slices.IndexFunc(live.Views, func(view catalog.View) bool {
		return view.Schema == schema && view.Name == name
	})
	c.Assert(index >= 0, qt.IsTrue, qt.Commentf("view %s/%s is not in the read", schema, name))
	return live.Views[index]
}

// countOf answers a one-number query.
func countOf(c *qt.C, conn *dbschema.DatabaseConnection, query string) int64 {
	c.Helper()
	var n int64
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&n), qt.IsNil, qt.Commentf("query: %s", query))
	return n
}

// TestYDBViews_RoundTrip_NothingLeftToPlan is the views family's round trip:
// tables and views rendered, applied statement by statement, read back and
// compared with nothing left to plan, and the same declaration applied again
// with nothing planned after it. A view over a view is created after the view
// it reads, and a view in a directory is created at its path.
func TestYDBViews_RoundTrip_NothingLeftToPlan(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, viewsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, viewsSchemas) })

			declared := viewsDeclaration(activeQuery)
			first := planAgainst(c, conn, declared, viewsSchemas)
			apply(c, conn, first)

			c.Assert(planAgainst(c, conn, declared, viewsSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, viewsSchemas))
			c.Assert(planAgainst(c, conn, declared, viewsSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBViews_ReadsWhatTheServerStored pins what the reader reports for the
// round trip's views, read from the server: each view by its directory, and
// its query in the server's form -- tokens joined by single spaces, with no
// comment and no semicolon. The views run: the view over a view counts the
// rows the view under it lets through.
func TestYDBViews_ReadsWhatTheServerStored(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, viewsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, viewsSchemas) })
			apply(c, conn, planAgainst(c, conn, viewsDeclaration(activeQuery), viewsSchemas))
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"UPSERT INTO `ptah_ydb_views/items` (`id`, `label`, `qty`) VALUES (1l, 'a'u, 1), (2l, 'b'u, 0)"), qt.IsNil)

			live := readScoped(c, conn, viewsSchemas)

			c.Assert(viewNames(live), qt.DeepEquals, []string{
				"ptah_ydb_views/archive|old_ids",
				"ptah_ydb_views|active",
				"ptah_ydb_views|active_count",
			})
			c.Assert(viewNamed(c, live, viewsSchema, "active").Body, qt.Equals,
				"select id , `label` FROM `ptah_ydb_views/items` WHERE qty > 0")
			c.Assert(viewNamed(c, live, viewsArchiveSchema, "old_ids").Body, qt.Equals,
				"SELECT id FROM `ptah_ydb_views/archive/old`")
			c.Assert(countOf(c, conn, "SELECT n FROM `ptah_ydb_views/active_count`"), qt.Equals, int64(1))
		})
	}
}

// TestYDBViews_ChangedQueryIsDroppedAndCreated changes a view's query. YDB
// has no CREATE OR REPLACE VIEW, so the plan drops the view and creates it
// with the new query, nothing is left to plan after it, and the view answers
// with the new query.
func TestYDBViews_ChangedQueryIsDroppedAndCreated(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, viewsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, viewsSchemas) })
			apply(c, conn, planAgainst(c, conn, viewsDeclaration(activeQuery), viewsSchemas))
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"UPSERT INTO `ptah_ydb_views/items` (`id`, `label`, `qty`) VALUES (1l, 'a'u, 1), (2l, 'b'u, 0)"), qt.IsNil)

			changed := viewsDeclaration("SELECT id, label FROM `ptah_ydb_views/items`")
			planned := planAgainst(c, conn, changed, viewsSchemas)
			apply(c, conn, planned)

			c.Assert(planned, qt.DeepEquals, []string{
				"DROP VIEW `ptah_ydb_views/active`",
				"CREATE VIEW `ptah_ydb_views/active` WITH (security_invoker = TRUE) AS\n" +
					"SELECT id, label FROM `ptah_ydb_views/items`",
			})
			c.Assert(planAgainst(c, conn, changed, viewsSchemas), qt.HasLen, 0)
			c.Assert(countOf(c, conn, "SELECT n FROM `ptah_ydb_views/active_count`"), qt.Equals, int64(2))
		})
	}
}

// TestYDBViews_DroppedBeforeTheirTables removes every table and view. YDB
// drops a table a view reads and keeps the view, so the plan drops the views
// first, a view before the view it reads, and the migration it writes passes
// the lint rule that reports such a drop (YD106). Nothing is left to plan or
// to read after it.
func TestYDBViews_DroppedBeforeTheirTables(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, viewsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, viewsSchemas) })
			created := planAgainst(c, conn, viewsDeclaration(activeQuery), viewsSchemas)
			apply(c, conn, created)

			empty := &schemamodel.Database{}
			dropped := planAgainst(c, conn, empty, viewsSchemas)
			rules := lintPlans(c, conn, created, dropped)
			// The control: the same table drops without the view drops before
			// them are what the rule reports.
			orphaning := lintPlans(c, conn, created, dropped[3:])
			apply(c, conn, dropped)

			c.Assert(dropped, qt.DeepEquals, []string{
				"DROP VIEW `ptah_ydb_views/archive/old_ids`",
				"DROP VIEW `ptah_ydb_views/active_count`",
				"DROP VIEW `ptah_ydb_views/active`",
				"DROP TABLE `ptah_ydb_views/items`",
				"DROP TABLE `ptah_ydb_views/archive/old`",
			})
			c.Assert(rules, qt.Not(qt.Contains), "YD106")
			c.Assert(orphaning, qt.Contains, "YD106")
			c.Assert(planAgainst(c, conn, empty, viewsSchemas), qt.HasLen, 0)
			live := readScoped(c, conn, viewsSchemas)
			c.Assert(live.Views, qt.HasLen, 0)
			c.Assert(live.Tables, qt.HasLen, 0)
		})
	}
}

// databasePath is the absolute path of the line's database, such as /local,
// which a path prefix has to start with: YDB refuses a view created under a
// relative one (`Table path not in database`).
func databasePath(c *qt.C, line ydbLine) string {
	c.Helper()
	parsed, err := url.Parse(dbtarget.URL(c, line.engine))
	c.Assert(err, qt.IsNil)
	return parsed.Path
}

// lintPlans lints a two-version directory, one plan after another, against
// what the connected server established about itself, and returns every rule
// reported on the second.
func lintPlans(c *qt.C, conn *dbschema.DatabaseConnection, first, second []string) []string {
	c.Helper()
	info := conn.Info()
	files := fstest.MapFS{
		"0000000001_first.up.sql":  {Data: []byte(strings.Join(first, ";\n") + ";\n")},
		"0000000002_second.up.sql": {Data: []byte(strings.Join(second, ";\n") + ";\n")},
	}
	findings, err := lint.LintFS(files, lint.Options{
		Dialect: info.Dialect,
		Target:  lint.TargetFromServer(info.Dialect, info.Version, info.Capabilities, ""),
	})
	c.Assert(err, qt.IsNil)
	var rules []string
	for _, finding := range findings {
		if finding.File == "0000000002_second.up.sql" {
			rules = append(rules, finding.Rule)
		}
	}
	return rules
}

// TestYDBViews_PragmaIsPartOfTheView creates a view the way a script that sets
// a path prefix does. YDB stores the pragma with the view's query, because it
// decides what the query's names mean, so the view is not the declaration of
// the same query without it: the plan replaces it once, and nothing is left
// to plan after that. The view answers alike before and after, because the
// declaration names the whole path the pragma supplied.
func TestYDBViews_PragmaIsPartOfTheView(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, viewsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, viewsSchemas) })
			declared := viewsDeclaration(activeQuery)
			apply(c, conn, planAgainst(c, conn, declared, viewsSchemas))
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "DROP VIEW `ptah_ydb_views/archive/old_ids`"), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"PRAGMA TablePathPrefix('"+databasePath(c, line)+"/ptah_ydb_views/archive');\n"+
					"CREATE VIEW old_ids WITH (security_invoker = TRUE) AS SELECT id FROM old"), qt.IsNil)

			stored := viewNamed(c, readScoped(c, conn, viewsSchemas), viewsArchiveSchema, "old_ids").Body
			planned := planAgainst(c, conn, declared, viewsSchemas)
			apply(c, conn, planned)

			c.Assert(stored, qt.Matches, `PRAGMA TablePathPrefix\('/\w+/ptah_ydb_views/archive'\);\nSELECT id FROM old`)
			c.Assert(planned, qt.DeepEquals, []string{
				"DROP VIEW `ptah_ydb_views/archive/old_ids`",
				"CREATE VIEW `ptah_ydb_views/archive/old_ids` WITH (security_invoker = TRUE) AS\n" +
					"SELECT id FROM `ptah_ydb_views/archive/old`",
			})
			c.Assert(planAgainst(c, conn, declared, viewsSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBViews_IntrospectedModelsPlanNothing writes the views of a database
// as Go models with ptah introspect: each view's query is the text the
// server stores, and the models, planned against the database they came
// from, plan nothing.
func TestYDBViews_IntrospectedModelsPlanNothing(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, viewsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, viewsSchemas) })
			apply(c, conn, planAgainst(c, conn, viewsDeclaration(activeQuery), viewsSchemas))
			out := c.TempDir()

			stdout, err := runCommand(introspect.NewIntrospectCommand(),
				"--db-url", dbtarget.URL(c, line.engine), "--schemas", strings.Join(viewsSchemas, ","), "--out", out)
			c.Assert(err, qt.IsNil, qt.Commentf("introspect:\n%s", stdout))
			models, err := goschema.ParseDir(builtintest.Annotations(), out)
			c.Assert(err, qt.IsNil)

			c.Assert(viewNames(readScoped(c, conn, viewsSchemas)), qt.HasLen, 3)
			c.Assert(models.Views, qt.HasLen, 3)
			c.Assert(planAgainst(c, conn, models, viewsSchemas), qt.HasLen, 0)
		})
	}
}
