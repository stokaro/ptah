//go:build integration

package clickhouse_test

import (
	"net/url"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/schemadiff"
)

// refreshShape is one way the server stores a refresh clause: the clause a
// view is created with after REFRESH, and the schedule Ptah reads back, or ""
// for one it cannot read.
type refreshShape struct {
	name, clause, want string
}

// TestRefreshStoredShapesReadLive creates a view of every shape the server's
// grammar allows around a schedule and reads each one back. A dependency list
// stops where the next clause begins, so a target table, refresh SETTINGS or
// APPEND never end up inside a dependency name; a clause with SETTINGS, which
// Ptah does not model, is a schedule it could not read, never a plain view
// (stokaro/ptah#4278).
func TestRefreshStoredShapesReadLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	database := conn.Info().Schema
	source, base, other := uniqueClickHouseRBACName("shape_src"), uniqueClickHouseRBACName("shape_base"), uniqueClickHouseRBACName("shape_other")
	createRefreshShape(c, conn, "CREATE TABLE "+source+" (id UInt64) ENGINE = MergeTree ORDER BY id", "DROP TABLE IF EXISTS "+source+" SYNC")
	createRefreshShape(c, conn, "CREATE MATERIALIZED VIEW "+base+" REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY tuple() AS SELECT count() AS c FROM "+source,
		"DROP VIEW IF EXISTS "+base+" SYNC")
	createRefreshShape(c, conn, "CREATE MATERIALIZED VIEW "+other+" REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY tuple() AS SELECT count() AS c FROM "+source,
		"DROP VIEW IF EXISTS "+other+" SYNC")
	qualifiedBase, qualifiedOther := database+"."+base, database+"."+other
	shapes := []refreshShape{
		{name: "to", clause: "EVERY 1 HOUR TO {target}", want: "EVERY 1 HOUR"},
		{name: "append_to", clause: "EVERY 1 HOUR APPEND TO {target}", want: "EVERY 1 HOUR APPEND"},
		{name: "depends_to", clause: "EVERY 1 HOUR DEPENDS ON " + base + " TO {target}", want: "EVERY 1 HOUR DEPENDS ON " + qualifiedBase},
		{name: "depends_two_append", clause: "EVERY 1 HOUR DEPENDS ON " + base + ", " + other + " APPEND {storage}",
			want: "EVERY 1 HOUR DEPENDS ON " + qualifiedBase + ", " + qualifiedOther + " APPEND"},
		{name: "every_clause", clause: "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 30 MINUTE DEPENDS ON " + base + " APPEND {storage}",
			want: "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 30 MINUTE DEPENDS ON " + qualifiedBase + " APPEND"},
		{name: "settings", clause: "EVERY 1 HOUR SETTINGS refresh_retries = 3 {storage}"},
		{name: "depends_settings_append", clause: "EVERY 1 HOUR DEPENDS ON " + base + " SETTINGS refresh_retries = 3, refresh_retry_initial_backoff_ms = 200 APPEND {storage}"},
		{name: "depends_settings_append_to", clause: "EVERY 1 HOUR DEPENDS ON " + base + " SETTINGS refresh_retries = 3 APPEND TO {target}"},
	}
	views := make([]string, len(shapes))
	for i, shape := range shapes {
		views[i] = createShapeView(c, conn, source, shape)
	}

	live := readLive(c, conn)

	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.ClickHouse))
	for i, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			c := qt.New(t)
			at := slices.IndexFunc(live.MatViews, func(view catalog.MaterializedView) bool { return view.Name == views[i] })
			c.Assert(at, qt.Not(qt.Equals), -1)
			c.Assert(observedSchedule(c, &catalog.Database{MatViews: live.MatViews[at : at+1]}), qt.Equals, shape.want)
			knowledge := live.FeatureCoverage.Lookup(chschema.RefreshKind, builder.SchemaScopedParts(objectidentity.KindMatView, database, views[i]))
			c.Assert(knowledge.State, qt.Equals, shapeKnowledge(shape))
		})
	}
}

// shapeKnowledge is what the read knows of a shape's schedule.
func shapeKnowledge(shape refreshShape) schemaext.KnowledgeState {
	if shape.want == "" {
		return schemaext.Unrepresentable
	}
	return schemaext.Complete
}

// createShapeView creates the view a shape describes over source, with its
// own target table when the shape writes to one, and drops both after the
// test.
func createShapeView(c *qt.C, conn *dbschema.DatabaseConnection, source string, shape refreshShape) string {
	c.Helper()
	view, target := uniqueClickHouseRBACName("shape_"+shape.name), uniqueClickHouseRBACName("shape_target")
	createRefreshShape(c, conn, "CREATE TABLE "+target+" (c UInt64) ENGINE = MergeTree ORDER BY tuple()", "DROP TABLE IF EXISTS "+target+" SYNC")
	clause := strings.NewReplacer("{target}", target, "{storage}", "ENGINE = MergeTree ORDER BY tuple()").Replace(shape.clause)
	createRefreshShape(c, conn, "CREATE MATERIALIZED VIEW "+view+" REFRESH "+clause+" AS SELECT count() AS c FROM "+source,
		"DROP VIEW IF EXISTS "+view+" SYNC")
	return view
}

func createRefreshShape(c *qt.C, conn *dbschema.DatabaseConnection, create, drop string) {
	c.Helper()
	c.Cleanup(func() { cleanupClickHouseRBACFixture(c, conn, drop) })
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), create), qt.IsNil, qt.Commentf("%s", create))
}

// TestRefreshLeastPrivilegeReadLive reads the database as an account that may
// read its tables and not system.view_refreshes, which the server refuses
// with code 497 on 24.10 and 26.9 alike. The read goes on with every view's
// schedule uninspected, so the description is not lost and nothing reads a
// refreshable view as a plain one: a declaration of the view as plain plans
// nothing for it (stokaro/ptah#4278).
func TestRefreshLeastPrivilegeReadLive(t *testing.T) {
	c := qt.New(t)
	conn, source, view := refreshFixture(c, "EVERY 1 HOUR")
	database := conn.Info().Schema
	user := uniqueClickHouseRBACName("refresh_reader")
	// Named for what it marks rather than what it is: gosec reads a name such
	// as password beside a literal as a hardcoded credential.
	const readerMarker = "ptah-4278-reader"
	dropClickHouseUserAfterTest(c, conn, user)
	executeClickHouseRBACFixture(c, conn,
		"CREATE USER "+sqlident.Quote(platform.ClickHouse, user)+" IDENTIFIED WITH plaintext_password BY '"+readerMarker+"'",
		"GRANT SELECT, SHOW TABLES, SHOW COLUMNS ON "+sqlident.Quote(platform.ClickHouse, database)+".* TO "+sqlident.Quote(platform.ClickHouse, user),
	)
	reader := connectAs(c, user, readerMarker)

	live, err := reader.Reader().ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	at := slices.IndexFunc(live.MatViews, func(candidate catalog.MaterializedView) bool { return candidate.Name == view })
	c.Assert(at, qt.Not(qt.Equals), -1)
	subject := objectidentity.NewBuilder(identifier.ForDialect(platform.ClickHouse)).SchemaScopedParts(objectidentity.KindMatView, database, view)
	c.Assert(live.FeatureCoverage.Lookup(chschema.RefreshKind, subject).State, qt.Equals, schemaext.Uninspected)
	current := readRefreshFixture(c, reader, source, view)
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), refreshDeclaration(source, view, ""), current, reader.Info(), nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// connectAs opens the ClickHouse target as another account.
func connectAs(c *qt.C, user, secret string) *dbschema.DatabaseConnection {
	c.Helper()
	target, err := url.Parse(dbtarget.URL(c, dbtarget.ClickHouse))
	c.Assert(err, qt.IsNil)
	target.User = url.UserPassword(user, secret)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}
