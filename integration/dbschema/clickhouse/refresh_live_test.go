//go:build integration

package clickhouse_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// refreshFixture creates a source table with two rows and a materialized view
// over it, refreshable on the given clause or plain when it is empty.
func refreshFixture(c *qt.C, clause string) (conn *dbschema.DatabaseConnection, source, view string) {
	c.Helper()
	conn = openLiveClickHouseRBACTarget(c)
	source, view = uniqueClickHouseRBACName("refresh_src"), uniqueClickHouseRBACName("refresh_mv")
	c.Cleanup(func() {
		cleanupClickHouseRBACFixture(c, conn, "DROP VIEW IF EXISTS "+view+" SYNC")
		cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+source+" SYNC")
	})
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE "+source+" (id UInt64) ENGINE = MergeTree ORDER BY id"), qt.IsNil)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "INSERT INTO "+source+" VALUES (1), (2)"), qt.IsNil)
	refresh := ""
	if clause != "" {
		refresh = "REFRESH " + clause + " "
	}
	c.Assert(conn.Writer().ExecuteSQL(c.Context(),
		"CREATE MATERIALIZED VIEW "+view+" "+refresh+"ENGINE = MergeTree ORDER BY tuple() AS SELECT count() AS c FROM "+source), qt.IsNil)
	return conn, source, view
}

// readRefreshFixture keeps the fixture's table, its view and their coverage,
// so a comparison against a shared server does not plan other tests' objects.
func readRefreshFixture(c *qt.C, conn *dbschema.DatabaseConnection, source, view string) *catalog.Database {
	c.Helper()
	live := readLive(c, conn)
	table := slices.IndexFunc(live.Tables, func(table catalog.Table) bool { return table.Name == source })
	matview := slices.IndexFunc(live.MatViews, func(candidate catalog.MaterializedView) bool { return candidate.Name == view })
	c.Assert(table, qt.Not(qt.Equals), -1)
	c.Assert(matview, qt.Not(qt.Equals), -1)
	builder := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	kept := []objectidentity.Key{
		builder.TableParts(live.Tables[table].Schema, source).Key(),
		builder.SchemaScopedParts(objectidentity.KindMatView, live.MatViews[matview].Schema, view).Key(),
	}
	coverage := live.FeatureCoverage.SelectSubjects(func(id objectidentity.ID) bool { return slices.Contains(kept, id.Key()) })
	return &catalog.Database{Tables: []catalog.Table{live.Tables[table]}, MatViews: []catalog.MaterializedView{live.MatViews[matview]}, FeatureCoverage: coverage}
}

// refreshDeclaration declares the fixture with the given schedule, the way a
// Go annotation source does: a view without one declares a plain view.
func refreshDeclaration(source, view, clause string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: source, StructName: "Source"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Source", Type: "UInt64", Primary: true}},
		MaterializedViews: []schemamodel.MaterializedView{{
			Name: view, StructName: "View", Body: "SELECT count() AS c FROM " + source,
			Facets: must.Must(chsource.RefreshFacets(clause)),
		}},
		FeatureCoverage: must.Must(chsource.RefreshCoverage()),
	}
}

func observedSchedule(c *qt.C, db *catalog.Database) string {
	c.Helper()
	schedule, found, err := schemaext.FacetAs[*chschema.ObservedRefresh](db.MatViews[0].Facets, chschema.RefreshKind)
	c.Assert(err, qt.IsNil)
	if !found {
		return ""
	}
	return schedule.Clause()
}

func viewUUID(c *qt.C, conn *dbschema.DatabaseConnection, view string) string {
	c.Helper()
	var uuid string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = ?", view).Scan(&uuid), qt.IsNil)
	return uuid
}

// planRefresh compares the declaration with the fixture and returns both
// directions of the plan as statements.
func planRefresh(c *qt.C, conn *dbschema.DatabaseConnection, desired *schemamodel.Database, current *catalog.Database) (forward, reverse []string) {
	c.Helper()
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "clickhouse", Capabilities: conn.Info().Capabilities,
	})
	c.Assert(err, qt.IsNil)
	up, err := builtin.RenderSQL("clickhouse", plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	down, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	return sqlutil.SplitStatementsForDialect("clickhouse", up), sqlutil.SplitStatementsForDialect("clickhouse", down)
}

func assertConverged(c *qt.C, conn *dbschema.DatabaseConnection, desired *schemamodel.Database, current *catalog.Database) {
	c.Helper()
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, conn.Info(), nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// A refreshable view whose schedule changes to another is changed in place:
// the plan alters the view rather than dropping it, the server reports the new
// schedule, and the view is the same object afterwards. The rollback changes
// the schedule back the same way.
func TestRefreshScheduleChangeKeepsTheViewLive(t *testing.T) {
	c := qt.New(t)
	conn, source, view := refreshFixture(c, "EVERY 1 HOUR")
	before := viewUUID(c, conn, view)
	c.Assert(before, qt.Not(qt.Equals), "00000000-0000-0000-0000-000000000000")
	current := readRefreshFixture(c, conn, source, view)
	c.Assert(observedSchedule(c, current), qt.Equals, "EVERY 1 HOUR")
	desired := refreshDeclaration(source, view, "every 120 minute")

	forward, reverse := planRefresh(c, conn, desired, current)

	c.Assert(forward, qt.HasLen, 1)
	c.Assert(forward[0], qt.Contains, "MODIFY REFRESH EVERY 2 HOUR")
	applyStatements(c, conn, forward)
	changed := readRefreshFixture(c, conn, source, view)
	c.Assert(observedSchedule(c, changed), qt.Equals, "EVERY 2 HOUR")
	c.Assert(viewUUID(c, conn, view), qt.Equals, before)
	assertConverged(c, conn, desired, changed)
	c.Assert(joinStatements(reverse), qt.Not(qt.Contains), "DROP VIEW")
	c.Assert(joinStatements(reverse), qt.Contains, "MODIFY REFRESH EVERY 1 HOUR")
	applyStatements(c, conn, reverse)
	restored := readRefreshFixture(c, conn, source, view)
	c.Assert(observedSchedule(c, restored), qt.Equals, "EVERY 1 HOUR")
	c.Assert(viewUUID(c, conn, view), qt.Equals, before)
}

// A plain view gaining a schedule is replaced, because the server refuses
// MODIFY REFRESH on a plain view; the recreated view carries the schedule, and
// the rollback replaces it again without one.
func TestRefreshScheduleGainedReplacesTheViewLive(t *testing.T) {
	c := qt.New(t)
	conn, source, view := refreshFixture(c, "")
	current := readRefreshFixture(c, conn, source, view)
	c.Assert(observedSchedule(c, current), qt.Equals, "")
	desired := refreshDeclaration(source, view, "after 30 minute append")

	forward, reverse := planRefresh(c, conn, desired, current)

	c.Assert(joinStatements(forward), qt.Contains, "DROP VIEW IF EXISTS")
	c.Assert(joinStatements(forward), qt.Contains, "REFRESH AFTER 30 MINUTE APPEND")
	applyStatements(c, conn, forward)
	changed := readRefreshFixture(c, conn, source, view)
	c.Assert(observedSchedule(c, changed), qt.Equals, "AFTER 30 MINUTE APPEND")
	assertConverged(c, conn, desired, changed)
	c.Assert(joinStatements(reverse), qt.Not(qt.Contains), "REFRESH AFTER")
	applyStatements(c, conn, reverse)
	c.Assert(observedSchedule(c, readRefreshFixture(c, conn, source, view)), qt.Equals, "")
}

// A declaration stating every clause, in a spelling the server rewrites, is
// applied and then compares as synchronized against what the server stored.
// Adding APPEND replaces the view, because MODIFY REFRESH refuses to.
func TestRefreshScheduleEveryClauseConvergesLive(t *testing.T) {
	c := qt.New(t)
	conn, source, view := refreshFixture(c, "EVERY 1 HOUR")
	current := readRefreshFixture(c, conn, source, view)
	desired := refreshDeclaration(source, view, "every 1 day offset 120 minute randomize for 1800 second append")

	forward, _ := planRefresh(c, conn, desired, current)

	c.Assert(joinStatements(forward), qt.Contains, "DROP VIEW IF EXISTS")
	c.Assert(joinStatements(forward), qt.Not(qt.Contains), "MODIFY REFRESH")
	applyStatements(c, conn, forward)
	changed := readRefreshFixture(c, conn, source, view)
	c.Assert(observedSchedule(c, changed), qt.Equals, "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 30 MINUTE APPEND")
	assertConverged(c, conn, desired, changed)
}

// Adding a dependency is made in place, and the view keeps its identity.
func TestRefreshScheduleDependencyIsChangedInPlaceLive(t *testing.T) {
	c := qt.New(t)
	conn, source, view := refreshFixture(c, "EVERY 1 HOUR")
	_, _, other := refreshFixture(c, "EVERY 1 HOUR")
	before := viewUUID(c, conn, view)
	current := readRefreshFixture(c, conn, source, view)
	desired := refreshDeclaration(source, view, "every 1 hour depends on "+other)

	forward, _ := planRefresh(c, conn, desired, current)

	c.Assert(forward, qt.HasLen, 1)
	c.Assert(forward[0], qt.Contains, "MODIFY REFRESH EVERY 1 HOUR DEPENDS ON ")
	applyStatements(c, conn, forward)
	changed := readRefreshFixture(c, conn, source, view)
	c.Assert(observedSchedule(c, changed), qt.Contains, "DEPENDS ON ")
	c.Assert(viewUUID(c, conn, view), qt.Equals, before)
	assertConverged(c, conn, desired, changed)
}

// A source that cannot state a schedule leaves the server's alone: a
// declaration without refresh coverage compares as synchronized against a
// refreshable view, rather than planning the replacement that would remove
// the schedule and the view's rows.
func TestRefreshScheduleOfAnUndescribingSourceIsKeptLive(t *testing.T) {
	c := qt.New(t)
	conn, source, view := refreshFixture(c, "EVERY 1 HOUR")
	current := readRefreshFixture(c, conn, source, view)
	desired := refreshDeclaration(source, view, "")
	desired.FeatureCoverage = schemaext.Coverage{}

	assertConverged(c, conn, desired, current)
}

func joinStatements(statements []string) string {
	return strings.Join(statements, ";\n")
}
