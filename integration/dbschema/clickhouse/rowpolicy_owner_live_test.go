//go:build integration

package clickhouse_test

import (
	"database/sql"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// The row policy owner is registered dormant: no source or reader produces
// the model yet. These tests build both sides themselves -- the declaration,
// and the observation from system.row_policies -- and drive them through the
// shipping comparison, planner and renderer, then check what the server
// enforces by querying as the users the policies name.

// rowPolicyFixture is a table with three tenants' rows and two users allowed
// to read it, alice and bob, neither of whom a policy names yet.
type rowPolicyFixture struct {
	conn         *dbschema.DatabaseConnection
	table        string
	alice, bob   string
	aliceConn    *dbschema.DatabaseConnection
	bobConn      *dbschema.DatabaseConnection
	policyByName string
}

const rowPolicyReaderMarker = "ptah-4140-row-policy-reader"

func newRowPolicyFixture(c *qt.C) rowPolicyFixture {
	c.Helper()
	conn := openLiveClickHouseRBACTarget(c)
	fixture := rowPolicyFixture{conn: conn, table: uniqueClickHouseRBACName("rp_orders"),
		alice: uniqueClickHouseRBACName("rp_alice"), bob: uniqueClickHouseRBACName("rp_bob"), policyByName: "tenant_rows"}
	quote := func(name string) string { return sqlident.Quote(platform.ClickHouse, name) }
	table := quote(conn.Info().Schema) + "." + quote(fixture.table)
	c.Cleanup(func() {
		cleanupClickHouseRBACFixture(c, conn, "DROP ROW POLICY IF EXISTS "+quote(fixture.policyByName)+" ON "+table)
		cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+table+" SYNC")
	})
	dropClickHouseUserAfterTest(c, conn, fixture.alice)
	dropClickHouseUserAfterTest(c, conn, fixture.bob)
	executeClickHouseRBACFixture(c, conn,
		"CREATE TABLE "+table+" (id UInt64, tenant UInt8) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO "+table+" VALUES (1, 1), (2, 2), (3, 3)",
	)
	for _, user := range []string{fixture.alice, fixture.bob} {
		executeClickHouseRBACFixture(c, conn,
			"CREATE USER "+quote(user)+" IDENTIFIED WITH plaintext_password BY '"+rowPolicyReaderMarker+"'",
			"GRANT SELECT ON "+table+" TO "+quote(user))
	}
	fixture.aliceConn, fixture.bobConn = connectAs(c, fixture.alice, rowPolicyReaderMarker), connectAs(c, fixture.bob, rowPolicyReaderMarker)
	return fixture
}

// visible is the ids a user reads from the table, in order.
func (f rowPolicyFixture) visible(c *qt.C, conn *dbschema.DatabaseConnection) []uint64 {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT id FROM "+sqlident.Quote(platform.ClickHouse, f.table)+" ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	ids := make([]uint64, 0, 3)
	for rows.Next() {
		var id uint64
		c.Assert(rows.Scan(&id), qt.IsNil)
		ids = append(ids, id)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return ids
}

// declaration declares the table and, when policy is set, the policy on it,
// with the database left to the connection as a source would leave it.
func (f rowPolicyFixture) declaration(policy *chschema.DesiredRowPolicy) *schemamodel.Database {
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Orders", Name: f.table}},
		Fields: []schemamodel.Field{
			{StructName: "Orders", Name: "id", Type: "UInt64", Primary: true},
			{StructName: "Orders", Name: "tenant", Type: "UInt8"},
		},
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	if policy != nil {
		declared.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(chschema.DesiredRowPolicyObject(
			chschema.RowPolicyRef("", f.table, f.policyByName), *policy))))
	}
	schemamodel.Finalize(declared)
	return declared
}

// current reads the table the way the shipping reader does and the policies
// on it from system.row_policies, which no shipping reader turns into the
// owner's model yet. The common policy list is left out, so the common path
// plans nothing for them.
func (f rowPolicyFixture) current(c *qt.C) *catalog.Database {
	c.Helper()
	live := readLive(c, f.conn)
	at := slices.IndexFunc(live.Tables, func(table catalog.Table) bool { return table.Name == f.table })
	c.Assert(at, qt.Not(qt.Equals), -1)
	rows, err := f.conn.QueryContext(c.Context(), `
		SELECT short_name, select_filter, is_restrictive, apply_to_all, apply_to_list, apply_to_except
		FROM system.row_policies WHERE database = currentDatabase() AND table = ?`, f.table)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var objects []schemaext.Object
	for rows.Next() {
		var name string
		var filter sql.NullString
		var restrictive, all bool
		var names, except []string
		c.Assert(rows.Scan(&name, &filter, &restrictive, &all, &names, &except), qt.IsNil)
		observed := chschema.ObservedRowPolicy{Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: all, Names: names, Except: except}}
		if restrictive {
			observed.Composition = chschema.Restrictive
		}
		if filter.Valid {
			observed.Filter = new(filter.String)
		}
		objects = append(objects, must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef(f.conn.Info().Schema, f.table, name), observed)))
	}
	c.Assert(rows.Err(), qt.IsNil)
	coverage := must.Must(live.FeatureCoverage.Combine(must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))))
	return &catalog.Database{Tables: []catalog.Table{live.Tables[at]}, FeatureObjects: must.Must(schemaext.NewObjects(objects...)), FeatureCoverage: coverage}
}

// plan compares the declaration with the server through the connection, so
// the owner's probe spells the declared filter, and returns both directions.
func (f rowPolicyFixture) plan(c *qt.C, declared *schemamodel.Database) (forward, reverse []string) {
	c.Helper()
	runtime := must.Must(builtin.New())
	current := f.current(c)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), f.conn, declared, current, nil, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: declared, CurrentSchema: current, Dialect: "clickhouse", Capabilities: f.conn.Info().Capabilities,
	})
	c.Assert(err, qt.IsNil)
	up := must.Must(builtin.RenderSQL("clickhouse", plan.Forward.Nodes...))
	down := must.Must(builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...))
	return sqlutil.SplitStatementsForDialect("clickhouse", up), sqlutil.SplitStatementsForDialect("clickhouse", down)
}

// A declared policy is created, enforced for the users it names and not for
// the rest, read back as the same policy, and dropped by the reverse plan.
func TestRowPolicyOwnerCreatesAndEnforcesLive(t *testing.T) {
	c := qt.New(t)
	fixture := newRowPolicyFixture(c)
	declared := fixture.declaration(&chschema.DesiredRowPolicy{Filter: new("tenant=1"), Roles: chschema.RoleSelection{Names: []string{fixture.alice}}})

	forward, reverse := fixture.plan(c, declared)

	c.Assert(forward, qt.HasLen, 1)
	c.Assert(forward[0], qt.Matches, "CREATE ROW POLICY `tenant_rows` ON `"+fixture.table+"` USING \\(tenant=1\\) AS PERMISSIVE TO `"+fixture.alice+"`;?")
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{1, 2, 3})
	applyStatements(c, fixture.conn, forward)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{1})
	c.Assert(fixture.visible(c, fixture.bobConn), qt.DeepEquals, []uint64{1, 2, 3})
	again, _ := fixture.plan(c, declared)
	c.Assert(again, qt.HasLen, 0)
	applyStatements(c, fixture.conn, reverse)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{1, 2, 3})
	c.Assert(fixture.current(c).FeatureObjects.Len(), qt.Equals, 0)
}

// A policy changed in place to restrictive, for every user but bob, with
// another filter, keeps one object: the server reports the restrictive
// composition and the exception, alice reads only what the restrictive filter
// admits, and the reverse plan changes it back. The server stores the filter
// as (id >= 2) AND (id <= 3), so the comparison converges only through the
// server's own spelling of the declared one.
func TestRowPolicyOwnerChangesARestrictivePolicyInPlaceLive(t *testing.T) {
	c := qt.New(t)
	fixture := newRowPolicyFixture(c)
	created, _ := fixture.plan(c, fixture.declaration(&chschema.DesiredRowPolicy{Filter: new("tenant = 1"),
		Roles: chschema.RoleSelection{Names: []string{fixture.alice}}}))
	applyStatements(c, fixture.conn, created)
	changed := fixture.declaration(&chschema.DesiredRowPolicy{Filter: new("id BETWEEN 2 AND 3"), Composition: chschema.Restrictive,
		Roles: chschema.RoleSelection{All: true, Except: []string{fixture.bob}}})

	forward, reverse := fixture.plan(c, changed)

	c.Assert(forward, qt.HasLen, 1)
	c.Assert(forward[0], qt.Matches, "ALTER ROW POLICY `tenant_rows` ON `"+fixture.table+"` USING \\(id BETWEEN 2 AND 3\\) AS RESTRICTIVE TO ALL EXCEPT `"+fixture.bob+"`;?")
	applyStatements(c, fixture.conn, forward)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{2, 3})
	c.Assert(fixture.visible(c, fixture.bobConn), qt.DeepEquals, []uint64{1, 2, 3})
	read := must.Must(fixture.current(c).FeatureObjects.All())
	c.Assert(read, qt.HasLen, 1)
	observed := read[0].Value.(*chschema.ObservedRowPolicy)
	c.Assert(observed.Composition, qt.Equals, chschema.Restrictive)
	c.Assert(observed.Roles, qt.DeepEquals, chschema.RoleSelection{All: true, Except: []string{fixture.bob}})
	again, _ := fixture.plan(c, changed)
	c.Assert(again, qt.HasLen, 0)
	applyStatements(c, fixture.conn, reverse)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{1})
	c.Assert(fixture.visible(c, fixture.bobConn), qt.DeepEquals, []uint64{1, 2, 3})
}

// A policy the declaration no longer states is dropped, and its users read
// every row again.
func TestRowPolicyOwnerDropsAPolicyLive(t *testing.T) {
	c := qt.New(t)
	fixture := newRowPolicyFixture(c)
	created, _ := fixture.plan(c, fixture.declaration(&chschema.DesiredRowPolicy{Filter: new("tenant = 3"), Composition: chschema.Restrictive,
		Roles: chschema.RoleSelection{Names: []string{fixture.alice}}}))
	applyStatements(c, fixture.conn, created)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{3})

	forward, reverse := fixture.plan(c, fixture.declaration(nil))

	c.Assert(forward, qt.HasLen, 1)
	c.Assert(forward[0], qt.Matches, "DROP ROW POLICY `tenant_rows` ON (`[^`]+`\\.)?`"+fixture.table+"`;?")
	applyStatements(c, fixture.conn, forward)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{1, 2, 3})
	applyStatements(c, fixture.conn, reverse)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{3})
}
