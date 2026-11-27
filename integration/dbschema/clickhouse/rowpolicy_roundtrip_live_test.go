//go:build integration

package clickhouse_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
)

// TestRowPolicyFromGoSourceRoundTripsLive drives the whole shipping path: a
// row policy declared with the owner's Go directive, restrictive and for every
// user but one, is read through the runtime's annotation set, planned against
// what the reader reports, applied, enforced for the users it names, read back
// restrictive, and planned as nothing after (stokaro/ptah#4343).
func TestRowPolicyFromGoSourceRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	fixture := newRowPolicyFixture(c)
	declared := must.Must(goschema.ParseSource(must.Must(builtin.Annotations()), "orders.go", fmt.Sprintf(`package models

//ptah:schema:table name=%[1]q
//ptah:schema:rowpolicy name=%[2]q table=%[1]q using="tenant = 2" to="ALL EXCEPT %[3]s" as="RESTRICTIVE"
type Orders struct {
	//ptah:schema:field name="id" type="UInt64" primary="true"
	ID uint64
	//ptah:schema:field name="tenant" type="UInt8" not_null="true"
	Tenant uint8
}
`, fixture.table, fixture.policyByName, fixture.bob)))
	schemamodel.Finalize(&declared)

	forward, _ := fixture.plan(c, &declared)

	c.Assert(forward, qt.HasLen, 1)
	c.Assert(forward[0], qt.Matches, "CREATE ROW POLICY `tenant_rows` ON `"+fixture.table+"` USING \\(tenant = 2\\) AS RESTRICTIVE TO ALL EXCEPT `"+fixture.bob+"`;?")
	applyStatements(c, fixture.conn, forward)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{2})
	c.Assert(fixture.visible(c, fixture.bobConn), qt.DeepEquals, []uint64{1, 2, 3})
	read := must.Must(fixture.current(c).FeatureObjects.All())
	c.Assert(read, qt.HasLen, 1)
	observed := read[0].Value.(*chschema.ObservedRowPolicy)
	c.Assert(observed.Composition, qt.Equals, chschema.Restrictive)
	c.Assert(observed.Roles.Equal(chschema.RoleSelection{All: true, Except: []string{fixture.bob}}), qt.IsTrue)
	again, _ := fixture.plan(c, &declared)
	c.Assert(again, qt.HasLen, 0)
}

// TestClickHouseSwallowsAWriteCheckLive measures the acceptance a source
// refuses a write check over: `WITH CHECK` is not a syntax error here -- the
// statement succeeds -- and the clause does not survive into the catalog, so
// a policy carrying one would filter reads and leave writes open.
func TestClickHouseSwallowsAWriteCheckLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	database := conn.Info().Schema
	table := uniqueClickHouseRBACName("check_orders")
	policy := uniqueClickHouseRBACName("check_policy")
	createClickHouseRBACTable(c, conn, database, table)
	dropClickHouseRowPolicyAfterTest(c, conn, database, table, policy)

	c.Assert(conn.Writer().ExecuteSQL(c.Context(),
		"CREATE ROW POLICY "+policy+" ON "+database+"."+table+
			" USING tenant_id = 1 WITH CHECK tenant_id = 1"),
		qt.IsNil, qt.Commentf("the engine is expected to ACCEPT this, which is the whole point"))

	// The catalog has one filter column, and nothing anywhere records the check.
	c.Assert(rowPolicyFilters(c, conn, policy), qt.DeepEquals, []string{"tenant_id = 1"})
}

// rowPolicyFilters is the filter of each row policy named name that the
// shipping reader reports.
func rowPolicyFilters(c *qt.C, conn *dbschema.DatabaseConnection, name string) []string {
	c.Helper()
	schema, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	var filters []string
	for _, object := range must.Must(schema.FeatureObjects.All()) {
		if object.Ref.Kind == objectidentity.Kind(chschema.RowPolicyKind) && object.Ref.Name.Source == name {
			filters = append(filters, *object.Value.(*chschema.ObservedRowPolicy).Filter)
		}
	}
	return filters
}

// TestRowPolicyOfADroppedTableIsReadLive reads, on the server itself, what
// ClickHouse keeps of a policy whose table was dropped and of policies on
// relations the read does not describe as tables or on every table of the
// database. The policy on the dropped table is read as an observed policy,
// which the comparison plans as a policy of its own; each of the others is
// recorded as one the read cannot describe, naming the relation's engine as
// system.tables reports it, so the comparison keeps it and reports it.
func TestRowPolicyOfADroppedTableIsReadLive(t *testing.T) {
	c := qt.New(t)
	relations := []struct {
		name, create string
		want         schemaext.Knowledge
	}{
		{"base", "CREATE TABLE base (id UInt64) ENGINE = MergeTree ORDER BY id", schemaext.Knowledge{State: schemaext.Complete}},
		{"gone", "CREATE TABLE gone (id UInt64) ENGINE = MergeTree ORDER BY id", schemaext.Knowledge{State: schemaext.Complete}},
		{"v", "CREATE VIEW v AS SELECT id FROM base", undescribedRelation("v", "View")},
		{"mv", "CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM base", undescribedRelation("mv", "MaterializedView")},
		{"nul", "CREATE TABLE nul (id UInt64) ENGINE = Null", undescribedRelation("nul", "Null")},
		{"dict", "CREATE DICTIONARY dict (id UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)", undescribedRelation("dict", "Dictionary")},
		{"ptah_schema_apply_log", "CREATE TABLE ptah_schema_apply_log (id UInt64) ENGINE = MergeTree ORDER BY id", schemaext.Knowledge{State: schemaext.Unrepresentable,
			Reason: `the policy is on "ptah_schema_apply_log", Ptah's own bookkeeping table, which the read leaves out`}},
	}
	var names []string
	for _, relation := range relations {
		names = append(names, relation.name)
	}
	conn := openOutlivedPolicyDatabase(c, names...)
	wide := "p_wide ON " + sqlident.Quote(platform.ClickHouse, conn.Info().Schema) + ".*"
	c.Cleanup(func() { cleanupClickHouseRBACFixture(c, conn, "DROP ROW POLICY IF EXISTS "+wide) })
	for _, relation := range relations {
		executeClickHouseRBACFixture(c, conn, relation.create, "CREATE ROW POLICY p_"+relation.name+" ON "+relation.name+" USING 1 TO ALL")
	}
	executeClickHouseRBACFixture(c, conn, "CREATE ROW POLICY "+wide+" USING 1 TO ALL", "DROP TABLE gone SYNC")

	live := readLive(c, conn)

	var read []string
	for _, object := range must.Must(live.FeatureObjects.All()) {
		read = append(read, object.Ref.String())
	}
	c.Assert(read, qt.DeepEquals, []string{chschema.RowPolicyRef("", "base", "p_base").String(), chschema.RowPolicyRef("", "gone", "p_gone").String()})
	for _, relation := range relations {
		c.Assert(live.FeatureCoverage.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("", relation.name, "p_"+relation.name)),
			qt.DeepEquals, relation.want, qt.Commentf("%s", relation.name))
	}
	c.Assert(live.FeatureCoverage.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("", "*", "p_wide")), qt.DeepEquals, schemaext.Knowledge{
		State: schemaext.Unrepresentable, Reason: "the policy is written ON " + conn.Info().Schema + ".*, for every table of the database, " +
			"which Ptah does not model"})
}

// undescribedRelation is what a read knows about a policy on the relation
// name, whose engine it does not describe as a table.
func undescribedRelation(name, engine string) schemaext.Knowledge {
	return schemaext.Knowledge{State: schemaext.Unrepresentable,
		Reason: fmt.Sprintf("the policy is on %q (engine %s), which the read does not describe as a table", name, engine)}
}

// openOutlivedPolicyDatabase creates a database for one test and connects to
// it, so a policy the test leaves on a missing table is in no other test's
// database. When the test ends it drops the database and, before it, the
// policy p_<relation> on each of relations: ClickHouse keeps a row policy
// when its table or its database is dropped.
func openOutlivedPolicyDatabase(c *qt.C, relations ...string) *dbschema.DatabaseConnection {
	c.Helper()
	admin := openLiveClickHouseRBACTarget(c)
	database := uniqueClickHouseRBACName("outlived")
	quoted := sqlident.Quote(platform.ClickHouse, database)
	executeClickHouseRBACFixture(c, admin, "CREATE DATABASE "+quoted)
	c.Cleanup(func() { cleanupClickHouseRBACFixture(c, admin, "DROP DATABASE IF EXISTS "+quoted+" SYNC") })
	for _, relation := range relations {
		c.Cleanup(func() {
			cleanupClickHouseRBACFixture(c, admin, "DROP ROW POLICY IF EXISTS "+sqlident.Quote(platform.ClickHouse, "p_"+relation)+
				" ON "+sqlident.Qualified(platform.ClickHouse, database, relation))
		})
	}
	scratch, err := atlasurl.WithDatabaseName(dbtarget.URL(c, dbtarget.ClickHouse), database)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), scratch)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// dropClickHouseRowPolicyAfterTest removes the policy this suite created, so a
// shared database is not left carrying a filter nobody declared.
func dropClickHouseRowPolicyAfterTest(c *qt.C, conn *dbschema.DatabaseConnection, database, table, policy string) {
	c.Helper()
	c.Cleanup(func() {
		cleanupClickHouseRBACFixture(c, conn,
			"DROP ROW POLICY IF EXISTS "+sqlident.Quote(platform.ClickHouse, policy)+
				" ON "+sqlident.Qualified(platform.ClickHouse, database, table))
	})
}
