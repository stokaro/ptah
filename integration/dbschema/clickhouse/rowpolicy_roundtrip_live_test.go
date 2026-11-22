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
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/schemadiff"
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

// TestRowPolicyOfAMissingTableIsNotReadLive keeps a policy whose table is gone
// out of the read. The server keeps a policy when its table is dropped, and
// read, the policy has no table to be compared beside, so every comparison of
// the database fails with "feature object has no parent on its source side".
func TestRowPolicyOfAMissingTableIsNotReadLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	database := conn.Info().Schema
	table := uniqueClickHouseRBACName("gone_orders")
	policy := uniqueClickHouseRBACName("gone_policy")
	createClickHouseRBACTable(c, conn, database, table)
	dropClickHouseRowPolicyAfterTest(c, conn, database, table, policy)
	qualified := sqlident.Qualified(platform.ClickHouse, database, table)
	executeClickHouseRBACFixture(c, conn,
		"CREATE ROW POLICY "+sqlident.Quote(platform.ClickHouse, policy)+" ON "+qualified+" USING id > 0 TO ALL",
		"DROP TABLE "+qualified+" SYNC",
	)
	c.Assert(rowPolicyTables(c, conn, policy), qt.DeepEquals, []string{table})

	live := readLive(c, conn)
	_, err := schemadiff.CompareWithDatabase(c.Context(), conn, &schemamodel.Database{}, live, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(rowPolicyFilters(c, conn, policy), qt.HasLen, 0)
}

// rowPolicyTables reads the tables the server holds the named policy on,
// whether or not they exist.
func rowPolicyTables(c *qt.C, conn *dbschema.DatabaseConnection, name string) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT table FROM system.row_policies WHERE short_name = ? ORDER BY table", name)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var tables []string
	for rows.Next() {
		var table string
		c.Assert(rows.Scan(&table), qt.IsNil)
		tables = append(tables, table)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
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
