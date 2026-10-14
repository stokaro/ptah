//go:build integration

package clickhouse_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// A SQL source uses the same live normalization and owner planner as Go and
// YAML. Applying the resulting policy must converge and enforce its filter.
func TestRowPolicyFromSQLSourceRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	fixture := newRowPolicyFixture(c)
	runtime := must.Must(builtin.New())
	source := fmt.Sprintf("CREATE TABLE `%s` (id UInt64, tenant UInt8) ENGINE = MergeTree ORDER BY id;\n"+
		"CREATE ROW POLICY `%s` ON `%s` FOR SELECT USING tenant=2 AS RESTRICTIVE TO ALL EXCEPT `%s`;",
		fixture.table, fixture.policyByName, fixture.table, fixture.bob)
	declared, _, err := sqlschema.ReadWith(c.Context(), []byte(source), "clickhouse", nil, sqlschema.Options{Owners: runtime.SQL()})
	c.Assert(err, qt.IsNil)
	schemamodel.Finalize(&declared)
	forward, _ := fixture.plan(c, &declared)
	c.Assert(forward, qt.HasLen, 1)
	c.Assert(forward[0], qt.Matches, "CREATE ROW POLICY .*")
	applyStatements(c, fixture.conn, forward)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{2})
	c.Assert(fixture.visible(c, fixture.bobConn), qt.DeepEquals, []uint64{1, 2, 3})
	again, _ := fixture.plan(c, &declared)
	c.Assert(again, qt.HasLen, 0)
}
