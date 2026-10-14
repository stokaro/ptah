//go:build integration

package clickhouse_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// HCL retains the filter, composition, and recipients through apply and export.
func TestRowPolicyFromHCLSourceRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	fixture := newRowPolicyFixture(c)
	runtime := must.Must(builtin.New())
	source := fmt.Sprintf(`row_policy %q {
 on = table[%q]
 using = sql("tenant=2")
 as = "RESTRICTIVE"
 to = "ALL EXCEPT %s"
 }`, fixture.policyByName, fixture.table, fixture.bob)
	declared, err := atlashcl.ParseWithOptions([]byte(source), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	table := fixture.declaration(nil)
	declared.Tables, declared.Fields = table.Tables, table.Fields
	schemamodel.Finalize(declared)
	forward, reverse := fixture.plan(c, declared)
	c.Assert(forward, qt.HasLen, 1)
	c.Assert(forward[0], qt.Matches, "CREATE ROW POLICY .*")
	applyStatements(c, fixture.conn, forward)
	c.Assert(fixture.visible(c, fixture.aliceConn), qt.DeepEquals, []uint64{2})
	c.Assert(fixture.visible(c, fixture.bobConn), qt.DeepEquals, []uint64{1, 2, 3})
	again, _ := fixture.plan(c, declared)
	c.Assert(again, qt.HasLen, 0)
	exported, err := atlashclrender.Render(declared, atlashclrender.Options{HCL: runtime.HCL(), Dialect: "clickhouse"})
	c.Assert(err, qt.IsNil)
	c.Assert(exported.Diagnostics, qt.HasLen, 0)
	reloaded, err := atlashcl.ParseWithOptions(exported.Data, "copy.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	again, _ = fixture.plan(c, reloaded)
	c.Assert(again, qt.HasLen, 0)
	applyStatements(c, fixture.conn, reverse)
	c.Assert(fixture.current(c).FeatureObjects.Len(), qt.Equals, 0)
}
