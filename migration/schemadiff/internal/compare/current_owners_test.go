package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/migration/schemadiff/internal/compare"
)

// ownedObjects is a read that reports the owner of a view, of a materialized
// view and of a table, and a view whose owner it could not name.
func ownedObjects() *catalog.Database {
	return &catalog.Database{ObjectOwners: []catalog.ObjectOwner{
		{Kind: "view", Schema: "public", Name: "recent", Owner: "app"},
		{Kind: "materialized view", Schema: "public", Name: "totals", Owner: "reporting"},
		{Kind: "table", Schema: "public", Name: "items", Owner: "app"},
		{Kind: "view", Schema: "public", Name: "unnamed", Owner: ""},
	}}
}

// TestCurrentOwners_HappyPath pins which owners a comparison carries for a
// plan to give back. A target whose descriptor manages the owner of a view
// and of a materialized view carries both, and nothing else the read
// reports. A target that manages no owner carries none: SQL Server reads the
// owners and has no plan that restores one, and a name no descriptor
// describes, as a provider's engine is, reaches a projection that refuses
// every field it has no wire form for.
func TestCurrentOwners_HappyPath(t *testing.T) {
	managed := []difftypes.OwnerRef{
		{Kind: "VIEW", Name: "public.recent", Owner: "app"},
		{Kind: "MATERIALIZED VIEW", Name: "public.totals", Owner: "reporting"},
	}
	tests := []struct {
		name, target string
		want         []difftypes.OwnerRef
	}{
		{name: "PostgreSQL", target: platform.Postgres, want: managed},
		{name: "a spelling PostgreSQL folds", target: "postgresql", want: managed},
		{name: "CockroachDB", target: platform.CockroachDB, want: managed},
		{name: "YugabyteDB", target: platform.YugabyteDB, want: managed},
		{name: "SQL Server, which reads owners and manages none", target: platform.SQLServer},
		{name: "YDB, which manages no owner", target: "ydb"},
		{name: "a name no descriptor describes, as a provider's engine is", target: "no-such-engine"},
		{name: "no target", target: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := compare.CurrentOwners(ownedObjects(), test.target)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
