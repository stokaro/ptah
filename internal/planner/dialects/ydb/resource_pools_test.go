package ydb_test

import (
	"context"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/internal/ydbpool"
	"ptah.run/migration/schemadiff/difftypes"
)

// withPools is a YDB line on a cluster whose EnableResourcePools flag is on.
func withPools() capability.Capabilities {
	return capability.YDB251().With(capability.ResourcePools, true)
}

// TestGenerateMigrationAST_ResourcePools_HappyPath pins where pools and
// classifiers sit in a plan: after the users the plan creates, since a
// classifier names one, and after the grants, and before the users it drops;
// drops before creations, classifiers before the pools they name when
// dropped, and pools before the classifiers that name them when created.
func TestGenerateMigrationAST_ResourcePools_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		RolesAdded:   difftypes.RoleChanges{{Name: "etl", Login: true}},
		RolesRemoved: difftypes.RoleChanges{{Name: "olduser"}},
		GrantsAdded: []difftypes.GrantRef{
			{Role: "etl", Privilege: "YDB.DATABASE.CONNECT", ObjectType: "DATABASE"},
		},
		CurrentDatabasePath: "/local",
		ResourcePoolsAdded: difftypes.ResourcePoolChanges{{Name: "batch",
			Spec: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(10))}}},
		ResourcePoolsRemoved: difftypes.ResourcePoolChanges{{Name: "old"}},
		ResourcePoolsModified: []difftypes.ResourcePoolDiff{{Name: "default",
			Desired: ast.ResourcePoolSpec{ResourceWeight: new(30.0)}, Current: ast.ResourcePoolSpec{}}},
		ResourcePoolClassifiersAdded: difftypes.ResourcePoolClassifierChanges{{Name: "etl_users",
			Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 10}}},
		ResourcePoolClassifiersRemoved: difftypes.ResourcePoolClassifierChanges{{Name: "old_users",
			Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "old", Rank: 20}}},
		ResourcePoolClassifiersModified: []difftypes.ResourcePoolClassifierDiff{{Name: "everyone",
			Desired: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", Rank: 1000},
			Current: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 1000}}},
	}

	got := render(c, withPools(), diff)

	c.Assert(got, qt.Equals, "CREATE USER `etl`;\n"+
		"GRANT 'ydb.database.connect' ON `/local` TO `etl`;\n"+
		"DROP RESOURCE POOL CLASSIFIER `old_users`;\n"+
		"DROP RESOURCE POOL `old`;\n"+
		"CREATE RESOURCE POOL `batch` WITH (CONCURRENT_QUERY_LIMIT = 10);\n"+
		"ALTER RESOURCE POOL `default` SET (RESOURCE_WEIGHT = 30);\n"+
		"ALTER RESOURCE POOL CLASSIFIER `everyone` SET (RESOURCE_POOL = 'batch', RANK = 1000);\n"+
		"CREATE RESOURCE POOL CLASSIFIER `etl_users` WITH (RESOURCE_POOL = 'batch', RANK = 10, MEMBER_NAME = 'etl');\n"+
		"DROP USER IF EXISTS `olduser`;\n")
}

// YDB keeps one classifier per rank at every step, so classifiers that trade
// ranks cannot be altered one after the other: each is dropped and created
// again once both ranks are free. A classifier moving onto a rank no
// classifier of the plan holds is altered in place.
func TestGenerateMigrationAST_ResourcePoolClassifierRanks_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ResourcePoolClassifiersModified: []difftypes.ResourcePoolClassifierDiff{
			{Name: "a", RankChanged: true,
				Desired: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 2},
				Current: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 1}},
			{Name: "b", RankChanged: true,
				Desired: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 1},
				Current: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 2}},
			{Name: "c", RankChanged: true,
				Desired: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 30},
				Current: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 3}},
		},
	}

	got := render(c, withPools(), diff)

	c.Assert(got, qt.Equals, "DROP RESOURCE POOL CLASSIFIER `a`;\n"+
		"DROP RESOURCE POOL CLASSIFIER `b`;\n"+
		"ALTER RESOURCE POOL CLASSIFIER `c` SET (RESOURCE_POOL = 'default', RANK = 30);\n"+
		"CREATE RESOURCE POOL CLASSIFIER `a` WITH (RESOURCE_POOL = 'default', RANK = 2);\n"+
		"CREATE RESOURCE POOL CLASSIFIER `b` WITH (RESOURCE_POOL = 'default', RANK = 1);\n")
}

// A pool or classifier change YDB cannot make is refused before any node is
// returned.
func TestGenerateMigrationAST_ResourcePools_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "a pool on a line whose flag is off",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{ResourcePoolsAdded: difftypes.ResourcePoolChanges{{Name: "batch"}}},
			want: `resource pool "batch", which requires target capability resource_pools, unavailable on this ydb ` +
				`target; ` + regexp.QuoteMeta(ydbpool.FlagHint),
		},
		{
			name: "a classifier on a line whose flag is off",
			caps: capability.YDB251(),
			diff: &difftypes.SchemaDiff{ResourcePoolClassifiersRemoved: difftypes.ResourcePoolClassifierChanges{{
				Name: "c", Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "default"},
			}}},
			want: `resource pool classifier "c", which requires target capability resource_pools, .*`,
		},
		{
			name: "dropping the pool default",
			caps: withPools(),
			diff: &difftypes.SchemaDiff{ResourcePoolsRemoved: difftypes.ResourcePoolChanges{{Name: "default"}}},
			want: "DROP RESOURCE POOL default: it is the pool YDB runs every query in that no classifier sends " +
				"elsewhere, .*",
		},
		{
			name: "two classifiers on one rank once the plan ran",
			caps: withPools(),
			diff: &difftypes.SchemaDiff{
				ResourcePoolClassifiersAdded: difftypes.ResourcePoolClassifierChanges{{Name: "a",
					Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 5}}},
				ResourcePoolClassifiersModified: []difftypes.ResourcePoolClassifierDiff{{Name: "b", RankChanged: true,
					Desired: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 5},
					Current: ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 6}}},
			},
			want: `resource pool classifier "b": its rank 5 is the rank classifier "a" takes in the same plan, and ` +
				`YDB keeps one classifier per rank`,
		},
		{
			name: "a pool built by hand that YDB refuses",
			caps: withPools(),
			diff: &difftypes.SchemaDiff{ResourcePoolsModified: []difftypes.ResourcePoolDiff{{Name: "batch",
				Desired: ast.ResourcePoolSpec{QueueSize: new(int32(1))}}}},
			want: `resource pool "batch": a queue needs concurrent_query_limit or database_load_cpu_threshold .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				test.diff,
			)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
