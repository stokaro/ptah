package ydb_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

func withPools() capability.Capabilities {
	return capability.YDB251().With(capability.ResourcePools, true)
}

func poolChange(name string, before *ydbworkload.ObservedPool, after *ydbworkload.DesiredPool) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbworkload.PoolRef(name), Value: &ydbdiff.ResourcePool{Before: before, After: after}}
}

func classifierChange(name string, before *ydbworkload.ObservedClassifier, after *ydbworkload.DesiredClassifier) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbworkload.ClassifierRef(name), Value: &ydbdiff.ResourcePoolClassifier{Before: before, After: after}}
}

// The host contributes principals while the owner contributes workload changes.
// Their graph releases classifiers before pools and creates principals before
// workload changes, with principal removal after the workload is settled.
func TestGenerateMigrationAST_ResourcePools_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		RolesAdded:          difftypes.RoleChanges{{Name: "etl", Login: true}},
		RolesRemoved:        difftypes.RoleChanges{{Name: "olduser"}},
		GrantsAdded:         []difftypes.GrantRef{{Role: "etl", Privilege: "YDB.DATABASE.CONNECT", ObjectType: "DATABASE"}},
		CurrentDatabasePath: "/local",
		FeatureChanges: []schemaext.ChangeRecord{
			poolChange("batch", nil, &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(10))}}),
			poolChange("old", &ydbworkload.ObservedPool{}, nil),
			poolChange("default", &ydbworkload.ObservedPool{}, &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ResourceWeight: new(30.0)}}),
			classifierChange("etl_users", nil, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 10}}),
			classifierChange("old_users", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "old", Rank: 20}}, nil),
			classifierChange("everyone", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 1000}}, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 1000}}),
		},
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

// Rank exchanges require releasing occupied ranks, while a move to an unused
// rank stays in place. The public planner must preserve the owner's batch.
func TestGenerateMigrationAST_ResourcePoolClassifierRanks_HappyPath(t *testing.T) {
	c := qt.New(t)
	var changes []schemaext.ChangeRecord
	for _, move := range []struct {
		name          string
		before, after int64
	}{{"a", 1, 2}, {"b", 2, 1}, {"c", 3, 30}} {
		changes = append(changes, classifierChange(move.name,
			&ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: move.before}},
			&ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: move.after}}))
	}
	got := render(c, withPools(), &difftypes.SchemaDiff{FeatureChanges: changes})
	c.Assert(got, qt.Equals, "DROP RESOURCE POOL CLASSIFIER `a`;\n"+
		"DROP RESOURCE POOL CLASSIFIER `b`;\n"+
		"ALTER RESOURCE POOL CLASSIFIER `c` SET (RESOURCE_POOL = 'default', RANK = 30);\n"+
		"CREATE RESOURCE POOL CLASSIFIER `a` WITH (RESOURCE_POOL = 'default', RANK = 2);\n"+
		"CREATE RESOURCE POOL CLASSIFIER `b` WITH (RESOURCE_POOL = 'default', RANK = 1);\n")
}

func TestGenerateMigrationAST_ResourcePools_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		caps     capability.Capabilities
		changes  []schemaext.ChangeRecord
		want     string
		sentinel error
	}{
		{
			name: "pool feature disabled", caps: capability.YDB262(),
			changes: []schemaext.ChangeRecord{poolChange("batch", nil, &ydbworkload.DesiredPool{})},
			want:    `(?s).*resource pools and classifiers; ` + regexp.QuoteMeta(ydbworkload.FlagHint) + `.*`, sentinel: ptaherr.ErrUnsupportedFeature,
		},
		{
			name: "classifier feature disabled", caps: capability.YDB251(),
			changes: []schemaext.ChangeRecord{classifierChange("c", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default"}}, nil)},
			want:    `(?s).*resource pools and classifiers; ` + regexp.QuoteMeta(ydbworkload.FlagHint) + `.*`, sentinel: ptaherr.ErrUnsupportedFeature,
		},
		{
			name: "default cannot be dropped", caps: withPools(),
			changes: []schemaext.ChangeRecord{poolChange("default", &ydbworkload.ObservedPool{}, nil)},
			want:    `(?s).*default pool cannot be created or dropped.*`, sentinel: ptaherr.ErrInvalidSchemaDiff,
		},
		{
			name: "duplicate final rank", caps: withPools(),
			changes: []schemaext.ChangeRecord{
				classifierChange("a", nil, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 5}}),
				classifierChange("b", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 6}}, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 5}}),
			},
			want: `(?s).*classifiers .* cannot share rank 5.*`, sentinel: ptaherr.ErrInvalidSchemaDiff,
		},
		{
			name: "invalid queue settings", caps: withPools(),
			changes: []schemaext.ChangeRecord{poolChange("batch", &ydbworkload.ObservedPool{}, &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{QueueSize: new(int32(1))}})},
			want:    `(?s).*a queue needs concurrent_query_limit or database_load_cpu_threshold.*`, sentinel: schemaext.ErrInvalidValue,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(t.Context(), must.Must(builtin.New()), &difftypes.SchemaDiff{FeatureChanges: test.changes})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, test.sentinel)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// A capability bit cannot install YDB's workload provider on another dialect.
func TestResourcePoolChangesRefuseOtherTargets(t *testing.T) {
	for _, target := range []string{"postgres", "mysql", "mariadb", "sqlite", "clickhouse", "sqlserver", "oracle", "cockroachdb", "spanner"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			for _, change := range []schemaext.ChangeRecord{
				poolChange("batch", nil, &ydbworkload.DesiredPool{}),
				poolChange("batch", &ydbworkload.ObservedPool{}, nil),
				poolChange("batch", &ydbworkload.ObservedPool{}, &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ResourceWeight: new(30.0)}}),
				classifierChange("route", nil, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default"}}),
				classifierChange("route", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default"}}, nil),
				classifierChange("route", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default"}}, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 1}}),
			} {
				statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), must.Must(builtin.New()),
					&difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{change}}, target,
					planner.Options{Capabilities: capability.ForDialect(target).With(capability.ResourcePools, true)})
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(statements, qt.IsNil)
			}
		})
	}
}
