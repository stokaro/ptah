package planner_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// topicRefusals is what each planner but YDB's answers a topic change with.
// The topic owner is registered for YDB alone, so a planner without a feature
// host refuses the change by its subject, and the hosts of ClickHouse, the
// PostgreSQL family and SQL Server refuse it by its kind.
var topicRefusals = []struct {
	dialect string
	wantErr string
}{
	{dialect: platform.ClickHouse, wantErr: `unsupported feature: no planning service for "clickhouse"/"ptah\.run/ydb/topic-change"`},
	{dialect: platform.CockroachDB, wantErr: `unsupported feature: no planning service for "cockroachdb"/"ptah\.run/ydb/topic-change"`},
	{dialect: platform.MariaDB, wantErr: `unsupported feature: the mariadb planner has no feature handler for ptah\.run/ydb/topic events`},
	{dialect: platform.MySQL, wantErr: `unsupported feature: the mysql planner has no feature handler for ptah\.run/ydb/topic events`},
	{dialect: platform.Oracle, wantErr: `unsupported feature: the oracle planner has no feature handler for ptah\.run/ydb/topic events`},
	{dialect: platform.Postgres, wantErr: `unsupported feature: no planning service for "postgres"/"ptah\.run/ydb/topic-change"`},
	{dialect: platform.Spanner, wantErr: `unsupported feature: no planning service for "spanner"/"ptah\.run/ydb/topic-change"`},
	{dialect: platform.SQLite, wantErr: `unsupported feature: the sqlite planner has no feature handler for ptah\.run/ydb/topic events`},
	{dialect: platform.SQLServer, wantErr: `unsupported feature: no planning service for "sqlserver"/"ptah\.run/ydb/topic-change"`},
	{dialect: platform.YugabyteDB, wantErr: `unsupported feature: no planning service for "yugabytedb"/"ptah\.run/ydb/topic-change"`},
}

// TestEveryPlannerButYDBRefusesTopicChanges drives a hand-built diff that
// creates, drops and changes a topic through every registered planner but
// YDB's. The comparison feeding those planners refuses a declared topic
// first, so only such a diff reaches them, and a planner that planned nothing
// for it would report the topic applied.
func TestEveryPlannerButYDBRefusesTopicChanges(t *testing.T) {
	changes := map[string]*ydbdiff.Topic{
		"added":    {After: &ydbtopic.Desired{}},
		"removed":  {Before: &ydbtopic.Observed{}},
		"modified": {Before: &ydbtopic.Observed{}, After: &ydbtopic.Desired{Spec: ydbtopic.Spec{RetentionPeriod: "PT1H"}}},
	}
	for _, test := range topicRefusals {
		for name, change := range changes {
			t.Run(test.dialect+"/"+name, func(t *testing.T) {
				c := qt.New(t)
				diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("", "events"), Value: change}}}
				nodes, err := planner.GenerateSchemaDiffAST(context.Background(), must.Must(builtin.New()), diff, test.dialect)
				c.Assert(err, qt.ErrorMatches, test.wantErr)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(nodes, qt.IsNil)
			})
		}
	}
}

// TestEveryPlannerButYDBRefusesTopicChanges_CoversEveryPlanner holds the table
// above to the planners Ptah ships, so a new one cannot plan a topic change
// unmeasured. Another test of this package registers planners of its own under
// names NormalizeDialect does not know; those plan whatever their test asks.
func TestEveryPlannerButYDBRefusesTopicChanges_CoversEveryPlanner(t *testing.T) {
	c := qt.New(t)
	dialects, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	dialects = slices.DeleteFunc(dialects, func(dialect string) bool {
		return dialect == platform.YDB || platform.NormalizeDialect(dialect) != dialect
	})
	var covered []string
	for _, test := range topicRefusals {
		covered = append(covered, test.dialect)
	}
	slices.Sort(dialects)
	slices.Sort(covered)
	c.Assert(covered, qt.DeepEquals, dialects)
}
