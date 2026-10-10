package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
)

// TestSecrets_RollbackRestoresWhatItCan plans a change that creates, drops and
// rotates a secret, and its rollback. The rollback drops the created secret
// and creates the dropped one again with the default variable for its path,
// since the database never returned the old value; it does not rotate back,
// because the earlier value was never read. Each value the rollback cannot
// restore is written into it as a recovery limit, and no plan holds a value.
func TestSecrets_RollbackRestoresWhatItCan(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262()
	current := &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbsecret.ObservedObject("app", "pg.password"), ydbsecret.ObservedObject("", "token"))),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	declared := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbsecret.DesiredObject("ext", "s3_key", "", "PTAH_SECRET_S3"), ydbsecret.DesiredObject("", "token", "", "PTAH_SECRET_TOKEN"))),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps},
		&config.CompareOptions{FeatureRequests: must.Must(ydbsecret.RotationRequests([]string{"token"}))}, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: declared, CurrentSchema: current, Dialect: "ydb", Capabilities: caps})

	c.Assert(err, qt.IsNil)
	forward := must.Must(builtin.RenderSQLWithCapabilities("ydb", caps, plan.Forward.Nodes...))
	reverse := must.Must(builtin.RenderSQLWithCapabilities("ydb", caps, plan.Reverse.Nodes...))
	c.Assert(forward, qt.Equals, "ALTER SECRET `token` WITH (value = $PTAH_SECRET_TOKEN);\n"+
		"DROP SECRET `app/pg.password`;\n"+
		"CREATE SECRET `ext/s3_key` WITH (value = $PTAH_SECRET_S3);\n")
	c.Assert(reverse, qt.Contains, "CREATE SECRET `app/pg.password` WITH (value = $PTAH_SECRET_APP_PG_PASSWORD);\n")
	c.Assert(reverse, qt.Contains, "DROP SECRET `ext/s3_key`;\n")
	c.Assert(reverse, qt.Not(qt.Contains), "ALTER SECRET")
	c.Assert(reverse, qt.Contains, "the value secret token held before the rotation was never read; the rollback keeps the rotated value")
	c.Assert(reverse, qt.Contains, "the dropped value of secret app/pg.password was never read; the rollback takes the value PTAH_SECRET_APP_PG_PASSWORD holds when it runs")
}

// TestSecrets_RotationRollbackChangesNothing plans the rollback of a plan that
// only rotates a secret. The earlier value was never read, so the rollback
// has no statement: its diff holds no change and no safety finding, and it
// carries only the note saying the rotated value stays.
func TestSecrets_RotationRollbackChangesNothing(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262()
	current := &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbsecret.ObservedObject("", "token"))),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	declared := &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbsecret.DesiredObject("", "token", "", "PTAH_SECRET_TOKEN"))),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps},
		&config.CompareOptions{FeatureRequests: must.Must(ydbsecret.RotationRequests([]string{"token"}))}, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: declared, CurrentSchema: current, Dialect: "ydb", Capabilities: caps})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Forward.Diff.HasChanges(), qt.IsTrue)
	c.Assert(plan.Reverse.Diff.HasChanges(), qt.IsFalse)
	c.Assert(plan.Reverse.Diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(safety.ClassifySchemaDiff(plan.Reverse.Diff), qt.HasLen, 0)
	c.Assert(plan.Reverse.Recovery, qt.HasLen, 1)
	c.Assert(plan.Reverse.Recovery[0].Change, qt.DeepEquals, schemaext.ChangeRecord{Subject: ydbsecret.Ref("", "token")})
	reverse := must.Must(builtin.RenderSQLWithCapabilities("ydb", caps, plan.Reverse.Nodes...))
	c.Assert(reverse, qt.Not(qt.Contains), "SECRET `token`")
	c.Assert(reverse, qt.Contains, "the value secret token held before the rotation was never read; the rollback keeps the rotated value")
}
