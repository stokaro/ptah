package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
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
	desired := must.Must(ydbsecret.RequestRotation(declared, []string{"token"}))
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current,
		catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: caps})

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
