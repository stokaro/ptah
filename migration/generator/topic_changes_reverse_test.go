package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// TestTopics_RollbackRestoresWhatItCan plans a change that creates, drops and
// changes a topic, and its rollback. The rollback drops the topic the forward
// plan created, creates again the one it dropped from the settings and
// consumers the read reported, and moves a changed topic back to the nearest
// state YDB reaches in place: the partitions the forward change added stay,
// and the consumer it dropped comes back. What it cannot restore -- messages,
// consumer positions, partitions -- is written into it as a recovery limit.
func TestTopics_RollbackRestoresWhatItCan(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262()
	current := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbtopic.ObservedObject("", "gone", ydbtopic.Spec{RetentionPeriod: "PT2H"}),
			ydbtopic.ObservedObject("", "events", ydbtopic.Spec{MinActivePartitions: 1, Consumers: []ydbtopic.ConsumerSpec{{Name: "billing"}}}))),
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	declared := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbtopic.DesiredObject("", "fresh", "", ydbtopic.Spec{}),
			ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{MinActivePartitions: 3}))),
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, catalog.ServerInfo{Dialect: platform.YDB, Capabilities: caps}, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: declared, CurrentSchema: current, Dialect: platform.YDB, Capabilities: caps})

	c.Assert(err, qt.IsNil)
	forward := must.Must(builtin.RenderSQLWithCapabilities(platform.YDB, caps, plan.Forward.Nodes...))
	reverse := must.Must(builtin.RenderSQLWithCapabilities(platform.YDB, caps, plan.Reverse.Nodes...))
	c.Assert(forward, qt.Equals, "ALTER TOPIC `events` SET (min_active_partitions = 3, auto_partitioning_strategy = 'disabled', "+
		"retention_period = Interval('P1D'), partition_write_speed_bytes_per_second = 1048576, partition_write_burst_bytes = 1048576, "+
		"supported_codecs = ''), DROP CONSUMER `billing`;\n"+
		"CREATE TOPIC `fresh`;\n"+
		"DROP TOPIC `gone`;\n")
	c.Assert(reverse, qt.Contains, "ALTER TOPIC `events` ADD CONSUMER `billing`;\n")
	c.Assert(reverse, qt.Contains, "DROP TOPIC `fresh`;\n")
	c.Assert(reverse, qt.Contains, "CREATE TOPIC `gone` WITH (retention_period = Interval('PT2H'));\n")
	c.Assert(reverse, qt.Contains, "topic events keeps the 3 partitions the change gave it, since YDB never removes a partition")
	c.Assert(reverse, qt.Contains, `consumer \"billing\" of topic events was dropped; the rollback adds it again without its position`)
	c.Assert(reverse, qt.Contains, "the messages topic gone held and its consumers' positions were dropped; the rollback creates it empty")
}
