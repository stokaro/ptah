package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// readTopicSpec is a topic as the reader describes one created with a
// retention of two hours and important consumer billing, measured on
// 26.2.1.14.
func readTopicSpec() ydbtopic.Spec {
	return ydbtopic.Spec{
		MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "PT2H",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576,
		Consumers: []ydbtopic.ConsumerSpec{{Name: "billing", Important: true}},
	}
}

// declaredTopics declares topics from a source that describes the topic
// namespace.
func declaredTopics(objects ...schemaext.Object) *schemamodel.Database {
	return &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// heldTopics is a read that listed every topic and found these.
func heldTopics(objects ...schemaext.Object) *catalog.Database {
	return &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// TestCompare_Topics compares a declaration and a read of one topic equal
// however the declaration spells its settings, plans the creation of a
// declared topic the database lacks and the drop of a held one the
// declaration leaves out, and reports a change once per topic, carrying both
// specs.
func TestCompare_Topics(t *testing.T) {
	billing := []ydbtopic.ConsumerSpec{{Name: "billing", Important: true}}
	changed := ydbtopic.Spec{RetentionPeriod: "PT3H", Consumers: []ydbtopic.ConsumerSpec{{Name: "audit"}}}
	tests := []struct {
		name    string
		desired *schemamodel.Database
		current *catalog.Database
		want    []schemaext.ChangeRecord
	}{
		{name: "the same topic",
			desired: declaredTopics(ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{RetentionPeriod: "PT120M", Consumers: billing})),
			current: heldTopics(ydbtopic.ObservedObject("app", "events", readTopicSpec()))},
		{name: "a topic only the declaration has",
			desired: declaredTopics(ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{})), current: heldTopics(),
			want: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"), Value: &ydbdiff.Topic{After: &ydbtopic.Desired{}}}}},
		{name: "a topic only the database has",
			desired: declaredTopics(), current: heldTopics(ydbtopic.ObservedObject("app", "events", readTopicSpec())),
			want: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"), Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: readTopicSpec()}}}}},
		{name: "another retention and another consumer",
			desired: declaredTopics(ydbtopic.DesiredObject("app", "events", "", changed)),
			current: heldTopics(ydbtopic.ObservedObject("app", "events", readTopicSpec())),
			want: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"),
				Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: readTopicSpec()}, After: &ydbtopic.Desired{Spec: changed}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, test.current, platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.FeatureChanges, qt.DeepEquals, test.want)
			c.Assert(diff.HasChanges(), qt.Equals, len(test.want) > 0)
		})
	}
}

// TestCompare_TopicKeptWhereTheDesiredStateCannotNameIt plans no drop of a
// held topic when the desired state makes no claim about topics, as an HCL
// document does not, and still plans the drop where it claims to describe
// them and names none.
func TestCompare_TopicKeptWhereTheDesiredStateCannotNameIt(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    []schemaext.ChangeRecord
	}{
		{name: "a document that cannot name a topic", desired: &schemamodel.Database{}},
		{name: "a document that can, and names none", desired: declaredTopics(),
			want: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"), Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: readTopicSpec()}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, heldTopics(ydbtopic.ObservedObject("app", "events", readTopicSpec())),
				platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.FeatureChanges, qt.DeepEquals, test.want)
		})
	}
}

// TestCompare_TopicNotCreatedWhereTheReadDidNotLook withholds the creation of
// a declared topic when the read recorded that it could not read the topic at
// its path: CREATE TOPIC carries no guard Ptah writes, so planning it over a
// topic that is there would fail. The withheld creation is reported, and a
// topic the read did look for is still created.
func TestCompare_TopicNotCreatedWhereTheReadDidNotLook(t *testing.T) {
	c := qt.New(t)
	held := &catalog.Database{FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: ydbtopic.Kind, Subject: ydbtopic.Ref("app", "events"),
			Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbtopic.QueueGroupReason}}}))}
	declared := declaredTopics(ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{}), ydbtopic.DesiredObject("app", "queue", "", ydbtopic.Spec{}))

	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), declared, held, &config.CompareOptions{Dialect: platform.YDB}, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.Features, qt.HasLen, 1)
	c.Assert(diagnostics.Features[0].Subject, qt.DeepEquals, ydbtopic.Ref("app", "events"))
	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{
		{Subject: ydbtopic.Ref("app", "queue"), Value: &ydbdiff.Topic{After: &ydbtopic.Desired{}}},
	})
}
