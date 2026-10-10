package ydbreverse_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
)

func feed(edit func(*ydbschema.ChangefeedSpec)) *ydbschema.ChangefeedSpec {
	spec := &ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	if edit != nil {
		edit(spec)
	}
	return spec
}

func change(before, after *ydbschema.ChangefeedSpec) schemaext.ChangeRecord {
	value := &ydbdiff.Changefeed{}
	if before != nil {
		value.Before = &ydbschema.ObservedChangefeed{Spec: before.Clone()}
	}
	if after != nil {
		value.After = &ydbschema.DesiredChangefeed{Spec: after.Clone()}
	}
	return schemaext.ChangeRecord{Subject: ydbschema.ChangefeedRef("", "table.with.dot", "updates"), Value: value}
}

func request(changes ...schemaext.ChangeRecord) schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: "ydb", Capabilities: capability.YDB262(), Changes: changes}
}

func TestReversalRefusesIndependentReplicationStreamOperations(t *testing.T) {
	observed := &ydbschema.ObservedChangefeed{Spec: *feed(nil),
		Replication: &ydbschema.ReplicationBinding{DestinationPath: "/remote/replica", ItemID: "1"}}
	for _, test := range []struct {
		name  string
		value *ydbdiff.Changefeed
	}{{"drop", &ydbdiff.Changefeed{Before: observed}}, {"create", &ydbdiff.Changefeed{After: observed.Desired()}}} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			record := schemaext.ChangeRecord{Subject: ydbschema.ChangefeedRef("", "table.with.dot", "updates"), Value: test.value}
			result, err := (ydbreverse.Service{}).ReverseChanges(t.Context(), request(record))
			c.Assert(err, qt.ErrorIs, schemaext.ErrIrreversible)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestRegisteredYDBReversalRestoresDefinitionsAndReportsStateLoss(t *testing.T) {
	for _, test := range []struct {
		name          string
		before, after *ydbschema.ChangefeedSpec
		strategy      string
		limitation    string
	}{
		{name: "create", after: feed(nil), strategy: "drop the created changefeed", limitation: "unread messages"},
		{name: "mode recreation", before: feed(nil), after: feed(func(s *ydbschema.ChangefeedSpec) { s.Mode = "NEW_IMAGE" }), strategy: "recreate the prior changefeed definition", limitation: "original messages or consumer positions"},
		{name: "retention", before: feed(nil), after: feed(func(s *ydbschema.ChangefeedSpec) { s.RetentionPeriod = "PT12H" }), strategy: "restore topic settings in place", limitation: "already expired"},
		{name: "drop consumer", before: feed(func(s *ydbschema.ChangefeedSpec) { s.Consumers = []ydbtopic.ConsumerSpec{{Name: "audit"}} }), after: feed(nil), strategy: "restore topic settings in place", limitation: `"audit"`},
		{name: "remove added consumer", before: feed(nil), after: feed(func(s *ydbschema.ChangefeedSpec) { s.Consumers = []ydbtopic.ConsumerSpec{{Name: "audit"}} }), strategy: "restore topic settings in place", limitation: `"audit"`},
		{name: "reverse codec reset", before: feed(func(s *ydbschema.ChangefeedSpec) { s.Consumers = []ydbtopic.ConsumerSpec{{Name: "audit"}} }), after: feed(func(s *ydbschema.ChangefeedSpec) {
			s.Consumers = []ydbtopic.ConsumerSpec{{Name: "audit", SupportedCodecs: []string{"raw"}}}
		}), strategy: "restore topic settings in place", limitation: `"audit"`},
		{name: "forward codec reset", before: feed(func(s *ydbschema.ChangefeedSpec) {
			s.Consumers = []ydbtopic.ConsumerSpec{{Name: "audit", SupportedCodecs: []string{"raw"}}}
		}), after: feed(func(s *ydbschema.ChangefeedSpec) { s.Consumers = []ydbtopic.ConsumerSpec{{Name: "audit"}} }), strategy: "restore topic settings in place", limitation: `"audit"`},
		{name: "consumer settings in place", before: feed(func(s *ydbschema.ChangefeedSpec) { s.Consumers = []ydbtopic.ConsumerSpec{{Name: "audit"}} }), after: feed(func(s *ydbschema.ChangefeedSpec) {
			s.Consumers = []ydbtopic.ConsumerSpec{{Name: "audit", Important: true}}
		}), strategy: "restore topic settings in place"},
		{name: "disabled stream settings in place", before: feed(func(s *ydbschema.ChangefeedSpec) { s.Disabled = true }), after: feed(func(s *ydbschema.ChangefeedSpec) { s.Disabled = true; s.RetentionPeriod = "PT12H" }), strategy: "restore topic settings in place", limitation: "already expired"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			input := change(test.before, test.after)
			original, err := input.Clone()
			c.Assert(err, qt.IsNil)
			result, err := runtime.ReverseChanges(t.Context(), request(input))
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change.Subject, qt.Equals, input.Subject)
			c.Assert(result[0].Strategy, qt.Equals, test.strategy)
			c.Assert(len(result[0].Limitations) == 0, qt.Equals, test.limitation == "")
			c.Assert(strings.Join(result[0].Limitations, "\n"), qt.Contains, test.limitation)
			expected := change(test.after, test.before)
			c.Assert(result[0].Change, qt.DeepEquals, expected)
			c.Assert(result[0].ForwardState, qt.HasLen, 1)
			projection := result[0].ForwardState[0]
			c.Assert(projection.Placement, qt.Equals, schemaext.ObjectPlacement)
			c.Assert(projection.Kind, qt.Equals, ydbschema.ChangefeedKind)
			c.Assert(projection.Value, qt.DeepEquals, &ydbschema.ObservedChangefeed{Spec: test.after.Clone()})
			c.Assert(input, qt.DeepEquals, original)
		})
	}
}

func TestReversalPreservesOnlyEstablishedPartitionCounts(t *testing.T) {
	for _, test := range []struct {
		name  string
		after *ydbschema.ChangefeedSpec
		want  uint64
	}{
		{name: "in-place change retains current count", after: feed(func(s *ydbschema.ChangefeedSpec) { s.RetentionPeriod = "PT12H" }), want: 4},
		{name: "recreation uses an unknown table-derived count", after: feed(func(s *ydbschema.ChangefeedSpec) { s.Mode = "NEW_IMAGE" }), want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := feed(func(s *ydbschema.ChangefeedSpec) { s.TopicMinActivePartitions = 4 })
			result, err := (ydbreverse.Service{}).ReverseChanges(t.Context(), request(change(before, test.after)))
			c.Assert(err, qt.IsNil)
			reversed := result[0].Change.Value.(*ydbdiff.Changefeed)
			c.Assert(reversed.Before.Spec.TopicMinActivePartitions, qt.Equals, test.want)
			c.Assert(reversed.After.Spec.TopicMinActivePartitions, qt.Equals, uint64(4))
			projection := result[0].ForwardState[0].Value.(*ydbschema.ObservedChangefeed)
			c.Assert(projection.Spec.TopicMinActivePartitions, qt.Equals, test.want)
			projection.Spec.TopicMinActivePartitions = 99
			c.Assert(reversed.Before.Spec.TopicMinActivePartitions, qt.Equals, test.want)
		})
	}
}

func TestReversalRefusesUnreconstructibleOrMalformedChanges(t *testing.T) {
	for _, test := range []struct {
		name  string
		input schemaext.ChangeRecord
		want  error
	}{
		{name: "dropped disabled stream", input: change(feed(func(s *ydbschema.ChangefeedSpec) { s.Disabled = true }), nil), want: schemaext.ErrIrreversible},
		{name: "re-enabled disabled stream", input: change(feed(func(s *ydbschema.ChangefeedSpec) { s.Disabled = true }), feed(nil)), want: schemaext.ErrIrreversible},
		{name: "invalid forward disabled creation", input: change(nil, feed(func(s *ydbschema.ChangefeedSpec) { s.Disabled = true })), want: ptaherr.ErrUnsupportedFeature},
		{name: "no operands", input: change(nil, nil), want: schemaext.ErrInvalidValue},
		{name: "no semantic change", input: change(feed(nil), feed(func(s *ydbschema.ChangefeedSpec) { s.RetentionPeriod = "PT24H" })), want: schemaext.ErrInvalidValue},
		{name: "payload name disagrees", input: change(nil, feed(func(s *ydbschema.ChangefeedSpec) { s.Name = "different" })), want: schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreverse.Service{}).ReverseChanges(t.Context(), request(change(nil, feed(nil)), test.input))
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestReversalValidatesContextTargetAndCapabilities(t *testing.T) {
	c := qt.New(t)
	service := ydbreverse.Service{}
	var missing context.Context
	result, err := service.ReverseChanges(missing, request())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err = service.ReverseChanges(ctx, request(change(nil, feed(nil))))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	input := request(change(nil, feed(nil)))
	input.Target = "postgres"
	result, err = service.ReverseChanges(t.Context(), input)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result, qt.IsNil)
	input.Target, input.Capabilities = "ydb", nil
	result, err = service.ReverseChanges(t.Context(), input)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.IsNil)
}

func TestRegisteredYDBReversalCapturesForwardAbsence(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	before := feed(nil)
	input := change(before, nil)
	original, err := input.Clone()
	c.Assert(err, qt.IsNil)
	result, err := runtime.ReverseChanges(t.Context(), request(input))
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	c.Assert(result[0].Change, qt.DeepEquals, change(nil, before))
	c.Assert(result[0].Strategy, qt.Equals, "recreate the dropped changefeed")
	c.Assert(strings.Join(result[0].Limitations, "\n"), qt.Contains, "original stream was dropped")
	c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement, Kind: ydbschema.ChangefeedKind}})
	c.Assert(input, qt.DeepEquals, original)
}
