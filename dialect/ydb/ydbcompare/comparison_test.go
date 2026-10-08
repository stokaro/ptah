package ydbcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
)

func feed(name string) ydbschema.ChangefeedSpec {
	return ydbschema.ChangefeedSpec{Name: name, Mode: "UPDATES", Format: "JSON"}
}

func table(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(schema, name)
}

func source(c *qt.C, representation schemaext.Representation, knowledge schemaext.KnowledgeState, specs []ydbschema.ChangefeedSpec, limits ...schemaext.SubjectCoverage) schemaext.ObjectState {
	c.Helper()
	var objects []schemaext.Object
	for _, spec := range specs {
		object := ydbschema.DesiredObject("", "orders", spec)
		if representation == schemaext.Observed {
			object = ydbschema.ObservedObject("", "orders", spec)
		}
		objects = append(objects, object)
	}
	collection, err := schemaext.NewObjects(objects...)
	c.Assert(err, qt.IsNil)
	state := schemaext.ObjectState{Objects: collection}
	if knowledge == schemaext.Complete || len(limits) > 0 {
		coverage, err := ydbschema.ChangefeedCoverage(representation, limits)
		c.Assert(err, qt.IsNil)
		if knowledge != schemaext.Complete {
			kinds := coverage.KindRecords()
			kinds[0].Knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "source did not enumerate streams"}
			coverage, err = schemaext.NewCoverage(representation, kinds, limits)
			c.Assert(err, qt.IsNil)
		}
		state.Coverage = coverage
	}
	return state
}

func limit(state schemaext.KnowledgeState) schemaext.SubjectCoverage {
	return schemaext.SubjectCoverage{Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("", "orders", "updates"), Knowledge: schemaext.Knowledge{State: state, Reason: "opaque stream setting"}}
}

func TestComparison_AbsenceRequiresEvidenceAndParentsOwnChildren(t *testing.T) {
	both := schemaext.ParentState{Subject: table("", "orders"), Desired: true, Current: true}
	cases := []struct {
		name                               string
		desiredKnowledge, currentKnowledge schemaext.KnowledgeState
		desired, current                   []ydbschema.ChangefeedSpec
		desiredLimits, currentLimits       []schemaext.SubjectCoverage
		parent                             schemaext.ParentState
		changes, diagnostics, objects      int
		before, after                      bool
	}{
		{parent: both, name: "known removal", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, current: []ydbschema.ChangefeedSpec{feed("updates")}, changes: 1, before: true},
		{parent: both, name: "known addition", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, desired: []ydbschema.ChangefeedSpec{feed("updates")}, changes: 1, after: true, objects: 1},
		{parent: both, name: "unknown source adopts", currentKnowledge: schemaext.Complete, current: []ydbschema.ChangefeedSpec{feed("updates")}, objects: 1},
		{parent: both, name: "unknown observation withholds addition", desiredKnowledge: schemaext.Complete, desired: []ydbschema.ChangefeedSpec{feed("updates")}, diagnostics: 2, objects: 1},
		{parent: both, name: "present objects survive incomplete enumeration", desiredKnowledge: schemaext.Complete, desired: []ydbschema.ChangefeedSpec{feed("updates")}, current: []ydbschema.ChangefeedSpec{feed("updates")}, diagnostics: 1, objects: 1},
		{parent: both, name: "unrepresentable observation withholds", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, desired: []ydbschema.ChangefeedSpec{feed("updates")}, current: []ydbschema.ChangefeedSpec{feed("updates")}, currentLimits: []schemaext.SubjectCoverage{limit(schemaext.Unrepresentable)}, diagnostics: 1, objects: 1},
		{parent: both, name: "unrepresentable desired value withholds", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, desired: []ydbschema.ChangefeedSpec{feed("updates")}, current: []ydbschema.ChangefeedSpec{feed("updates")}, desiredLimits: []schemaext.SubjectCoverage{limit(schemaext.Unrepresentable)}, diagnostics: 1, objects: 1},
		{parent: both, name: "missing opaque stream is not absence", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, currentLimits: []schemaext.SubjectCoverage{limit(schemaext.Unrepresentable)}, diagnostics: 1},
		{parent: both, name: "unknown source preserves opaque limit", currentKnowledge: schemaext.Complete, currentLimits: []schemaext.SubjectCoverage{limit(schemaext.Unrepresentable)}, diagnostics: 1},
		{name: "new table captures children", desiredKnowledge: schemaext.Complete, desired: []ydbschema.ChangefeedSpec{feed("updates")}, parent: schemaext.ParentState{Subject: table("", "orders"), Desired: true}, objects: 1},
		{name: "removed table captures children", currentKnowledge: schemaext.Complete, current: []ydbschema.ChangefeedSpec{feed("updates")}, parent: schemaext.ParentState{Subject: table("", "orders"), Current: true}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			result, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb", Capabilities: capability.YDB262(),
				Desired: source(c, schemaext.Desired, test.desiredKnowledge, test.desired, test.desiredLimits...), Current: source(c, schemaext.Observed, test.currentKnowledge, test.current, test.currentLimits...), Parents: []schemaext.ParentState{test.parent}})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.diagnostics)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, test.objects)
			for _, record := range result.Changes {
				change := record.Value.(*ydbdiff.Changefeed)
				c.Assert(change.Before != nil, qt.Equals, test.before)
				c.Assert(change.After != nil, qt.Equals, test.after)
				c.Assert(record.Subject, qt.DeepEquals, ydbschema.ChangefeedRef("", "orders", "updates"))
			}
		})
	}
}

func TestComparison_UsesServerDefaultsWithoutRewritingDeclarations(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired := feed("updates")
	desired.ResolvedTimestamps = "PT60S"
	desired.Consumers = []ast.TopicConsumerSpec{{Name: "worker", SupportedCodecs: []string{"ZSTD", "RAW"}}}
	current := desired.Clone()
	current.Mode, current.Format, current.RetentionPeriod, current.ResolvedTimestamps = "updates", "json", "PT24H", "PT1M"
	current.TopicMinActivePartitions = 8
	current.Consumers[0].SupportedCodecs = []string{"raw", "zstd"}
	current.Consumers[0].ReadFrom = "1970-01-01T00:00:00Z"
	request := schemaext.ObjectComparisonRequest{Target: "ydb", Capabilities: capability.YDB262(), Desired: source(c, schemaext.Desired, schemaext.Complete, []ydbschema.ChangefeedSpec{desired}), Current: source(c, schemaext.Observed, schemaext.Complete, []ydbschema.ChangefeedSpec{current}), Parents: []schemaext.ParentState{{Subject: table("", "orders"), Desired: true, Current: true}}}
	result, err := runtime.CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	retained, _, err := result.Desired.Objects.Get(ydbschema.ChangefeedRef("", "orders", "updates"))
	c.Assert(err, qt.IsNil)
	c.Assert(retained.Value.(*ydbschema.DesiredChangefeed).Spec, qt.DeepEquals, desired)
}

func TestComparison_PreservesDisabledStreamAndItsCoverageForCapture(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	current := feed("updates")
	current.Disabled = true
	current.Consumers = []ast.TopicConsumerSpec{{Name: "worker", SupportedCodecs: []string{"raw"}}}
	request := schemaext.ObjectComparisonRequest{Target: "ydb", Capabilities: capability.YDB262(), Desired: source(c, schemaext.Desired, schemaext.Uninspected, nil), Current: source(c, schemaext.Observed, schemaext.Complete, []ydbschema.ChangefeedSpec{current}), Parents: []schemaext.ParentState{{Subject: table("", "orders"), Desired: true, Current: true}}}
	result, err := runtime.CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	preserved, _, err := result.Desired.Objects.Get(ydbschema.ChangefeedRef("", "orders", "updates"))
	c.Assert(err, qt.IsNil)
	c.Assert(preserved.Value.(*ydbschema.DesiredChangefeed).Spec, qt.DeepEquals, current)
	c.Assert(result.Desired.Coverage.Lookup(ydbschema.ChangefeedKind, table("", "orders")).State, qt.Equals, schemaext.Complete)
	c.Assert(result.Desired.Coverage.Lookup(ydbschema.ChangefeedKind, table("", "other")).State, qt.Equals, schemaext.Uninspected)
	request.Desired = result.Desired
	second, err := runtime.CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(second.Changes, qt.HasLen, 0)
}

func TestComparison_RefusesInvalidInputsWithoutPartialResults(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*schemaext.ObjectComparisonRequest)
		want   error
	}{
		{name: "invalid interval", want: schemaext.ErrInvalidValue, mutate: func(r *schemaext.ObjectComparisonRequest) {
			objects, _ := r.Desired.Objects.All()
			objects[0].Value.(*ydbschema.DesiredChangefeed).Spec.RetentionPeriod = "bad"
			r.Desired.Objects, _ = schemaext.NewObjects(objects...)
		}},
		{name: "orphaned stream", want: schemaext.ErrInvalidValue, mutate: func(r *schemaext.ObjectComparisonRequest) { r.Parents = nil }},
		{name: "wrong representation", want: schemaext.ErrInvalidValue, mutate: func(r *schemaext.ObjectComparisonRequest) { r.Desired.Objects = r.Current.Objects }},
		{name: "absent but present", want: schemaext.ErrInvalidValue, mutate: func(r *schemaext.ObjectComparisonRequest) {
			r.Desired.Coverage, _ = ydbschema.ChangefeedCoverage(schemaext.Desired, []schemaext.SubjectCoverage{limit(schemaext.Absent)})
		}},
		{name: "undefined default stream", want: schemaext.ErrInvalidValue, mutate: func(r *schemaext.ObjectComparisonRequest) {
			r.Desired.Objects = schemaext.Objects{}
			r.Desired.Coverage, _ = ydbschema.ChangefeedCoverage(schemaext.Desired, []schemaext.SubjectCoverage{limit(schemaext.Defaulted)})
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			request := schemaext.ObjectComparisonRequest{Target: "ydb", Capabilities: capability.YDB262(), Desired: source(c, schemaext.Desired, schemaext.Complete, []ydbschema.ChangefeedSpec{feed("updates")}), Current: source(c, schemaext.Observed, schemaext.Complete, []ydbschema.ChangefeedSpec{feed("updates")}), Parents: []schemaext.ParentState{{Subject: table("", "orders"), Desired: true, Current: true}}}
			test.mutate(&request)
			result, err := runtime.CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
		})
	}
}
