package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// widgetLimit maps the policy kind onto the widget model, naming a widget by
// its own name.
var widgetLimit = schemaext.LimitMapping{Header: coverage.Policy, Model: widgetKind,
	Names: func(ref objectidentity.ID) []string { return []string{ref.Name.Source} }}

// widgetCoverage enrolls the widget model with knowledge, and subjects.
func widgetCoverage(c *qt.C, knowledge schemaext.Knowledge, subjects ...schemaext.SubjectCoverage) schemaext.Coverage {
	c.Helper()
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	enrolled, err := schemaext.NewCoverage(schemaext.Desired,
		[]schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.GlobalKnowledge(knowledge)}}, subjects)
	c.Assert(err, qt.IsNil)
	return enrolled
}

func TestCoverageWithLimits_HappyPath(t *testing.T) {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	first, second := widgetRef("orders", "first"), widgetRef("orders", "second")

	t.Run("a record naming the whole kind leaves the model uninspected", func(t *testing.T) {
		c := qt.New(t)
		limits := coverage.Set{}.With(coverage.Object{Kind: coverage.Policy, Reason: coverage.SuppressedByPolicy, Provenance: coverage.Defaulted})

		got, err := widgetCoverage(c, complete).WithLimits(limits, widgetLimit, nil)

		c.Assert(err, qt.IsNil)
		c.Assert(got.Lookup(widgetKind, first), qt.Equals, schemaext.Knowledge{State: schemaext.Uninspected,
			Reason: "the description does not describe policy: a compatibility policy left them out of the description"})
		c.Assert(got.SubjectRecords(), qt.HasLen, 0)
	})

	t.Run("a record naming one object leaves that subject uninspected", func(t *testing.T) {
		c := qt.New(t)
		limits := coverage.Set{}.WithObject(coverage.Policy, "first")

		got, err := widgetCoverage(c, complete).WithLimits(limits, widgetLimit, []objectidentity.ID{first, second})

		c.Assert(err, qt.IsNil)
		c.Assert(got.Lookup(widgetKind, first), qt.Equals, schemaext.Knowledge{State: schemaext.Uninspected,
			Reason: `the description does not describe policy "first"`})
		c.Assert(got.Lookup(widgetKind, second), qt.Equals, complete)
	})

	t.Run("a record naming an object no subject is does nothing", func(t *testing.T) {
		c := qt.New(t)
		limits := coverage.Set{}.WithObject(coverage.Policy, "third")

		got, err := widgetCoverage(c, complete).WithLimits(limits, widgetLimit, []objectidentity.ID{first, second})

		c.Assert(err, qt.IsNil)
		c.Assert(got.SubjectRecords(), qt.HasLen, 0)
		c.Assert(got.Lookup(widgetKind, first), qt.Equals, complete)
	})

	t.Run("a record of another kind changes nothing", func(t *testing.T) {
		c := qt.New(t)
		source := widgetCoverage(c, complete)

		got, err := source.WithLimits(coverage.Set{}.WithKind(coverage.Sequence).WithObject(coverage.Sequence, "first"),
			widgetLimit, []objectidentity.ID{first})

		c.Assert(err, qt.IsNil)
		c.Assert(got.KindRecords(), qt.DeepEquals, source.KindRecords())
		c.Assert(got.SubjectRecords(), qt.HasLen, 0)
	})

	t.Run("a source's own claim about a subject is kept", func(t *testing.T) {
		c := qt.New(t)
		claim := schemaext.SubjectCoverage{Kind: widgetKind, Subject: first, Knowledge: schemaext.Knowledge{State: schemaext.Absent}}

		got, err := widgetCoverage(c, complete, claim).WithLimits(coverage.Set{}.WithObject(coverage.Policy, "first"),
			widgetLimit, []objectidentity.ID{first})

		c.Assert(err, qt.IsNil)
		c.Assert(got.SubjectRecords(), qt.DeepEquals, []schemaext.SubjectCoverage{claim})
	})

	t.Run("an unknown model keeps its own reason", func(t *testing.T) {
		c := qt.New(t)
		unread := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the read met a policy it cannot describe"}

		got, err := widgetCoverage(c, unread).WithLimits(coverage.Set{}.WithKind(coverage.Policy), widgetLimit, nil)

		c.Assert(err, qt.IsNil)
		c.Assert(got.Lookup(widgetKind, first), qt.Equals, unread)
	})

	t.Run("a model the source does not enroll is left alone", func(t *testing.T) {
		c := qt.New(t)
		registry, err := schemaext.NewRegistry(owned(widgetCodec(otherKind, schemaext.Desired)))
		c.Assert(err, qt.IsNil)
		source, err := schemaext.NewCoverage(schemaext.Desired,
			[]schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.GlobalKnowledge(complete)}}, nil)
		c.Assert(err, qt.IsNil)

		got, err := source.WithLimits(coverage.Set{}.WithKind(coverage.Policy), widgetLimit, []objectidentity.ID{first})

		c.Assert(err, qt.IsNil)
		c.Assert(got.KindRecords(), qt.DeepEquals, source.KindRecords())
		c.Assert(got.SubjectRecords(), qt.HasLen, 0)
	})

	t.Run("the receiver is not changed", func(t *testing.T) {
		c := qt.New(t)
		source := widgetCoverage(c, complete)

		_, err := source.WithLimits(coverage.Set{}.WithKind(coverage.Policy), widgetLimit, nil)

		c.Assert(err, qt.IsNil)
		c.Assert(source.Lookup(widgetKind, first), qt.Equals, complete)
	})
}

func TestCoverageWithLimits_FailurePath(t *testing.T) {
	names := func(objectidentity.ID) []string { return nil }
	tests := []struct {
		name    string
		mapping schemaext.LimitMapping
	}{
		{name: "no not-described kind", mapping: schemaext.LimitMapping{Header: " ", Model: widgetKind, Names: names}},
		{name: "no model", mapping: schemaext.LimitMapping{Header: coverage.Policy, Names: names}},
		{name: "no names", mapping: schemaext.LimitMapping{Header: coverage.Policy, Model: widgetKind}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := widgetCoverage(c, schemaext.Knowledge{State: schemaext.Complete}).
				WithLimits(coverage.Set{}.WithKind(coverage.Policy), test.mapping, nil)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got.Representation(), qt.Equals, schemaext.Representation(""))
		})
	}
}
