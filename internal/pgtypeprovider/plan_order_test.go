package pgtypeprovider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// summary is a planned step as an ordering test reads it: the type it names
// and its payload's kind.
type summary struct {
	Subject string
	Kind    schemaext.Kind
}

// inPhase keeps the steps of one phase, in plan order.
func inPhase(steps []planned, phase featureplan.Phase) []planned {
	return slices.DeleteFunc(slices.Clone(steps), func(step planned) bool { return step.Phase != phase })
}

func summaries(steps []planned) []summary {
	listed := make([]summary, 0, len(steps))
	for _, step := range steps {
		listed = append(listed, summary{Subject: step.Subject, Kind: step.Payload.Kind()})
	}
	return listed
}

// TestPlanTypes_OrdersTheTypeWindow pins the order of the type creation
// window: the drops of rebuilt types, a type that names another first by the
// definitions the database holds; then every creation, a type another names
// first by the declarations; then the changes in place. A type that names a
// rebuilt one is rebuilt too, whatever its own change, because the drop would
// fail while it still names the type (stokaro/ptah#1717).
func TestPlanTypes_OrdersTheTypeWindow(t *testing.T) {
	address := func(fieldType string) []pgtype.Attribute {
		return []pgtype.Attribute{{Name: "street", Type: fieldType}}
	}
	tests := []struct {
		name    string
		changes []schemaext.ChangeRecord
		want    []summary
	}{
		{name: "a domain over a composite type, both created", changes: []schemaext.ChangeRecord{
			typeChange(pgtype.DomainKind, "d", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "addr"}}),
			typeChange(pgtype.CompositeKind, "addr", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: address("text")}}),
		}, want: []summary{{"composite type addr", pgtype.CreateCompositeKind}, {"domain d", pgtype.CreateDomainKind}}},
		{name: "a composite type over a domain, both rebuilt", changes: []schemaext.ChangeRecord{
			typeChange(pgtype.CompositeKind, "addr", &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: address("d")},
				After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "zip", Type: "text"}, {Name: "street", Type: "d"}}},
				Differs: []string{pgtype.DiffAttributes}}),
			typeChange(pgtype.DomainKind, "d", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"},
				After: &pgtype.DesiredDomain{BaseType: "varchar(20)"}, Differs: []string{pgtype.DiffBaseType}}),
		}, want: []summary{
			{"composite type addr", pgtype.DropKind}, {"domain d", pgtype.DropKind},
			{"domain d", pgtype.CreateDomainKind}, {"composite type addr", pgtype.CreateCompositeKind},
		}},
		{name: "a composite type with an attribute delta over a rebuilt domain", changes: []schemaext.ChangeRecord{
			typeChange(pgtype.CompositeKind, "addr", &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: address("d")},
				After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "d"}, {Name: "zip", Type: "text"}}},
				Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{{Name: "zip", Type: "text"}}}),
			typeChange(pgtype.DomainKind, "d", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"},
				After: &pgtype.DesiredDomain{BaseType: "varchar(20)"}, Differs: []string{pgtype.DiffBaseType}}),
		}, want: []summary{
			{"composite type addr", pgtype.DropKind}, {"domain d", pgtype.DropKind},
			{"domain d", pgtype.CreateDomainKind}, {"composite type addr", pgtype.CreateCompositeKind},
		}},
		{name: "a domain changed in place over a rebuilt composite type", changes: []schemaext.ChangeRecord{
			typeChange(pgtype.DomainKind, "d", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "addr"},
				After: &pgtype.DesiredDomain{BaseType: "addr", NotNull: true}, Differs: []string{pgtype.DiffNotNull}}),
			typeChange(pgtype.CompositeKind, "addr", &pgtype.CompositeChange{
				Before:  &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int"}, {Name: "b", Type: "int"}}},
				After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "b", Type: "int"}, {Name: "a", Type: "int"}}},
				Differs: []string{pgtype.DiffAttributes}}),
		}, want: []summary{
			{"domain d", pgtype.DropKind}, {"composite type addr", pgtype.DropKind},
			{"composite type addr", pgtype.CreateCompositeKind}, {"domain d", pgtype.CreateDomainKind},
		}},
		{name: "a creation before a change in place", changes: []schemaext.ChangeRecord{
			typeChange(pgtype.DomainKind, "a", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"},
				After: &pgtype.DesiredDomain{BaseType: "text", NotNull: true}, Differs: []string{pgtype.DiffNotNull}}),
			typeChange(pgtype.DomainKind, "b", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}}),
		}, want: []summary{{"domain b", pgtype.CreateDomainKind}, {"domain a", pgtype.AlterDomainKind}}},
		{name: "domains changed in place before composite types", changes: []schemaext.ChangeRecord{
			typeChange(pgtype.CompositeKind, "a", &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: address("text")},
				After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "text"}, {Name: "zip", Type: "text"}}},
				Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{{Name: "zip", Type: "text"}}}),
			typeChange(pgtype.DomainKind, "b", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"},
				After: &pgtype.DesiredDomain{BaseType: "text", NotNull: true}, Differs: []string{pgtype.DiffNotNull}}),
		}, want: []summary{{"domain b", pgtype.AlterDomainKind}, {"composite type a", pgtype.AlterCompositeKind}}},
		{name: "independent creations by kind, then by name", changes: []schemaext.ChangeRecord{
			typeChange(pgtype.CompositeKind, "a", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: address("text")}}),
			typeChange(pgtype.RangeKind, "b", &pgtype.RangeChange{After: &pgtype.DesiredRange{Subtype: "int4"}}),
			typeChange(pgtype.DomainKind, "d", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}}),
			typeChange(pgtype.DomainKind, "c", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}}),
		}, want: []summary{
			{"domain c", pgtype.CreateDomainKind}, {"domain d", pgtype.CreateDomainKind},
			{"range type b", pgtype.CreateRangeKind}, {"composite type a", pgtype.CreateCompositeKind},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning(test.changes...)

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(summaries(stepsOf(c, result.Contributions)), qt.DeepEquals, test.want)
		})
	}
}

// TestPlanTypes_OrdersRemovalsAndComments pins the two other windows: drops
// by kind and name, and the comments of surviving types by kind and name, as
// the common comparison orders comment changes.
func TestPlanTypes_OrdersRemovalsAndComments(t *testing.T) {
	c := qt.New(t)
	commented := func(value string) *pgtype.DesiredDomain {
		return &pgtype.DesiredDomain{BaseType: "text", Comment: value}
	}
	request := planning(
		typeChange(pgtype.RangeKind, "a", &pgtype.RangeChange{Before: &pgtype.ObservedRange{Subtype: "int4"}}),
		typeChange(pgtype.DomainKind, "c", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"}}),
		typeChange(pgtype.DomainKind, "b", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"}}),
		typeChange(pgtype.DomainKind, "e", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"}, After: commented(" e "),
			Differs: []string{pgtype.DiffComment}}),
		typeChange(pgtype.CompositeKind, "f", &pgtype.CompositeChange{
			Before: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}},
			After:  &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}, Comment: "f"}, Differs: []string{pgtype.DiffComment}}),
	)

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	steps := stepsOf(c, result.Contributions)
	c.Assert(summaries(inPhase(steps, featureplan.PhaseType)), qt.DeepEquals, []summary{
		{"domain b", pgtype.DropKind}, {"domain c", pgtype.DropKind}, {"range type a", pgtype.DropKind},
	})
	c.Assert(summaries(inPhase(steps, featureplan.PhaseDependent)), qt.DeepEquals, []summary{
		{"composite type f", pgtype.CommentKind}, {"domain e", pgtype.CommentKind},
	})
}

// TestPlanTypes_TrimsASurvivorsComment pins that a comment set on a type that
// survives is written without the whitespace the comparison ignored.
func TestPlanTypes_TrimsASurvivorsComment(t *testing.T) {
	c := qt.New(t)
	request := planning(typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"},
		After: &pgtype.DesiredDomain{BaseType: "text", Comment: " mail\n"}, Differs: []string{pgtype.DiffComment}}))

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(stepsOf(c, result.Contributions)[0].Payload, qt.DeepEquals, &pgtype.Comment{Target: domainTarget, Comment: "mail"})
}

// TestPlanTypes_StepsCarryTheirImpact pins that each step carries the effect
// its statement has, which safety analysis reads: a drop destroys, a creation
// adds, and a change in place changes behavior.
func TestPlanTypes_StepsCarryTheirImpact(t *testing.T) {
	text := &pgtype.ObservedDomain{BaseType: "text"}
	tests := []struct {
		name   string
		change *pgtype.DomainChange
		want   schemaext.Impact
	}{
		{name: "a drop", change: &pgtype.DomainChange{Before: text}, want: schemaext.Destructive},
		{name: "a creation", change: &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}}, want: schemaext.Additive},
		{name: "a change in place", change: &pgtype.DomainChange{Before: text, After: &pgtype.DesiredDomain{BaseType: "text", NotNull: true},
			Differs: []string{pgtype.DiffNotNull}}, want: schemaext.Behavioral},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).PlanFeatures(t.Context(), planning(typeChange(pgtype.DomainKind, "t", test.change)))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 1)
			c.Assert(result.Contributions[0].Steps, qt.HasLen, 1)
			c.Assert(result.Contributions[0].Steps[0].Impact.Impact, qt.Equals, test.want)
		})
	}
}

// TestPlanTypes_KeepsTwoCreationsOfOneName pins that two creations a server
// would refuse under one name both reach the plan, so the server's refusal
// names them rather than the plan dropping one in silence.
func TestPlanTypes_KeepsTwoCreationsOfOneName(t *testing.T) {
	c := qt.New(t)
	request := planning(
		typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}}),
		typeChange(pgtype.RangeKind, "t", &pgtype.RangeChange{After: &pgtype.DesiredRange{Subtype: "int4"}}),
	)

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(summaries(stepsOf(c, result.Contributions)), qt.DeepEquals, []summary{
		{"domain t", pgtype.CreateDomainKind}, {"range type t", pgtype.CreateRangeKind},
	})
}

// TestPlanTypes_LeavesACommentOnlyTypeNamedLikeARebuiltOne pins that a type
// whose only difference is its comment is not rebuilt because a rebuilt type
// in another schema has its name: the closure matches references by bare
// name, and a type that did not change has no reference to follow.
func TestPlanTypes_LeavesACommentOnlyTypeNamedLikeARebuiltOne(t *testing.T) {
	c := qt.New(t)
	attributes := []pgtype.Attribute{{Name: "n", Type: "int"}}
	request := planning(
		schemaext.ChangeRecord{Subject: pgtype.Ref(pgtype.DomainKind, "a", "t"), Value: &pgtype.DomainChange{
			Before: &pgtype.ObservedDomain{BaseType: "text"}, After: &pgtype.DesiredDomain{BaseType: "integer"}, Differs: []string{pgtype.DiffBaseType}}},
		schemaext.ChangeRecord{Subject: pgtype.Ref(pgtype.CompositeKind, "b", "t"), Value: &pgtype.CompositeChange{
			Before: &pgtype.ObservedComposite{Attributes: attributes}, After: &pgtype.DesiredComposite{Attributes: attributes, Comment: "pair"},
			Differs: []string{pgtype.DiffComment}}},
	)

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(summaries(stepsOf(c, result.Contributions)), qt.DeepEquals, []summary{
		{"domain t", pgtype.DropKind}, {"domain t", pgtype.CreateDomainKind}, {"composite type t", pgtype.CommentKind},
	})
}
