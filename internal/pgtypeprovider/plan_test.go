package pgtypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// planned is one step as a test reads it: the type its effect names, its
// payload, notes and phase, and the effect's action.
type planned struct {
	Subject string
	Payload ast.ExtensionPayload
	Notes   []string
	Phase   featureplan.Phase
	Effect  plangraph.Action
}

// planning plans changes on PostgreSQL 17, which hosts every kind and stores
// every comment.
func planning(changes ...schemaext.ChangeRecord) featureplan.Request {
	return featureplan.Request{Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres17(),
		Changes: changes}
}

// typeChange is a change of the type of kind named name, which names no
// schema.
func typeChange(kind schemaext.Kind, name string, value schemaext.ChangeValue) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: pgtype.Ref(kind, "", name), Value: value}
}

// stepsOf lists the planned steps in the order the owner's dependencies put
// them. Each step names one type, under the common kind of its model.
func stepsOf(c *qt.C, contributions []plangraph.Contribution[featureplan.Operation]) []planned {
	c.Helper()
	c.Assert(contributions, qt.HasLen, 1)
	plan := must.Must(plangraph.Schedule(c.Context(), contributions[0]))
	listed := make([]planned, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		c.Assert(step.Effects, qt.HasLen, 1)
		subject := step.Effects[0].Subject
		listed = append(listed, planned{Subject: string(subject.Kind) + " " + subject.Name.Source, Payload: step.Payload.Payload,
			Notes: step.Payload.Notes, Phase: step.Payload.Phase, Effect: step.Effects[0].Action})
	}
	return listed
}

// named is the target of the type of kind named name, which names no schema.
func named(kind schemaext.Kind, name string) pgtype.Target {
	return pgtype.Target{Kind: kind, Name: name}
}

var (
	domainTarget    = named(pgtype.DomainKind, "t")
	compositeTarget = named(pgtype.CompositeKind, "t")
	rangeTarget     = named(pgtype.RangeKind, "t")
)

// TestPlanTypes_HappyPath pins the steps of each change of one type: a
// creation and the comment it declares, a drop with its warning, a change in
// place, a rebuild, and a comment, each in its phase.
func TestPlanTypes_HappyPath(t *testing.T) {
	typePhase, dependent := featureplan.PhaseType, featureplan.PhaseDependent
	text := &pgtype.ObservedDomain{BaseType: "text"}
	checked := &pgtype.ObservedDomain{BaseType: "text", Check: "(VALUE <> ''::text) AND (VALUE <> 'x'::text)",
		Checks: []pgtype.DomainCheck{{Name: "t_check", Expression: "(VALUE <> ''::text)"}, {Name: "t_check1", Expression: "(VALUE <> 'x'::text)"}}}
	everything := &pgtype.DesiredDomain{BaseType: "text", NotNull: true, Default: "it's", Check: "VALUE > ''", Comment: "mail"}
	expression := &pgtype.DesiredDomain{BaseType: "text", Default: "x", DefaultExpr: "lower('X')"}
	integer := &pgtype.DesiredDomain{BaseType: "integer", Comment: "mail"}
	commented := &pgtype.DesiredDomain{BaseType: "text", Comment: "mail"}
	uncommented := &pgtype.ObservedDomain{BaseType: "text", Comment: "mail"}
	plain := &pgtype.DesiredDomain{BaseType: "text"}
	nullable := &pgtype.DesiredDomain{BaseType: "text", Comment: "mail"}
	notNull := &pgtype.ObservedDomain{BaseType: "text", NotNull: true}

	a := pgtype.Attribute{Name: "a", Type: "integer"}
	b := pgtype.Attribute{Name: "b", Type: "text"}
	cc := pgtype.Attribute{Name: "c", Type: "text"}
	ab := &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{a, b}}
	replaced := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{a, cc}}
	moved := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{b, a}}

	numeric := &pgtype.ObservedRange{Subtype: "numeric"}
	bigints := &pgtype.DesiredRange{Subtype: "int8", Collation: "C"}
	spanned := &pgtype.DesiredRange{Subtype: "numeric", Comment: "span"}
	tests := []struct {
		name   string
		change schemaext.ChangeRecord
		want   []planned
	}{
		{name: "a domain created with its comment", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{After: everything}),
			want: []planned{
				{Subject: "domain t", Payload: &pgtype.CreateDomain{Target: domainTarget, BaseType: "text", NotNull: true, Default: "it's",
					Check: "VALUE > ''"}, Phase: typePhase, Effect: plangraph.Create},
				{Subject: "domain t", Payload: &pgtype.Comment{Target: domainTarget, Comment: "mail"}, Phase: typePhase, Effect: plangraph.Alter},
			}},
		{name: "a composite type created", change: typeChange(pgtype.CompositeKind, "t", &pgtype.CompositeChange{After: replaced.Copy()}),
			want: []planned{{Subject: "composite type t", Payload: &pgtype.CreateComposite{Target: compositeTarget, Attributes: replaced.Attributes},
				Phase: typePhase, Effect: plangraph.Create}}},
		{name: "a range type created", change: typeChange(pgtype.RangeKind, "t", &pgtype.RangeChange{After: bigints}),
			want: []planned{{Subject: "range type t", Payload: &pgtype.CreateRange{Target: rangeTarget, Subtype: "int8", Collation: "C"},
				Phase: typePhase, Effect: plangraph.Create}}},
		{name: "a domain dropped", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{Before: text}),
			want: []planned{{Subject: "domain t", Payload: &pgtype.Drop{Target: domainTarget, IfExists: true, Cascade: true},
				Notes: []string{"WARNING: Make sure no columns use this domain!"}, Phase: typePhase, Effect: plangraph.Drop}}},
		{name: "a composite type dropped", change: typeChange(pgtype.CompositeKind, "t", &pgtype.CompositeChange{Before: ab}),
			want: []planned{{Subject: "composite type t", Payload: &pgtype.Drop{Target: compositeTarget, IfExists: true, Cascade: true},
				Notes: []string{"WARNING: Make sure no columns use this composite type!"}, Phase: typePhase, Effect: plangraph.Drop}}},
		{name: "a range type dropped", change: typeChange(pgtype.RangeKind, "t", &pgtype.RangeChange{Before: numeric}),
			want: []planned{{Subject: "range type t", Payload: &pgtype.Drop{Target: rangeTarget, IfExists: true, Cascade: true},
				Notes: []string{"WARNING: Make sure no columns use this range type!"}, Phase: typePhase, Effect: plangraph.Drop}}},
		{name: "a domain's CHECK, default and NOT NULL changed in place", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{
			Before: checked, After: everything, Differs: []string{pgtype.DiffCheck, pgtype.DiffDefault, pgtype.DiffNotNull}}),
			want: []planned{{Subject: "domain t", Payload: &pgtype.AlterDomain{Target: domainTarget, DropChecks: []string{"t_check", "t_check1"},
				AddCheck: "VALUE > ''", CheckName: "ptah_check", SetDefault: "'it''s'", NotNull: new(true)}, Phase: typePhase, Effect: plangraph.Alter}}},
		{name: "a CHECK changed to none beside NOT NULL drops the constraints", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{
			Before: checked, After: &pgtype.DesiredDomain{BaseType: "text", NotNull: true}, Differs: []string{pgtype.DiffCheck, pgtype.DiffNotNull}}),
			want: []planned{{Subject: "domain t", Payload: &pgtype.AlterDomain{Target: domainTarget, DropChecks: []string{"t_check", "t_check1"}, NotNull: new(true)}, Phase: typePhase,
				Effect: plangraph.Alter}}},
		{name: "a domain's default expression", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{
			Before: text, After: expression, Differs: []string{pgtype.DiffDefault}}),
			want: []planned{{Subject: "domain t", Payload: &pgtype.AlterDomain{Target: domainTarget, SetDefault: "lower('X')"},
				Phase: typePhase, Effect: plangraph.Alter}}},
		{name: "a domain's NOT NULL dropped, then its comment", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{
			Before: notNull, After: nullable, Differs: []string{pgtype.DiffComment, pgtype.DiffNotNull}}),
			want: []planned{
				{Subject: "domain t", Payload: &pgtype.AlterDomain{Target: domainTarget, NotNull: new(false)}, Phase: typePhase, Effect: plangraph.Alter},
				{Subject: "domain t", Payload: &pgtype.Comment{Target: domainTarget, Comment: "mail"}, Phase: dependent, Effect: plangraph.Alter},
			}},
		{name: "a domain rebuilt over another base type, with its comment", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{
			Before: text, After: integer, Differs: []string{pgtype.DiffBaseType, pgtype.DiffComment}}),
			want: []planned{
				{Subject: "domain t", Payload: &pgtype.Drop{Target: domainTarget, IfExists: true},
					Notes: []string{"Recreate modified domain; drop is non-CASCADE and fails if the domain is in use"}, Phase: typePhase, Effect: plangraph.Drop},
				{Subject: "domain t", Payload: &pgtype.CreateDomain{Target: domainTarget, BaseType: "integer"}, Phase: typePhase, Effect: plangraph.Create},
				{Subject: "domain t", Payload: &pgtype.Comment{Target: domainTarget, Comment: "mail"}, Phase: typePhase, Effect: plangraph.Alter},
			}},
		{name: "a domain's comment", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{
			Before: text, After: commented, Differs: []string{pgtype.DiffComment}}),
			want: []planned{{Subject: "domain t", Payload: &pgtype.Comment{Target: domainTarget, Comment: "mail"}, Phase: dependent, Effect: plangraph.Alter}}},
		{name: "a domain's comment removed", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{
			Before: uncommented, After: plain, Differs: []string{pgtype.DiffComment}}),
			want: []planned{{Subject: "domain t", Payload: &pgtype.Comment{Target: domainTarget}, Phase: dependent, Effect: plangraph.Alter}}},
		{name: "a composite type's attributes changed in place", change: typeChange(pgtype.CompositeKind, "t", &pgtype.CompositeChange{
			Before: ab, After: replaced, Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{cc}, Removed: []string{"b"}}),
			want: []planned{{Subject: "composite type t", Payload: &pgtype.AlterComposite{Target: compositeTarget, Drop: []string{"b"},
				Add: []pgtype.Attribute{cc}}, Phase: typePhase, Effect: plangraph.Alter}}},
		{name: "a composite type rebuilt in another order", change: typeChange(pgtype.CompositeKind, "t", &pgtype.CompositeChange{
			Before: ab, After: moved, Differs: []string{pgtype.DiffAttributes}}),
			want: []planned{
				{Subject: "composite type t", Payload: &pgtype.Drop{Target: compositeTarget, IfExists: true},
					Notes: []string{"Recreate modified composite type; drop is non-CASCADE and fails if the type is in use"}, Phase: typePhase,
					Effect: plangraph.Drop},
				{Subject: "composite type t", Payload: &pgtype.CreateComposite{Target: compositeTarget, Attributes: moved.Attributes}, Phase: typePhase,
					Effect: plangraph.Create},
			}},
		{name: "a range type rebuilt over another subtype", change: typeChange(pgtype.RangeKind, "t", &pgtype.RangeChange{
			Before: numeric, After: bigints, Differs: []string{pgtype.RangeCollation, pgtype.DiffSubtype}}),
			want: []planned{
				{Subject: "range type t", Payload: &pgtype.Drop{Target: rangeTarget, IfExists: true},
					Notes: []string{"Recreate modified range type; drop is non-CASCADE and fails if the type is in use"}, Phase: typePhase, Effect: plangraph.Drop},
				{Subject: "range type t", Payload: &pgtype.CreateRange{Target: rangeTarget, Subtype: "int8", Collation: "C"}, Phase: typePhase,
					Effect: plangraph.Create},
			}},
		{name: "a range type's comment", change: typeChange(pgtype.RangeKind, "t", &pgtype.RangeChange{
			Before: numeric, After: spanned, Differs: []string{pgtype.DiffComment}}),
			want: []planned{{Subject: "range type t", Payload: &pgtype.Comment{Target: rangeTarget, Comment: "span"}, Phase: dependent,
				Effect: plangraph.Alter}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning(test.change)

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(stepsOf(c, result.Contributions), qt.DeepEquals, test.want)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Steps, qt.HasLen, len(test.want))
		})
	}
}

func TestPlanTypes_ExactDropsLeaveDependenciesToPostgres(t *testing.T) {
	for _, test := range []struct {
		name   string
		change schemaext.ChangeRecord
		target pgtype.Target
	}{
		{name: "domain", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "integer"}}), target: domainTarget},
		{name: "composite", change: typeChange(pgtype.CompositeKind, "t", &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{
			Attributes: []pgtype.Attribute{{Name: "a", Type: "integer"}},
		}}), target: compositeTarget},
		{name: "range", change: typeChange(pgtype.RangeKind, "t", &pgtype.RangeChange{Before: &pgtype.ObservedRange{Subtype: "integer"}}), target: rangeTarget},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning(test.change)
			request.Policy.Statements = featureplan.StatementStyleExact

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			steps := stepsOf(c, result.Contributions)
			c.Assert(steps, qt.HasLen, 1)
			c.Assert(steps[0].Payload, qt.DeepEquals, &pgtype.Drop{Target: test.target})
		})
	}
}
