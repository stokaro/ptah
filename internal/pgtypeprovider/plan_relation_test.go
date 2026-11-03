package pgtypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/feature/pgtype"
)

var relations = objectidentity.NewBuilder(identifier.ForDialect("postgres"))

// hostStep is a common step of the PostgreSQL host named name.
func hostStep(name string, effects ...plangraph.Effect) featureplan.CommonStep {
	return featureplan.CommonStep{ID: plangraph.StepID{Owner: "ptah.run/postgres", Name: name}, Effects: effects}
}

func acts(subject objectidentity.ID, action plangraph.Action) plangraph.Effect {
	return plangraph.Effect{Subject: subject, Action: action}
}

func relation(kind objectidentity.Kind, name string) objectidentity.ID {
	ref := relations.Table(name)
	ref.Kind = kind
	return ref
}

// wrapOver is the composite type wrap whose attribute r has the row type of
// table, beside an integer.
func wrapOver(table string) []pgtype.Attribute {
	return []pgtype.Attribute{{Name: "r", Type: table}, {Name: "n", Type: "int"}}
}

// placedStep is an owner step as a placement test reads it: the type it
// names, its action and the phase it joins.
type placedStep struct {
	Subject string
	Action  plangraph.Action
	Phase   featureplan.Phase
}

// placed lists the owner's steps in contribution order, and the edges
// between them and the host's steps, each as "before -> after".
func placed(c *qt.C, contributions []plangraph.Contribution[featureplan.Operation]) ([]placedStep, []string) {
	c.Helper()
	c.Assert(contributions, qt.HasLen, 1)
	var steps []placedStep
	for _, step := range contributions[0].Steps {
		subject := step.Effects[0].Subject
		steps = append(steps, placedStep{Subject: string(subject.Kind) + " " + subject.Name.Source, Action: step.Effects[0].Action, Phase: step.Payload.Phase})
	}
	var edges []string
	for _, edge := range contributions[0].Dependencies {
		if edge.Before.Owner != pgtype.Owner || edge.After.Owner != pgtype.Owner {
			edges = append(edges, edge.Before.Name+" -> "+edge.After.Name)
		}
	}
	return steps, edges
}

// TestPlanTypes_CreatesATypeOverACreatedTableAfterIt pins where a type goes
// whose attribute is the row type of a table the plan creates: the default
// phase, which the host runs after its tables and columns, ordered after the
// table's step. So does a domain over it. A type over no created relation
// keeps the type phase, before the tables. Without this the type was created
// first, and the server answered `type "t" does not exist`.
func TestPlanTypes_CreatesATypeOverACreatedTableAfterIt(t *testing.T) {
	c := qt.New(t)
	request := planning(
		typeChange(pgtype.CompositeKind, "wrap", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: wrapOver("t")}}),
		typeChange(pgtype.DomainKind, "dw", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "wrap"}}),
		typeChange(pgtype.CompositeKind, "pair", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: wrapOver("int")}}),
	)
	request.CommonSteps = []featureplan.CommonStep{hostStep("common/000001", acts(relations.Table("t"), plangraph.Create))}

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	steps, edges := placed(c, result.Contributions)
	c.Assert(steps, qt.DeepEquals, []placedStep{
		{"composite type pair", plangraph.Create, featureplan.PhaseType},
		{"composite type wrap", plangraph.Create, featureplan.PhaseDefault},
		{"domain dw", plangraph.Create, featureplan.PhaseDefault},
	})
	c.Assert(edges, qt.DeepEquals, []string{"common/000001 -> type/000000/create", "common/000001 -> type/000001/create"})
	c.Assert(result.Changes[0].Strategy, qt.Equals, "create the type after the relations whose row types it names, and before the types that name it")
	c.Assert(result.Changes[2].Strategy, qt.Equals, "create the type before the columns, routines and types that name it")
}

// TestPlanTypes_ChangesATypeInPlaceAfterACreatedTable pins a type the plan
// changes in place to name the row type of a table the plan creates: the
// ALTER TYPE joins the default phase after the table's step. A table the plan
// drops whose column holds the type reads it too, but that drop runs after
// every creation, so it refuses nothing.
func TestPlanTypes_ChangesATypeInPlaceAfterACreatedTable(t *testing.T) {
	c := qt.New(t)
	request := planning(typeChange(pgtype.CompositeKind, "wrap", &pgtype.CompositeChange{
		Before:  &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}},
		After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}, {Name: "r", Type: "t"}}},
		Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{{Name: "r", Type: "t"}}}))
	request.CommonSteps = []featureplan.CommonStep{
		hostStep("common/000001", acts(relations.Table("t"), plangraph.Create)),
		hostStep("common/000002", acts(relations.Table("t2"), plangraph.Drop), acts(relations.SchemaScopedParts(objectidentity.KindComposite, "", "wrap"), plangraph.Read)),
	}

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	steps, edges := placed(c, result.Contributions)
	c.Assert(steps, qt.DeepEquals, []placedStep{{"composite type wrap", plangraph.Alter, featureplan.PhaseDefault}})
	c.Assert(edges, qt.DeepEquals, []string{"common/000001 -> type/000000/alter"})
}

// TestPlanTypes_DropsATypeOverADroppedTableAhead pins the removal side: a
// removed type over the row type of a table the plan drops joins the default
// phase, which the host runs before its table drops, and is ordered before
// the table's step, so the table's drop needs no CASCADE. A type a later drop
// still uses, here a dropped table's column, stays in the type phase after
// the tables.
func TestPlanTypes_DropsATypeOverADroppedTableAhead(t *testing.T) {
	wrap := typeChange(pgtype.CompositeKind, "wrap", &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: wrapOver("t")}})
	dropT := hostStep("common/000001", acts(relations.Table("t"), plangraph.Drop))
	tests := []struct {
		name     string
		steps    []featureplan.CommonStep
		want     []placedStep
		edges    []string
		strategy string
	}{
		{name: "nothing else uses it", steps: []featureplan.CommonStep{dropT},
			want:  []placedStep{{"composite type wrap", plangraph.Drop, featureplan.PhaseDefault}},
			edges: []string{"type/000000/drop -> common/000001"}, strategy: "drop the type before the tables whose row types it names, after the objects that use it"},
		{name: "a dropped table's column uses it", steps: []featureplan.CommonStep{dropT,
			hostStep("common/000002", acts(relations.Table("t2"), plangraph.Drop), acts(relations.SchemaScopedParts(objectidentity.KindComposite, "", "wrap"), plangraph.Read))},
			want: []placedStep{{"composite type wrap", plangraph.Drop, featureplan.PhaseType}}, strategy: "drop the type with CASCADE after the objects that use it"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning(wrap)
			request.CommonSteps = test.steps

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			steps, edges := placed(c, result.Contributions)
			c.Assert(steps, qt.DeepEquals, test.want)
			c.Assert(edges, qt.DeepEquals, test.edges)
			c.Assert(result.Changes[0].Strategy, qt.Equals, test.strategy)
		})
	}
}

// TestPlanTypes_RefusesATypeOverACreatedRelationNoWindowHolds pins the plans
// no window orders: a type over a created table's row type that a column, a
// table or a routine of the plan uses, since the type waits until after them;
// a type over the row type of a view the plan creates, since the window after
// the tables lies before the views; and a type over a created table that is
// rebuilt, since a rebuild happens before the tables. Each would stop at a
// statement naming something the server does not have yet.
func TestPlanTypes_RefusesATypeOverACreatedRelationNoWindowHolds(t *testing.T) {
	wrapRef := relations.SchemaScopedParts(objectidentity.KindComposite, "", "wrap")
	createT := hostStep("common/000001", acts(relations.Table("t"), plangraph.Create))
	created := typeChange(pgtype.CompositeKind, "wrap", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: wrapOver("t")}})
	tests := []struct {
		name    string
		changes []schemaext.ChangeRecord
		steps   []featureplan.CommonStep
		want    string
	}{
		{name: "a created table's column", changes: []schemaext.ChangeRecord{created}, steps: []featureplan.CommonStep{createT,
			hostStep("common/000002", acts(relations.Table("t2"), plangraph.Create), acts(wrapRef, plangraph.Read))},
			want: "composite type wrap depends on the row type of table t, which the plan creates; table t2 uses it, and Ptah creates such a type " +
				"after the tables and columns, so it cannot exist when table t2 is created"},
		{name: "an added column", changes: []schemaext.ChangeRecord{created}, steps: []featureplan.CommonStep{createT,
			hostStep("common/000002", acts(relations.Table("kept"), plangraph.Alter), acts(relations.Column("kept", "w"), plangraph.Create), acts(wrapRef, plangraph.Read))},
			want: "composite type wrap depends on the row type of table t, which the plan creates; column kept.w uses it, and Ptah creates such a type " +
				"after the tables and columns, so it cannot exist when column kept.w is created"},
		{name: "a created view", changes: []schemaext.ChangeRecord{
			typeChange(pgtype.CompositeKind, "wrap", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: wrapOver("v")}})},
			steps: []featureplan.CommonStep{hostStep("common/000003", acts(relation(objectidentity.KindView, "v"), plangraph.Create))},
			want:  "composite type wrap depends on the row type of view v, which the plan creates; Ptah creates such a type after the tables and before the views"},
		{name: "a rebuilt type", changes: []schemaext.ChangeRecord{typeChange(pgtype.CompositeKind, "wrap", &pgtype.CompositeChange{
			Before:  &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}, {Name: "r", Type: "text"}}},
			After:   &pgtype.DesiredComposite{Attributes: wrapOver("t")},
			Differs: []string{pgtype.DiffAttributes}})},
			steps: []featureplan.CommonStep{createT},
			want: "composite type wrap depends on the row type of table t, which the plan creates; Ptah creates such a type after the tables, " +
				"and drops and creates again before them a type it cannot change in place"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning(test.changes...)
			request.CommonSteps = test.steps

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, []featureplan.Diagnostic{{Change: new(0), Problem: schemavalidation.Diagnostic{
				Code: schemavalidation.UnsupportedFeature, Kind: string(pgtype.CompositeChangeKind), Object: "wrap",
				Feature: "CREATE TYPE ... AS (...)", Message: test.want}}})
			c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(result.Contributions, qt.HasLen, 0)
		})
	}
}

// TestPlanTypes_DeclaresATypeOverATableAfterIt pins the same placement in a
// whole-schema render, which orders a default-phase step by its dependencies
// alone: the type over the table's row type follows the table's step.
func TestPlanTypes_DeclaresATypeOverATableAfterIt(t *testing.T) {
	c := qt.New(t)
	request := featureplan.DeclarationRequest{Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres17(),
		Objects:     []schemaext.Object{must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, "", "wrap"), &pgtype.DesiredComposite{Attributes: wrapOver("t")}))},
		CommonSteps: []featureplan.CommonStep{hostStep("common/000001", acts(relations.Table("t"), plangraph.Create))}}

	result, err := newRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	steps, edges := placed(c, result.Contributions)
	c.Assert(steps, qt.DeepEquals, []placedStep{{"composite type wrap", plangraph.Create, featureplan.PhaseDefault}})
	c.Assert(edges, qt.DeepEquals, []string{"common/000001 -> type/000000/create"})
}
