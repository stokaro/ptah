package typeorder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/deporder"
	"ptah.run/internal/typeorder"
)

var (
	semantics = identifier.ForDialect("postgres")
	builder   = objectidentity.NewBuilder(semantics)
)

func composite(name string) objectidentity.ID {
	return builder.SchemaScopedParts(objectidentity.KindComposite, "", name)
}

func view(name string) objectidentity.ID {
	ref := builder.Table(name)
	ref.Kind = objectidentity.KindView
	return ref
}

// common is a common step named name with effects.
func common(name string, effects ...plangraph.Effect) featureplan.CommonStep {
	return featureplan.CommonStep{ID: plangraph.StepID{Owner: "ptah.run/postgres", Name: name}, Effects: effects}
}

func on(subject objectidentity.ID, action plangraph.Action) plangraph.Effect {
	return plangraph.Effect{Subject: subject, Action: action}
}

// relationSteps names the relation steps of a placement by step name.
func relationSteps(placed map[string][]typeorder.RelationStep) map[string][]string {
	named := make(map[string][]string, len(placed))
	for name, steps := range placed {
		for _, step := range steps {
			named[name] = append(named[name], step.Step.Name+" "+step.Relation.String())
		}
	}
	return named
}

// TestReads_ResolvesSpellingsToDefinedTypes pins how a host's type spellings
// become reads: array dimensions and a schema the definition does not write
// resolve to the one type of that name, each type is read once, and a
// built-in type or an unknown name reads nothing.
func TestReads_ResolvesSpellingsToDefinedTypes(t *testing.T) {
	c := qt.New(t)
	definitions := []schemaext.DefinedType{{ID: composite("wrap")}, {ID: builder.Domain("posint")}}

	reads := typeorder.Reads([]string{"wrap[]", "integer", "public.wrap", "posint", "missing"}, definitions)

	c.Assert(reads, qt.DeepEquals, []plangraph.Effect{on(composite("wrap"), plangraph.Read), on(builder.Domain("posint"), plangraph.Read)})
}

// TestCreatedAfterRelations_FindsTypesOverCreatedRowTypes pins which types
// wait for a relation the plan creates: one whose attribute names its row
// type or an array of it, one that names such a type in turn, and one over a
// view's row type. A type over a relation no step creates waits for nothing.
func TestCreatedAfterRelations_FindsTypesOverCreatedRowTypes(t *testing.T) {
	steps := []featureplan.CommonStep{
		common("common/000001", on(builder.Table("t"), plangraph.Create)),
		common("common/000002", on(view("v"), plangraph.Create)),
		common("common/000003", on(builder.Table("kept"), plangraph.Alter)),
	}
	tests := []struct {
		name  string
		types []deporder.UserType
		want  map[string][]string
	}{
		{name: "an attribute of the row type", types: []deporder.UserType{{Name: "wrap", References: []string{"t", "int"}}},
			want: map[string][]string{"wrap": {"common/000001 table public.t"}}},
		{name: "an array of the row type", types: []deporder.UserType{{Name: "wrap", References: []string{"t[]"}}},
			want: map[string][]string{"wrap": {"common/000001 table public.t"}}},
		{name: "a domain over such a type", types: []deporder.UserType{{Name: "dw", References: []string{"wrap"}}, {Name: "wrap", References: []string{"t"}}},
			want: map[string][]string{"dw": {"common/000001 table public.t"}, "wrap": {"common/000001 table public.t"}}},
		{name: "a view's row type", types: []deporder.UserType{{Name: "wv", References: []string{"v"}}},
			want: map[string][]string{"wv": {"common/000002 view public.v"}}},
		{name: "a relation the plan only changes", types: []deporder.UserType{{Name: "wk", References: []string{"kept"}}},
			want: make(map[string][]string)},
		{name: "no relation", types: []deporder.UserType{{Name: "pair", References: []string{"int", "text"}}},
			want: make(map[string][]string)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			placed := typeorder.CreatedAfterRelations(test.types, steps, semantics)

			c.Assert(relationSteps(placed), qt.DeepEquals, test.want)
		})
	}
}

// TestDroppedBeforeTables_KeepsTypesADropStillUses pins which removed types
// go ahead of the tables whose row types they name. A type a drop step reads
// stays behind, and so does every type it names, since it is dropped first. A
// type the plan keeps, and one over a view's row type, do not go ahead.
func TestDroppedBeforeTables_KeepsTypesADropStillUses(t *testing.T) {
	dropT := common("common/000001", on(builder.Table("t"), plangraph.Drop))
	dropV := common("common/000002", on(view("v"), plangraph.Drop))
	tests := []struct {
		name    string
		types   []deporder.UserType
		removed []string
		steps   []featureplan.CommonStep
		want    map[string][]string
	}{
		{name: "a removed type over a dropped table", types: []deporder.UserType{{Name: "wrap", References: []string{"t", "int"}}},
			removed: []string{"wrap"}, steps: []featureplan.CommonStep{dropT},
			want: map[string][]string{"wrap": {"common/000001 table public.t"}}},
		{name: "a dropped table's column uses it", types: []deporder.UserType{{Name: "wrap", References: []string{"t"}}},
			removed: []string{"wrap"}, steps: []featureplan.CommonStep{dropT,
				common("common/000003", on(builder.Table("t2"), plangraph.Drop), on(composite("wrap"), plangraph.Read))},
			want: make(map[string][]string)},
		{name: "a dropped table's column uses a domain over it", types: []deporder.UserType{{Name: "dw", References: []string{"wrap"}}, {Name: "wrap", References: []string{"t"}}},
			removed: []string{"dw", "wrap"}, steps: []featureplan.CommonStep{dropT,
				common("common/000003", on(builder.Table("t2"), plangraph.Drop), on(builder.Domain("dw"), plangraph.Read))},
			want: make(map[string][]string)},
		{name: "a created table's column uses it", types: []deporder.UserType{{Name: "wrap", References: []string{"t"}}},
			removed: []string{"wrap"}, steps: []featureplan.CommonStep{dropT,
				common("common/000003", on(builder.Table("t2"), plangraph.Create), on(composite("wrap"), plangraph.Read))},
			want: map[string][]string{"wrap": {"common/000001 table public.t"}}},
		{name: "a type the plan keeps", types: []deporder.UserType{{Name: "wrap", References: []string{"t"}}},
			removed: nil, steps: []featureplan.CommonStep{dropT},
			want: make(map[string][]string)},
		{name: "a removed type over a dropped view", types: []deporder.UserType{{Name: "wv", References: []string{"v"}}},
			removed: []string{"wv"}, steps: []featureplan.CommonStep{dropV},
			want: make(map[string][]string)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			placed := typeorder.DroppedBeforeTables(test.types, test.removed, test.steps, semantics)

			c.Assert(relationSteps(placed), qt.DeepEquals, test.want)
		})
	}
}
