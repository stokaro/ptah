package featurehost_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/featurehost"
)

// commonNodes is a host's sequence: a schema precondition, then the creations,
// then the removals, each node a comment naming its position.
func commonNodes() []ast.Node {
	return []ast.Node{ast.NewComment("schemas"), ast.NewComment("types"), ast.NewComment("tables"), ast.NewComment("drop tables")}
}

// prerequisiteWindows places the prerequisite creations after the schemas and
// their drops after everything, around default windows that follow the types.
func prerequisiteWindows() featurehost.Windows {
	return featurehost.Windows{Creation: 2, Dependent: 3, Removal: 3, Prerequisites: &featurehost.PrerequisiteWindows{Creation: 1, Removal: 4}}
}

// ownerStep is one feature step that writes subject with action.
func ownerStep(name string, action plangraph.Action) plangraph.Step[[]ast.Node] {
	subject := objectidentity.ID{Kind: objectidentity.KindExtension, Name: objectidentity.Part{Source: name, Normalized: name}}
	return plangraph.Step[[]ast.Node]{ID: plangraph.StepID{Owner: "example.org/owner", Name: name},
		Payload: []ast.Node{ast.NewComment(name)}, Effects: []plangraph.Effect{{Subject: subject, Action: action}}}
}

// comments returns the text of each comment node, in order.
func comments(nodes []ast.Node) []string {
	texts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		texts = append(texts, node.(*ast.CommentNode).Text)
	}
	return texts
}

func prerequisiteResult(phase featureplan.Phase) featurehost.Result {
	created, dropped := ownerStep("installs", plangraph.Create), ownerStep("removes", plangraph.Drop)
	return featurehost.Result{
		Contributions: []plangraph.Contribution[[]ast.Node]{{Owner: "example.org/owner", Steps: []plangraph.Step[[]ast.Node]{created, dropped}}},
		Phases:        map[plangraph.StepID]featureplan.Phase{created.ID: phase, dropped.ID: phase},
	}
}

func TestGraph_PrerequisiteWindows_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		phase featureplan.Phase
		want  []string
	}{
		{
			name: "a prerequisite installs after the schemas and is removed after everything", phase: featureplan.PhasePrerequisite,
			want: []string{"schemas", "installs", "types", "tables", "drop tables", "removes"},
		},
		{
			name: "control: the default phase joins the default windows", phase: featureplan.PhaseDefault,
			want: []string{"schemas", "types", "installs", "tables", "removes", "drop tables"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			graph := must.Must(featurehost.NewGraph("example.org/host", commonNodes(), nil, prerequisiteWindows()))

			nodes, err := graph.Schedule(t.Context(), prerequisiteResult(test.phase))

			c.Assert(err, qt.IsNil)
			c.Assert(comments(nodes), qt.DeepEquals, test.want)
		})
	}
}

// typeNodes is a host's sequence around the type windows: the enums, the
// tables, then the table and enum drops.
func typeNodes() []ast.Node {
	return []ast.Node{ast.NewComment("enums"), ast.NewComment("tables"), ast.NewComment("drop tables"), ast.NewComment("drop enums")}
}

// typeWindows places the type creations after the enums and before the
// tables, and the type drops after the table drops and before the enum drops,
// around default windows that follow the tables.
func typeWindows() featurehost.Windows {
	return featurehost.Windows{Creation: 2, Dependent: 3, Removal: 3, Types: &featurehost.TypeWindows{Creation: 1, Removal: 3}}
}

// typeResult is one owner's steps, each of phase, the first before the next:
// a step named "drops X" drops the type X and one named "creates X" creates
// it.
func typeResult(phase featureplan.Phase, steps ...plangraph.Step[[]ast.Node]) featurehost.Result {
	contribution := plangraph.Contribution[[]ast.Node]{Owner: "example.org/owner", Steps: steps}
	phases := make(map[plangraph.StepID]featureplan.Phase, len(steps))
	var previous plangraph.StepID
	for _, step := range steps {
		phases[step.ID] = phase
		if previous != (plangraph.StepID{}) {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: previous, After: step.ID})
		}
		previous = step.ID
	}
	return featurehost.Result{Contributions: []plangraph.Contribution[[]ast.Node]{contribution}, Phases: phases}
}

// typeStep is a step that writes the type name with action, named by both.
func typeStep(action plangraph.Action, name string) plangraph.Step[[]ast.Node] {
	text := map[plangraph.Action]string{plangraph.Create: "creates ", plangraph.Drop: "drops "}[action] + name
	subject := objectidentity.ID{Kind: objectidentity.KindDomain, Name: objectidentity.Part{Source: name, Normalized: name}}
	return plangraph.Step[[]ast.Node]{ID: plangraph.StepID{Owner: "example.org/owner", Name: text},
		Payload: []ast.Node{ast.NewComment(text)}, Effects: []plangraph.Effect{{Subject: subject, Action: action}}}
}

func TestGraph_TypeWindows_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		result featurehost.Result
		want   []string
	}{
		{name: "a type is created after the enums and before the tables",
			result: typeResult(featureplan.PhaseType, typeStep(plangraph.Create, "zip")),
			want:   []string{"enums", "creates zip", "tables", "drop tables", "drop enums"}},
		{name: "a type is dropped after the tables and before the enums",
			result: typeResult(featureplan.PhaseType, typeStep(plangraph.Drop, "zip")),
			want:   []string{"enums", "tables", "drop tables", "drops zip", "drop enums"}},
		{name: "a rebuilt type is dropped and created again before the tables",
			result: typeResult(featureplan.PhaseType, typeStep(plangraph.Drop, "zip"), typeStep(plangraph.Create, "zip")),
			want:   []string{"enums", "drops zip", "creates zip", "tables", "drop tables", "drop enums"}},
		{name: "a drop beside the creation of another type stays a removal",
			result: typeResult(featureplan.PhaseType, typeStep(plangraph.Create, "zip"), typeStep(plangraph.Drop, "old")),
			want:   []string{"enums", "creates zip", "tables", "drop tables", "drops old", "drop enums"}},
		{name: "control: the default phase joins the default windows",
			result: typeResult(featureplan.PhaseDefault, typeStep(plangraph.Create, "zip")),
			want:   []string{"enums", "tables", "creates zip", "drop tables", "drop enums"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			graph := must.Must(featurehost.NewGraph("example.org/host", typeNodes(), nil, typeWindows()))

			nodes, err := graph.Schedule(t.Context(), test.result)

			c.Assert(err, qt.IsNil)
			c.Assert(comments(nodes), qt.DeepEquals, test.want)
		})
	}
}

func TestGraph_TypeWindows_FailurePath(t *testing.T) {
	t.Run("a host without the windows refuses the phase", func(t *testing.T) {
		c := qt.New(t)
		graph := must.Must(featurehost.NewGraph("example.org/host", typeNodes(), nil, featurehost.Windows{Creation: 2, Dependent: 3, Removal: 3}))

		nodes, err := graph.Schedule(t.Context(), typeResult(featureplan.PhaseType, typeStep(plangraph.Create, "zip")))

		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(err, qt.ErrorMatches, `feature step example.org/owner/creates zip: .*the "type" phase has no window in this host`)
		c.Assert(nodes, qt.IsNil)
	})

	t.Run("a rebuild split across the default phase is refused", func(t *testing.T) {
		c := qt.New(t)
		graph := must.Must(featurehost.NewGraph("example.org/host", typeNodes(), nil, typeWindows()))

		nodes, err := graph.Schedule(t.Context(),
			typeResult(featureplan.PhaseDefault, typeStep(plangraph.Drop, "zip"), typeStep(plangraph.Create, "zip")))

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, `.*creates domain zip, which step .* drops; a replacement is one step, or both halves ask for the dependent or the type phase`)
		c.Assert(nodes, qt.IsNil)
	})

	tests := []struct {
		name  string
		types featurehost.TypeWindows
	}{
		{name: "creation after the default creations", types: featurehost.TypeWindows{Creation: 3, Removal: 3}},
		{name: "removal before the default removals", types: featurehost.TypeWindows{Creation: 1, Removal: 2}},
		{name: "removal outside the sequence", types: featurehost.TypeWindows{Creation: 1, Removal: 5}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			windows := featurehost.Windows{Creation: 2, Dependent: 3, Removal: 3, Types: &test.types}

			graph, err := featurehost.NewGraph("example.org/host", typeNodes(), nil, windows)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `.*feature windows .* are out of order or outside a sequence of 4 nodes`)
			c.Assert(graph, qt.IsNil)
		})
	}
}

func TestGraph_PrerequisiteWindows_FailurePath(t *testing.T) {
	t.Run("a host without the windows refuses the phase", func(t *testing.T) {
		c := qt.New(t)
		graph := must.Must(featurehost.NewGraph("example.org/host", commonNodes(), nil, featurehost.Windows{Creation: 2, Dependent: 3, Removal: 3}))

		nodes, err := graph.Schedule(t.Context(), prerequisiteResult(featureplan.PhasePrerequisite))

		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(err, qt.ErrorMatches, `feature step example.org/owner/installs: .*the "prerequisite" phase has no window in this host`)
		c.Assert(nodes, qt.IsNil)
	})

	tests := []struct {
		name          string
		prerequisites featurehost.PrerequisiteWindows
	}{
		{name: "creation after the default creations", prerequisites: featurehost.PrerequisiteWindows{Creation: 3, Removal: 4}},
		{name: "removal before the default removals", prerequisites: featurehost.PrerequisiteWindows{Creation: 1, Removal: 2}},
		{name: "removal outside the sequence", prerequisites: featurehost.PrerequisiteWindows{Creation: 1, Removal: 5}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			windows := featurehost.Windows{Creation: 2, Dependent: 3, Removal: 3, Prerequisites: &test.prerequisites}

			graph, err := featurehost.NewGraph("example.org/host", commonNodes(), nil, windows)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `.*feature windows .* are out of order or outside a sequence of 4 nodes`)
			c.Assert(graph, qt.IsNil)
		})
	}
}
