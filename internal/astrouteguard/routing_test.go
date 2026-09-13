package astrouteguard_test

import (
	"path"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/astrouteguard"
)

// This is the gate the collapse of ast.Visitor traded for. One method per node
// kind made the compiler ask every renderer about every kind; a type switch asks
// nothing, and a case nobody wrote falls to a default arm. The default arm
// errors, so the failure is loud at run time -- but a node kind reaching it is a
// kind that renders as an error on a target that could have rendered it, and
// that is a question to answer before a release rather than during one.

// TestRouting_EveryRendererDecidesAboutEveryNodeKind is the gate.
func TestRouting_EveryRendererDecidesAboutEveryNodeKind(t *testing.T) {
	c := qt.New(t)

	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)

	kinds, err := astrouteguard.NodeKinds(root)
	c.Assert(err, qt.IsNil)
	c.Assert(len(kinds) >= astrouteguard.NodeKindFloor, qt.IsTrue,
		qt.Commentf("the node corpus holds %d kinds, below the floor of %d", len(kinds), astrouteguard.NodeKindFloor))

	renderers, err := astrouteguard.Renderers(root)
	c.Assert(err, qt.IsNil)
	c.Assert(len(renderers) >= astrouteguard.RendererFloor, qt.IsTrue,
		qt.Commentf("the guard found %d renderers, below the floor of %d", len(renderers), astrouteguard.RendererFloor))

	byPackage := make(map[string]astrouteguard.Renderer, len(renderers))
	for _, renderer := range renderers {
		byPackage[path.Base(renderer.Package)] = renderer
	}

	for _, renderer := range renderers {
		t.Run(path.Base(renderer.Package), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(answeredBy(kinds, renderer, byPackage), qt.HasLen, 0,
				qt.Commentf("%s names no case for these node kinds and no renderer it delegates to does either",
					renderer.Package))
		})
	}
}

// answeredBy reports the node kinds a renderer neither names nor reaches
// through a delegation.
//
// A forward is followed rather than counted. A renderer that answers "the other
// one handles it" and delegates to a renderer that does not name the kind
// either is two renderers agreeing to drop it, which is the shape a gate that
// stopped at the first edge would call covered.
func answeredBy(
	kinds []astrouteguard.NodeKind,
	renderer astrouteguard.Renderer,
	byPackage map[string]astrouteguard.Renderer,
) []string {
	routed := make(map[string]bool)
	seen := make(map[string]bool)
	for current := renderer; ; {
		for _, name := range current.Routed {
			routed[name] = true
		}
		if current.DelegatesTo == "" || seen[current.DelegatesTo] {
			break
		}
		seen[current.DelegatesTo] = true
		next, ok := byPackage[current.DelegatesTo]
		if !ok {
			break
		}
		current = next
	}

	var missing []string
	for _, kind := range kinds {
		if !routed[kind.Name] {
			missing = append(missing, kind.Name)
		}
	}
	return missing
}

// TestRouting_TheDelegatingRenderersAreTheOnesThatDelegate pins which renderers
// answer by forwarding.
//
// Forwarding widens what the gate above accepts: a renderer that names nothing
// and delegates passes on its target's cases. Without this row, a renderer
// could drop its own cases into a forward and the gate would still be green.
func TestRouting_TheDelegatingRenderersAreTheOnesThatDelegate(t *testing.T) {
	c := qt.New(t)

	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	renderers, err := astrouteguard.Renderers(root)
	c.Assert(err, qt.IsNil)

	delegating := delegationEdges(renderers)

	c.Assert(delegating, qt.DeepEquals, []string{
		"mariadb -> mysqllike",
		"mysql -> mysqllike",
	})
}

// TestRouting_SelfTest is the gate's control: a renderer that names a kind is
// covered, one that names nothing and delegates nowhere is not, and a forward
// is followed to the renderer that answers.
func TestRouting_SelfTest(t *testing.T) {
	kinds := []astrouteguard.NodeKind{{Name: "CreateTableNode"}, {Name: "IndexNode"}}
	shared := astrouteguard.Renderer{
		Package: "core/renderer/internal/dialects/shared",
		Routed:  []string{"CreateTableNode", "IndexNode"},
	}
	byPackage := map[string]astrouteguard.Renderer{"shared": shared}

	tests := []struct {
		name     string
		renderer astrouteguard.Renderer
		want     []string
	}{
		{
			name:     "a renderer naming every kind",
			renderer: shared,
			want:     nil,
		},
		{
			name: "a renderer forwarding to one that answers",
			renderer: astrouteguard.Renderer{
				Package:     "core/renderer/internal/dialects/thin",
				Routed:      []string{"CreateTableNode"},
				DelegatesTo: "shared",
			},
			want: nil,
		},
		{
			name: "a renderer forwarding nowhere",
			renderer: astrouteguard.Renderer{
				Package: "core/renderer/internal/dialects/gap",
				Routed:  []string{"CreateTableNode"},
			},
			want: []string{"IndexNode"},
		},
		{
			name: "a renderer forwarding to a package the guard does not know",
			renderer: astrouteguard.Renderer{
				Package:     "core/renderer/internal/dialects/lost",
				Routed:      []string{"CreateTableNode"},
				DelegatesTo: "absent",
			},
			want: []string{"IndexNode"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(answeredBy(kinds, test.renderer, byPackage), qt.DeepEquals, test.want)
		})
	}
}

// TestRouting_EveryRendererPackageIsUnderTheDialectsDirectory is the control on
// the discovery: a guard that found renderers somewhere else would be measuring
// something other than the eight that ship.
func TestRouting_EveryRendererPackageIsUnderTheDialectsDirectory(t *testing.T) {
	c := qt.New(t)

	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	renderers, err := astrouteguard.Renderers(root)
	c.Assert(err, qt.IsNil)

	for _, renderer := range renderers {
		t.Run(path.Base(renderer.Package), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(strings.HasPrefix(renderer.Package, "core/renderer/internal/dialects/"), qt.IsTrue,
				qt.Commentf("renderer found at %s", renderer.Package))
			c.Assert(len(renderer.Routed) > 0, qt.IsTrue,
				qt.Commentf("%s names no node kind at all", renderer.Package))
		})
	}
}

// delegationEdges names every renderer that forwards its default arm, as
// `renderer -> target`, sorted.
func delegationEdges(renderers []astrouteguard.Renderer) []string {
	var edges []string
	for _, renderer := range renderers {
		if renderer.DelegatesTo == "" {
			continue
		}
		edges = append(edges, path.Base(renderer.Package)+" -> "+renderer.DelegatesTo)
	}
	slices.Sort(edges)
	return edges
}

// TestRouting_EveryPathIsSlashSpelled is the portable half of the Windows
// contract.
//
// Every path the guard reports comes from `git ls-files`, which spells them
// with forward slashes on every platform. Deriving a directory with
// path/filepath instead of path answers `core\renderer\...` on Windows, and a
// package spelled that way matches nothing a caller compares it against -- while
// the same code passes everywhere else, because filepath and path agree on a
// platform whose separator is already the slash.
//
// Asserting the absence of a backslash is what makes that visible on the
// platform it breaks on, rather than only in a CI log.
func TestRouting_EveryPathIsSlashSpelled(t *testing.T) {
	c := qt.New(t)

	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)

	kinds, err := astrouteguard.NodeKinds(root)
	c.Assert(err, qt.IsNil)
	for _, kind := range kinds {
		c.Assert(kind.File, qt.Not(qt.Contains), `\`,
			qt.Commentf("node kind %s reports the file as %q", kind.Name, kind.File))
	}

	renderers, err := astrouteguard.Renderers(root)
	c.Assert(err, qt.IsNil)
	for _, renderer := range renderers {
		c.Assert(renderer.Package, qt.Not(qt.Contains), `\`,
			qt.Commentf("renderer package reads %q", renderer.Package))
	}
}
