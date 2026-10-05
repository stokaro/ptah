package astrouteguard_test

import (
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/astrouteguard"
)

// The corpus is the whole gate. Everything downstream asks "is this kind routed
// by this renderer", and a corpus that lost a kind answers that question about a
// smaller set while reporting nothing missing -- which reads exactly like a tree
// where every renderer answers everything.

func TestNodeKinds_HappyPath(t *testing.T) {
	c := qt.New(t)

	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)

	kinds, err := astrouteguard.NodeKinds(root)
	c.Assert(err, qt.IsNil)

	c.Assert(len(kinds) >= astrouteguard.NodeKindFloor, qt.IsTrue,
		qt.Commentf("the corpus holds %d node kinds, below the floor of %d",
			len(kinds), astrouteguard.NodeKindFloor))

	seen := make(map[string]astrouteguard.NodeKind, len(kinds))
	for _, kind := range kinds {
		c.Assert(seen[kind.Name].Name, qt.Equals, "",
			qt.Commentf("%s is reported twice, at %s:%d and %s:%d",
				kind.Name, seen[kind.Name].File, seen[kind.Name].Line, kind.File, kind.Line))
		seen[kind.Name] = kind
		c.Assert(path.Dir(kind.File), qt.Equals, "core/ast",
			qt.Commentf("%s is reported from %s", kind.Name, kind.File))
		c.Assert(kind.Line > 0, qt.IsTrue)
	}
}

// TestNodeKinds_ReportsTheKindsThatReachNoVisitorMethod is the measurement the
// gate exists because of.
//
// The corpus is wider than any one dispatch table, so a few well-known kinds are
// named here as a control: they implement ast.Node and a renderer has to decide
// about each one. Naming a handful rather than all of them keeps this a control
// on the enumeration and leaves the complete list derived.
func TestNodeKinds_ReportsTheKindsThatReachNoVisitorMethod(t *testing.T) {
	c := qt.New(t)

	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	kinds, err := astrouteguard.NodeKinds(root)
	c.Assert(err, qt.IsNil)

	found := make(map[string]bool, len(kinds))
	for _, kind := range kinds {
		found[kind.Name] = true
	}

	for _, name := range []string{
		"DropColumnOperation",
		"AddColumnOperation",
		"EnumTypeDef",
		"StatementList",
		"PostgresDoBlockNode",
		"CreateTableNode",
	} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(found[name], qt.IsTrue,
				qt.Commentf("%s implements ast.Node and is not in the corpus", name))
		})
	}
}

// TestNodeKinds_SelfTest is the gate's own control: the parser has to accept an
// Accept method that takes a Visitor and reject one that does not.
//
// Without the negative half the predicate could match every method named Accept
// and the corpus would still look right, because core/ast happens to declare no
// other Accept.
func TestNodeKinds_SelfTest(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	writeSelfTestRepository(c, root)

	kinds, err := astrouteguard.NodeKinds(root)
	c.Assert(err, qt.IsNil)

	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		names = append(names, kind.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"RealNode"},
		qt.Commentf("the corpus reads %v", names))
}

// writeSelfTestRepository builds a throwaway git repository holding one file
// with three Accept methods: one that is a node, one whose parameter is not a
// Visitor, and one declared in a test file.
func writeSelfTestRepository(c *qt.C, root string) {
	c.Helper()

	c.Assert(os.MkdirAll(filepath.Join(root, "core", "ast"), 0o750), qt.IsNil)
	source := `package ast

type Visitor interface{ VisitNode(Node) error }
type Node interface{ Accept(visitor Visitor) error }

type RealNode struct{}

func (n *RealNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

type Listener struct{}

func (l *Listener) Accept(connection int) error { return nil }

type WrongResult struct{}

func (w *WrongResult) Accept(visitor Visitor) {}
`
	c.Assert(os.WriteFile(filepath.Join(root, "core", "ast", "nodes.go"), []byte(source), 0o600), qt.IsNil)

	testSource := `package ast

type TestOnlyNode struct{}

func (n *TestOnlyNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }
`
	c.Assert(os.WriteFile(filepath.Join(root, "core", "ast", "nodes_test.go"), []byte(testSource), 0o600), qt.IsNil)

	for _, arguments := range [][]string{
		{"init"},
		{"add", "-A"},
	} {
		command := exec.Command("git", arguments...)
		command.Dir = root
		c.Assert(command.Run(), qt.IsNil, qt.Commentf("git %v", arguments))
	}
}

func TestNodeKinds_FailurePath(t *testing.T) {
	t.Run("a directory git does not know", func(t *testing.T) {
		c := qt.New(t)
		kinds, err := astrouteguard.NodeKinds(filepath.Join(t.TempDir(), "absent"))
		c.Assert(err, qt.ErrorMatches, `astrouteguard: listing core/ast/\*\.go: .*`)
		c.Assert(kinds, qt.IsNil)
	})
}

// TestMarkedKinds_HappyPath holds each fragment corpus to the node corpus:
// every fragment is a node, so a kind found here and not there is a parse that
// matched something other than the marker.
func TestMarkedKinds_HappyPath(t *testing.T) {
	// A handful per marker, named as a control on the enumeration for the
	// reason the node corpus names its own: the complete lists stay derived.
	// Each marker's controls include a kind from another marker as an absence,
	// so a reader that merged the markers would fail.
	tests := []struct {
		marker  astrouteguard.Marker
		present []string
		absent  []string
	}{
		{
			marker:  astrouteguard.AlterOperationMarker,
			present: []string{"AddColumnOperation", "AddChangefeedOperation", "DropRowDeletionPolicyOperation"},
			absent:  []string{"EnumTypeDef", "AddEnumValueOperation", "AlterTableNode"},
		},
		{
			marker:  astrouteguard.TypeDefinitionMarker,
			present: []string{"EnumTypeDef", "DomainTypeDef", "CompositeTypeDef", "RangeTypeDef"},
			absent:  []string{"AddEnumValueOperation", "AddColumnOperation", "CreateTypeNode"},
		},
		{
			marker:  astrouteguard.TypeOperationMarker,
			present: []string{"AddEnumValueOperation", "DomainNotNullOperation", "CompositeAttributeOperation"},
			absent:  []string{"EnumTypeDef", "AddColumnOperation", "AlterTypeNode"},
		},
	}

	c := qt.New(t)
	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	nodes, err := astrouteguard.NodeKinds(root)
	c.Assert(err, qt.IsNil)
	isNode := make(map[string]bool, len(nodes))
	for _, kind := range nodes {
		isNode[kind.Name] = true
	}

	for _, test := range tests {
		t.Run(string(test.marker), func(t *testing.T) {
			c := qt.New(t)
			kinds, err := astrouteguard.MarkedKinds(root, test.marker)
			c.Assert(err, qt.IsNil)

			names := make([]string, 0, len(kinds))
			for _, kind := range kinds {
				names = append(names, kind.Name)
				c.Assert(isNode[kind.Name], qt.IsTrue, qt.Commentf("%s carries the marker and is not a node", kind.Name))
				c.Assert(path.Dir(kind.File), qt.Equals, "core/ast", qt.Commentf("%s is reported from %s", kind.Name, kind.File))
			}
			for _, name := range test.present {
				c.Assert(names, qt.Contains, name)
			}
			for _, name := range test.absent {
				c.Assert(names, qt.Not(qt.Contains), name)
			}
		})
	}
}

// TestMarkers_HappyPath pins the markers core/ast declares. A new one is a new
// kind of fragment, and the renderer has to learn the statement that carries
// it before this list may grow.
func TestMarkers_HappyPath(t *testing.T) {
	c := qt.New(t)

	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	markers, err := astrouteguard.Markers(root)
	c.Assert(err, qt.IsNil)

	c.Assert(markers, qt.DeepEquals, []astrouteguard.Marker{
		astrouteguard.AlterOperationMarker,
		astrouteguard.TypeDefinitionMarker,
		astrouteguard.TypeOperationMarker,
	})
}

// TestMarkers_SelfTest is the parser's control: a method with a parameter, a
// result, a body or an exported name is not a marker, and a test file's types
// are not the package's.
func TestMarkers_SelfTest(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(root, "core", "ast"), 0o750), qt.IsNil)
	source := `package ast

type RealOperation struct{}

func (op *RealOperation) alterOperation() {}

type ValueReceiver struct{}

func (op ValueReceiver) alterOperation() {}

type OtherFragment struct{}

func (f *OtherFragment) otherFragment() {}

type TakesAnArgument struct{}

func (op *TakesAnArgument) alterOperation(int) {}

type ReturnsAValue struct{}

func (op ReturnsAValue) alterOperation() error { return nil }

type HasABody struct{}

func (op *HasABody) alterOperation() { _ = op }

type Exported struct{}

func (e *Exported) Marker() {}

func freeFunction() {}
`
	c.Assert(os.WriteFile(filepath.Join(root, "core", "ast", "operations.go"), []byte(source), 0o600), qt.IsNil)
	testSource := `package ast

type TestOnlyOperation struct{}

func (op *TestOnlyOperation) alterOperation() {}

func (op *TestOnlyOperation) testOnlyMarker() {}
`
	c.Assert(os.WriteFile(filepath.Join(root, "core", "ast", "operations_test.go"), []byte(testSource), 0o600), qt.IsNil)
	for _, arguments := range [][]string{{"init"}, {"add", "-A"}} {
		command := exec.Command("git", arguments...)
		command.Dir = root
		c.Assert(command.Run(), qt.IsNil, qt.Commentf("git %v", arguments))
	}

	markers, err := astrouteguard.Markers(root)
	c.Assert(err, qt.IsNil)
	c.Assert(markers, qt.DeepEquals, []astrouteguard.Marker{"alterOperation", "otherFragment"})

	kinds, err := astrouteguard.MarkedKinds(root, astrouteguard.AlterOperationMarker)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		names = append(names, kind.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"RealOperation", "ValueReceiver"})
}

func TestMarkers_FailurePath(t *testing.T) {
	c := qt.New(t)
	markers, err := astrouteguard.Markers(filepath.Join(t.TempDir(), "absent"))
	c.Assert(err, qt.ErrorMatches, `astrouteguard: listing core/ast/\*\.go: .*`)
	c.Assert(markers, qt.IsNil)
}

func TestMarkedKinds_FailurePath(t *testing.T) {
	c := qt.New(t)
	kinds, err := astrouteguard.MarkedKinds(filepath.Join(t.TempDir(), "absent"), astrouteguard.AlterOperationMarker)
	c.Assert(err, qt.ErrorMatches, `astrouteguard: listing core/ast/\*\.go: .*`)
	c.Assert(kinds, qt.IsNil)
}
