// Package nodedispatch holds the answers every dialect renderer's VisitNode
// dispatcher has to give identically.
//
// What the eight renderers emit differs by design. Whether a caller handed them
// a node at all does not, and a predicate written eight times agrees on the day
// the eighth is written and stops agreeing the first time one is extended. Nor
// does the refusal of a node only YDB renders, which every other target
// answers by the same capability key.
//
// Other error texts stay with each renderer: they carry that dialect's own
// sentinel and its own RenderError shape, which a shared constructor would have
// to flatten.
package nodedispatch

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
)

// IsAbsent reports whether a caller handed the renderer no node at all.
//
// It answers only for a nil interface. A non-nil interface holding a nil
// pointer is left to the type switch, which matches that kind's case and hands
// the handler the nil pointer -- which is what a handler's own nil check
// answers, in words that name the kind. A dispatcher catching the typed nil
// first replaces every one of those messages with one naming no kind: SQL
// Server answers `upsert node is nil` for a nil upsert, and a typed-nil guard
// ahead of the switch turns that into `AST node is nil`.
//
// A handler with no nil check dereferences the pointer, exactly as it does when
// the same typed nil arrives through Accept. This predicate does not change that
// either way; it decides only which layer answers.
func IsAbsent(node ast.Node) bool { return node == nil }

// RefuseSerialSequence refuses node, a change to the sequence behind a Serial
// column, on dialect, a target without [capability.SerialSequenceOptions].
//
// The answer is the same on every such target, because none of them has a
// sequence a Serial column owns that Ptah addresses: written as anything
// else, the change would apply to some other object or to none, and the plan
// would report the column converged while its sequence is not.
func RefuseSerialSequence(dialect string, node *ast.AlterSerialSequenceNode) error {
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: string(capability.SerialSequenceOptions),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("changing the sequence of Serial column %q of table %q, which requires target "+
			"capability %s, unavailable on this %s target", node.Column, node.Table, capability.SerialSequenceOptions, dialect),
	}
}

// RefuseTopic refuses a topic node on a renderer whose engine has no topic
// Ptah models: every renderer but YDB's. The central renderer refuses a topic
// on a target without [capability.Topics] before a dialect sees the node, so
// this answers a caller that visits the node with a dialect renderer directly,
// whatever capability set it claims, and names the renderer that refused.
//
// It is one constructor rather than one per renderer because the refusal names
// the capability rather than a dialect's own construct, and eight copies of it
// would agree only until the first one changed.
func RefuseTopic(dialect string, node ast.Node) error {
	subject := "a topic"
	switch typed := node.(type) {
	case *ast.CreateTopicNode:
		subject = "topic " + typed.Name
	case *ast.AlterTopicNode:
		subject = "ALTER TOPIC " + typed.Name
	case *ast.DropTopicNode:
		subject = "DROP TOPIC " + typed.Name
	}
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: string(capability.Topics),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: the %s renderer writes no topic; a topic needs target capability %s, which only "+
			"YDB has", subject, dialect, capability.Topics),
	}
}

// RefuseResourcePool refuses a resource pool or classifier node on a renderer
// whose engine has no resource pool Ptah models: every renderer but YDB's. The
// central renderer refuses one on a target without [capability.ResourcePools]
// before a dialect sees the node, so this answers a caller that visits the
// node with a dialect renderer directly, whatever capability set it claims,
// and names the renderer that refused.
func RefuseResourcePool(dialect string, node ast.Node) error {
	subject := "a resource pool"
	switch typed := node.(type) {
	case *ast.CreateResourcePoolNode:
		subject = "resource pool " + typed.Name
	case *ast.AlterResourcePoolNode:
		subject = "ALTER RESOURCE POOL " + typed.Name
	case *ast.DropResourcePoolNode:
		subject = "DROP RESOURCE POOL " + typed.Name
	case *ast.CreateResourcePoolClassifierNode:
		subject = "resource pool classifier " + typed.Name
	case *ast.AlterResourcePoolClassifierNode:
		subject = "ALTER RESOURCE POOL CLASSIFIER " + typed.Name
	case *ast.DropResourcePoolClassifierNode:
		subject = "DROP RESOURCE POOL CLASSIFIER " + typed.Name
	}
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: string(capability.ResourcePools),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: the %s renderer writes no resource pool; a resource pool needs target "+
			"capability %s, which only YDB has", subject, dialect, capability.ResourcePools),
	}
}
