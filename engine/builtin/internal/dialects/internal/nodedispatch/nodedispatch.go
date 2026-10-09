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
	case *ast.AddTopicConsumerNode:
		subject = "ALTER TOPIC " + typed.Name + " ADD CONSUMER " + typed.Consumer.Name
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

// RefuseReplication refuses an async replication or transfer node on a
// renderer whose engine has neither: every renderer but YDB's. The central
// renderer refuses one on a target without the capability before a dialect
// sees the node, so this answers a caller that visits the node with a dialect
// renderer directly, whatever capability set it claims, and names the
// renderer that refused.
//
// It is one constructor rather than one per renderer for the reason
// [RefuseSerialSequence] is: the refusal names the capability rather than a
// dialect's own construct.
func RefuseReplication(dialect string, node ast.Node) error {
	key, subject := ReplicationSubject(node)
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: string(key),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: the %s renderer writes no async replication or transfer; it needs target "+
			"capability %s, which only YDB has", subject, dialect, key),
	}
}

// ReplicationSubject names an async replication or transfer node as a
// refusal does, with the capability it needs.
func ReplicationSubject(node ast.Node) (capability.Capability, string) {
	switch typed := node.(type) {
	case *ast.CreateAsyncReplicationNode:
		return capability.AsyncReplication, "async replication " + typed.Name
	case *ast.AlterAsyncReplicationNode:
		return capability.AsyncReplication, "ALTER ASYNC REPLICATION " + typed.Name
	case *ast.DropAsyncReplicationNode:
		return capability.AsyncReplication, "DROP ASYNC REPLICATION " + typed.Name
	case *ast.CreateTransferNode:
		return capability.Transfers, "transfer " + typed.Name
	case *ast.AlterTransferNode:
		return capability.Transfers, "ALTER TRANSFER " + typed.Name
	case *ast.DropTransferNode:
		return capability.Transfers, "DROP TRANSFER " + typed.Name
	default:
		return capability.AsyncReplication, "an async replication"
	}
}

// RefuseSecret refuses node, a YDB secret statement, on a renderer whose
// engine has no secret Ptah models: every renderer but YDB's. The central
// renderer refuses a secret on a target without [capability.Secrets] before a
// dialect sees the node, so this answers a caller that visits the node with a
// dialect renderer directly, and names the renderer that refused. The
// refusal names the secret and never a value, which the node does not hold.
func RefuseSecret(dialect string, node ast.Node) error {
	subject := "a secret"
	switch typed := node.(type) {
	case *ast.CreateSecretNode:
		subject = "secret " + typed.Name
	case *ast.AlterSecretNode:
		subject = "ALTER SECRET " + typed.Name
	case *ast.DropSecretNode:
		subject = "DROP SECRET " + typed.Name
	}
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: string(capability.Secrets),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: the %s renderer writes no secret; a secret needs target capability %s, which only "+
			"YDB has", subject, dialect, capability.Secrets),
	}
}

// RefuseExternal refuses node, a YDB external data source or external table
// statement, on a renderer whose engine has neither object as Ptah models it:
// every renderer but YDB's. As with [RefuseSecret], the central renderer
// refuses the object on a target without [capability.ExternalDataSources]
// first, and this answers a caller that visits the node with a dialect
// renderer directly.
func RefuseExternal(dialect string, node ast.Node) error {
	subject := "an external object"
	switch typed := node.(type) {
	case *ast.CreateExternalDataSourceNode:
		subject = "external data source " + typed.Name
	case *ast.DropExternalDataSourceNode:
		subject = "DROP EXTERNAL DATA SOURCE " + typed.Name
	case *ast.CreateExternalTableNode:
		subject = "external table " + typed.Name
	case *ast.DropExternalTableNode:
		subject = "DROP EXTERNAL TABLE " + typed.Name
	}
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: string(capability.ExternalDataSources),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: the %s renderer writes no external data source or external table; each needs "+
			"target capability %s, which only YDB has", subject, dialect, capability.ExternalDataSources),
	}
}
