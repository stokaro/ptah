package ydbstream

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// Operation selects the streaming-query statement represented by a Node.
type Operation uint8

const (
	// CreateOperation creates a query and its checkpoint state.
	CreateOperation Operation = iota + 1
	// AlterOperation changes settings or explicitly replaces the query body.
	AlterOperation
	// DropOperation removes the query and its checkpoints.
	DropOperation
)

// Node carries a streaming-query statement. It stays in this package because
// only YDB has this statement; the shared AST visitor accepts dialect nodes.
type Node struct {
	// Operation selects creation, alteration, or removal.
	Operation Operation
	// Name is the directory-qualified reference.
	Name string
	// Spec is the desired persistent declaration.
	Spec ast.StreamingQuerySpec
	// Previous is the held declaration for an alteration.
	Previous ast.StreamingQuerySpec
	// AllowStateReset permits a changed body to discard aggregation state.
	AllowStateReset bool
}

// Accept sends the node to the renderer's visitor.
func (n *Node) Accept(visitor ast.Visitor) error { return visitor.VisitNode(n) }

// Statement validates the node and renders one YQL statement.
func (n *Node) Statement(caps capability.Capabilities) (string, error) {
	if err := Refuse("ydb", caps, "streaming query "+n.Name); err != nil {
		return "", err
	}
	if n.Operation != DropOperation {
		if err := Validate(n.Spec); err != nil {
			return "", fmt.Errorf("streaming query %q: %w", n.Name, err)
		}
	}
	switch n.Operation {
	case CreateOperation:
		return Create(n.Name, n.Spec), nil
	case AlterOperation:
		return Alter(n.Name, n.Spec, n.Previous, AlterOptions{AllowStateReset: n.AllowStateReset})
	case DropOperation:
		return Drop(n.Name), nil
	default:
		return "", fmt.Errorf("unknown streaming query operation %d", n.Operation)
	}
}
