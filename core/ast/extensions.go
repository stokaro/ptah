package ast

import (
	"fmt"
	"reflect"

	"ptah.run/core/schemaext"
)

// ExtensionPayload is a typed, owner-defined operation. CloneExtension returns
// an independent snapshot of the same concrete type and kind. Payloads contain
// data and local metadata; they must not render SQL or access a database.
type ExtensionPayload interface {
	Kind() schemaext.Kind
	CloneExtension() ExtensionPayload
}

// ExtensionRole identifies the grammatical position occupied by a payload.
type ExtensionRole string

const (
	// StatementExtension is an independently executable statement.
	StatementExtension ExtensionRole = "statement"
	// AlterExtension is an operation whose owner is an ALTER TABLE statement.
	AlterExtension ExtensionRole = "alter-table"
)

// ExtensionStatement carries an owner-defined standalone statement. Rendering
// requires an explicitly registered handler for its kind and grammatical role.
type ExtensionStatement struct {
	Payload ExtensionPayload
}

// Accept hands the extension boundary to the visitor.
func (n *ExtensionStatement) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// ExtensionAlterOperation carries an owner-defined table operation while
// retaining the sealed AlterOperation contract. A standalone render validates
// target support and then reports that the real ALTER TABLE parent is required.
type ExtensionAlterOperation struct {
	Payload ExtensionPayload
}

// Accept hands the extension boundary to the visitor.
func (n *ExtensionAlterOperation) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

func (n *ExtensionAlterOperation) alterOperation() {}

// CloneExtensionPayload validates the interface boundary and returns an
// independent payload. Nil, typed nil, invalid kinds, and clones changing kind
// or concrete type are errors. Provider implementations own deep-copy behavior.
func CloneExtensionPayload(payload ExtensionPayload) (ExtensionPayload, error) {
	if absentPayload(payload) {
		return nil, fmt.Errorf("extension payload is nil")
	}
	kind := payload.Kind()
	if !kind.Valid() {
		return nil, fmt.Errorf("invalid extension kind %q", kind)
	}
	cloned := payload.CloneExtension()
	if absentPayload(cloned) || reflect.TypeOf(cloned) != reflect.TypeOf(payload) || cloned.Kind() != kind {
		return nil, fmt.Errorf("extension %q returned an invalid clone", kind)
	}
	return cloned, nil
}

func absentPayload(payload ExtensionPayload) bool {
	if payload == nil {
		return true
	}
	value := reflect.ValueOf(payload)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// ExtensionPlacement describes how a node carries owner-defined operations.
// Safety reports attribute an owner's verdict to the statements a node
// renders, which is sound only when the node holds that one operation.
type ExtensionPlacement int

const (
	// NoExtension is a node that carries no owner operation.
	NoExtension ExtensionPlacement = iota
	// IsolatedExtension is a node that is exactly one owner operation: an
	// ExtensionStatement, an ExtensionAlterOperation, an ALTER TABLE whose only
	// operation is one, or a statement list holding only such a node.
	IsolatedExtension
	// MixedExtension is a node that carries an owner operation beside other
	// operations or statements, or more than one owner operation.
	MixedExtension
)

// PlacementOf classifies node. A typed nil extension envelope counts as an
// isolated owner operation, so its unknown effects are not lost.
func PlacementOf(node Node) ExtensionPlacement {
	owned, other := ownerParts(node)
	switch {
	case len(owned) == 0:
		return NoExtension
	case len(owned) == 1 && other == 0:
		return IsolatedExtension
	default:
		return MixedExtension
	}
}

// OwnerOperations returns the owner payloads node carries, in order: the
// payload of an extension envelope, of each extension operation of an ALTER
// TABLE, and of each such node in a statement list. A typed nil envelope
// contributes a nil payload. It reads the same node shapes [PlacementOf]
// reads, so the two cannot disagree about what a node carries.
func OwnerOperations(node Node) []ExtensionPayload {
	owned, _ := ownerParts(node)
	return owned
}

// ownerParts walks the node shapes that can carry owner operations and
// returns their payloads and the number of other operations and statements
// beside them.
func ownerParts(node Node) (owned []ExtensionPayload, other int) {
	switch typed := node.(type) {
	case *ExtensionStatement:
		if typed == nil {
			return []ExtensionPayload{nil}, 0
		}
		return []ExtensionPayload{typed.Payload}, 0
	case *ExtensionAlterOperation:
		if typed == nil {
			return []ExtensionPayload{nil}, 0
		}
		return []ExtensionPayload{typed.Payload}, 0
	case *AlterTableNode:
		if typed == nil {
			return nil, 0
		}
		for _, operation := range typed.Operations {
			extension, ok := operation.(*ExtensionAlterOperation)
			if !ok {
				other++
				continue
			}
			if extension == nil {
				owned = append(owned, nil)
				continue
			}
			owned = append(owned, extension.Payload)
		}
		return owned, other
	case *StatementList:
		if typed == nil {
			return nil, 0
		}
		for _, child := range typed.Statements {
			childOwned, childOther := ownerParts(child)
			if len(childOwned) == 0 {
				other++
				continue
			}
			owned = append(owned, childOwned...)
			other += childOther
		}
		return owned, other
	default:
		return nil, 0
	}
}
