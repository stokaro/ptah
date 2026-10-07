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
