package schemaext

import (
	"context"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// RelationSubject identifies one model value without confusing a named object
// with a facet of a common object. Kind identifies the model in either case.
type RelationSubject struct {
	Kind      Kind
	Placement Placement
	Subject   objectidentity.ID
}

// RelationValue captures a concrete definition for dependency discovery. Missing
// or unreadable definitions belong in Coverage, never in a nil Value. A service
// discovers references from this captured value without reading another source.
type RelationValue struct {
	Subject RelationSubject
	Value   Value
}

// MarshalJSON requires an explicit model codec at a serialization boundary.
func (RelationValue) MarshalJSON() ([]byte, error) { return nil, ErrExplicitCodec }

// UnmarshalJSON refuses reconstruction without a selected model codec.
func (*RelationValue) UnmarshalJSON([]byte) error { return ErrExplicitCodec }

// RelationRequest supplies one representation of a captured schema. Kinds
// states the models whose references need assessment, including empty or
// unenrolled namespaces. Runtime dispatch derives it from selected ownership;
// it does not add claims to Coverage. Values retain their original input order.
// Identifier and capability facts belong to the same target as the capture.
type RelationRequest struct {
	Target         string
	Representation Representation
	Identifiers    identifier.Semantics
	Capabilities   capability.Capabilities
	Kinds          []Kind
	Values         []RelationValue
	Coverage       Coverage
}

// ValueRelations records how a value relates to common objects beyond its
// exclusive owner. A value relates to a table in one of three ways, and each
// says what the server does when the table is dropped, what a scope does with
// the value, and who plans the value's changes:
//
//   - A child names its table in Subject.Parent, and a facet is the common
//     owner's own. The model holds a child only with its table, a scope keeps
//     it with its table, and the table's transition claims its changes unless
//     its owner declares them. What the server does with a child on DROP
//     TABLE is the owner's: ClickHouse 25.8 keeps a row policy.
//   - A Dependency is an object the value references. The server refuses to
//     drop it while the value references it, as SQL Server refuses a table a
//     security policy binds (Msg 3729), a scope that keeps some of the
//     value's tables and not the others is refused, and the value's owner
//     plans its changes after its dependencies.
//   - An Attachment is the table the value hangs off without being its child.
//     The server drops the value with the table, as SQL Server drops an
//     extended property, a scope keeps or leaves the value with the table,
//     and the value's owner plans its changes alone.
//
// Dependencies and Attachment must not repeat Subject.Parent or a facet's
// owner, and a child has no Attachment. Dependencies may name several tables
// or functions without giving the value several parents; Attachment names one
// table, and is the zero value for a value attached to none. Complete confirms
// that the record is exhaustive for this value; false requires a Reason and is
// never an empty-set claim. This says nothing about uncaptured members of the
// model namespace.
type ValueRelations struct {
	Subject      RelationSubject
	Attachment   objectidentity.ID
	Dependencies []objectidentity.ID
	Complete     bool
	Reason       string
}

// RelationResult accounts for a complete owner batch, with one relation record
// per value in input order. Complete confirms that the service finished, even
// when an individual record describes unresolved references. A zero result is
// not a successful no-op. Errors and cancellation discard the entire result.
type RelationResult struct {
	Complete bool
	Values   []ValueRelations
}

// RelationService discovers references as a pure contextual batch. It performs
// no database or filesystem access and does not mutate its inputs. A process
// adapter sends one batch, retaining structured identities and explicit model
// records; it never serializes Go AST nodes or performs per-value lookups.
type RelationService interface {
	DescribeRelations(context.Context, RelationRequest) (RelationResult, error)
}

// RelationRuntime captures selected owner results with the source knowledge
// needed to distinguish a complete dependency closure from a partial listing.
type RelationRuntime interface {
	ModelRuntime
	CaptureRelations(context.Context, RelationRequest) (RelationSnapshot, error)
}
