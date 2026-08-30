// Package ptaherr defines the typed errors Ptah's public Go APIs return, in
// two layers a caller can branch on. The sentinel errors answer errors.Is:
// [ErrUnsupportedDialect], [ErrUnsupportedFeature], [ErrInvalidSchemaDiff],
// and the annotation family [ErrUnknownAttribute],
// [ErrMissingRequiredAttribute], [ErrInvalidAttributeValue] and
// [ErrRetiredAttribute]. The structured types answer errors.As and carry the
// failure's context in their fields: [ParseError], [PlanError], [RenderError]
// and [CapabilityError].
//
// A structured error wraps its cause through Unwrap, so both idioms work on
// one returned error: an annotation refusal from core/goschema matches
// errors.As against *ParseError and errors.Is against the annotation sentinel
// that names the kind of refusal, a diff the planner refuses matches both
// *PlanError and [ErrInvalidSchemaDiff], and a dialect nothing renders or
// plans for matches [ErrUnsupportedDialect] through the structured type that
// reported it.
package ptaherr

import (
	"errors"

	"go.5x5.cz/ptah/core/ast"
)

var (
	// ErrUnsupportedDialect marks errors caused by an unknown or unsupported
	// database dialect.
	ErrUnsupportedDialect = errors.New("unsupported database dialect")

	// ErrUnknownAttribute marks Go annotation directives containing an
	// attribute Ptah does not recognize.
	ErrUnknownAttribute = errors.New("unknown annotation attribute")

	// ErrMissingRequiredAttribute marks Go annotation directives missing a
	// required attribute.
	ErrMissingRequiredAttribute = errors.New("missing required annotation attribute")

	// ErrInvalidAttributeValue marks Go annotation directives containing a
	// recognized attribute with an invalid value.
	ErrInvalidAttributeValue = errors.New("invalid annotation attribute value")

	// ErrRetiredAttribute marks a declaration attribute Ptah still recognizes
	// and no longer accepts.
	//
	// It is deliberately not ErrUnknownAttribute: a caller branching on that
	// one treats the attribute as a typo, and a retired attribute is the
	// opposite -- it was spelled correctly and meant something once. The
	// refusal carries the reason it stopped meaning anything.
	ErrRetiredAttribute = errors.New("retired declaration attribute")

	// ErrUnsupportedFeature marks dialect or capability feature mismatches.
	ErrUnsupportedFeature = errors.New("unsupported feature")

	// ErrInvalidSchemaDiff marks malformed or internally conflicting schema
	// changes that cannot be planned safely.
	ErrInvalidSchemaDiff = errors.New("invalid schema diff")
)

// ParseError reports a Go annotation or source parsing failure. The
// core/goschema parser returns it, and errors.As(err, &parseErr) retrieves
// it. Err usually wraps one of the annotation sentinels --
// [ErrUnknownAttribute], [ErrMissingRequiredAttribute],
// [ErrInvalidAttributeValue] or [ErrRetiredAttribute] -- so errors.Is on the
// same error tells the refusals apart. Source that does not parse as Go at
// all is reported the same way, with Err holding the syntax error rather than
// an annotation sentinel, so a caller that branches on the sentinels needs a
// default branch as well.
type ParseError struct {
	// File names the source file being parsed. For in-memory parsing it is
	// the name the caller supplied for diagnostics.
	File string
	// Line is the 1-based line of the annotation comment at fault, and zero
	// when the failure is not tied to a single line. Check it before
	// reporting a position.
	Line int
	// Directive names the //ptah: directive at fault without its leading
	// slashes, such as "ptah:schema:field". Empty when the failure is not
	// about one directive.
	Directive string
	// Attribute names the directive attribute at fault. Empty when the
	// failure is not about one attribute.
	Attribute string
	// Err is the wrapped cause, reachable through Unwrap.
	Err error
	// Message, when set, replaces Err's text in Error.
	Message string
}

// Error returns Message when set and Err's text otherwise. An error carrying
// neither still formats as a non-empty description of its kind, and a nil
// receiver formats rather than panicking.
func (e *ParseError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "parse error"
}

// Unwrap returns Err, so errors.Is and errors.As reach the wrapped cause. A
// nil receiver returns nil.
func (e *ParseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// PlanError reports a migration planning failure. The migration/planner
// package returns it, and errors.As(err, &planErr) retrieves it. Err
// conventionally wraps [ErrInvalidSchemaDiff] for a schema diff the planner
// refuses to plan and [ErrUnsupportedDialect] for a dialect Ptah cannot plan
// for, so errors.Is on the same error selects the branch.
type PlanError struct {
	// Dialect names the dialect the plan targeted. It may be empty when the
	// failure is not tied to one dialect.
	Dialect string
	// Err is the wrapped cause, reachable through Unwrap.
	Err error
	// Message, when set, replaces Err's text in Error.
	Message string
}

// Error returns Message when set and Err's text otherwise. An error carrying
// neither still formats as a non-empty description of its kind, and a nil
// receiver formats rather than panicking.
func (e *PlanError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "plan error"
}

// Unwrap returns Err, so errors.Is and errors.As reach the wrapped cause. A
// nil receiver returns nil.
func (e *PlanError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// RenderError reports a SQL rendering failure. The core/renderer package
// returns it, and errors.As(err, &renderErr) retrieves it. Err wraps
// [ErrUnsupportedDialect] for a dialect Ptah cannot render,
// [ErrInvalidSchemaDiff] for a schema or node that is malformed, and
// otherwise whatever the render target reported. Branch with errors.Is on the
// sentinels rather than on this concrete type alone: a construct the target
// refuses arrives as a [CapabilityError] instead.
type RenderError struct {
	// Dialect names the render target the failure occurred on.
	Dialect string
	// Node is the AST node whose rendering failed, when the reporting site
	// held one; it is nil for failures not tied to a single node.
	Node ast.Node
	// Err is the wrapped cause, reachable through Unwrap.
	Err error
	// Message, when set, replaces Err's text in Error.
	Message string
}

// Error returns Message when set and Err's text otherwise. An error carrying
// neither still formats as a non-empty description of its kind, and a nil
// receiver formats rather than panicking.
func (e *RenderError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "render error"
}

// Unwrap returns Err, so errors.Is and errors.As reach the wrapped cause. A
// nil receiver returns nil.
func (e *RenderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// CapabilityError reports a requested feature that is not available for the
// selected dialect or concrete server capability set. Retrieve it with
// errors.As(err, &capErr); Err conventionally wraps [ErrUnsupportedFeature],
// so errors.Is(err, ErrUnsupportedFeature) matches the refusal.
type CapabilityError struct {
	// Dialect names the dialect or server the feature was refused for.
	Dialect string
	// Feature names the refused feature or capability. It is a diagnostic
	// label meant to be reported, not a key to branch on -- branch on the
	// sentinel Err wraps.
	Feature string
	// Err is the wrapped cause, reachable through Unwrap.
	Err error
	// Message, when set, replaces Err's text in Error.
	Message string
}

// Error returns Message when set and Err's text otherwise. An error carrying
// neither still formats as a non-empty description of its kind, and a nil
// receiver formats rather than panicking.
func (e *CapabilityError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "capability error"
}

// Unwrap returns Err, so errors.Is and errors.As reach the wrapped cause. A
// nil receiver returns nil.
func (e *CapabilityError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
