// Package ptaherr defines typed errors returned by Ptah's public Go APIs.
package ptaherr

import (
	"errors"

	"github.com/stokaro/ptah/core/ast"
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

	// ErrUnsupportedFeature marks dialect or capability feature mismatches.
	ErrUnsupportedFeature = errors.New("unsupported feature")
)

// ParseError reports a Go annotation or source parsing failure.
type ParseError struct {
	File      string
	Line      int
	Directive string
	Attribute string
	Err       error
	Message   string
}

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

func (e *ParseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// PlanError reports migration planning failures.
type PlanError struct {
	Dialect string
	Err     error
	Message string
}

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

func (e *PlanError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// RenderError reports SQL rendering failures.
type RenderError struct {
	Dialect string
	Node    ast.Node
	Err     error
	Message string
}

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

func (e *RenderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// CapabilityError reports a requested feature that is not available for the
// selected dialect or concrete server capability set.
type CapabilityError struct {
	Dialect string
	Feature string
	Err     error
	Message string
}

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

func (e *CapabilityError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
