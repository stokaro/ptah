package ptaherr_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
)

func TestParseError(t *testing.T) {
	c := qt.New(t)

	err := &ptaherr.ParseError{
		File:      "models.go",
		Line:      12,
		Directive: "ptah:schema:field",
		Attribute: "bogus",
		Err:       ptaherr.ErrUnknownAttribute,
		Message:   `unknown annotation attribute "bogus" on //ptah:schema:field at models.go:12`,
	}

	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.File, qt.Equals, "models.go")
	c.Assert(parseErr.Line, qt.Equals, 12)
	c.Assert(parseErr.Directive, qt.Equals, "ptah:schema:field")
	c.Assert(parseErr.Attribute, qt.Equals, "bogus")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
	c.Assert(err.Error(), qt.Equals, `unknown annotation attribute "bogus" on //ptah:schema:field at models.go:12`)
	c.Assert(err.Unwrap(), qt.Equals, ptaherr.ErrUnknownAttribute)
}

// A position opens the text once a reader has given the error a column: the
// file form where a file is named, the spelled-out form where none is. An error
// that knows a line and no column is written as it always was, since its message
// states its own place.
func TestParseError_Position(t *testing.T) {
	tests := []struct {
		name string
		err  *ptaherr.ParseError
		want string
	}{
		{
			name: "a file, a line and a column",
			err:  &ptaherr.ParseError{File: "schema.sql", Line: 7, Column: 12, Err: errors.New("expected a name")},
			want: "schema.sql:7:12: expected a name",
		},
		{
			name: "a line and a column",
			err:  &ptaherr.ParseError{Line: 7, Column: 12, Err: errors.New("expected a name")},
			want: "line 7, column 12: expected a name",
		},
		{
			name: "a message in place of the cause",
			err:  &ptaherr.ParseError{Line: 7, Column: 12, Message: "stated", Err: errors.New("cause")},
			want: "line 7, column 12: stated",
		},
		{
			name: "a file and a line, with no column",
			err:  &ptaherr.ParseError{File: "models.go", Line: 7, Err: errors.New("expected a name")},
			want: "expected a name",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.err.Error(), qt.Equals, test.want)
		})
	}
}

// At states an offset and nothing else: the text is the cause's until a reader
// gives it a line and column, and the cause stays reachable.
func TestAt(t *testing.T) {
	c := qt.New(t)

	err := ptaherr.At(180, ptaherr.ErrUnsupportedFeature)

	c.Assert(err, qt.ErrorMatches, ptaherr.ErrUnsupportedFeature.Error())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	at, ok := errors.AsType[*ptaherr.ParseError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(at.Offset, qt.Equals, 180)
	c.Assert(at.Line, qt.Equals, 0)
}

func TestAt_NilIsNil(t *testing.T) {
	c := qt.New(t)

	c.Assert(ptaherr.At(180, nil), qt.IsNil)
}

func TestParseErrorFallbackMessages(t *testing.T) {
	c := qt.New(t)

	err := &ptaherr.ParseError{Err: ptaherr.ErrMissingRequiredAttribute}
	c.Assert(err.Error(), qt.Equals, "missing required annotation attribute")

	err = &ptaherr.ParseError{}
	c.Assert(err.Error(), qt.Equals, "parse error")

	var nilErr *ptaherr.ParseError
	c.Assert(nilErr.Error(), qt.Equals, "<nil>")
	c.Assert(nilErr.Unwrap(), qt.IsNil)
}

func TestPlanError(t *testing.T) {
	c := qt.New(t)

	err := &ptaherr.PlanError{
		Dialect: "sqlserver",
		Err:     ptaherr.ErrUnsupportedDialect,
		Message: "unsupported database dialect: sqlserver",
	}

	var planErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &planErr)
	c.Assert(planErr.Dialect, qt.Equals, "sqlserver")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(err.Error(), qt.Equals, "unsupported database dialect: sqlserver")
	c.Assert(err.Unwrap(), qt.Equals, ptaherr.ErrUnsupportedDialect)
}

func TestPlanErrorFallbackMessages(t *testing.T) {
	c := qt.New(t)

	err := &ptaherr.PlanError{Err: ptaherr.ErrUnsupportedFeature}
	c.Assert(err.Error(), qt.Equals, "unsupported feature")

	err = &ptaherr.PlanError{}
	c.Assert(err.Error(), qt.Equals, "plan error")

	var nilErr *ptaherr.PlanError
	c.Assert(nilErr.Error(), qt.Equals, "<nil>")
	c.Assert(nilErr.Unwrap(), qt.IsNil)
}

func TestRenderError(t *testing.T) {
	c := qt.New(t)

	node := ast.NewComment("hello")
	err := &ptaherr.RenderError{
		Dialect: "sqlite",
		Node:    node,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: "render SQL for sqlite node *ast.CommentNode: unsupported feature",
	}

	var renderErr *ptaherr.RenderError
	c.Assert(err, qt.ErrorAs, &renderErr)
	c.Assert(renderErr.Dialect, qt.Equals, "sqlite")
	c.Assert(renderErr.Node, qt.Equals, node)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Equals, "render SQL for sqlite node *ast.CommentNode: unsupported feature")
	c.Assert(err.Unwrap(), qt.Equals, ptaherr.ErrUnsupportedFeature)
}

func TestRenderErrorFallbackMessages(t *testing.T) {
	c := qt.New(t)

	err := &ptaherr.RenderError{Err: ptaherr.ErrUnsupportedDialect}
	c.Assert(err.Error(), qt.Equals, "unsupported database dialect")

	err = &ptaherr.RenderError{}
	c.Assert(err.Error(), qt.Equals, "render error")

	var nilErr *ptaherr.RenderError
	c.Assert(nilErr.Error(), qt.Equals, "<nil>")
	c.Assert(nilErr.Unwrap(), qt.IsNil)
}

func TestCapabilityError(t *testing.T) {
	c := qt.New(t)

	err := &ptaherr.CapabilityError{
		Dialect: "sqlite",
		Feature: "materialized views",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: "sqlite does not support materialized views",
	}

	var capabilityErr *ptaherr.CapabilityError
	c.Assert(err, qt.ErrorAs, &capabilityErr)
	c.Assert(capabilityErr.Dialect, qt.Equals, "sqlite")
	c.Assert(capabilityErr.Feature, qt.Equals, "materialized views")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Equals, "sqlite does not support materialized views")
	c.Assert(err.Unwrap(), qt.Equals, ptaherr.ErrUnsupportedFeature)
}

func TestCapabilityErrorFallbackMessages(t *testing.T) {
	c := qt.New(t)

	err := &ptaherr.CapabilityError{Err: ptaherr.ErrUnsupportedFeature}
	c.Assert(err.Error(), qt.Equals, "unsupported feature")

	err = &ptaherr.CapabilityError{}
	c.Assert(err.Error(), qt.Equals, "capability error")

	var nilErr *ptaherr.CapabilityError
	c.Assert(nilErr.Error(), qt.Equals, "<nil>")
	c.Assert(nilErr.Unwrap(), qt.IsNil)
}

func TestSentinelErrorsMatchThemselves(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "outcome unknown", err: ptaherr.ErrOutcomeUnknown},
		{name: "unsupported dialect", err: ptaherr.ErrUnsupportedDialect},
		{name: "unknown attribute", err: ptaherr.ErrUnknownAttribute},
		{name: "missing required attribute", err: ptaherr.ErrMissingRequiredAttribute},
		{name: "invalid attribute value", err: ptaherr.ErrInvalidAttributeValue},
		{name: "unsupported feature", err: ptaherr.ErrUnsupportedFeature},
		{name: "invalid schema diff", err: ptaherr.ErrInvalidSchemaDiff},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.err, qt.ErrorIs, tt.err)
		})
	}
}

func TestSentinelErrorsAreDistinct(t *testing.T) {
	tests := []struct {
		name  string
		left  error
		right error
	}{
		{name: "unsupported dialect vs unknown attribute", left: ptaherr.ErrUnsupportedDialect, right: ptaherr.ErrUnknownAttribute},
		{name: "unsupported dialect vs missing required attribute", left: ptaherr.ErrUnsupportedDialect, right: ptaherr.ErrMissingRequiredAttribute},
		{name: "unsupported dialect vs invalid attribute value", left: ptaherr.ErrUnsupportedDialect, right: ptaherr.ErrInvalidAttributeValue},
		{name: "unsupported dialect vs unsupported feature", left: ptaherr.ErrUnsupportedDialect, right: ptaherr.ErrUnsupportedFeature},
		{name: "unsupported dialect vs invalid schema diff", left: ptaherr.ErrUnsupportedDialect, right: ptaherr.ErrInvalidSchemaDiff},
		{name: "unknown attribute vs missing required attribute", left: ptaherr.ErrUnknownAttribute, right: ptaherr.ErrMissingRequiredAttribute},
		{name: "unknown attribute vs invalid attribute value", left: ptaherr.ErrUnknownAttribute, right: ptaherr.ErrInvalidAttributeValue},
		{name: "unknown attribute vs unsupported feature", left: ptaherr.ErrUnknownAttribute, right: ptaherr.ErrUnsupportedFeature},
		{name: "unknown attribute vs invalid schema diff", left: ptaherr.ErrUnknownAttribute, right: ptaherr.ErrInvalidSchemaDiff},
		{name: "missing required attribute vs invalid attribute value", left: ptaherr.ErrMissingRequiredAttribute, right: ptaherr.ErrInvalidAttributeValue},
		{name: "missing required attribute vs unsupported feature", left: ptaherr.ErrMissingRequiredAttribute, right: ptaherr.ErrUnsupportedFeature},
		{name: "missing required attribute vs invalid schema diff", left: ptaherr.ErrMissingRequiredAttribute, right: ptaherr.ErrInvalidSchemaDiff},
		{name: "invalid attribute value vs unsupported feature", left: ptaherr.ErrInvalidAttributeValue, right: ptaherr.ErrUnsupportedFeature},
		{name: "invalid attribute value vs invalid schema diff", left: ptaherr.ErrInvalidAttributeValue, right: ptaherr.ErrInvalidSchemaDiff},
		{name: "unsupported feature vs invalid schema diff", left: ptaherr.ErrUnsupportedFeature, right: ptaherr.ErrInvalidSchemaDiff},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.left, qt.Not(qt.ErrorIs), tt.right)
			c.Assert(tt.right, qt.Not(qt.ErrorIs), tt.left)
		})
	}
}
