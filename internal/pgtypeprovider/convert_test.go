package pgtypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/feature/pgtype/typeconvert"
)

// TestConvertTypes pins each projection: a declaration predicts the
// observation that holds what it writes, and an observation keeps itself.
func TestConvertTypes(t *testing.T) {
	attributes := []pgtype.Attribute{{Name: "street", Type: "text"}}
	tests := []struct {
		name     string
		from, to schemaext.Representation
		value    schemaext.Value
		want     schemaext.Value
	}{
		{name: "a domain declaration", from: schemaext.Desired, to: schemaext.Observed,
			value: &pgtype.DesiredDomain{BaseType: "text", NotNull: true, Default: "x", DefaultExpr: "lower('X')", Check: "VALUE <> ''", Comment: "mail",
				StructName: "Mail"},
			want: &pgtype.ObservedDomain{BaseType: "text", NotNull: true, Default: "lower('X')", Check: "VALUE <> ''", Comment: "mail"}},
		{name: "a domain observation", from: schemaext.Observed, to: schemaext.Desired,
			value: &pgtype.ObservedDomain{BaseType: "text", Default: "'x'::text", Check: "(VALUE <> ''::text)",
				Checks: []pgtype.DomainCheck{{Name: "t_check", Expression: "(VALUE <> ''::text)"}}},
			want: &pgtype.DesiredDomain{BaseType: "text", DefaultExpr: "'x'::text", Check: "(VALUE <> ''::text)"}},
		{name: "a composite type declaration", from: schemaext.Desired, to: schemaext.Observed,
			value: &pgtype.DesiredComposite{Attributes: attributes, Comment: "address", StructName: "Address"},
			want:  &pgtype.ObservedComposite{Attributes: attributes, Comment: "address"}},
		{name: "a composite type observation", from: schemaext.Observed, to: schemaext.Desired,
			value: &pgtype.ObservedComposite{Attributes: attributes},
			want:  &pgtype.DesiredComposite{Attributes: attributes}},
		{name: "a range type declaration", from: schemaext.Desired, to: schemaext.Observed,
			value: &pgtype.DesiredRange{Subtype: "numeric", Collation: "C", Cleared: []string{pgtype.RangeCanonical}, Comment: "span"},
			want:  &pgtype.ObservedRange{Subtype: "numeric", Collation: "C", Comment: "span"}},
		{name: "a range type observation", from: schemaext.Observed, to: schemaext.Desired,
			value: &pgtype.ObservedRange{Subtype: "numeric", SubtypeOpClass: "numeric_ops", SubtypeDiff: "span_diff"},
			want:  &pgtype.DesiredRange{Subtype: "numeric", SubtypeOpClass: "numeric_ops", SubtypeDiff: "span_diff"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := newRuntime(c).ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: test.from, To: test.to,
				Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(converted, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}

// TestConvertTypes_FailurePath refuses a conversion no projection answers.
// The runtime refuses most of these itself, so each is asked of the service
// directly.
func TestConvertTypes_FailurePath(t *testing.T) {
	declare := func(values ...schemaext.Value) schemaext.ConversionRequest {
		return schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: values}
	}
	observe := func(values ...schemaext.Value) schemaext.ConversionRequest {
		return schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Desired, Values: values}
	}
	outside := declare()
	outside.Target = "mysql"
	tests := []struct {
		name    string
		request schemaext.ConversionRequest
		is      error
	}{
		{name: "a target outside the family", request: outside, is: ptaherr.ErrUnsupportedDialect},
		{name: "one representation to itself", is: schemaext.ErrInvalidValue,
			request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Desired}},
		{name: "no representation", is: schemaext.ErrInvalidValue,
			request: schemaext.ConversionRequest{Target: "postgres", To: schemaext.Desired}},
		{name: "an observation named a declaration", is: schemaext.ErrInvalidValue,
			request: declare(&pgtype.ObservedDomain{BaseType: "text"})},
		{name: "a declaration named an observation", is: schemaext.ErrInvalidValue,
			request: observe(&pgtype.DesiredRange{Subtype: "int4"})},
		{name: "a domain declaration named an observation", is: schemaext.ErrInvalidValue,
			request: observe(&pgtype.DesiredDomain{BaseType: "text"})},
		{name: "a composite type observation named a declaration", is: schemaext.ErrInvalidValue,
			request: declare(&pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}})},
		{name: "an invalid domain declaration", is: schemaext.ErrInvalidValue, request: declare(&pgtype.DesiredDomain{})},
		{name: "an invalid composite type declaration", is: schemaext.ErrInvalidValue, request: declare(&pgtype.DesiredComposite{})},
		{name: "an invalid range type declaration", is: schemaext.ErrInvalidValue, request: declare(&pgtype.DesiredRange{})},
		{name: "an invalid domain observation", is: schemaext.ErrInvalidValue, request: observe(&pgtype.ObservedDomain{})},
		{name: "an invalid composite type observation", is: schemaext.ErrInvalidValue, request: observe(&pgtype.ObservedComposite{})},
		{name: "an invalid range type observation", is: schemaext.ErrInvalidValue, request: observe(&pgtype.ObservedRange{})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := typeconvert.Service{}.ConvertFeatures(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(converted, qt.IsNil)
		})
	}
}
