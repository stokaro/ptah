package pgtype_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// codec returns the codec of the model of kind in representation.
func codec(c *qt.C, kind schemaext.Kind, representation schemaext.Representation) schemaext.Codec {
	c.Helper()
	for _, candidate := range pgtype.Codecs() {
		if candidate.Prototype.Kind() == kind && candidate.Representation == representation {
			return candidate
		}
	}
	c.Fatalf("no %s codec of kind %s", representation, kind)
	return schemaext.Codec{}
}

func TestCodecs_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
		wire           string
	}{
		{name: "a bare domain", representation: schemaext.Desired,
			value: &pgtype.DesiredDomain{BaseType: "text"}, wire: `{"base_type":"text"}`},
		{name: "a domain with every clause", representation: schemaext.Desired,
			value: &pgtype.DesiredDomain{BaseType: "integer", NotNull: true, Default: "7", DefaultExpr: "f()", Check: "VALUE = 7",
				Comment: "quantity", StructName: "Qty", Normalized: &pgtype.NormalizedDomain{Check: "(VALUE = 7)", Default: "7"}},
			wire: `{"base_type":"integer","not_null":true,"default":"7","default_expr":"f()","check":"VALUE = 7","comment":"quantity",` +
				`"struct_name":"Qty","normalized":{"check":"(VALUE = 7)","default":"7"}}`},
		{name: "a server that spelled nothing", representation: schemaext.Desired,
			value: &pgtype.DesiredDomain{BaseType: "text", Normalized: &pgtype.NormalizedDomain{}},
			wire:  `{"base_type":"text","normalized":{}}`},
		{name: "an observed domain", representation: schemaext.Observed,
			value: &pgtype.ObservedDomain{BaseType: "integer", NotNull: true, Default: "7", Check: "(VALUE = 7)",
				Checks: []pgtype.DomainCheck{{Name: "qty_check", Expression: "(VALUE = 7)"}}, Comment: "quantity"},
			wire: `{"base_type":"integer","not_null":true,"default":"7","check":"(VALUE = 7)",` +
				`"checks":[{"name":"qty_check","expression":"(VALUE = 7)"}],"comment":"quantity"}`},
		{name: "a composite type", representation: schemaext.Desired,
			value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "city", Type: "text"}, {Name: "n", Type: "numeric(10,2)"}},
				StructName: "Addr"},
			wire: `{"attributes":[{"name":"city","type":"text"},{"name":"n","type":"numeric(10,2)"}],"struct_name":"Addr"}`},
		{name: "an observed composite type", representation: schemaext.Observed,
			value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "city", Type: "text"}}, Comment: "address"},
			wire:  `{"attributes":[{"name":"city","type":"text"}],"comment":"address"}`},
		{name: "a range type clearing two attributes, encoded sorted", representation: schemaext.Desired,
			value: &pgtype.DesiredRange{Subtype: "float8", SubtypeOpClass: "float8_ops", Cleared: []string{pgtype.RangeSubtypeDiff, pgtype.RangeCanonical}},
			wire:  `{"subtype":"float8","subtype_opclass":"float8_ops","cleared":["canonical","subtype_diff"]}`},
		{name: "an observed range type", representation: schemaext.Observed,
			value: &pgtype.ObservedRange{Subtype: "double precision", SubtypeOpClass: "float8_ops", Collation: "C", Canonical: "c",
				SubtypeDiff: "float8mi", Comment: "range"},
			wire: `{"subtype":"double precision","subtype_opclass":"float8_ops","collation":"C","canonical":"c","subtype_diff":"float8mi",` +
				`"comment":"range"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			model := codec(c, test.value.Kind(), test.representation)

			encoded := must.Must(model.Encode(test.value))
			decoded := must.Must(model.Decode(encoded))

			c.Assert(string(encoded), qt.Equals, test.wire)
			c.Assert(decoded.(schemaext.Value).Equal(test.value), qt.IsTrue)
		})
	}
}

func TestCodecs_FailurePath(t *testing.T) {
	tests := []struct {
		name           string
		kind           schemaext.Kind
		representation schemaext.Representation
		wire           string
	}{
		{name: "a domain without a base type", kind: pgtype.DomainKind, representation: schemaext.Desired, wire: `{}`},
		{name: "a blank base type", kind: pgtype.DomainKind, representation: schemaext.Desired, wire: `{"base_type":" "}`},
		{name: "NOT NULL spelled false", kind: pgtype.DomainKind, representation: schemaext.Desired, wire: `{"base_type":"text","not_null":false}`},
		{name: "an empty comment spelled out", kind: pgtype.DomainKind, representation: schemaext.Desired, wire: `{"base_type":"text","comment":""}`},
		{name: "a key in another case", kind: pgtype.DomainKind, representation: schemaext.Desired, wire: `{"Base_Type":"text"}`},
		{name: "an unknown normalized key", kind: pgtype.DomainKind, representation: schemaext.Desired,
			wire: `{"base_type":"text","normalized":{"comment":"x"}}`},
		{name: "a named check without its expression", kind: pgtype.DomainKind, representation: schemaext.Observed,
			wire: `{"base_type":"text","checks":[{"name":"c"}]}`},
		{name: "an empty check list spelled out", kind: pgtype.DomainKind, representation: schemaext.Observed,
			wire: `{"base_type":"text","checks":[]}`},
		{name: "a composite type without attributes", kind: pgtype.CompositeKind, representation: schemaext.Desired, wire: `{"attributes":[]}`},
		{name: "an attribute without a type", kind: pgtype.CompositeKind, representation: schemaext.Desired,
			wire: `{"attributes":[{"name":"a"}]}`},
		{name: "an attribute named twice", kind: pgtype.CompositeKind, representation: schemaext.Observed,
			wire: `{"attributes":[{"name":"a","type":"int"},{"name":"a","type":"text"}]}`},
		{name: "a range without a subtype", kind: pgtype.RangeKind, representation: schemaext.Desired, wire: `{"collation":"C"}`},
		{name: "an attribute both stated and cleared", kind: pgtype.RangeKind, representation: schemaext.Desired,
			wire: `{"subtype":"int4","collation":"C","cleared":["collation"]}`},
		{name: "a cleared attribute that is not one", kind: pgtype.RangeKind, representation: schemaext.Desired,
			wire: `{"subtype":"int4","cleared":["subtype"]}`},
		{name: "an observation that clears", kind: pgtype.RangeKind, representation: schemaext.Observed,
			wire: `{"subtype":"int4","cleared":["collation"]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := codec(c, test.kind, test.representation).Decode(json.RawMessage(test.wire))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestCodecs_DefineEachModelOnce pins that the six codecs are the three
// models in both representations, each once.
func TestCodecs_DefineEachModelOnce(t *testing.T) {
	c := qt.New(t)
	seen := make(map[string]bool)

	for _, model := range pgtype.Codecs() {
		key := string(model.Prototype.Kind()) + "/" + string(model.Representation)
		c.Assert(seen[key], qt.IsFalse, qt.Commentf("%s twice", key))
		seen[key] = true
	}

	c.Assert(seen, qt.HasLen, 6)
}
