package oratypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
)

// TestReverseTypes_RestoresWhatTheCatalogHeld pins each inverse: a creation is
// dropped, a drop creates the type again as the catalog held it, and a
// replaced object type goes back to its attributes. The forward state is the
// declaration's prediction, and absent after a drop.
func TestReverseTypes_RestoresWhatTheCatalogHeld(t *testing.T) {
	before := &oratype.ObservedObjectType{Attributes: []oratype.Attribute{{Name: "N", Type: "NUMBER"}}}
	after := &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER(5)"}}}
	domain := &oratype.ObservedDomain{BaseType: "NUMBER", Default: "SYSDATE"}
	tests := []struct {
		name    string
		forward schemaext.ChangeRecord
		want    schemaext.ChangeValue
		state   schemaext.Value
	}{
		{name: "a creation", forward: change(oratype.ObjectTypeKind, "t", &oratype.ObjectTypeChange{After: after}),
			want: &oratype.ObjectTypeChange{Before: after.Observed()}, state: after.Observed()},
		{name: "a drop", forward: change(oratype.DomainKind, "D", &oratype.DomainChange{Before: domain}),
			want: &oratype.DomainChange{After: &oratype.DesiredDomain{BaseType: "NUMBER", DefaultExpr: "SYSDATE"}}},
		{name: "a replacement", forward: change(oratype.ObjectTypeKind, "t", &oratype.ObjectTypeChange{Before: before, After: after,
			Differs: []string{oratype.DiffAttributes}}),
			want:  &oratype.ObjectTypeChange{Before: after.Observed(), After: before.Desired(), Differs: []string{oratype.DiffAttributes}},
			state: after.Observed()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversed, err := oratype.ReverseService{}.ReverseChanges(t.Context(),
				schemaext.ReversalRequest{Target: platform.Oracle, Changes: []schemaext.ChangeRecord{test.forward}})

			c.Assert(err, qt.IsNil)
			c.Assert(reversed, qt.HasLen, 1)
			c.Assert(reversed[0].Change, qt.DeepEquals, schemaext.ChangeRecord{Subject: test.forward.Subject, Value: test.want})
			c.Assert(reversed[0].ForwardState, qt.HasLen, 1)
			c.Assert(reversed[0].ForwardState[0].Value, qt.DeepEquals, test.state)
		})
	}
}

// TestConvertTypes_ProjectsBothWays pins the projections: a declaration's
// default expression is the default the catalog reports, and the catalog's
// default reads back as an expression or a literal by its spelling.
func TestConvertTypes_ProjectsBothWays(t *testing.T) {
	tests := []struct {
		name     string
		from, to schemaext.Representation
		value    schemaext.Value
		want     schemaext.Value
	}{
		{name: "a declared default expression", from: schemaext.Desired, to: schemaext.Observed,
			value: &oratype.DesiredDomain{BaseType: "DATE", Default: "ignored", DefaultExpr: "SYSDATE", Comment: "c", StructName: "S"},
			want:  &oratype.ObservedDomain{BaseType: "DATE", Default: "SYSDATE"}},
		{name: "an observed literal default", from: schemaext.Observed, to: schemaext.Desired,
			value: &oratype.ObservedDomain{BaseType: "VARCHAR2(9)", NotNull: true, Default: "'x'", Check: "VALUE <> 'y'"},
			want:  &oratype.DesiredDomain{BaseType: "VARCHAR2(9)", NotNull: true, Default: "'x'", Check: "VALUE <> 'y'"}},
		{name: "an observed default expression", from: schemaext.Observed, to: schemaext.Desired,
			value: &oratype.ObservedDomain{BaseType: "NUMBER", Default: "7"},
			want:  &oratype.DesiredDomain{BaseType: "NUMBER", DefaultExpr: "7"}},
		{name: "a declared object type", from: schemaext.Desired, to: schemaext.Observed,
			value: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}}, Comment: "c"},
			want:  &oratype.ObservedObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := oratype.ConvertService{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: platform.Oracle,
				From: test.from, To: test.to, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(converted, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}

// TestReportTypes_CountsEachKind pins the metric each kind is counted under,
// the names the PostgreSQL owner counts its own under.
func TestReportTypes_CountsEachKind(t *testing.T) {
	c := qt.New(t)

	reports, err := oratype.ReportService{}.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed,
		Values: []schemaext.Value{&oratype.ObservedDomain{BaseType: "NUMBER"},
			&oratype.ObservedObjectType{Attributes: []oratype.Attribute{{Name: "N", Type: "NUMBER"}}}}})

	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{
		{Kind: oratype.DomainKind, Counts: []schemaext.MetricCount{{Name: "domains", Value: 1}}},
		{Kind: oratype.ObjectTypeKind, Counts: []schemaext.MetricCount{{Name: "composite_types", Value: 1}}},
	})
}

// TestCodecs_RoundTripEveryPayload pins that every model, change and
// operation encodes and decodes to itself through the runtime's registry.
func TestCodecs_RoundTripEveryPayload(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		payload        schemaext.Payload
	}{
		{name: "desired domain", representation: schemaext.Desired, payload: &oratype.DesiredDomain{BaseType: "NUMBER", NotNull: true,
			Default: "1", DefaultExpr: "2", Check: "VALUE > 0", Comment: "c", StructName: "S"}},
		{name: "observed domain", representation: schemaext.Observed, payload: &oratype.ObservedDomain{BaseType: "NUMBER", Default: "1"}},
		{name: "desired object type", representation: schemaext.Desired, payload: &oratype.DesiredObjectType{
			Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}}, Comment: "c"}},
		{name: "observed object type", representation: schemaext.Observed, payload: &oratype.ObservedObjectType{
			Attributes: []oratype.Attribute{{Name: "N", Type: "NUMBER"}}}},
		{name: "domain change", representation: schemaext.Change, payload: &oratype.DomainChange{
			Before: &oratype.ObservedDomain{BaseType: "NUMBER"}, After: &oratype.DesiredDomain{BaseType: "NUMBER", NotNull: true},
			Differs: []string{oratype.DiffNotNull}}},
		{name: "object type change", representation: schemaext.Change, payload: &oratype.ObjectTypeChange{
			After: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}}}}},
		{name: "domain creation", representation: schemaext.Operation, payload: &oratype.CreateDomain{Target: domainTarget("d"),
			BaseType: "NUMBER", Check: "VALUE > 0", Comment: "c"}},
		{name: "object type statement", representation: schemaext.Operation, payload: &oratype.ReplaceObjectType{
			Target: objectTypeTarget("t"), Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}}, Replaces: true}},
		{name: "drop", representation: schemaext.Operation, payload: &oratype.Drop{Target: domainTarget("d")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codecs := newRuntime(c).Codecs()

			data, err := codecs.Marshal(t.Context(), test.representation, []schemaext.Payload{test.payload})
			c.Assert(err, qt.IsNil)
			decoded, err := codecs.Unmarshal(t.Context(), data)

			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.payload})
		})
	}
}
