package chschema_test

import (
	"encoding/json"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

func TestSkippingIndexCapturedValuesRoundTripWithoutDefaulting(t *testing.T) {
	for _, test := range []struct {
		name  string
		value chschema.ObservedIndex
	}{
		{"ordinary", chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}},
		{"parameterized type", chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: 64}},
		{"exact integer", chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := test.value.Desired()
			observed, err := desired.Observed()
			c.Assert(err, qt.IsNil)
			c.Assert(observed, qt.DeepEquals, &test.value)
			for index, value := range []schemaext.Value{desired, &test.value} {
				codec := chschema.IndexCodecs()[index]
				encoded, err := codec.Canonical(value)
				c.Assert(err, qt.IsNil)
				decoded, err := codec.Decode(encoded)
				c.Assert(err, qt.IsNil)
				c.Assert(decoded, qt.DeepEquals, value)
				again, err := codec.Canonical(decoded)
				c.Assert(err, qt.IsNil)
				c.Assert(again, qt.DeepEquals, encoded)
			}
		})
	}
}

func TestSkippingIndexDeclarationIntentStaysDistinct(t *testing.T) {
	for _, test := range []struct {
		name  string
		value chschema.DesiredIndex
		wire  string
	}{
		{"unmanaged", chschema.DesiredIndex{}, `{}`},
		{"default", chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Default},
			Granularity: chschema.GranularitySetting{State: chschema.Default}},
			`{"index_type":{"state":"default"},"granularity":{"state":"default"}}`},
		{"explicit", chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Explicit, Value: "minmax"},
			Granularity: chschema.GranularitySetting{State: chschema.Explicit, Value: 1}},
			`{"index_type":{"state":"explicit","value":"minmax"},"granularity":{"state":"explicit","value":1}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := chschema.IndexCodecs()[0]
			data, err := codec.Canonical(&test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(string(data), qt.Equals, test.wire)
			decoded, err := codec.Decode(data)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, &test.value)
		})
	}
}

func TestSkippingIndexUnresolvedSettingsCannotBecomeObservations(t *testing.T) {
	for _, state := range []chschema.SettingState{chschema.Unspecified, chschema.Default} {
		t.Run(string(state), func(t *testing.T) {
			c := qt.New(t)
			value := &chschema.DesiredIndex{IndexType: chschema.Setting{State: state},
				Granularity: chschema.GranularitySetting{State: state}}
			observed, err := value.Observed()
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(observed, qt.IsNil)
		})
	}
}

func TestSkippingIndexCodecsRefuseAmbiguousOrInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name  string
		codec int
		wire  string
	}{
		{"desired null", 0, `null`},
		{"expression belongs to common index", 0, `{"expression":"payload"}`},
		{"null setting", 0, `{"index_type":null}`},
		{"missing intent", 0, `{"index_type":{"value":"minmax"}}`},
		{"empty explicit type", 0, `{"index_type":{"state":"explicit"}}`},
		{"contradictory type", 0, `{"index_type":{"state":"default","value":"minmax"}}`},
		{"unknown type intent", 0, `{"index_type":{"state":"future"}}`},
		{"unknown granularity intent", 0, `{"granularity":{"state":"future"}}`},
		{"zero explicit granularity", 0, `{"granularity":{"state":"explicit"}}`},
		{"contradictory granularity", 0, `{"granularity":{"state":"","value":1}}`},
		{"null granularity value", 0, `{"granularity":{"state":"explicit","value":null}}`},
		{"field alias", 0, `{"index_type":{"state":"default"},"Index_type":{"state":"explicit","value":"set(100)"}}`},
		{"nested Unicode alias", 0, `{"index_type":{"state":"default","\u017ftate":"explicit","value":"minmax"}}`},
		{"duplicate field", 0, `{"index_type":{"state":"default"},"index_type":{"state":"default"}}`},
		{"unknown field", 0, `{"future":true}`},
		{"NUL type", 0, `{"index_type":{"state":"explicit","value":"minmax\u0000"}}`},
		{"observed null", 1, `null`},
		{"missing observed granularity", 1, `{"index_type":"minmax"}`},
		{"missing observed type", 1, `{"granularity":1}`},
		{"empty observed type", 1, `{"index_type":"","granularity":1}`},
		{"zero observed granularity", 1, `{"index_type":"minmax","granularity":0}`},
		{"negative granularity", 1, `{"index_type":"minmax","granularity":-1}`},
		{"fractional granularity", 1, `{"index_type":"minmax","granularity":1.5}`},
		{"overflow", 1, `{"index_type":"minmax","granularity":18446744073709551616}`},
		{"string number", 1, `{"index_type":"minmax","granularity":"1"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := chschema.IndexCodecs()[test.codec].Decode(json.RawMessage(test.wire))
			c.Assert(err, qt.IsNotNil)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestSkippingIndexRegistryPreservesScopeAndRepresentation(t *testing.T) {
	codecs := chschema.IndexCodecs()
	var owned []schemaext.OwnedCodec
	for _, codec := range codecs {
		owned = append(owned, schemaext.OwnedCodec{Owner: "ptah.run/clickhouse", Codec: codec})
	}
	registry := must.Must(schemaext.NewRegistry(owned...))
	observed := &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}
	for index, value := range []schemaext.Value{observed.Desired(), observed} {
		t.Run(string(codecs[index].Representation), func(t *testing.T) {
			c := qt.New(t)
			facets := must.Must(schemaext.NewFacets(value))
			facets = must.Must(facets.WithTargetScope(chschema.IndexKind, "clickhouse"))
			document, err := registry.EncodeFacets(t.Context(), codecs[index].Representation, facets)
			c.Assert(err, qt.IsNil)
			decoded, err := registry.DecodeFacets(t.Context(), codecs[index].Representation, document)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded.Equal(facets), qt.IsTrue)
			c.Assert(decoded.TargetScope(chschema.IndexKind), qt.DeepEquals, []string{"clickhouse"})
		})
	}
}

func TestSkippingIndexCodecsRefuseWrongRepresentationAndTypedNil(t *testing.T) {
	for _, test := range []struct {
		name  string
		codec int
		value schemaext.Value
	}{
		{"observed in desired", 0, &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}},
		{"desired in observed", 1, &chschema.DesiredIndex{}},
		{"nil desired", 0, (*chschema.DesiredIndex)(nil)},
		{"nil observed", 1, (*chschema.ObservedIndex)(nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := chschema.IndexCodecs()[test.codec]
			data, err := codec.Encode(test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(data, qt.IsNil)
			clone, err := codec.Clone(test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(clone, qt.IsNil)
		})
	}
}
