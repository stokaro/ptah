package schemafingerprint_test

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/schemafingerprint"
)

func TestDesiredFingerprintBindsObjectsAndCoverage(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	empty := &schemamodel.Database{}
	unknown := must.Must(schemafingerprint.Desired(t.Context(), runtime, empty))
	empty.FeatureCoverage = must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil))
	absent := must.Must(schemafingerprint.Desired(t.Context(), runtime, empty))
	c.Assert(absent, qt.Not(qt.Equals), unknown)
	a := ydbschema.DesiredObject("", "events", ydbschema.ChangefeedSpec{Name: "a", Mode: "UPDATES", Format: "JSON", Consumers: []ydbtopic.ConsumerSpec{{Name: "audit"}, {Name: "worker"}}})
	b := ydbschema.DesiredObject("", "events", ydbschema.ChangefeedSpec{Name: "b", Mode: "KEYS_ONLY", Format: "JSON"})
	empty.FeatureObjects = must.Must(schemaext.NewObjects(a, b))
	first := must.Must(schemafingerprint.Desired(t.Context(), runtime, empty))
	slices.Reverse(a.Value.(*ydbschema.DesiredChangefeed).Spec.Consumers)
	empty.FeatureObjects = must.Must(schemaext.NewObjects(b, a))
	c.Assert(must.Must(schemafingerprint.Desired(t.Context(), runtime, empty)), qt.Equals, first)
	a.Value.(*ydbschema.DesiredChangefeed).Spec.Mode = "NEW_IMAGE"
	empty.FeatureObjects = must.Must(schemaext.NewObjects(a, b))
	c.Assert(must.Must(schemafingerprint.Desired(t.Context(), runtime, empty)), qt.Not(qt.Equals), first)
	empty.FeatureCoverage = must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, []schemaext.SubjectCoverage{{Kind: ydbschema.ChangefeedKind, Subject: a.Ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "source cannot describe this stream"}}}))
	c.Assert(must.Must(schemafingerprint.Desired(t.Context(), runtime, empty)), qt.Not(qt.Equals), first)
}

const testKind schemaext.Kind = "example.org/fingerprint-facet"

type facet struct{ Number int }

func (*facet) Kind() schemaext.Kind     { return testKind }
func (v *facet) Clone() schemaext.Value { return &facet{Number: v.Number} }
func (v *facet) Equal(other schemaext.Value) bool {
	value, ok := other.(*facet)
	return ok && value != nil && *value == *v
}

func selectedRuntime(c *qt.C, version uint32) *engine.Runtime {
	c.Helper()
	encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) }
	codecs := make([]schemaext.Codec, 0, 2)
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		codecs = append(codecs, schemaext.Codec{Prototype: &facet{}, Representation: representation, Version: version,
			Definition: json.RawMessage(`{"type":"object","properties":{"Number":{"type":"integer"}}}`),
			Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(*facet).Clone(), nil },
			Encode:     encode, Canonical: encode, Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*facet](data) },
		})
	}
	runtime, err := engine.New(engine.Provider{ID: "example.org/fingerprint", Codecs: codecs})
	c.Assert(err, qt.IsNil)
	return runtime
}

// Every envelope carrying facets gets an instance, including new envelopes
// added later. This tests source ownership independently of the clone's field
// list and avoids a fixture that silently stops covering a newly added slice.
func populateFacetEnvelopes(value reflect.Value) {
	for _, field := range value.Fields() {
		if field.Kind() != reflect.Slice || field.Type().Elem().Kind() != reflect.Struct {
			continue
		}
		if _, found := field.Type().Elem().FieldByName("Facets"); !found {
			continue
		}
		field.Set(reflect.MakeSlice(field.Type(), 1, 1))
		populateFacetEnvelopes(field.Index(0))
	}
}

func TestFingerprintPreservesAllSourceFacets(t *testing.T) {
	c := qt.New(t)
	runtime := selectedRuntime(c, 1)
	desired := &schemamodel.Database{}
	observed := &catalog.Database{}
	populateFacetEnvelopes(reflect.ValueOf(desired).Elem())
	populateFacetEnvelopes(reflect.ValueOf(observed).Elem())
	desiredSlots, observedSlots := desired.FacetSlots(), observed.FacetSlots()
	c.Assert(len(desiredSlots) > 15, qt.IsTrue)
	c.Assert(len(observedSlots) > 15, qt.IsTrue)
	for _, slots := range [][]*schemaext.Facets{desiredSlots, observedSlots} {
		for i, slot := range slots {
			*slot = must.Must(schemaext.NewFacets(&facet{Number: i + 1}))
		}
	}
	first, err := schemafingerprint.Desired(t.Context(), runtime, desired)
	c.Assert(err, qt.IsNil)
	current, err := schemafingerprint.Observed(t.Context(), runtime, observed)
	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.Not(qt.Equals), current)
	for _, slots := range [][]*schemaext.Facets{desiredSlots, observedSlots} {
		for i, slot := range slots {
			values, err := slot.Values()
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.HasLen, 1)
			c.Assert(values[0].(*facet).Number, qt.Equals, i+1)
		}
	}
	desired.Tables[0].Facets, desired.Fields[0].Facets = desired.Fields[0].Facets, desired.Tables[0].Facets
	c.Assert(must.Must(schemafingerprint.Desired(t.Context(), runtime, desired)), qt.Not(qt.Equals), first)
	c.Assert(must.Must(schemafingerprint.Observed(t.Context(), selectedRuntime(c, 2), observed)), qt.Not(qt.Equals), current)
}

func TestFingerprintRefusesMissingContextRuntimeAndCodecs(t *testing.T) {
	c := qt.New(t)
	runtime := selectedRuntime(c, 1)
	value, err := schemafingerprint.Desired(t.Context(), nil, &schemamodel.Database{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(value, qt.Equals, "")
	var missingContext context.Context
	value, err = schemafingerprint.Desired(missingContext, runtime, &schemamodel.Database{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(value, qt.Equals, "")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	value, err = schemafingerprint.Observed(ctx, runtime, &catalog.Database{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(value, qt.Equals, "")
	source := &catalog.Database{Facets: must.Must(schemaext.NewFacets(&facet{Number: 3}))}
	value, err = schemafingerprint.Observed(t.Context(), must.Must(builtin.New()), source)
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(value, qt.Equals, "")
	values, err := source.Facets.Values()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].(*facet).Number, qt.Equals, 3)
}
