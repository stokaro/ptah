//go:build integration

package providercli_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/convert/features"
)

func TestProviderConversionPreservesObjectsFacetsAndBoundedKnowledge(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "observed-models")
	connection := observedConnection(c, host, "scoped-native")
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(connection.Close(), qt.IsNil)
	// Conversion needs the owner's process, but no open database session.
	table, column := observed.Tables[0].Facets, observed.Tables[0].Columns[0].Facets
	objects, coverage, err := features.Convert(c.Context(), runtime, "fixture", schemaext.Observed, schemaext.Desired, observed.FeatureObjects, observed.FeatureCoverage, []*schemaext.Facets{&table, &column})
	c.Assert(err, qt.IsNil)
	c.Assert(objects.Refs(), qt.DeepEquals, observed.FeatureObjects.Refs())
	kind := schemaext.Kind("example.org/fixture/retention")
	identity, found := runtime.Codecs().Identity(kind, schemaext.Desired)
	c.Assert(found, qt.IsTrue)
	c.Assert(coverage.KindRecords()[0].Model, qt.DeepEquals, identity)
	missing := objects.Refs()[0]
	missing.Name = objectidentity.Part{Source: "Missing", Normalized: "missing"}
	c.Assert(coverage.Lookup(kind, missing).State, qt.Equals, schemaext.Complete)
	missing.Namespace = objectidentity.NewParts(append(missing.Namespace.Values(), objectidentity.Part{Source: "Hidden", Normalized: "hidden"})...)
	c.Assert(coverage.Lookup(kind, missing).State, qt.Equals, schemaext.Uninspected)
	c.Assert(coverage.KindRecords()[0].Knowledge.Unscoped().State, qt.Equals, schemaext.Uninspected)
	serialized, err := runtime.Codecs().EncodeObjects(c.Context(), schemaext.Desired, objects)
	c.Assert(err, qt.IsNil)
	restored, err := runtime.Codecs().DecodeObjects(c.Context(), schemaext.Desired, serialized)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Refs(), qt.DeepEquals, objects.Refs())
	for _, facet := range []schemaext.Facets{table, column} {
		encoded, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, facet)
		c.Assert(err, qt.IsNil)
		c.Assert(encoded, qt.HasLen, 1)
		var record struct {
			Payload  []byte
			Features []string
		}
		c.Assert(json.Unmarshal(encoded[0].Value.Payload, &record), qt.IsNil)
		c.Assert(string(record.Payload), qt.Equals, `{"seconds":9007199254740993}`)
		c.Assert(record.Features, qt.DeepEquals, []string{"tables"})
	}
	// A reverse conversion retains the same bounded knowledge and identities.
	roundtrip, knowledge, err := features.Convert(c.Context(), runtime, "fixture", schemaext.Desired, schemaext.Observed, objects, coverage, []*schemaext.Facets{&table, &column})
	c.Assert(err, qt.IsNil)
	c.Assert(roundtrip.Refs(), qt.DeepEquals, observed.FeatureObjects.Refs())
	c.Assert(knowledge.Lookup(kind, missing).State, qt.Equals, schemaext.Uninspected)
	original, err := runtime.Codecs().EncodeObjects(c.Context(), schemaext.Observed, observed.FeatureObjects)
	c.Assert(err, qt.IsNil)
	encoded, err := runtime.Codecs().EncodeObjects(c.Context(), schemaext.Observed, roundtrip)
	c.Assert(err, qt.IsNil)
	c.Assert(encoded, qt.DeepEquals, original)
	host.Close()
	offline, err := runtime.Codecs().EncodeObjects(c.Context(), schemaext.Desired, restored)
	c.Assert(err, qt.IsNil)
	c.Assert(offline, qt.DeepEquals, serialized)
	refused, _, err := features.Convert(c.Context(), runtime, "fixture", schemaext.Observed, schemaext.Desired, observed.FeatureObjects, observed.FeatureCoverage, nil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(refused.Len(), qt.Equals, 0)
}

func TestProviderConversionRefusesMixedCapturedProfiles(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "provider")
	definition, found := runtime.Codecs().Identity("example.org/fixture/retention", schemaext.Desired)
	c.Assert(found, qt.IsTrue)
	first := providerModelEnvelope(c, definition, `{}`)
	second := providerModelEnvelope(c, definition, `{"minutes":2}`)
	var wire map[string]any
	c.Assert(json.Unmarshal(second.Payload, &wire), qt.IsNil)
	wire["features"] = []string{"tables"}
	data, err := json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	second.Payload = data
	decoded, err := runtime.Codecs().Decode(c.Context(), []schemaext.Envelope{first, second})
	c.Assert(err, qt.IsNil)
	values := []schemaext.Value{decoded[0].(schemaext.Value), decoded[1].(schemaext.Value)}
	request := schemaext.ConversionRequest{Target: "fixture", From: schemaext.Desired, To: schemaext.Observed, Values: values}
	result, err := runtime.ConvertFeatures(c.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
	request.Values = request.Values[:1]
	result, err = runtime.ConvertFeatures(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = runtime.ConvertFeatures(canceled, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
}
