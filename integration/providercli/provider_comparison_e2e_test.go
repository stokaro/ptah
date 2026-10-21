//go:build integration

package providercli_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

const comparisonNative = schemaext.Kind("example.org/fixture/retention-object")
const comparisonFacet = schemaext.Kind("example.org/fixture/retention")

func comparisonRules() identifier.Semantics {
	return identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonASCIIInsensitive, ColumnNames: identifier.ComparisonASCIIInsensitive, IndexNames: identifier.ComparisonASCIIInsensitive, IndexNamespace: identifier.IndexNamespaceSchema}
}

func comparisonModel(c *qt.C, runtime *engine.Runtime, representation schemaext.Representation, kind schemaext.Kind, subject objectidentity.ID, payload string) schemaext.Value {
	c.Helper()
	definition, found := runtime.Codecs().Identity(kind, representation)
	c.Assert(found, qt.IsTrue)
	envelope := providerModelEnvelope(c, definition, payload)
	var wire map[string]any
	c.Assert(json.Unmarshal(envelope.Payload, &wire), qt.IsNil)
	path := make([]string, 0)
	for _, part := range subject.Namespace.Values() {
		path = append(path, part.Source)
	}
	if subject.Parent.Source != "" {
		path = append(path, subject.Parent.Source)
	}
	wire["subject"] = map[string]any{"engine": "fixture", "kind": string(subject.Kind), "name": subject.Name.Source, "schema": subject.Schema.Source, "path": path}
	encoded, err := json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	envelope.Payload = encoded
	values, err := runtime.Codecs().Decode(c.Context(), []schemaext.Envelope{envelope})
	c.Assert(err, qt.IsNil)
	return values[0].(schemaext.Value)
}

func comparisonCoverage(c *qt.C, runtime *engine.Runtime, kind schemaext.Kind, representation schemaext.Representation, knowledge schemaext.Knowledge) schemaext.Coverage {
	c.Helper()
	model, found := runtime.Codecs().Identity(kind, representation)
	c.Assert(found, qt.IsTrue)
	result, err := schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.GlobalKnowledge(knowledge)}}, nil)
	c.Assert(err, qt.IsNil)
	return result
}

func TestProviderComparesNativeModelsAndCapturesChangeFacts(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "comparison-models")
	subject := objectidentity.ID{Kind: objectidentity.Kind(comparisonNative), Database: objectidentity.Part{Source: "Tenant", Normalized: "tenant"}, Namespace: objectidentity.NewParts(objectidentity.Part{Source: "a/b", Normalized: "a/b"}, objectidentity.Part{Source: "c", Normalized: "c"}), Name: objectidentity.Part{Source: "retention", Normalized: "retention"}}
	desired := comparisonModel(c, runtime, schemaext.Desired, comparisonNative, subject, `{"minutes":1}`)
	current := comparisonModel(c, runtime, schemaext.Observed, comparisonNative, subject, `{"seconds":120}`)
	desiredObjects, err := schemaext.NewObjects(schemaext.Object{Ref: subject, Value: desired})
	c.Assert(err, qt.IsNil)
	currentObjects, err := schemaext.NewObjects(schemaext.Object{Ref: subject, Value: current})
	c.Assert(err, qt.IsNil)
	complete := schemaext.Knowledge{State: schemaext.Complete}
	request := schemaext.ObjectComparisonRequest{Target: "fixture", Identifiers: comparisonRules(), Desired: schemaext.ObjectState{Objects: desiredObjects, Coverage: comparisonCoverage(c, runtime, comparisonNative, schemaext.Desired, complete)}, Current: schemaext.ObjectState{Objects: currentObjects, Coverage: comparisonCoverage(c, runtime, comparisonNative, schemaext.Observed, complete)}}
	result, err := runtime.CompareObjects(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 1)
	change := result.Changes[0]
	c.Assert(change.Subject, qt.DeepEquals, subject)
	c.Assert(schemaext.RemovesState(change.Value), qt.IsTrue)
	c.Assert(schemaext.ReplacesOwner(change.Value), qt.IsFalse)
	c.Assert(change.Value.(schemaext.EffectSource).Effect().Impact, qt.Equals, schemaext.Behavioral)
	c.Assert(change.Value.(schemaext.AccessEffectSource).AccessEffect().Access, qt.Equals, schemaext.AccessUnchanged)
	encoded, err := runtime.Codecs().EncodeChanges(c.Context(), result.Changes)
	c.Assert(err, qt.IsNil)
	restored, err := runtime.Codecs().DecodeChanges(c.Context(), encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(schemaext.RemovesState(restored[0].Value), qt.IsTrue)
	// The persisted payload reconstructs effects; no saved normalization is trusted.
	var wire map[string]any
	c.Assert(json.Unmarshal(encoded[0].Value.Payload, &wire), qt.IsNil)
	wire["payload"] = []byte(`{"before":60,"after":120}`)
	changed, err := json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	encoded[0].Value.Payload = changed
	restored, err = runtime.Codecs().DecodeChanges(c.Context(), encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(schemaext.RemovesState(restored[0].Value), qt.IsFalse)

	equal := comparisonModel(c, runtime, schemaext.Observed, comparisonNative, subject, `{"seconds":60}`)
	request.Current.Objects, err = schemaext.NewObjects(schemaext.Object{Ref: subject, Value: equal})
	c.Assert(err, qt.IsNil)
	unchanged, err := runtime.CompareObjects(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(unchanged.Changes, qt.HasLen, 0)
	request.Requests = []schemaext.ChangeRequest{{Subject: subject, Action: "rotate"}}
	forced, err := runtime.CompareObjects(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(forced.Changes, qt.HasLen, 1)
	request.Requests = nil
	request.Current.Coverage = comparisonCoverage(c, runtime, comparisonNative, schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "Not inspected"})
	unknown, err := runtime.CompareObjects(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(unknown.Changes, qt.HasLen, 0)
	c.Assert(unknown.Undecided, qt.HasLen, 1)
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	refused, err := runtime.CompareObjects(canceled, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(refused.Complete, qt.IsFalse)
	host.Close()
	_, err = runtime.Codecs().EncodeChanges(c.Context(), result.Changes)
	c.Assert(err, qt.IsNil)
	_, err = runtime.Codecs().DecodeChanges(c.Context(), encoded)
	c.Assert(err, qt.IsNotNil)
}

func TestProviderFacetComparisonPreservesParentsAndExclusions(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "comparison-models")
	rules := comparisonRules()
	subject := objectidentity.NewBuilder(rules).ColumnParts("main", "records", "value")
	desired := comparisonModel(c, runtime, schemaext.Desired, comparisonFacet, subject, `{"minutes":1}`)
	current := comparisonModel(c, runtime, schemaext.Observed, comparisonFacet, subject, `{"seconds":120}`)
	desiredFacets, err := schemaext.NewFacets(desired)
	c.Assert(err, qt.IsNil)
	currentFacets, err := schemaext.NewFacets(current)
	c.Assert(err, qt.IsNil)
	complete := schemaext.Knowledge{State: schemaext.Complete}
	request := schemaext.FacetComparisonRequest{Target: "fixture", Identifiers: rules, Owners: []schemaext.ParentState{{Subject: subject, Desired: true, Current: true}}, Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: desiredFacets}}, Coverage: comparisonCoverage(c, runtime, comparisonFacet, schemaext.Desired, complete)}, Current: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: currentFacets}}, Coverage: comparisonCoverage(c, runtime, comparisonFacet, schemaext.Observed, complete)}}
	result, err := runtime.CompareFacets(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Change.Subject, qt.DeepEquals, subject)
	c.Assert(result.Changes[0].Kind, qt.Equals, comparisonFacet)
	bound, err := desiredFacets.WithTargetScope(comparisonFacet, "other")
	c.Assert(err, qt.IsNil)
	selection, err := schemaext.NewTargetSelection("fixture")
	c.Assert(err, qt.IsNil)
	excluded, err := bound.ForTarget(selection)
	c.Assert(err, qt.IsNil)
	request.Desired.Records[0].Values = excluded
	result, err = runtime.CompareFacets(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Desired.Records[0].Values.Kinds(), qt.HasLen, 0)
	c.Assert(result.Desired.Records[0].Values.TargetScope(comparisonFacet), qt.DeepEquals, []string{"other"})
	request.Desired.Records[0].Values = desiredFacets
	request.Current.Records[0].Values, err = schemaext.NewFacets(comparisonModel(c, runtime, schemaext.Observed, comparisonFacet, subject, `{"seconds":60}`))
	c.Assert(err, qt.IsNil)
	result, err = runtime.CompareFacets(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
}
