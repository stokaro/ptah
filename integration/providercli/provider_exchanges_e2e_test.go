//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/encoding/protojson"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/providerexchange"
)

// exchangeRequest is the comparison of one retention object whose declared
// minutes differ from the seconds a read found, which the fixture provider
// reports as one change.
func exchangeRequest(c *qt.C, runtime *engine.Runtime) schemaext.ObjectComparisonRequest {
	c.Helper()
	subject := objectidentity.ID{Kind: objectidentity.Kind(comparisonNative), Database: objectidentity.Part{Source: "Tenant", Normalized: "tenant"},
		Namespace: objectidentity.NewParts(objectidentity.Part{Source: "app", Normalized: "app"}), Name: objectidentity.Part{Source: "retention", Normalized: "retention"}}
	desiredObjects, err := schemaext.NewObjects(schemaext.Object{Ref: subject, Value: comparisonModel(c, runtime, schemaext.Desired, comparisonNative, subject, `{"minutes":1}`)})
	c.Assert(err, qt.IsNil)
	currentObjects, err := schemaext.NewObjects(schemaext.Object{Ref: subject, Value: comparisonModel(c, runtime, schemaext.Observed, comparisonNative, subject, `{"seconds":120}`)})
	c.Assert(err, qt.IsNil)
	complete := schemaext.Knowledge{State: schemaext.Complete}
	return schemaext.ObjectComparisonRequest{Target: "fixture", Identifiers: comparisonRules(),
		Desired: schemaext.ObjectState{Objects: desiredObjects, Coverage: comparisonCoverage(c, runtime, comparisonNative, schemaext.Desired, complete)},
		Current: schemaext.ObjectState{Objects: currentObjects, Coverage: comparisonCoverage(c, runtime, comparisonNative, schemaext.Observed, complete)}}
}

// exchangeFiles returns the files a recording directory holds that match
// pattern.
func exchangeFiles(c *qt.C, dir, pattern string) []string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	c.Assert(err, qt.IsNil)
	return matches
}

// A comparison sent to a provider under a context that carries a recorder is
// written as the contract's JSON: the request as the host sent it and the
// provider's reply, which read back as the messages the run exchanged.
func TestProviderComparisonIsRecorded_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "comparison-models")
	request := exchangeRequest(c, runtime)
	dir := filepath.Join(c.TempDir(), "exchanges")
	recorder, err := providerexchange.New(dir)
	c.Assert(err, qt.IsNil)

	result, err := runtime.CompareObjects(providerexchange.With(c.Context(), recorder), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	requests, replies := exchangeFiles(c, dir, "0001-comparemodels-*.request.json"), exchangeFiles(c, dir, "0001-comparemodels-*.reply.json")
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(replies, qt.HasLen, 1)
	c.Assert(exchangeFiles(c, dir, "*"), qt.HasLen, 2)
	sent := &protocol.CompareModelsRequest{}
	encoded, err := os.ReadFile(requests[0])
	c.Assert(err, qt.IsNil)
	c.Assert(protojson.Unmarshal(encoded, sent), qt.IsNil)
	c.Assert(sent.GetEngine(), qt.Equals, "fixture")
	c.Assert(sent.GetDesired().GetRecords(), qt.HasLen, 1)
	c.Assert(sent.GetDesired().GetRecords()[0].GetSubject().GetName().GetNormalized(), qt.Equals, "retention")
	answered := &protocol.ComparedModels{}
	encoded, err = os.ReadFile(replies[0])
	c.Assert(err, qt.IsNil)
	c.Assert(protojson.Unmarshal(encoded, answered), qt.IsNil)
	c.Assert(answered.GetChanges(), qt.HasLen, 1)
}

// A comparison the provider refuses is written as the request and the error
// the host received, which carries the provider's reason, and the refusal
// still reaches the caller.
func TestProviderComparisonIsRecorded_FailurePath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "comparison-models")
	request := relationsRequest(c, runtime, "keep")
	dir := filepath.Join(c.TempDir(), "exchanges")
	recorder, err := providerexchange.New(dir)
	c.Assert(err, qt.IsNil)

	result, err := runtime.CompareObjects(providerexchange.With(c.Context(), recorder), request)

	c.Assert(err, qt.ErrorMatches, `.*Retention object keep takes the name of declared table keep`)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
	errors := exchangeFiles(c, dir, "0001-comparemodels-*.error.txt")
	c.Assert(errors, qt.HasLen, 1)
	c.Assert(exchangeFiles(c, dir, "0001-comparemodels-*.request.json"), qt.HasLen, 1)
	c.Assert(exchangeFiles(c, dir, "*.reply.json"), qt.HasLen, 0)
	text, err := os.ReadFile(errors[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(text), qt.Contains, "Retention object keep takes the name of declared table keep")
}
