//go:build integration

package providercli_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
)

func TestProviderYAMLNativeObjectNamespaces(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "namespaced-yaml")
	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte("tables:\n  records:\n    columns:\n      id: {type: INTEGER}\n    retention: {seconds: 60}\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 3)
	refs := db.FeatureObjects.Refs()
	c.Assert(refs[0].Database.Source, qt.Equals, "other")
	c.Assert(refs[1].Namespace.Values()[0].Source, qt.Equals, "a")
	c.Assert(refs[2].Namespace.Values()[0].Source, qt.Equals, "a/b")
	for _, ref := range refs {
		c.Assert(db.FeatureCoverage.Lookup(schemaext.Kind(ref.Kind), ref).State, qt.Equals, schemaext.Complete)
	}
	envelopes, err := runtime.Codecs().EncodeObjects(c.Context(), schemaext.Desired, db.FeatureObjects)
	c.Assert(err, qt.IsNil)
	encoded, err := json.Marshal(envelopes)
	c.Assert(err, qt.IsNil)
	c.Assert(json.Unmarshal(encoded, &envelopes), qt.IsNil)
	restored, err := runtime.Codecs().DecodeObjects(c.Context(), schemaext.Desired, envelopes)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Refs(), qt.DeepEquals, refs)
	host.Close()
	offline, err := runtime.Codecs().EncodeObjects(c.Context(), schemaext.Desired, restored)
	c.Assert(err, qt.IsNil)
	c.Assert(offline, qt.DeepEquals, envelopes)
}

func TestProviderYAMLRefusesUnrepresentableNamespacePlacement(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "namespaced-yaml")
	for _, name := range []string{"facet", "reshape", "overload"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			source := []byte("tables:\n  " + name + ":\n    columns:\n      id: {type: INTEGER}\n    retention: {seconds: 60}\n")
			db, err := yamlschema.Parse(c.Context(), runtime.YAML(), source)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "native YAML")
			c.Assert(db, qt.IsNil)
		})
	}
}
