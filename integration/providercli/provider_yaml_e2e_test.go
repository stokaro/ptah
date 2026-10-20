//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/engine"
	"ptah.run/internal/providerhost"
)

func TestProviderInterpretsNativeYAML(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProvider(c)
	source := []byte("retention: {on: future, seconds: 9007199254740993}\ntables:\n  future:\n    name: records\n    columns:\n      id: {type: INTEGER}\n")
	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), source)
	c.Assert(err, qt.IsNil)
	registry := runtime.Codecs()
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Facets.DeclaredKinds(), qt.DeepEquals, []schemaext.Kind{"example.org/fixture/retention"})
	c.Assert(db.FeatureCoverage.KindRecords(), qt.HasLen, 1)
	identity, ok := registry.Identity("example.org/fixture/retention", schemaext.Desired)
	c.Assert(ok, qt.IsTrue)
	c.Assert(db.FeatureCoverage.KindRecords()[0].Model, qt.DeepEquals, identity)
	c.Assert(db.FeatureCoverage.SubjectRecords()[0].Knowledge.State, qt.Equals, schemaext.Complete)
	host.Close()
	envelopes, err := registry.EncodeFacets(c.Context(), schemaext.Desired, db.Tables[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(envelopes, qt.HasLen, 1)
	var wire struct {
		Payload []byte `json:"payload"`
	}
	c.Assert(json.Unmarshal(envelopes[0].Value.Payload, &wire), qt.IsNil)
	c.Assert(string(wire.Payload), qt.Equals, `{"seconds":9007199254740993}`)
}

func yamlProvider(c *qt.C) (*providerhost.Host, *engine.Runtime) {
	c.Helper()
	return yamlProviderCommand(c, "provider")
}

func yamlProviderCommand(c *qt.C, name string) (*providerhost.Host, *engine.Runtime) {
	c.Helper()
	executable := independentCLIProviderCommand(c, name)
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	host, err := providerhost.Start(c.Context(), providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"})
	c.Assert(err, qt.IsNil)
	c.Cleanup(host.Close)
	runtime, err := engine.New(host.Provider())
	c.Assert(err, qt.IsNil)
	return host, runtime
}

func TestProviderYAMLTableFacetAndEmptyCoverage(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProvider(c)
	source := []byte("tables:\n  records:\n    columns:\n      id: {type: INTEGER}\n    retention: {}\n")
	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), source)
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureCoverage.SubjectRecords()[0].Knowledge.State, qt.Equals, schemaext.Defaulted)
	encoded, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, db.Tables[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(encoded[0].Targets, qt.DeepEquals, []string{"fixture"})
	empty, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte("tables: {}\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(empty.FeatureCoverage.KindRecords(), qt.HasLen, 1)
	c.Assert(empty.FeatureCoverage.KindRecords()[0].Knowledge.State, qt.Equals, schemaext.Complete)
	c.Assert(empty.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
}

func TestProviderYAMLRefusalDiscardsSchema(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProvider(c)
	for _, row := range []struct{ name, value string }{
		{"unknown field", "{typo: 12}"}, {"explicit null", "null"},
		{"number too large", "{seconds: 1e1000}"}, {"missing binding", "{on: missing, seconds: 60}"},
		{"unknown tag", "!custom 12"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			source := []byte("retention: " + row.value + "\ntables:\n  records:\n    columns:\n      id: {type: INTEGER}\n    retention: {seconds: 60}\n")
			db, err := yamlschema.Parse(c.Context(), runtime.YAML(), source)
			c.Assert(err, qt.IsNotNil)
			c.Assert(db, qt.IsNil)
		})
	}
	host.Close()
	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte("tables: {}\n"))
	c.Assert(err, qt.IsNotNil)
	c.Assert(db, qt.IsNil)
}

func TestProviderYAMLDoesNotClaimAbsenceForOtherFormats(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "mixed-yaml")
	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte("tables: {}\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureCoverage.KindRecords(), qt.HasLen, 1)
	c.Assert(db.FeatureCoverage.KindRecords()[0].Knowledge.State, qt.Equals, schemaext.Uninspected)
	c.Assert(db.FeatureCoverage.KindRecords()[0].Knowledge.Reason, qt.Contains, "every registered engine")
	source := []byte("tables:\n  records:\n    columns:\n      id: {type: INTEGER}\n    retention: {seconds: 60}\n")
	db, err = yamlschema.Parse(c.Context(), runtime.YAML(), source)
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureCoverage.KindRecords()[0].Knowledge.State, qt.Equals, schemaext.Uninspected)
	c.Assert(db.FeatureCoverage.SubjectRecords()[0].Knowledge.State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.SubjectRecords()[0].Targets, qt.DeepEquals, []string{"fixture"})
}
