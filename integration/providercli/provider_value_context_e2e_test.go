//go:build integration

package providercli_test

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/schemaload"
)

func TestProviderSourceBindingSurvivesCaptureAndRefusesAnotherTarget(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "value-context")
	dir := c.TempDir()
	path := filepath.Join(dir, "target.db")
	address := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/"), RawQuery: url.Values{"source": {"/authored"}}.Encode()}).String()
	bound, err := runtime.ResolveDatabaseContext(c.Context(), schemaext.DatabaseContextRequest{Target: "fixture-context", Address: address})
	c.Assert(err, qt.IsNil)
	c.Assert(bound.Database, qt.Equals, path)
	c.Assert(bound.SourceDatabase, qt.Equals, "/authored")
	_, err = os.Stat(path)
	c.Assert(os.IsNotExist(err), qt.IsTrue)
	yaml := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(yaml, []byte("tables:\n  records:\n    columns:\n      id: {type: INTEGER}\n    retention: {seconds: 60}\n"), 0o600), qt.IsNil)
	load := func(address string) (*schemamodel.Database, error) {
		return schemafile.LoadPath(c.Context(), yaml, schemafile.Options{Contexts: runtime, DatabaseURL: address, Dialect: "fixture-context", YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL(), CoverageVocabulary: runtime.CoverageVocabulary()})
	}
	unbound, err := load("")
	c.Assert(err, qt.IsNil)
	initial, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, unbound.Tables[0].Facets)
	c.Assert(err, qt.IsNil)
	var pending struct {
		RequiresDatabaseContext bool `json:"requires_database_context"`
	}
	c.Assert(json.Unmarshal(initial[0].Value.Payload, &pending), qt.IsNil)
	c.Assert(pending.RequiresDatabaseContext, qt.IsTrue)
	unresolved, err := unbound.Tables[0].Facets.Values()
	c.Assert(err, qt.IsNil)
	refusedDependencies, err := unresolved[0].(schemaext.Requirer).Requires()
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(refusedDependencies, qt.IsNil)

	database, err := load(address)
	c.Assert(err, qt.IsNil)
	encoded, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, database.Tables[0].Facets)
	c.Assert(err, qt.IsNil)
	var captured struct {
		Context                 schemaext.DatabaseContext `json:"context"`
		RequiresDatabaseContext bool                      `json:"requires_database_context"`
		Subject                 struct {
			Database string `json:"database"`
			Name     string `json:"name"`
		} `json:"subject"`
	}
	c.Assert(json.Unmarshal(encoded[0].Value.Payload, &captured), qt.IsNil)
	c.Assert(captured.Context, qt.Equals, bound)
	c.Assert(captured.RequiresDatabaseContext, qt.IsFalse)
	c.Assert(captured.Subject.Database, qt.Equals, "")
	c.Assert(captured.Subject.Name, qt.Equals, "records")
	boundValues, err := database.Tables[0].Facets.Values()
	c.Assert(err, qt.IsNil)
	dependencies, err := boundValues[0].(schemaext.Requirer).Requires()
	c.Assert(err, qt.IsNil)
	c.Assert(dependencies, qt.HasLen, 1)
	c.Assert(dependencies[0].Database.Source, qt.Equals, bound.Database)

	restored, err := runtime.Codecs().DecodeFacets(c.Context(), schemaext.Desired, encoded)
	c.Assert(err, qt.IsNil)
	values, err := restored.Values()
	c.Assert(err, qt.IsNil)
	converted, err := runtime.ConvertFeatures(c.Context(), schemaext.ConversionRequest{Target: "fixture", From: schemaext.Desired, To: schemaext.Observed, Values: values})
	c.Assert(err, qt.IsNil)
	back, err := runtime.ConvertFeatures(c.Context(), schemaext.ConversionRequest{Target: "fixture", From: schemaext.Observed, To: schemaext.Desired, Values: converted})
	c.Assert(err, qt.IsNil)
	boundAgain, err := runtime.BindValueContexts(c.Context(), schemaext.ValueContextRequest{Target: "fixture", Context: bound, Representation: schemaext.Desired, Values: back})
	c.Assert(err, qt.IsNil)
	c.Assert(boundAgain[0].Equal(values[0]), qt.IsTrue)
	other := bound
	other.Database += "-other"
	refused, err := runtime.BindValueContexts(c.Context(), schemaext.ValueContextRequest{Target: "fixture", Context: other, Representation: schemaext.Desired, Values: values})
	c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
	c.Assert(refused, qt.IsNil)
	goDir := filepath.Join(dir, "go-source")
	c.Assert(os.Mkdir(goDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(goDir, "records.go"), []byte("package p\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention seconds=60\ntype Record struct { ID int64 `db:\"id\"` }\n"), 0o600), qt.IsNil)
	goSource, err := schemaload.LoadContext(c.Context(), schemaload.Options{Owners: runtime, Dialect: "fixture", DatabaseURL: address, RootDirs: []string{goDir}})
	c.Assert(err, qt.IsNil)
	goEncoded, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, goSource.Tables[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(json.Unmarshal(goEncoded[0].Value.Payload, &captured), qt.IsNil)
	c.Assert(captured.Context, qt.Equals, bound)

	sql := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(sql, []byte("CREATE RETENTION cleanup SECONDS 60;"), 0o600), qt.IsNil)
	sqlSource, err := schemafile.LoadPath(c.Context(), sql, schemafile.Options{Contexts: runtime, DatabaseURL: address, Dialect: "fixture-context", YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL(), CoverageVocabulary: runtime.CoverageVocabulary()})
	c.Assert(err, qt.IsNil)
	sqlObjects, err := runtime.Codecs().EncodeObjects(c.Context(), schemaext.Desired, sqlSource.FeatureObjects)
	c.Assert(err, qt.IsNil)
	c.Assert(sqlObjects, qt.HasLen, 1)
	c.Assert(json.Unmarshal(sqlObjects[0].Value.Payload, &captured), qt.IsNil)
	c.Assert(captured.Context, qt.Equals, bound)
	c.Assert(captured.RequiresDatabaseContext, qt.IsFalse)
	c.Assert(captured.Subject.Database, qt.Equals, "")
	c.Assert(captured.Subject.Name, qt.Equals, "cleanup")
	_, err = os.Stat(path)
	c.Assert(os.IsNotExist(err), qt.IsTrue)
}
