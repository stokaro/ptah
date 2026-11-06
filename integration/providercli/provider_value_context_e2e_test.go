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
	"ptah.run/engine"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/schemaload"
	"ptah.run/internal/valuecontext"
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

// valueContextFixture is the value-context fixture's runtime and a schema
// whose table carries a retention facet.
type valueContextFixture struct {
	runtime   *engine.Runtime
	dir, yaml string
}

func newValueContextFixture(c *qt.C) valueContextFixture {
	c.Helper()
	_, runtime := yamlProviderCommand(c, "value-context")
	dir := c.TempDir()
	yaml := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(yaml, []byte("tables:\n  records:\n    columns:\n      id: {type: INTEGER}\n    retention: {seconds: 60}\n"), 0o600), qt.IsNil)
	return valueContextFixture{runtime: runtime, dir: dir, yaml: yaml}
}

// address names the fixture database name, whose authored references are
// read against root.
func (f valueContextFixture) address(name, root string) string {
	path := filepath.Join(f.dir, name)
	return (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/"), RawQuery: url.Values{"source": {root}}.Encode()}).String()
}

// load is the schema bound to address.
func (f valueContextFixture) load(c *qt.C, address string) *schemamodel.Database {
	c.Helper()
	database, err := schemafile.LoadPath(c.Context(), f.yaml, schemafile.Options{Contexts: f.runtime, DatabaseURL: address, Dialect: "fixture-context",
		YAML: f.runtime.YAML(), SQL: f.runtime.SQL(), HCL: f.runtime.HCL(), CoverageVocabulary: f.runtime.CoverageVocabulary()})
	c.Assert(err, qt.IsNil)
	return database
}

// boundFacet is the context the schema's retention facet is bound to, and the
// database of the dependency the provider computed under it.
func (f valueContextFixture) boundFacet(c *qt.C, database *schemamodel.Database) (schemaext.DatabaseContext, string) {
	c.Helper()
	encoded, err := f.runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, database.Tables[0].Facets)
	c.Assert(err, qt.IsNil)
	var captured struct {
		Context schemaext.DatabaseContext `json:"context"`
	}
	c.Assert(json.Unmarshal(encoded[0].Value.Payload, &captured), qt.IsNil)
	values, err := database.Tables[0].Facets.Values()
	c.Assert(err, qt.IsNil)
	dependencies, err := values[0].(schemaext.Requirer).Requires()
	c.Assert(err, qt.IsNil)
	c.Assert(dependencies, qt.HasLen, 1)
	return captured.Context, dependencies[0].Database.Source
}

// A value bound to one database moves to another database read against the
// same root, the way a rehearsal moves the target's schema into its dev
// realm: the provider binds it again, and the dependency it computes names
// the database the value moved to.
func TestProviderMovesValuesToAnotherContextOfTheirRoot_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newValueContextFixture(c)
	target, realm := f.address("target.db", "/authored"), f.address("realm.db", "/authored")
	database := f.load(c, target)
	bound, err := f.runtime.ResolveDatabaseContext(c.Context(), schemaext.DatabaseContextRequest{Target: "fixture-context", Address: realm})
	c.Assert(err, qt.IsNil)

	moved, err := valuecontext.Move(c.Context(), f.runtime, "fixture-context", target, realm, database)

	c.Assert(err, qt.IsNil)
	c.Assert(moved, qt.IsTrue)
	context, dependency := f.boundFacet(c, database)
	c.Assert(context, qt.Equals, bound)
	c.Assert(dependency, qt.Equals, bound.Database)
}

// A value bound to one database moves to a database whose authored
// references are read against another root, the way a rehearsal moves the
// target's schema into a dev database of another name: the provider binds it
// again under the new root, and the dependency it computes names the database
// the value moved to.
func TestProviderMovesValuesToAnotherRoot_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newValueContextFixture(c)
	target, other := f.address("target.db", "/authored"), f.address("other.db", "/other")
	database := f.load(c, target)
	bound, err := f.runtime.ResolveDatabaseContext(c.Context(), schemaext.DatabaseContextRequest{Target: "fixture-context", Address: other})
	c.Assert(err, qt.IsNil)

	moved, err := valuecontext.Move(c.Context(), f.runtime, "fixture-context", target, other, database)

	c.Assert(err, qt.IsNil)
	c.Assert(moved, qt.IsTrue)
	context, dependency := f.boundFacet(c, database)
	c.Assert(context, qt.Equals, bound)
	c.Assert(dependency, qt.Equals, bound.Database)
}

// A move of a value bound to neither context is refused, and the value keeps
// the binding it had.
func TestProviderMovesValuesToAnotherContextOfTheirRoot_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, from, to string
	}{
		{name: "a value bound to neither", from: "realm.db", to: "elsewhere.db"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newValueContextFixture(c)
			target := f.address("target.db", "/authored")
			database := f.load(c, target)
			before, _ := f.boundFacet(c, database)
			roots := map[string]string{"target.db": "/authored", "realm.db": "/authored", "elsewhere.db": "/authored"}
			resolve := func(name string) schemaext.DatabaseContext {
				bound, err := f.runtime.ResolveDatabaseContext(c.Context(), schemaext.DatabaseContextRequest{Target: "fixture-context", Address: f.address(name, roots[name])})
				c.Assert(err, qt.IsNil)
				return bound
			}
			values, err := database.Tables[0].Facets.Values()
			c.Assert(err, qt.IsNil)

			result, err := f.runtime.BindValueContexts(c.Context(), schemaext.ValueContextRequest{Target: "fixture", Context: resolve(test.to), From: resolve(test.from),
				Representation: schemaext.Desired, Values: values})

			c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
			c.Assert(result, qt.IsNil)
			after, _ := f.boundFacet(c, database)
			c.Assert(after, qt.Equals, before)
		})
	}
}
