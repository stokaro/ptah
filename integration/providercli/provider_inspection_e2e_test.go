//go:build integration

package providercli_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/internal/providerhost"
)

func observedConnection(c *qt.C, host *providerhost.Host, table string) *dbschema.DatabaseConnection {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), host.Connections())
	c.Assert(err, qt.IsNil)
	address := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(c.TempDir(), "observed.db")), "/")}).String()
	connection, err := dbschema.ConnectToDatabase(ctx, address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(connection.Close(), qt.IsNil) })
	c.Assert(connection.Writer().ExecuteSQL(ctx, `CREATE TABLE "`+table+`" ("Id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
	return connection
}

func TestProviderInspectionCapturesObjectsAndFacets(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "observed-models")
	connection := observedConnection(c, host, "a.b")
	db, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 3)
	kind := schemaext.Kind("example.org/fixture/retention")
	for _, ref := range db.FeatureObjects.Refs() {
		c.Assert(ref.Name.Source, qt.Equals, "Job")
		c.Assert(ref.Name.Normalized, qt.Equals, "job")
		c.Assert(ref.Catalog.Source, qt.Equals, "catalog")
		c.Assert(ref.Namespace.Values(), qt.DeepEquals, []objectidentity.Part{{Source: "a/b", Normalized: "a/b"}, {Source: "c", Normalized: "c"}})
		c.Assert(db.FeatureCoverage.Lookup(kind, ref).State, qt.Equals, schemaext.Complete)
	}
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{kind})
	c.Assert(db.Tables[0].Columns[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{kind})
	builder := objectidentity.NewBuilder(connection.Info().IdentifierSemantics)
	for _, ref := range []objectidentity.ID{builder.TableParts("", "a.b"), builder.ColumnParts("", "a.b", "Id")} {
		c.Assert(db.FeatureCoverage.Lookup(kind, ref).State, qt.Equals, schemaext.Complete)
	}
	c.Assert(db.FeatureCoverage.KindRecords()[0].Knowledge.Unscoped().State, qt.Equals, schemaext.Uninspected)
	absent := db.FeatureObjects.Refs()[0]
	absent.Name = objectidentity.Part{Source: "Absent", Normalized: "absent"}
	c.Assert(db.FeatureCoverage.Lookup(kind, absent).State, qt.Equals, schemaext.Absent)
	identity, ok := runtime.Codecs().Identity(kind, schemaext.Observed)
	c.Assert(ok, qt.IsTrue)
	c.Assert(db.FeatureCoverage.KindRecords()[0].Model, qt.DeepEquals, identity)
	objects, err := runtime.Codecs().EncodeObjects(c.Context(), schemaext.Observed, db.FeatureObjects)
	c.Assert(err, qt.IsNil)
	restored, err := runtime.Codecs().DecodeObjects(c.Context(), schemaext.Observed, objects)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Refs(), qt.DeepEquals, db.FeatureObjects.Refs())
	facets, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Observed, db.Tables[0].Columns[0].Facets)
	c.Assert(err, qt.IsNil)
	restoredFacets, err := runtime.Codecs().DecodeFacets(c.Context(), schemaext.Observed, facets)
	c.Assert(err, qt.IsNil)
	var wire struct {
		Features []string
		Payload  []byte
	}
	c.Assert(json.Unmarshal(facets[0].Value.Payload, &wire), qt.IsNil)
	c.Assert(wire.Features, qt.DeepEquals, []string{"tables"})
	c.Assert(string(wire.Payload), qt.Equals, `{"seconds":9007199254740993}`)
	c.Assert(connection.Close(), qt.IsNil)
	host.Close()
	values, err := restoredFacets.Values()
	c.Assert(err, qt.IsNil)
	report, err := runtime.ReportFeatures(c.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed, Values: values})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Definitions, qt.DeepEquals, []schemaext.ReportDefinition{{Kind: kind, DisplayName: "retention values"}})
	c.Assert(report.Values, qt.DeepEquals, []schemaext.ValueReport{{Kind: kind}})
	offline, err := runtime.Codecs().EncodeObjects(c.Context(), schemaext.Observed, restored)
	c.Assert(err, qt.IsNil)
	c.Assert(offline, qt.DeepEquals, objects)
	offlineFacets, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Observed, restoredFacets)
	c.Assert(err, qt.IsNil)
	c.Assert(offlineFacets, qt.DeepEquals, facets)
}

func TestProviderInspectionRefusalDiscardsWholeCatalog(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "observed-models")
	for _, name := range []string{"coverage-mismatch", "bad-feature", "missing-coverage", "bad-payload", "bad-database", "bad-parent", "scoped-foreign-database", "scoped-unbound-database", "scoped-normalized-database"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			connection := observedConnection(c, host, name)
			db, err := connection.Reader().ReadSchema()
			c.Assert(err, qt.IsNotNil)
			c.Assert(db, qt.IsNil)
		})
	}
	for _, name := range []string{"scope-missing", "scope-renamed", "scope-widened", "scope-outside", "scope-table-outside", "scope-foreign-database"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			connection := observedConnection(c, host, name)
			db, err := dbschema.ReadSchemaWithSchemas(connection, []string{"main"})
			c.Assert(err, qt.IsNotNil)
			c.Assert(db, qt.IsNil)
		})
	}
}

func TestProviderInspectionCancellationDiscardsWholeCatalog(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "observed-models")
	connection := observedConnection(c, host, "slow-normalize")
	reader := connection.Reader()
	ctx, cancel := context.WithTimeout(c.Context(), time.Second)
	defer cancel()
	db, err := reader.ReadSchemaContext(ctx)
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(db, qt.IsNil)
}

func TestNativeCLIInspectReportsProviderPayloadOmissions(t *testing.T) {
	c := qt.New(t)
	executable := independentCLIProviderCommand(c, "observed-models")
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	spec := providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}
	f := newProviderCLI(c, spec)
	host, err := providerhost.Start(c.Context(), spec)
	c.Assert(err, qt.IsNil)
	c.Cleanup(host.Close)
	ctx, err := dbschema.WithNativeConnections(c.Context(), host.Connections())
	c.Assert(err, qt.IsNil)
	connection, err := dbschema.ConnectToDatabase(ctx, f.address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(connection.Close(), qt.IsNil) })
	c.Assert(connection.Writer().ExecuteSQL(ctx, `CREATE TABLE "a.b" ("Id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
	c.Assert(connection.Close(), qt.IsNil)
	host.Close()
	output, diagnostics, code := f.run(c, c.Context(), "inspect", "--format", "json")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(json.Valid([]byte(output)), qt.IsTrue)
	c.Assert(output, qt.Contains, `"a.b"`)
	c.Assert(output, qt.Not(qt.Contains), "9007199254740993")
	c.Assert(diagnostics, qt.Contains, "retention values (3)")
	c.Assert(strings.Count(diagnostics, "retention values (1)"), qt.Equals, 2)
}

func TestProviderInspectionRetainsNativeNamespaceAuthority(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "observed-models")
	connection := observedConnection(c, host, "scoped-native")
	db, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	kind := schemaext.Kind("example.org/fixture/retention")
	missing := db.FeatureObjects.Refs()[0]
	missing.Name = objectidentity.Part{Source: "Missing", Normalized: "missing"}
	c.Assert(db.FeatureCoverage.Lookup(kind, missing).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.KindRecords()[0].Knowledge.Unscoped().State, qt.Equals, schemaext.Uninspected)
	for _, test := range []struct {
		name   string
		mutate func(*objectidentity.ID)
	}{
		{"another database", func(ref *objectidentity.ID) { ref.Database = objectidentity.Part{Source: "other", Normalized: "other"} }},
		{"another catalog", func(ref *objectidentity.ID) { ref.Catalog = objectidentity.Part{} }},
		{"another schema", func(ref *objectidentity.ID) { ref.Schema = objectidentity.Part{Source: "public", Normalized: "public"} }},
		{"different path components", func(ref *objectidentity.ID) {
			ref.Namespace = objectidentity.NewParts(objectidentity.Part{Source: "a", Normalized: "a"}, objectidentity.Part{Source: "b/c", Normalized: "b/c"})
		}},
		{"uninspected nested path", func(ref *objectidentity.ID) {
			ref.Namespace = objectidentity.NewParts(append(ref.Namespace.Values(), objectidentity.Part{Source: "hidden", Normalized: "hidden"})...)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := missing
			test.mutate(&ref)
			c.Assert(db.FeatureCoverage.Lookup(kind, ref).State, qt.Equals, schemaext.Uninspected)
		})
	}
	builder := objectidentity.NewBuilder(connection.Info().IdentifierSemantics)
	c.Assert(db.FeatureCoverage.Lookup(kind, builder.TableParts("private", "missing")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(db.FeatureCoverage.Lookup(kind, builder.ColumnParts("private", "missing", "column")).State, qt.Equals, schemaext.Uninspected)
	target, err := schemaext.NewTargetSelection("another")
	c.Assert(err, qt.IsNil)
	foreign, err := db.FeatureCoverage.ForTarget(target)
	c.Assert(err, qt.IsNil)
	c.Assert(foreign.Lookup(kind, missing).State, qt.Equals, schemaext.Uninspected)
	document, err := runtime.Codecs().EncodeCoverage(c.Context(), schemaext.Observed, db.FeatureCoverage)
	c.Assert(err, qt.IsNil)
	wire, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	c.Assert(string(wire), qt.Contains, `"source":"Private"`)
	var decoded schemaext.CoverageDocument
	c.Assert(json.Unmarshal(wire, &decoded), qt.IsNil)
	restored, err := runtime.Codecs().DecodeCoverage(c.Context(), decoded)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Lookup(kind, missing).State, qt.Equals, schemaext.Complete)
	c.Assert(connection.Close(), qt.IsNil)
	host.Close()
	c.Assert(restored.Lookup(kind, missing).State, qt.Equals, schemaext.Complete)
}

func TestFilteredInspectionMapsCommonAndNativeNamespaces(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "observed-models")
	connection := observedConnection(c, host, "scope-parent-unknown")
	db, err := dbschema.ReadSchemaWithSchemas(connection, []string{"main"})
	c.Assert(err, qt.IsNil)
	kind := schemaext.Kind("example.org/fixture/retention")
	builder := objectidentity.NewBuilder(connection.Info().IdentifierSemantics)
	missing := builder.TableParts("", "missing")
	column := builder.ColumnParts("", "missing", "other")
	unknown := builder.ColumnParts("", "Missing.Parent", "other")
	c.Assert(db.FeatureCoverage.Lookup(kind, missing).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(kind, column).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(kind, unknown).State, qt.Equals, schemaext.Uninspected)
	c.Assert(db.FeatureCoverage.Lookup(kind, builder.TableParts("", "Missing.Parent")).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(kind, builder.TableParts("other", "missing")).State, qt.Equals, schemaext.Uninspected)
	foreign := missing
	foreign.Namespace = objectidentity.NewParts(objectidentity.Part{Source: "extra", Normalized: "extra"})
	c.Assert(db.FeatureCoverage.Lookup(kind, foreign).State, qt.Equals, schemaext.Uninspected)
	foreign = missing
	foreign.Kind = objectidentity.Kind(kind)
	c.Assert(db.FeatureCoverage.Lookup(kind, foreign).State, qt.Equals, schemaext.Uninspected)
	native := db.FeatureObjects.Refs()[0]
	native.Name = objectidentity.Part{Source: "Missing", Normalized: "missing"}
	c.Assert(db.FeatureCoverage.Lookup(kind, native).State, qt.Equals, schemaext.Complete)
	document, err := runtime.Codecs().EncodeCoverage(c.Context(), schemaext.Observed, db.FeatureCoverage)
	c.Assert(err, qt.IsNil)
	restored, err := runtime.Codecs().DecodeCoverage(c.Context(), document)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Lookup(kind, unknown).State, qt.Equals, schemaext.Uninspected)
	c.Assert(restored.Lookup(kind, column).State, qt.Equals, schemaext.Complete)
}

func TestFilteredInspectionPreservesNonASCIICommonIdentifiers(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "observed-models")
	connection := observedConnection(c, host, "ÄBC")
	db, err := dbschema.ReadSchemaWithSchemas(connection, []string{"main"})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[0].Name, qt.Equals, "ÄBC")
	builder := objectidentity.NewBuilder(connection.Info().IdentifierSemantics)
	c.Assert(db.FeatureCoverage.Lookup("example.org/fixture/retention", builder.TableParts("", "ÄBC")).State, qt.Equals, schemaext.Complete)
}
