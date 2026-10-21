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
	for _, name := range []string{"coverage-mismatch", "bad-feature", "missing-coverage", "bad-payload", "bad-database", "bad-parent"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			connection := observedConnection(c, host, name)
			db, err := connection.Reader().ReadSchema()
			c.Assert(err, qt.IsNotNil)
			c.Assert(db, qt.IsNil)
		})
	}
	connection := observedConnection(c, host, "filtered")
	db, err := dbschema.ReadSchemaWithSchemas(connection, []string{"main"})
	c.Assert(err, qt.IsNotNil)
	c.Assert(db, qt.IsNil)
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

func TestNativeCLIInspectRefusesUnrepresentableProviderScope(t *testing.T) {
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
	c.Assert(code, qt.Equals, 2)
	c.Assert(output, qt.Equals, "")
	c.Assert(diagnostics, qt.Contains, "provider inspect failed")
}
