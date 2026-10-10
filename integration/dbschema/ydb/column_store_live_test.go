//go:build integration

package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbschema"
)

const columnStoreSchema = "ptah_ydb_column_store"

// columnStoreFacets declares the events table a column table hashed by id in
// one shard, with ttl as its tiered TTL.
func columnStoreFacets(ttl *ydbschema.TieredTTL) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnStore{ColumnStore: ydbschema.ColumnStore{HashColumns: []string{"id"}, Partitions: 1, TTL: ttl}}))
}

func columnStoreDeclaration(caps capability.Capabilities) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events", Schema: columnStoreSchema, Facets: columnStoreFacets(nil)}},
		Fields: []schemamodel.Field{{StructName: "Event", Name: "id", Type: "Uint64", Primary: true}, {StructName: "Event", Name: "body", Type: "Utf8", Nullable: true}},
	}
	if caps.Has(capability.LocalBloomIndexes) {
		db.Indexes = []schemamodel.Index{
			{StructName: "Event", Name: "bf", Fields: []string{"body"}, Type: "bloom_filter", StorageParams: map[string]string{"false_positive_probability": "0.01"}},
			{StructName: "Event", Name: "ng", Fields: []string{"body"}, Type: "bloom_ngram_filter"},
		}
	}
	schemamodel.Finalize(db)
	return db
}

// The old line keeps column storage and hash partitioning, while the newest
// line also keeps local indexes. DescribeTable alone cannot prove either index.
func TestYDBColumnStore_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			schemas := []string{columnStoreSchema}
			dropTables(c, conn, schemas)
			c.Cleanup(func() { dropTables(c, conn, schemas) })
			declared := columnStoreDeclaration(conn.Info().Capabilities)
			apply(c, conn, planAgainst(c, conn, declared, schemas))
			live := readScoped(c, conn, schemas)
			c.Assert(live.Tables, qt.HasLen, 1)
			store, held, err := schemaext.FacetAs[*ydbschema.ObservedColumnStore](live.Tables[0].Facets, ydbschema.ColumnStoreKind)
			c.Assert(err, qt.IsNil)
			c.Assert(held, qt.IsTrue)
			c.Assert(store.Desired(), qt.DeepEquals, must.Must(ydbschema.DeclaredColumnStore(declared.Tables[0].Facets)))
			c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
		})
	}
}

// The source and column table share a migration: the source must exist before
// the TTL is enabled. Replacing it must detach and restore the held policy.
func TestYDBColumnStore_TieredTTL(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	setClusterFlags(c, line, externalSourcesOn, clusterFlag{yaml: "enable_tiering_in_column_shard", page: "EnableTieringInColumnShard", on: true})
	conn := openYDB(c, line)
	schemas := []string{columnStoreSchema}
	const access = "ptah_column_ttl_access"
	const secret = "ptah_column_ttl_secret" // #nosec G101 -- a fixture secret object name, not a credential
	apply(c, conn, []string{
		"CREATE OBJECT " + access + " (TYPE SECRET) WITH (value='fixture-access')",
		"CREATE OBJECT " + secret + " (TYPE SECRET) WITH (value='fixture-secret')",
	})
	c.Cleanup(func() {
		dropTables(c, conn, schemas)
		for _, statement := range []string{"DROP EXTERNAL DATA SOURCE IF EXISTS `" + columnStoreSchema + "/archive`", "DROP OBJECT " + access + " (TYPE SECRET)", "DROP OBJECT " + secret + " (TYPE SECRET)"} {
			c.Check(conn.Writer().ExecuteSQL(context.Background(), statement), qt.IsNil)
		}
	})
	declared := columnStoreDeclaration(capability.YDB251())
	archive := func(location string) schemaext.Objects {
		return must.Must(schemaext.NewObjects(ydbexternal.DesiredSourceObject(columnStoreSchema, "archive", "", ydbexternal.DataSource{
			SourceType: "ObjectStorage", Location: location, AuthMethod: "AWS",
			Options: map[string]string{"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID_SECRET_NAME": access, "AWS_SECRET_ACCESS_KEY_SECRET_NAME": secret}})))
	}
	declared.FeatureObjects = archive("https://column.invalid/archive/")
	declared.Tables[0].Facets = columnStoreFacets(&ydbschema.TieredTTL{Column: "id", Unit: "SECONDS", Tiers: []ydbschema.TTLTier{{Interval: "P1D", ExternalSource: "/local/" + columnStoreSchema + "/archive"}, {Interval: "P7D"}}})
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	declared.FeatureObjects = archive("https://column.invalid/replaced/")
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	declared.Tables[0].Facets = must.Must(ttlFacets(&ydbschema.TTL{Column: "id", Interval: "P7D", Unit: "SECONDS"}).Merge(columnStoreFacets(nil)))
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
}

func TestYDBColumnStore_IndexChanges(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	schemas := []string{columnStoreSchema}
	dropTables(c, conn, schemas)
	c.Cleanup(func() { dropTables(c, conn, schemas) })
	declared := columnStoreDeclaration(capability.YDB262())
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	declared.Indexes[0].Name = "renamed_bloom"
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	declared.Indexes[0].StorageParams["false_positive_probability"] = "0.02"
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
}
