package clickhouse_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/dbschema/clickhouse"
	"ptah.run/internal/dbschema/dbtest"
)

func parameterizedIndexQuery(indexType string, granularity uint64) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "engine LIKE '%MergeTree'") {
			return settingsReaderQuery("MergeTree ORDER BY id")(query, args)
		}
		if !strings.Contains(query, "FROM system.data_skipping_indices") {
			return clickHouseIndexPresentReaderQuery(query, args)
		}
		// The short type column cannot reconstruct parameterized index DDL.
		if !strings.Contains(query, "expr, type_full, granularity") {
			return dbtest.QueryResult{}, fmt.Errorf("index inspection must read the complete type expression")
		}
		return dbtest.QueryResult{
			Columns: []string{"table", "name", "expr", "type_full", "granularity"},
			Rows:    [][]driver.Value{{"events", "idx_value", "value", indexType, granularity}},
		}, nil
	}
}

// errNoFullType is the answer of a server whose data_skipping_indices has no
// type_full column.
var errNoFullType = errors.New("missing columns: 'type_full'")

// withoutFullTypeQuery answers like a server without the type_full column.
func withoutFullTypeQuery(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "engine LIKE '%MergeTree'"):
		return settingsReaderQuery("MergeTree ORDER BY id")(query, args)
	case strings.Contains(query, "FROM system.data_skipping_indices") && strings.Contains(query, "type_full"):
		return dbtest.QueryResult{}, errNoFullType
	default:
		return clickHouseIndexPresentReaderQuery(query, args)
	}
}

func TestSkippingIndexReaderPreservesTypeParameters(t *testing.T) {
	for _, indexType := range []string{"set(100)", "bloom_filter(0.01)", "tokenbf_v1(256, 2, 0)"} {
		t.Run(indexType, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, parameterizedIndexQuery(indexType, 64))
			reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")
			schema, err := reader.ReadSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(schema.Indexes, qt.HasLen, 1)
			settings, found, err := schemaext.FacetAs[*chschema.ObservedIndex](schema.Indexes[0].Facets, chschema.IndexKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(settings, qt.DeepEquals, &chschema.ObservedIndex{IndexType: indexType, Granularity: 64})
			c.Assert(schema.Indexes[0].Definition, qt.Equals, "INDEX idx_value value TYPE "+indexType+" GRANULARITY 64")
		})
	}
}

// TestSkippingIndexReaderAttachesOwnedSettings pins where the settings live: a
// ClickHouse-bound facet carrying the whole uint64 range, with the key
// expression left in the common columns and complete coverage for the kind.
func TestSkippingIndexReaderAttachesOwnedSettings(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, parameterizedIndexQuery("minmax", math.MaxUint64))
	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Indexes, qt.HasLen, 1)
	index := schema.Indexes[0]
	c.Assert(index.Columns, qt.DeepEquals, []string{"value"})
	c.Assert(index.Method, qt.Equals, "")
	c.Assert(index.Facets.TargetScope(chschema.IndexKind), qt.DeepEquals, []string{"clickhouse"})
	settings, found, err := schemaext.FacetAs[*chschema.ObservedIndex](index.Facets, chschema.IndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(settings.Granularity, qt.Equals, uint64(math.MaxUint64))
	identities := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	for _, name := range []string{"idx_value", "not_listed"} {
		c.Assert(schema.FeatureCoverage.Lookup(chschema.IndexKind, identities.IndexParts("", "events", name)).State, qt.Equals, schemaext.Complete)
	}
	desired, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), schema, "clickhouse", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	declared, found, err := schemaext.FacetAs[*chschema.DesiredIndex](desired.Indexes[0].Facets, chschema.IndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(declared, qt.DeepEquals, settings.Desired())
	c.Assert(desired.Indexes[0].Type, qt.Equals, "")
}

// TestSkippingIndexReaderWithoutTheCatalogTableClaimsNothing keeps an old
// server's missing catalog table from reading as "no index has settings".
func TestSkippingIndexReaderWithoutTheCatalogTableClaimsNothing(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, settingsReaderQuery("MergeTree ORDER BY id"))
	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Indexes, qt.HasLen, 0)
	subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).IndexParts("", "events", "idx_value")
	c.Assert(schema.FeatureCoverage.Lookup(chschema.IndexKind, subject).State, qt.Equals, schemaext.Uninspected)
}

func TestSkippingIndexReaderRefusesAnIncompleteRow(t *testing.T) {
	for _, test := range []struct {
		name        string
		indexType   string
		granularity uint64
	}{
		{name: "empty type", granularity: 1},
		{name: "zero granularity", indexType: "minmax"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, parameterizedIndexQuery(test.indexType, test.granularity))
			schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(schema, qt.IsNil)
		})
	}
}

// TestSkippingIndexReaderWithoutTheFullTypeColumn_FailurePath refuses a server
// whose catalog has no type_full column, and says which column and which
// release lines. The bare type column drops parameters such as set(100), so a
// read from it could neither rebuild an index nor compare its settings.
func TestSkippingIndexReaderWithoutTheFullTypeColumn_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, withoutFullTypeQuery)
	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())
	c.Assert(err, qt.ErrorIs, errNoFullType)
	c.Assert(err, qt.ErrorMatches, `(?s).*type_full column, which every release line Ptah tests \(24\.10 through 26\.9\) has.*`)
	c.Assert(schema, qt.IsNil)
}
