package clickhouse_test

import (
	"database/sql/driver"
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

func settingsReaderQuery(engine string) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "engine LIKE '%MergeTree'") {
			return dbtest.QueryResult{Columns: []string{"name", "comment", "sorting_key", "primary_key", "engine_full", "partition_key", "sampling_key"}, Rows: [][]driver.Value{
				{"events", "", "id", "", engine, "", ""},
				{"schema_migrations", "", "id", "id", "MergeTree ORDER BY id", "", ""},
			}}, nil
		}
		return clickHouseViewReaderQuery(query, args)
	}
}

func TestReadSchemaCapturesSettingsAndOnlyInspectedTableCoverage(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, settingsReaderQuery("MergeTree ORDER BY id PRIMARY KEY tuple() SETTINGS index_granularity = 4096"))
	observed, err := clickhouse.NewClickHouseReader(db.SQL, "default").ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 1)
	value, found, err := schemaext.FacetAs[*chschema.ObservedTable](observed.Tables[0].Facets, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.DeepEquals, &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", Settings: "index_granularity = 4096"})
	identities := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	subject := identities.TableParts("", "events")
	c.Assert(observed.FeatureCoverage.Lookup(chschema.TableKind, subject).State, qt.Equals, schemaext.Complete)
	for _, name := range []string{"not_read", "schema_migrations"} {
		c.Assert(observed.FeatureCoverage.Lookup(chschema.TableKind, identities.TableParts("", name)).State, qt.Equals, schemaext.Uninspected)
	}
	runtime := must.Must(builtin.New())
	desired, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), observed, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(desired.FeatureCoverage.Lookup(chschema.TableKind, subject).State, qt.Equals, schemaext.Complete)
	c.Assert(desired.FeatureCoverage.Representation(), qt.Equals, schemaext.Desired)
}

func TestReadSchemaRejectsAnIncompleteSettingsObservation(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, settingsReaderQuery(""))
	observed, err := clickhouse.NewClickHouseReader(db.SQL, "default").ReadSchemaContext(t.Context())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(observed, qt.IsNil)
}
