package clickhouse_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/dbschema/clickhouse"
	"ptah.run/internal/dbschema/dbtest"
)

// storedViews are the statements system.tables.create_table_query holds for
// materialized views of each storage shape, as 24.10 and 26.9 store them, and
// what the read knows of each view's storage.
var storedViews = []struct {
	name, statement string
	want            schemaext.Knowledge
}{
	{name: "plain", want: schemaext.Knowledge{State: schemaext.Complete},
		statement: "CREATE MATERIALIZED VIEW analytics.plain (`c` UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 AS SELECT c FROM analytics.src"},
	{name: "described", want: schemaext.Knowledge{State: schemaext.Complete},
		statement: "CREATE MATERIALIZED VIEW analytics.described (`c` UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 " +
			"DEFINER = ptah_user SQL SECURITY DEFINER COMMENT 'hello' AS SELECT c FROM analytics.src"},
	{name: "refreshed", want: schemaext.Knowledge{State: schemaext.Complete},
		statement: "CREATE MATERIALIZED VIEW analytics.refreshed REFRESH EVERY 1 HOUR (`c` UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 AS SELECT c FROM analytics.src"},
	{name: "no_settings", want: schemaext.Knowledge{State: schemaext.Complete},
		statement: "CREATE MATERIALIZED VIEW analytics.no_settings (`c` UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT c FROM analytics.src"},
	{name: "keyed", want: schemaext.Knowledge{State: schemaext.Unrepresentable,
		Reason: "the view stores its rows with ENGINE = MergeTree ORDER BY c SETTINGS index_granularity = 8192, which Ptah does not model"},
		statement: "CREATE MATERIALIZED VIEW analytics.keyed (`c` UInt64) ENGINE = MergeTree ORDER BY c SETTINGS index_granularity = 8192 AS SELECT c FROM analytics.src"},
	{name: "summed", want: schemaext.Knowledge{State: schemaext.Unrepresentable,
		Reason: "the view stores its rows with ENGINE = SummingMergeTree PARTITION BY c % 10 ORDER BY c SETTINGS index_granularity = 1024, which Ptah does not model"},
		statement: "CREATE MATERIALIZED VIEW analytics.summed (`c` UInt64, `n` UInt64) ENGINE = SummingMergeTree PARTITION BY c % 10 ORDER BY c " +
			"SETTINGS index_granularity = 1024 AS SELECT c, count() AS n FROM analytics.src GROUP BY c"},
	{name: "granular", want: schemaext.Knowledge{State: schemaext.Unrepresentable,
		Reason: "the view stores its rows with ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024, which Ptah does not model"},
		statement: "CREATE MATERIALIZED VIEW analytics.granular (`c` UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024 AS SELECT c FROM analytics.src"},
	{name: "expiring", want: schemaext.Knowledge{State: schemaext.Unrepresentable,
		Reason: "the view stores its rows with ENGINE = MergeTree ORDER BY tuple() TTL toDateTime(c) + toIntervalDay(1) SETTINGS index_granularity = 8192, which Ptah does not model"},
		statement: "CREATE MATERIALIZED VIEW analytics.expiring (`c` UInt64) ENGINE = MergeTree ORDER BY tuple() TTL toDateTime(c) + toIntervalDay(1) " +
			"SETTINGS index_granularity = 8192 AS SELECT c FROM analytics.src"},
	// A column named like a clause keyword is part of the sorting key, not
	// the start of a COMMENT clause.
	{name: "by_comment", want: schemaext.Knowledge{State: schemaext.Unrepresentable,
		Reason: "the view stores its rows with ENGINE = MergeTree ORDER BY comment, which Ptah does not model"},
		statement: "CREATE MATERIALIZED VIEW analytics.by_comment (`comment` String) ENGINE = MergeTree ORDER BY comment AS SELECT comment FROM analytics.src"},
	{name: "routed", want: schemaext.Knowledge{State: schemaext.Unrepresentable,
		Reason: "the view sends its rows to another table with TO analytics.dst, which Ptah does not model"},
		statement: "CREATE MATERIALIZED VIEW analytics.routed TO analytics.dst (`c` UInt64) AS SELECT c FROM analytics.src"},
	{name: "unreadable", want: schemaext.Knowledge{State: schemaext.Unrepresentable,
		Reason: "the storage clause of the stored CREATE statement could not be read"},
		statement: "CREATE MATERIALIZED VIEW analytics.unreadable AS SELECT c FROM analytics.src"},
}

func storedViewsQuery(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	if strings.Contains(query, "engine = 'MaterializedView'") && !strings.Contains(query, "engine LIKE '%MergeTree'") {
		result := dbtest.QueryResult{Columns: []string{"name", "as_select", "comment", "create_table_query"}}
		for _, view := range storedViews {
			result.Rows = append(result.Rows, []driver.Value{view.name, "SELECT c FROM analytics.src", "", view.statement})
		}
		return result, nil
	}
	return clickHouseViewReaderQuery(query, args)
}

// A materialized view stored any way but the one Ptah creates it with is
// recorded as a view whose storage the read could not describe, with the
// clause the server stores, so nothing reads it as Ptah's storage.
func TestReadSchemaRecordsUndescribedViewStorage(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, storedViewsQuery)
	observed, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(observed.MatViews, qt.HasLen, len(storedViews))
	identities := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	for _, view := range storedViews {
		t.Run(view.name, func(t *testing.T) {
			c := qt.New(t)
			subject := identities.SchemaScopedParts(objectidentity.KindMatView, "analytics", view.name)
			c.Assert(observed.FeatureCoverage.Lookup(chschema.ViewStorageKind, subject), qt.DeepEquals, view.want)
			c.Assert(observed.FeatureCoverage.Lookup(chschema.RefreshKind, subject).State, qt.Equals, schemaext.Complete)
		})
	}
}
