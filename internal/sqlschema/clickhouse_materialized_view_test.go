package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// readClickHouseRefresh reads one materialized view from a ClickHouse schema
// file and returns its declared refresh schedule and whether it has one.
func readClickHouseRefresh(c *qt.C, sql string) (*chschema.DesiredRefresh, bool) {
	c.Helper()
	database, _, err := sqlschema.Read([]byte(sql), platform.ClickHouse)
	c.Assert(err, qt.IsNil)
	c.Assert(database.MaterializedViews, qt.HasLen, 1)
	c.Assert(database.MaterializedViews[0].Body, qt.Equals, "SELECT count() AS c FROM src")
	c.Assert(database.FeatureCoverage, qt.DeepEquals, must.Must(chsource.RefreshCoverage()))
	refresh, found, err := schemaext.FacetAs[*chschema.DesiredRefresh](database.MaterializedViews[0].Facets, chschema.RefreshKind)
	c.Assert(err, qt.IsNil)
	return refresh, found
}

// TestRead_ClickHouseMaterializedViewRefresh_HappyPath pins that a ClickHouse
// schema file declares a refresh schedule the way an annotation does.
//
// The reader refused `CREATE MATERIALIZED VIEW ... REFRESH` with "expected
// 'AS', got 'REFRESH'", so no SQL source could state a schedule on either
// side of a diff (stokaro/ptah#4298).
func TestRead_ClickHouseMaterializedViewRefresh_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "schedule and storage",
			sql:  "CREATE MATERIALIZED VIEW hourly REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY tuple() AS SELECT count() AS c FROM src;",
			want: "EVERY 1 HOUR",
		},
		{
			name: "schedule only, in another spelling",
			sql:  "CREATE MATERIALIZED VIEW hourly REFRESH every 60 minute offset 5 minute APPEND AS SELECT count() AS c FROM src;",
			want: "EVERY 1 HOUR OFFSET 5 MINUTE APPEND",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			refresh, found := readClickHouseRefresh(c, test.sql)
			c.Assert(found, qt.IsTrue)
			c.Assert(refresh.Clause(), qt.Equals, test.want)
		})
	}
}

// TestRead_ClickHouseMaterializedViewWithoutRefresh is the control: a view
// with no REFRESH clause declares none, with or without the storage clause.
func TestRead_ClickHouseMaterializedViewWithoutRefresh(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "storage, lower-case keywords",
			sql:  "CREATE MATERIALIZED VIEW hourly engine = MergeTree order by tuple() AS SELECT count() AS c FROM src;",
		},
		{
			name: "no clause",
			sql:  "CREATE MATERIALIZED VIEW hourly AS SELECT count() AS c FROM src;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, found := readClickHouseRefresh(c, test.sql)
			c.Assert(found, qt.IsFalse)
		})
	}
}

// TestRead_ClickHouseMaterializedViewRefresh_FailurePath pins the refusals: a
// schedule the owner's grammar rejects, and a storage clause other than the
// one Ptah creates a view with, which a read would otherwise replace in
// silence.
func TestRead_ClickHouseMaterializedViewRefresh_FailurePath(t *testing.T) {
	t.Run("schedule with SETTINGS", func(t *testing.T) {
		c := qt.New(t)
		database, _, err := sqlschema.Read([]byte(
			"CREATE MATERIALIZED VIEW hourly REFRESH EVERY 1 HOUR SETTINGS refresh_retries = 2 AS SELECT count() AS c FROM src;",
		), platform.ClickHouse)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(err, qt.ErrorMatches, `(?s)materialized view hourly: REFRESH: .*`)
		c.Assert(database.MaterializedViews, qt.HasLen, 0)
	})
	t.Run("another storage", func(t *testing.T) {
		c := qt.New(t)
		database, _, err := sqlschema.Read([]byte(
			"CREATE MATERIALIZED VIEW hourly ENGINE = ReplacingMergeTree ORDER BY c AS SELECT count() AS c FROM src;",
		), platform.ClickHouse)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `(?s).*materialized view hourly: storage "ENGINE = ReplacingMergeTree ORDER BY c" is not modeled.*`)
		c.Assert(database.MaterializedViews, qt.HasLen, 0)
	})
}

// TestRead_ClickHouseRenderedMaterializedViewReadsBack holds the reader's
// storage clause to the renderer's: a view the ClickHouse renderer writes,
// schedule included, reads back as the view it was rendered from.
func TestRead_ClickHouseRenderedMaterializedViewReadsBack(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateMaterializedView("hourly").SetBody("SELECT count() AS c FROM src")
	node.Facets = must.Must(chsource.RefreshFacets("EVERY 2 HOUR RANDOMIZE FOR 10 MINUTE"))
	rendered, err := builtin.RenderSQL(platform.ClickHouse, node)
	c.Assert(err, qt.IsNil)

	refresh, found := readClickHouseRefresh(c, rendered)
	c.Assert(found, qt.IsTrue)
	c.Assert(refresh.Clause(), qt.Equals, "EVERY 2 HOUR RANDOMIZE FOR 10 MINUTE")
}
