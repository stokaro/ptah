package schemadiff_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestCompareWithDatabaseInfoRefusesUnrepresentableMaterializedViewRefreshStrategy(t *testing.T) {
	tests := []struct {
		dialect  string
		strategy string
	}{
		{dialect: platform.Postgres, strategy: "concurrently"},
		{dialect: platform.Postgres, strategy: "every 5 minutes"},
		{dialect: platform.ClickHouse, strategy: "concurrently"},
		{dialect: platform.ClickHouse, strategy: "every 5 minutes"},
	}

	for _, test := range tests {
		t.Run(test.dialect+"/"+test.strategy, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				materializedViewDesired(test.strategy),
				materializedViewCurrent(),
				types.DBInfo{Dialect: test.dialect},
				nil,
			)

			c.Assert(diff, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(
				err,
				qt.ErrorMatches,
				fmt.Sprintf(
					`%s cannot represent materialized view "analytics.user_counts" refresh strategy %q; "manual" is the only strategy, .*`,
					test.dialect,
					test.strategy,
				),
			)
		})
	}
}

func TestCompareWithDatabaseInfoKeepsManualMaterializedViewSynchronized(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.ClickHouse} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				materializedViewDesired("manual"),
				materializedViewCurrent(),
				types.DBInfo{Dialect: dialect},
				nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.MaterializedViewsAdded, qt.HasLen, 0)
			c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
			c.Assert(diff.MaterializedViewsRemoved, qt.HasLen, 0)
		})
	}
}

func materializedViewDesired(strategy string) *goschema.Database {
	return &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		Name:            "analytics.user_counts",
		Body:            "SELECT count(*) AS total FROM analytics.users",
		RefreshStrategy: strategy,
	}}}
}

func materializedViewCurrent() *types.DBSchema {
	return &types.DBSchema{MatViews: []types.DBMatView{{
		Name:            "user_counts",
		Schema:          "analytics",
		Body:            "SELECT count(*) AS total FROM analytics.users",
		RefreshStrategy: "manual",
	}}}
}
