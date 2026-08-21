package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDatabaseInfoNeverDiffsARefreshStrategy is the comparison half
// of the decision.
//
// This file used to assert that a non-manual strategy failed the comparison and
// that a manual one compared clean. Both were consequences of carrying a field
// no catalog reports: the READ synthesized "manual" for every materialized view
// on both PostgreSQL and ClickHouse, so the comparison's other operand was
// invented. Ptah carries no strategy now, on either side (stokaro/ptah#1625).
//
// What must hold is that an unchanged materialized view compares clean and that
// no comparison can produce a refresh_strategy entry -- the drift key that
// existed to catch a mismatch between two invented values.
func TestCompareWithDatabaseInfoNeverDiffsARefreshStrategy(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.ClickHouse} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				materializedViewDesired(),
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

// TestCompareWithDatabaseInfoReportsABodyChangeWithoutARefreshKey keeps a live
// control on the drift map itself.
//
// A comparison that reported nothing at all would pass the test above, so this
// one makes a real change and asserts that what comes back names the body and
// nothing about refreshing.
func TestCompareWithDatabaseInfoReportsABodyChangeWithoutARefreshKey(t *testing.T) {
	c := qt.New(t)

	desired := materializedViewDesired()
	desired.MaterializedViews[0].Body = "SELECT count(*) AS total FROM analytics.accounts"

	diff, err := schemadiff.CompareWithDatabaseInfo(
		desired,
		materializedViewCurrent(),
		types.DBInfo{Dialect: platform.Postgres},
		nil,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Changes, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
	_, hasRefreshKey := diff.MaterializedViewsModified[0].Changes["refresh_strategy"]
	c.Assert(hasRefreshKey, qt.IsFalse)
}

func materializedViewDesired() *goschema.Database {
	return &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		Name: "analytics.user_counts",
		Body: "SELECT count(*) AS total FROM analytics.users"}}}
}

func materializedViewCurrent() *types.DBSchema {
	return &types.DBSchema{MatViews: []types.DBMatView{{
		Name:   "user_counts",
		Schema: "analytics",
		Body:   "SELECT count(*) AS total FROM analytics.users"}}}
}
