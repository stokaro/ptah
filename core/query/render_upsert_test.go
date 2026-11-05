package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/query"
)

// UPSERT INTO is YDB's statement. Every other dialect names the key an upsert
// watches, which the builder does not know for a table, so it refuses rather
// than guess one.
func TestRenderInsert_FailurePath_UpsertOutsideYDB(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.SQLite, platform.SQLServer} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			got, args, err := query.RenderInsert(
				query.UpsertInto("users").Columns("id", "name").Values(int64(1), "a").Build(), dialect)
			c.Assert(err, qt.ErrorMatches, "renderer: "+dialect+" has no UPSERT statement: UPSERT INTO is YDB's, "+
				"and here an upsert names the key it watches; use OnConflictDoUpdate")
			c.Assert(got, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

// RETURNING follows the target's key on every dialect: SQLite takes it from
// 3.35, and a caller that names the older line is refused.
func TestRenderInsertWithCapabilities_FailurePath_ReturningOnOldSQLite(t *testing.T) {
	c := qt.New(t)
	got, args, err := query.RenderInsertWithCapabilities(
		query.InsertInto("t").Columns("a").Values(int64(1)).Returning("id").Build(),
		platform.SQLite, capability.SQLite324())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "renderer: RETURNING, which requires target capability returning_clause, "+
		"unavailable on this sqlite target")
	c.Assert(got, qt.Equals, "")
	c.Assert(args, qt.IsNil)
}
