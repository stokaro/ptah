package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A local catalog body matches a declaration that explicitly names the
// connection's database. Both view families use that connection's scope.
func TestClickHouseLocalViewBody_MatchesCurrentDatabase(t *testing.T) {
	for _, test := range []struct{ name, declared, held string }{
		{name: "source", declared: "SELECT id FROM analytics.users", held: "SELECT id FROM users"},
		{name: "alias", declared: "SELECT analytics.id FROM analytics.users AS analytics", held: "SELECT analytics.id FROM users AS analytics"},
		{name: "quoted source", declared: "SELECT id FROM `analytics`.`users`", held: "SELECT id FROM users"},
		{name: "comment", declared: "SELECT 'analytics.users' AS id FROM analytics.users /* kept */;", held: "SELECT 'analytics.users' AS id FROM users"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, held := localViewBodies(test.declared, test.held)

			diff, err := schemadiff.CompareWithOptions(t.Context(), desired, held, liveOptions(), must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsFalse)
			c.Assert(desired.Views[0].Body, qt.Equals, test.declared)
			c.Assert(held.Views[0].Body, qt.Equals, test.held)
		})
	}
}

func TestClickHouseLocalViewBody_KeepsDifferentQueries(t *testing.T) {
	for _, test := range []struct{ name, declared, held string }{
		{name: "foreign database", declared: "SELECT id FROM other.users", held: "SELECT id FROM users"},
		{name: "different name case", declared: "SELECT id FROM analytics.Users", held: "SELECT id FROM users"},
		{name: "different literal", declared: "SELECT 'analytics.users' AS id FROM analytics.users", held: "SELECT 'users' AS id FROM users"},
		{name: "quoted keyword", declared: "SELECT NULL AS id FROM analytics.users", held: "SELECT `NULL` AS id FROM users"},
		{name: "multipart column", declared: "SELECT analytics.users.id FROM analytics.users", held: "SELECT users.id FROM users"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, held := localViewBodies(test.declared, test.held)

			diff, err := schemadiff.CompareWithOptions(t.Context(), desired, held, liveOptions(), must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, 1)
			c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
		})
	}
}

func localViewBodies(declared, held string) (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{
		Views:             []schemamodel.View{{Name: "v", Body: declared}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: declared}},
	}, &catalog.Database{
		Views:    []catalog.View{{Name: "v", Body: held}},
		MatViews: []catalog.MaterializedView{{Name: "mv", Body: held}},
	}
}
