package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/migration/schemadiff"
)

// An excluded object must survive a plan that removes the database containing
// it. Filtering its table alone cannot prevent DROP DATABASE from removing it.
func TestCompareWithDatabaseInfo_RefusesToDropADatabaseWithExcludedObjects(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range []struct {
			name    string
			current catalog.Database
			exclude string
			want    string
		}{
			{name: "table", current: catalog.Database{Tables: []catalog.Table{{Schema: "app", Name: "t"}}}, exclude: "app.t", want: "table app.t"},
			{name: "column", current: catalog.Database{Tables: []catalog.Table{{Schema: "app", Name: "t", Columns: []catalog.Column{{Name: "id", DataType: "int"}}}}}, exclude: "app.t.id", want: "column app.t.id"},
			{name: "view", current: catalog.Database{Views: []catalog.View{{Schema: "app", Name: "v"}}}, exclude: "app.v", want: "view app.v"},
			{name: "routine", current: catalog.Database{Functions: []catalog.Function{{Schema: "app", Name: "f"}}}, exclude: "app.f", want: "function app.f"},
			{name: "unread kind", exclude: "*[type=function]", want: "objects excluded by"},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				runtime := must.Must(builtin.New())
				current := test.current
				current.Schemas = []catalog.Schema{{Name: "app"}}
				filtered, err := atlasfilter.ScopeDatabase(&current, atlasfilter.Scope{
					Selections: runtime.Selections(), Dialect: dialect, RealmRelativePatterns: true, Exclude: []string{test.exclude},
				})
				c.Assert(err, qt.IsNil)
				diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &schemamodel.Database{}, filtered,
					catalog.ServerInfo{Dialect: dialect, WholeServer: true}, nil, runtime)
				c.Assert(err, qt.ErrorMatches, `(?s).*cannot drop database "app".*`+test.want+`.*outside the selection.*`)
				c.Assert(diff, qt.IsNil)
			})
		}
	}
}

func TestCompareWithDatabaseInfo_KeepsExcludedDatabaseProtectionAcrossFilters(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}},
		Tables:  []catalog.Table{{Schema: "app", Name: "t"}, {Schema: "app", Name: "u"}},
	}
	scope := atlasfilter.Scope{Selections: runtime.Selections(), Dialect: platform.MySQL, RealmRelativePatterns: true, Exclude: []string{"app.t"}}
	filtered, err := atlasfilter.ScopeDatabase(current, scope)
	c.Assert(err, qt.IsNil)
	scope.Exclude = []string{"app.u"}
	filtered, err = atlasfilter.ScopeDatabase(filtered, scope)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &schemamodel.Database{}, filtered,
		catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}, nil, runtime)
	c.Assert(err, qt.ErrorMatches, `(?s).*cannot drop database "app".*table app.t.*outside the selection.*`)
	c.Assert(diff, qt.IsNil)
	c.Assert(current.Tables, qt.HasLen, 2)
}

func TestCompareWithDatabaseInfo_ExcludedTableAllowsOtherTableRemoval(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{Schemas: []catalog.Schema{{Name: "app"}}, Tables: []catalog.Table{
		{Schema: "app", Name: "t"}, {Schema: "app", Name: "u"},
	}}
	filtered, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: platform.MySQL, RealmRelativePatterns: true, Exclude: []string{"app.t"},
	})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}}, filtered,
		catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.SchemasRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 1)
	c.Assert(diff.TablesRemoved[0].Name, qt.Equals, "app.u")
}
