package atlasschema

// White-box testing required: inDevDatabase decides which objects of the
// target's read the rehearsal baseline moves to the dev database, and only a
// live server reaches it through the public path. The cases that separate a
// qualifier from an alias of the same name, and a view of the target's
// database from one of another, need states no fixture server holds at once.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
)

// targetRead is a one-database MySQL read of app: a view over its own table, a
// view whose alias is spelled like the database, a view of another database, a
// routine and a sequence of app, and a table, which a read leaves unqualified.
func targetRead() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t1"}},
		Views: []catalog.View{
			{Name: "v1", Schema: "app", Body: "select `app`.`t1`.`id` AS `id` from `app`.`t1`"},
			{Name: "v_alias", Schema: "app", Body: "select `app`.`id` AS `id` from `app`.`t1` `app`"},
			{Name: "v_other", Schema: "other", Body: "select `other`.`x`.`id` AS `id` from `other`.`x`"},
		},
		Functions: []catalog.Function{{Name: "f1", Schema: "app"}},
		Sequences: []catalog.Sequence{{Name: "s1", Schema: "app"}},
	}
}

func TestInDevDatabase_HappyPath(t *testing.T) {
	moved := &catalog.Database{
		Tables: []catalog.Table{{Name: "t1"}},
		Views: []catalog.View{
			{Name: "v1", Body: "select `appdev`.`t1`.`id` AS `id` from `appdev`.`t1`"},
			{Name: "v_alias", Body: "select `app`.`id` AS `id` from `appdev`.`t1` `app`"},
			{Name: "v_other", Schema: "other", Body: "select `other`.`x`.`id` AS `id` from `other`.`x`"},
		},
		Functions: []catalog.Function{{Name: "f1"}},
		Sequences: []catalog.Sequence{{Name: "s1"}},
	}
	tests := []struct {
		name        string
		target, dev catalog.ServerInfo
		want        *catalog.Database
	}{
		{
			name:   "a dev database of another name",
			target: catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"},
			dev:    catalog.ServerInfo{Dialect: platform.MySQL, Schema: "appdev"},
			want:   moved,
		},
		{
			name:   "a dev database of the same name",
			target: catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"},
			dev:    catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"},
			want:   targetRead(),
		},
		{
			name:   "a whole target server",
			target: catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true},
			dev:    catalog.ServerInfo{Dialect: platform.MySQL, Schema: "appdev"},
			want:   targetRead(),
		},
		{
			name:   "a whole dev server",
			target: catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"},
			dev:    catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true},
			want:   targetRead(),
		},
		{
			name:   "a schema that is not a database",
			target: catalog.ServerInfo{Dialect: platform.Postgres, Schema: "app"},
			dev:    catalog.ServerInfo{Dialect: platform.Postgres, Schema: "appdev"},
			want:   targetRead(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			state := targetRead()

			got := inDevDatabase(state, test.target, test.dev)

			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(state, qt.DeepEquals, targetRead())
		})
	}
}

// A ClickHouse read records the database on a materialized view as on a view.
func TestInDevDatabase_MovesAClickHouseMaterializedView(t *testing.T) {
	c := qt.New(t)
	state := &catalog.Database{MatViews: []catalog.MaterializedView{{Name: "mv1", Schema: "app", Body: "SELECT id FROM app.t1"}}}

	got := inDevDatabase(state,
		catalog.ServerInfo{Dialect: platform.ClickHouse, Schema: "app"},
		catalog.ServerInfo{Dialect: platform.ClickHouse, Schema: "appdev"},
	)

	c.Assert(got.MatViews, qt.DeepEquals, []catalog.MaterializedView{{Name: "mv1", Body: "SELECT id FROM `appdev`.t1"}})
}

func TestInDevDatabase_NilState(t *testing.T) {
	c := qt.New(t)

	got := inDevDatabase(nil,
		catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"},
		catalog.ServerInfo{Dialect: platform.MySQL, Schema: "appdev"},
	)

	c.Assert(got, qt.IsNil)
}
