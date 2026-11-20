package atlasschema

// White-box testing required: RebindDatabase decides which objects of a
// one-database read move to another database, for the rehearsal baseline and
// for the drift check's shadow replay, and only a live server reaches it
// through the public path. The cases that separate a
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

func TestRebindDatabase_HappyPath(t *testing.T) {
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

			got := RebindDatabase(state, test.target, test.dev)

			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(state, qt.DeepEquals, targetRead())
		})
	}
}

// A ClickHouse read records the database on a materialized view as on a view.
func TestRebindDatabase_MovesAClickHouseMaterializedView(t *testing.T) {
	c := qt.New(t)
	state := &catalog.Database{MatViews: []catalog.MaterializedView{{Name: "mv1", Schema: "app", Body: "SELECT id FROM app.t1"}}}

	got := RebindDatabase(state,
		catalog.ServerInfo{Dialect: platform.ClickHouse, Schema: "app"},
		catalog.ServerInfo{Dialect: platform.ClickHouse, Schema: "appdev"},
	)

	c.Assert(got.MatViews, qt.DeepEquals, []catalog.MaterializedView{{Name: "mv1", Body: "SELECT id FROM `appdev`.t1"}})
}

func TestRebindDatabase_NilState(t *testing.T) {
	c := qt.New(t)

	got := RebindDatabase(nil,
		catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"},
		catalog.ServerInfo{Dialect: platform.MySQL, Schema: "appdev"},
	)

	c.Assert(got, qt.IsNil)
}

// A shadow database replays the target's history under a name of its own, and a
// one-database read of it names a view, a routine and a sequence by that name.
// The drift check compares the replay with the target's read, which names the
// same objects by the target's database, so the replay has to read as the
// target's own objects: this is the missing view and extra view pair a check of
// a clean target reported.
func TestRebindDatabase_ShadowReplayReadsAsTheTargetsOwnObjects(t *testing.T) {
	c := qt.New(t)
	shadow := catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app_shadow"}
	target := catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"}
	replay := &catalog.Database{
		Tables: []catalog.Table{{Name: "t1"}},
		Views: []catalog.View{
			{Name: "v1", Schema: "app_shadow", Body: "select `app_shadow`.`t1`.`id` AS `id` from `app_shadow`.`t1`"},
			{Name: "v_other", Schema: "other", Body: "select `other`.`x`.`id` AS `id` from `other`.`x`"},
		},
		Functions: []catalog.Function{{Name: "f1", Schema: "app_shadow"}},
		Sequences: []catalog.Sequence{{Name: "s1", Schema: "app_shadow"}},
	}

	got := RebindDatabase(replay, shadow, target)

	c.Assert(got.Views, qt.DeepEquals, []catalog.View{
		{Name: "v1", Body: "select `app`.`t1`.`id` AS `id` from `app`.`t1`"},
		{Name: "v_other", Schema: "other", Body: "select `other`.`x`.`id` AS `id` from `other`.`x`"},
	})
	c.Assert(got.Functions, qt.DeepEquals, []catalog.Function{{Name: "f1"}})
	c.Assert(got.Sequences, qt.DeepEquals, []catalog.Sequence{{Name: "s1"}})
	c.Assert(replay.Views[0].Schema, qt.Equals, "app_shadow", qt.Commentf("the read the caller holds is not modified"))
}
