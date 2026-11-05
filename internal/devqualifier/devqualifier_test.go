package devqualifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/devqualifier"
)

// TestBody_HappyPath pins the bodies a server prints, as the MySQL family and
// ClickHouse print them. What is not the database's qualifier stays: another
// database, a name that only ends in the database's name, and a string literal.
func TestBody_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "the MySQL family's print", body: "select `dev`.`t`.`id` AS `id`,`dev`.`t`.`a` AS `a` from `dev`.`t`", want: "select `t`.`id` AS `id`,`t`.`a` AS `a` from `t`"},
		{name: "a bare qualifier", body: "SELECT id, a FROM dev.t", want: "SELECT id, a FROM t"},
		{name: "another database", body: "select `other`.`t`.`id` AS `id` from `other`.`t`", want: "select `other`.`t`.`id` AS `id` from `other`.`t`"},
		{name: "a name ending in the database's name", body: "SELECT id FROM my_dev.t", want: "SELECT id FROM my_dev.t"},
		{name: "a string literal", body: "SELECT 'dev.t' AS x, 'it''s dev.t' AS y FROM dev.t", want: "SELECT 'dev.t' AS x, 'it''s dev.t' AS y FROM t"},
		{name: "an alias spelled like the database", body: "SELECT dev.id FROM dev.users AS dev", want: "SELECT dev.id FROM dev.users AS dev"},
		{name: "an alias spelled like the database, without the keyword", body: "select `dev`.`id` AS `id` from `dev`.`users` `dev`", want: "select `dev`.`id` AS `id` from `dev`.`users` `dev`"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(devqualifier.Body(test.body, "dev"), qt.Equals, test.want)
		})
	}
}

// heldObjects is a read of database dev.
func heldObjects() *catalog.Database {
	return &catalog.Database{
		Views:     []catalog.View{{Name: "v", Schema: "dev", Body: "select `dev`.`t`.`id` AS `id` from `dev`.`t`"}},
		MatViews:  []catalog.MaterializedView{{Name: "m", Schema: "dev", Body: "SELECT id FROM dev.t"}},
		Functions: []catalog.Function{{Name: "f", Schema: "dev"}},
		Sequences: []catalog.Sequence{{Name: "s", Schema: "dev"}},
	}
}

// TestRemove_HappyPath is a read of the one database the run is about: no object
// names it any more.
func TestRemove_HappyPath(t *testing.T) {
	c := qt.New(t)
	held := heldObjects()

	devqualifier.Remove(held, "dev", []string{"dev"})

	c.Assert(held.Views[0].Schema, qt.Equals, "")
	c.Assert(held.Views[0].Body, qt.Equals, "select `t`.`id` AS `id` from `t`")
	c.Assert(held.MatViews[0].Schema, qt.Equals, "")
	c.Assert(held.MatViews[0].Body, qt.Equals, "SELECT id FROM t")
	c.Assert(held.Functions[0].Schema, qt.Equals, "")
	c.Assert(held.Sequences[0].Schema, qt.Equals, "")
}

// TestRemove_KeepsTheNames_FailurePath is a read the name still tells apart: of
// several schemas, of another database, or with no database to name.
func TestRemove_KeepsTheNames_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		database  string
		readNames []string
	}{
		{name: "a read of several schemas", database: "dev", readNames: []string{"dev", "other"}},
		{name: "a read of another schema", database: "dev", readNames: []string{"other"}},
		{name: "no database", database: "", readNames: []string{"dev"}},
		{name: "another database", database: "app", readNames: []string{"app"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := heldObjects()

			devqualifier.Remove(held, test.database, test.readNames)

			c.Assert(held.Views[0].Schema, qt.Equals, "dev")
			c.Assert(held.Views[0].Body, qt.Equals, "select `dev`.`t`.`id` AS `id` from `dev`.`t`")
			c.Assert(held.Functions[0].Schema, qt.Equals, "dev")
		})
	}
}
