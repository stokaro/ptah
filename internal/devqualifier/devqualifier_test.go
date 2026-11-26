package devqualifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/devqualifier"
)

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

	devqualifier.Remove(held, "mysql", "dev", []string{"dev"})

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

			devqualifier.Remove(held, "mysql", test.database, test.readNames)

			c.Assert(held.Views[0].Schema, qt.Equals, "dev")
			c.Assert(held.Views[0].Body, qt.Equals, "select `dev`.`t`.`id` AS `id` from `dev`.`t`")
			c.Assert(held.Functions[0].Schema, qt.Equals, "dev")
		})
	}
}
