package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestRemovedForeignKeyAheadOfItsKey_HappyPath pins a foreign key and the UNIQUE key it
// stands on leaving in one plan. The diff lists removals by table, so the key's table
// may come first, and the server answers Error 1553 while the foreign key stands.
func TestRemovedForeignKeyAheadOfItsKey_HappyPath(t *testing.T) {
	const held = "CREATE TABLE a (id int PRIMARY KEY, code int, CONSTRAINT a_code_uq UNIQUE (code));\n" +
		"CREATE TABLE z (id int PRIMARY KEY, a_code int, CONSTRAINT z_a_fk FOREIGN KEY (a_code) REFERENCES a (code));\n"
	const desired = "CREATE TABLE a (id int PRIMARY KEY, code int);\nCREATE TABLE z (id int PRIMARY KEY, a_code int);\n"
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, dialect, held, desired)

			c.Assert(got, qt.DeepEquals, []string{
				"ALTER TABLE `z` DROP FOREIGN KEY `z_a_fk`",
				"ALTER TABLE `a` DROP INDEX `a_code_uq`",
			})
		})
	}
}

// TestRemovedForeignKeyAheadOfItsKey_OtherRemovalsKeepTheirOrder pins removals with no
// foreign key among them: each table's constraints stay in the order the diff lists them.
func TestRemovedForeignKeyAheadOfItsKey_OtherRemovalsKeepTheirOrder(t *testing.T) {
	c := qt.New(t)

	got := columnKeyPlan(c, "mysql",
		"CREATE TABLE a (id int PRIMARY KEY, code int, CONSTRAINT a_code_uq UNIQUE (code));\n"+
			"CREATE TABLE z (id int PRIMARY KEY, n int, CONSTRAINT z_n_rule CHECK (n > 0));\n",
		"CREATE TABLE a (id int PRIMARY KEY, code int);\nCREATE TABLE z (id int PRIMARY KEY, n int);\n",
	)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE `a` DROP INDEX `a_code_uq`",
		"ALTER TABLE `z` DROP CONSTRAINT `z_n_rule`",
	})
}
