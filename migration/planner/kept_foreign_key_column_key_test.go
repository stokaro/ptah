package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

// heldColumnKeyHolder is a read of child whose index pid, over the columns given, holds
// the name child.pid takes for its own UNIQUE, with the foreign keys given beside it.
func heldColumnKeyHolder(columns []string, keys ...catalog.Constraint) *catalog.Database {
	held := heldKeptKeyOnTwoIndexes(keys...)
	held.Indexes = []catalog.Index{{Name: "pid", TableName: "child", Columns: columns}}
	held.Constraints = keys
	return held
}

// declaredColumnKey declares child.pid UNIQUE, with the foreign keys given.
func declaredColumnKey(keys ...schemamodel.Constraint) *schemamodel.Database {
	declared := declaredKeptKeyBesideTwoKeys()
	for i := range declared.Fields {
		if declared.Fields[i].StructName == "Child" && declared.Fields[i].Name == "pid" {
			declared.Fields[i].Unique = true
		}
	}
	declared.Constraints = keys
	return declared
}

// TestKeptForeignKeyColumnKey_HappyPath pins the plan for a column that gains its own
// UNIQUE while the only index a foreign key stands on holds the name that UNIQUE takes.
// The plan dropped the index ahead of the column change, which the server answered with
// Error 1553, and gave a kept key an index of its own that dropped the column's new
// UNIQUE again. One statement frees the name and takes it where the column's UNIQUE serves
// the key, also for a key the plan removes later. Where it does not, the key's own index
// goes in, ahead of the column change.
func TestKeptForeignKeyColumnKey_HappyPath(t *testing.T) {
	pair := catalog.Constraint{
		Name: "k_fk", TableName: "child", Type: "FOREIGN KEY", ColumnNames: []string{"pid", "qid"},
		ForeignTable: new("parent"), ForeignColumns: []string{"id", "code"},
	}
	declaredPair := schemamodel.Constraint{
		StructName: "Child", Name: "k_fk", Type: "FOREIGN KEY", Table: "child", Columns: []string{"pid", "qid"},
		ForeignTable: "parent", ForeignColumns: []string{"id", "code"},
	}
	const takes = "ALTER TABLE `child` DROP INDEX `pid`, MODIFY COLUMN `pid` bigint UNIQUE"
	tests := []struct {
		name     string
		held     *catalog.Database
		declared *schemamodel.Database
		want     []string
	}{
		{
			name:     "one kept key",
			held:     heldColumnKeyHolder([]string{"pid"}, childKey("k_fk", "pid", "alt")),
			declared: declaredColumnKey(declaredChildKey("k_fk", "pid", "alt")),
			want:     []string{takes},
		},
		{
			name:     "two kept keys",
			held:     heldColumnKeyHolder([]string{"pid"}, childKey("j_fk", "pid", "id"), childKey("k_fk", "pid", "alt")),
			declared: declaredColumnKey(declaredChildKey("j_fk", "pid", "id"), declaredChildKey("k_fk", "pid", "alt")),
			want:     []string{takes},
		},
		{
			name:     "a key the plan removes, which stands until the removals",
			held:     heldColumnKeyHolder([]string{"pid"}, childKey("k_fk", "pid", "alt")),
			declared: declaredColumnKey(),
			want:     []string{takes, "ALTER TABLE `child` DROP FOREIGN KEY `k_fk`"},
		},
		{
			name:     "a kept key over two columns",
			held:     heldColumnKeyHolder([]string{"pid", "qid"}, pair),
			declared: declaredColumnKey(declaredPair),
			want: []string{
				"ALTER TABLE `child` DROP INDEX `pid`, ADD INDEX `k_fk` (`pid`, `qid`)",
				"ALTER TABLE `child` MODIFY COLUMN `pid` bigint UNIQUE",
			},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)

				got := plannedOverHeld(c, dialect, test.declared, test.held)

				c.Assert(got, qt.DeepEquals, test.want)
			})
		}
	}
}

// TestKeptForeignKeyColumnKey_FailurePath is the control: with no foreign key on the
// index that holds the name, or with keys only on another index, it is dropped in a
// statement of its own, ahead of the column change.
func TestKeptForeignKeyColumnKey_FailurePath(t *testing.T) {
	const modify = "ALTER TABLE `child` MODIFY COLUMN `pid` bigint UNIQUE"
	elsewhere := heldColumnKeyHolder([]string{"pid"}, childKey("k_fk", "qid", "alt"))
	tests := []struct {
		name     string
		held     *catalog.Database
		declared *schemamodel.Database
	}{
		{
			name:     "no key stands on the index",
			held:     heldColumnKeyHolder([]string{"pid"}),
			declared: declaredColumnKey(),
		},
		{
			name:     "the key stands on another column",
			held:     elsewhere,
			declared: declaredColumnKey(declaredChildKey("k_fk", "qid", "alt")),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := plannedOverHeld(c, "mysql", test.declared, test.held)

			c.Assert(got, qt.DeepEquals, []string{"DROP INDEX `pid` ON `child`", modify})
		})
	}
}

// TestKeptForeignKeyColumnKey_TheRollbackRebuildsTheIndexInOneStatement pins the rollback:
// the column's UNIQUE leaves and the index comes back under its name. The key stands on
// the UNIQUE until then, so one statement drops the one and adds the other.
func TestKeptForeignKeyColumnKey_TheRollbackRebuildsTheIndexInOneStatement(t *testing.T) {
	c := qt.New(t)
	const parent = `CREATE TABLE parent (id bigint PRIMARY KEY, alt bigint NOT NULL, CONSTRAINT parent_alt UNIQUE (alt));`

	got := rollbackOfPlan(c, "mysql",
		parent+`CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT k_fk FOREIGN KEY (pid) REFERENCES parent (alt));
			CREATE INDEX pid ON child (pid);`,
		parent+`CREATE TABLE child (id bigint PRIMARY KEY, pid bigint UNIQUE, CONSTRAINT k_fk FOREIGN KEY (pid) REFERENCES parent (alt));`,
	)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE `child` MODIFY COLUMN `pid` bigint",
		"ALTER TABLE `child` DROP INDEX `pid`, ADD INDEX `pid` (`pid`)",
	})
}
