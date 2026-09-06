package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestToDatabase_AForeignKeysBackingIndexTakesItsNameHappyPath covers
// stokaro/ptah#2769.
//
// A foreign key with no covering index makes the engine build one, named after
// the constraint. Ptah derived the same name for a later unnamed index, and
// since it emits the index as its own `CREATE INDEX` before the
// `ALTER TABLE ... ADD CONSTRAINT`, the constraint then failed with
// `ERROR 1061 (42000): Duplicate key name`.
//
// Measured on MySQL 8.4.11 and MariaDB 11.8.9: both build `b` for the
// constraint and `b_2` for the index.
func TestToDatabase_AForeignKeysBackingIndexTakesItsNameHappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := dialectSchema(c, test.dialect,
				"CREATE TABLE p (id INT PRIMARY KEY);"+
					"CREATE TABLE c (a INT, b INT, "+
					"CONSTRAINT b FOREIGN KEY (a) REFERENCES p(id), KEY (b));")

			c.Assert(err, qt.IsNil)
			c.Assert(indexNames(database), qt.DeepEquals, []string{"b_2"})
		})
	}
}

// TestToDatabase_ACoveringKeyLeavesTheForeignKeyNameFreeHappyPath is the
// control the reservation needs.
//
// Reserving unconditionally would be the easy fix and it is wrong: where a key
// already covers the referencing columns, the engine reuses it and creates
// nothing, so the name stays available. Measured -- `KEY b (a), CONSTRAINT b
// FOREIGN KEY (a)` keeps one `b(a)` on both engines.
func TestToDatabase_ACoveringKeyLeavesTheForeignKeyNameFreeHappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := dialectSchema(c, test.dialect,
				"CREATE TABLE p (id INT PRIMARY KEY);"+
					"CREATE TABLE c (a INT, KEY b (a), "+
					"CONSTRAINT b FOREIGN KEY (a) REFERENCES p(id));")

			c.Assert(err, qt.IsNil)
			c.Assert(indexNames(database), qt.DeepEquals, []string{"b"})
		})
	}
}

// TestToDatabase_CoverageFoldsColumnCaseHappyPath is the identity half.
//
// Measured on both engines: `KEY k(`+"`B`"+`)` backs `FOREIGN KEY (`+"`b`"+`)`,
// so the constraint consumes no name and a later unnamed `KEY (z)` keeps `z`.
// Comparing the spellings instead reserved `z` and derived `z_2` -- a table
// neither server builds, and one whose desired model can never converge with
// the database it describes.
func TestToDatabase_CoverageFoldsColumnCaseHappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := dialectSchema(c, test.dialect,
				"CREATE TABLE p (id INT PRIMARY KEY);"+
					"CREATE TABLE c (`B` INT, z INT, KEY k (`B`), "+
					"CONSTRAINT z FOREIGN KEY (`b`) REFERENCES p(id), KEY (z));")

			c.Assert(err, qt.IsNil)
			c.Assert(indexNames(database), qt.DeepEquals, []string{"k", "z"})
		})
	}
}

// TestToDatabase_ADescendingCandidateCoversPerEngine is the one rule the two
// engines answer differently, so it is the one a family-wide answer gets wrong
// for exactly one of them.
//
// Measured, `KEY k (a DESC), CONSTRAINT f FOREIGN KEY (a)`:
//
//	MySQL 8.4.11     keeps k descending and adds an ascending f
//	MariaDB 11.8.9   reuses k and adds nothing
//
// So with the constraint named for what a later unnamed index wants, the same
// document has different index names on the two engines.
func TestToDatabase_ADescendingCandidateCoversPerEngine(t *testing.T) {
	const document = "CREATE TABLE p (id INT PRIMARY KEY);" +
		"CREATE TABLE c (a INT, b INT, KEY k (a DESC), " +
		"CONSTRAINT b FOREIGN KEY (a) REFERENCES p(id), KEY (b));"

	t.Run("mysql builds the backing index, so the name is taken", func(t *testing.T) {
		c := qt.New(t)

		database, err := dialectSchema(c, platform.MySQL, document)

		c.Assert(err, qt.IsNil)
		c.Assert(indexNames(database), qt.DeepEquals, []string{"k", "b_2"})
	})

	t.Run("mariadb reuses the descending key, so it is not", func(t *testing.T) {
		c := qt.New(t)

		database, err := dialectSchema(c, platform.MariaDB, document)

		c.Assert(err, qt.IsNil)
		c.Assert(indexNames(database), qt.DeepEquals, []string{"k", "b"})
	})
}

// TestToDatabase_ADescendingPartPastTheCoveredPrefixIsIgnoredHappyPath pins
// that direction is asked about the LEADING columns only.
//
// `KEY k (a, b DESC)` backs a foreign key on `a` alone on both engines: the
// descending part is past the prefix the key needs, so it decides nothing. A
// rule that disqualified the whole key for holding a DESC anywhere would
// reserve a name MySQL never takes.
func TestToDatabase_ADescendingPartPastTheCoveredPrefixIsIgnoredHappyPath(t *testing.T) {
	c := qt.New(t)

	database, err := dialectSchema(c, platform.MySQL,
		"CREATE TABLE p (id INT PRIMARY KEY);"+
			"CREATE TABLE c (a INT, b INT, z INT, KEY k (a, b DESC), "+
			"CONSTRAINT z FOREIGN KEY (a) REFERENCES p(id), KEY (z));")

	c.Assert(err, qt.IsNil)
	c.Assert(indexNames(database), qt.DeepEquals, []string{"k", "z"})
}
