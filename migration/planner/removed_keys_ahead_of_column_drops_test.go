package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestKeysAheadOfColumnDrops_HappyPath pins the keys and indexes of a table that loses a
// column going ahead of the column changes. A key over only that column leaves with it,
// and its own drop then answers Error 1091, which MySQL cannot guard.
func TestKeysAheadOfColumnDrops_HappyPath(t *testing.T) {
	const desired = `CREATE TABLE t (id int PRIMARY KEY, a int);`
	const dropColumn = "ALTER TABLE `t` DROP COLUMN `b`"
	tests := []struct {
		name, held string
		want       []string
	}{
		{
			name: "an index over the column",
			held: `CREATE TABLE t (id int PRIMARY KEY, a int, b int); CREATE INDEX i ON t (b);`,
			want: []string{"DROP INDEX `i` ON `t`", dropColumn},
		},
		{
			name: "a UNIQUE of its own name over the column",
			held: `CREATE TABLE t (id int PRIMARY KEY, a int, b int, CONSTRAINT uq_b UNIQUE (b));`,
			want: []string{"ALTER TABLE `t` DROP INDEX `uq_b`", dropColumn},
		},
		{
			name: "a CHECK over the column",
			held: `CREATE TABLE t (id int PRIMARY KEY, a int, b int, CONSTRAINT k CHECK (b > 0));`,
			want: []string{"ALTER TABLE `t` DROP CONSTRAINT `k`", dropColumn},
		},
		{
			name: "an index over another column of that table",
			held: `CREATE TABLE t (id int PRIMARY KEY, a int, b int); CREATE INDEX i ON t (a);`,
			want: []string{"DROP INDEX `i` ON `t`", dropColumn},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, "mysql", test.held, desired)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestKeysAheadOfColumnDrops_ForeignKeysGoFirst pins a foreign key that may stand on what
// leaves: on the table that loses the column, or referencing it. It goes first, and a
// changed one comes back after the column changes.
func TestKeysAheadOfColumnDrops_ForeignKeysGoFirst(t *testing.T) {
	tests := []struct {
		name, held, desired string
		want                []string
	}{
		{
			name: "on the table, over a column it keeps",
			held: `CREATE TABLE p (id int PRIMARY KEY);
				CREATE TABLE t (id int PRIMARY KEY, x int, y int, CONSTRAINT t_fk FOREIGN KEY (x) REFERENCES p (id));
				CREATE INDEX i ON t (x);`,
			desired: `CREATE TABLE p (id int PRIMARY KEY); CREATE TABLE t (id int PRIMARY KEY, x int);`,
			want: []string{
				"ALTER TABLE `t` DROP FOREIGN KEY `t_fk`",
				"DROP INDEX `i` ON `t`",
				"ALTER TABLE `t` DROP COLUMN `y`",
			},
		},
		{
			name: "on another table, over the UNIQUE that leaves",
			held: `CREATE TABLE t (id int PRIMARY KEY, b int, CONSTRAINT u UNIQUE (b));
				CREATE TABLE z (id int PRIMARY KEY, tb int, CONSTRAINT z_fk FOREIGN KEY (tb) REFERENCES t (b));`,
			desired: `CREATE TABLE t (id int PRIMARY KEY); CREATE TABLE z (id int PRIMARY KEY, tb int);`,
			want: []string{
				"ALTER TABLE `z` DROP FOREIGN KEY `z_fk`",
				"ALTER TABLE `t` DROP INDEX `u`",
				"ALTER TABLE `t` DROP COLUMN `b`",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, "mysql", test.held, test.desired)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestKeysAheadOfColumnDrops_FailurePath pins the plans that keep their order: a table
// that loses no column keeps its removals where they are, an index the plan keeps stays,
// and the key a column's own UNIQUE names leaves with the column, with no statement.
func TestKeysAheadOfColumnDrops_FailurePath(t *testing.T) {
	tests := []struct {
		name, held, desired string
		want                []string
	}{
		{
			name:    "a table that loses no column",
			held:    `CREATE TABLE t (id int PRIMARY KEY, a int, b int); CREATE INDEX i ON t (b);`,
			desired: `CREATE TABLE t (id int PRIMARY KEY, a int, b int, c int);`,
			want:    []string{"ALTER TABLE `t` ADD COLUMN `c` int", "DROP INDEX `i` ON `t`"},
		},
		{
			name:    "an index the plan keeps",
			held:    `CREATE TABLE t (id int PRIMARY KEY, a int, b int); CREATE INDEX i ON t (a);`,
			desired: `CREATE TABLE t (id int PRIMARY KEY, a int); CREATE INDEX i ON t (a);`,
			want:    []string{"ALTER TABLE `t` DROP COLUMN `b`"},
		},
		{
			name:    "a column's own UNIQUE",
			held:    `CREATE TABLE t (id int PRIMARY KEY, a int, x int UNIQUE);`,
			desired: `CREATE TABLE t (id int PRIMARY KEY, a int);`,
			want:    []string{"ALTER TABLE `t` DROP COLUMN `x`"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, "mysql", test.held, test.desired)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
