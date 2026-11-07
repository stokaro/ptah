package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	// takeoverParent is the table the foreign key references.
	takeoverParent = "CREATE TABLE parent (id bigint PRIMARY KEY);\n"
	// takeoverChild opens child, whose foreign key c_fk stands on the index the server
	// built for it, c_fk.
	takeoverChild = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint"
	// takeoverKey is child's foreign key over pid.
	takeoverKey = ", CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id)"
	// takeoverBuilt is the index the server built for c_fk, as the catalog reads it.
	takeoverBuilt = "CREATE INDEX c_fk ON child (pid);\n"
	// takeoverHeld is the table the plans start from.
	takeoverHeld = takeoverParent + takeoverChild + takeoverKey + ");\n" + takeoverBuilt
	// takeoverRebuilt is the statement a rollback puts the index back with.
	takeoverRebuilt = "CREATE INDEX `c_fk` ON `child` (`pid`)"
)

// TestKeyTakesOverForeignKeyIndex_HappyPath pins the plans that add a key over the columns
// of a foreign key whose index the server built: a UNIQUE constraint, a column's own
// UNIQUE, a primary key. The server drops the index it built once the key is in, also
// after DROP FOREIGN KEY. Measured on MySQL 8.4 and 26.10 and MariaDB 12.3, the plan's
// DROP INDEX after the addition answered Error 1091 where the plan removed the key, and
// the rollback's removal of the addition answered Error 1553 where it kept the key. The
// statement that adds the key drops the old index, and the rollback puts the index back
// ahead of the removal. Where an index is added beside the key, the first statement drops
// the old index and the later one leaves it out.
func TestKeyTakesOverForeignKeyIndex_HappyPath(t *testing.T) {
	// MariaDB takes a guard on the removal of a key that leaves with the plan's removals.
	dialects := []struct{ name, dropRemovedKey string }{
		{name: "mysql", dropRemovedKey: "ALTER TABLE `child` DROP FOREIGN KEY `c_fk`"},
		{name: "mariadb", dropRemovedKey: "ALTER TABLE `child` DROP FOREIGN KEY IF EXISTS `c_fk`"},
	}
	for _, dialect := range dialects {
		for _, test := range keyTakeoverRows(dialect.dropRemovedKey) {
			t.Run(dialect.name+" "+test.name, func(t *testing.T) {
				c := qt.New(t)

				up, down := bothWaysOfPlan(c, dialect.name, test.held, test.desired)

				c.Assert(up, qt.DeepEquals, test.up)
				c.Assert(down, qt.DeepEquals, test.down)
			})
		}
	}
}

// keyTakeoverRow is one plan of [TestKeyTakesOverForeignKeyIndex_HappyPath]: the
// tables before and after, and the statements of the plan and of its rollback.
type keyTakeoverRow struct {
	name     string
	held     string
	desired  string
	up, down []string
}

// keyTakeoverRows are the rows of [TestKeyTakesOverForeignKeyIndex_HappyPath], with the
// statement that drops a foreign key the plan removes as the dialect writes it.
func keyTakeoverRows(dropRemovedKey string) []keyTakeoverRow {
	const (
		unique       = "ALTER TABLE `child` DROP INDEX `c_fk`, ADD CONSTRAINT `u` UNIQUE (`pid`)"
		columnUnique = "ALTER TABLE `child` DROP INDEX `c_fk`, MODIFY COLUMN `pid` bigint UNIQUE"
		dropKey      = "ALTER TABLE `child` DROP FOREIGN KEY `c_fk`"
		dropUnique   = "ALTER TABLE `child` DROP INDEX `u`"
		columnPlain  = "ALTER TABLE `child` MODIFY COLUMN `pid` bigint"
		dropColumnUq = "ALTER TABLE `child` DROP INDEX `pid`"
		addKey       = "ALTER TABLE `child` ADD CONSTRAINT `c_fk` FOREIGN KEY (`pid`) REFERENCES `parent`(`id`)"
		uniqueChild  = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint UNIQUE, qid bigint"
		keylessChild = "CREATE TABLE child (id bigint NOT NULL, pid bigint NOT NULL, qid bigint"
	)
	return []keyTakeoverRow{
		{
			name:    "a UNIQUE, the key kept",
			held:    takeoverHeld,
			desired: takeoverParent + takeoverChild + ", CONSTRAINT u UNIQUE (pid)" + takeoverKey + ");",
			up:      []string{unique},
			down:    []string{takeoverRebuilt, dropUnique},
		},
		{
			name:    "a UNIQUE, the key removed",
			held:    takeoverHeld,
			desired: takeoverParent + takeoverChild + ", CONSTRAINT u UNIQUE (pid));",
			up:      []string{unique, dropRemovedKey},
			down:    []string{takeoverRebuilt, addKey, dropUnique},
		},
		{
			name:    "a wider UNIQUE, the key kept",
			held:    takeoverHeld,
			desired: takeoverParent + takeoverChild + ", CONSTRAINT u UNIQUE (pid, qid)" + takeoverKey + ");",
			up:      []string{"ALTER TABLE `child` DROP INDEX `c_fk`, ADD CONSTRAINT `u` UNIQUE (`pid`, `qid`)"},
			down:    []string{takeoverRebuilt, dropUnique},
		},
		{
			name:    "the column's own UNIQUE, the key kept",
			held:    takeoverHeld,
			desired: takeoverParent + uniqueChild + takeoverKey + ");",
			up:      []string{columnUnique},
			down:    []string{columnPlain, takeoverRebuilt, dropColumnUq},
		},
		{
			name:    "the column's own UNIQUE, the key removed",
			held:    takeoverHeld,
			desired: takeoverParent + uniqueChild + ");",
			up:      []string{columnUnique, dropRemovedKey},
			down:    []string{columnPlain, takeoverRebuilt, addKey, dropColumnUq},
		},
		{
			name:    "a primary key, the key kept",
			held:    takeoverParent + keylessChild + takeoverKey + ");\n" + takeoverBuilt,
			desired: takeoverParent + keylessChild + ", PRIMARY KEY (pid, id)" + takeoverKey + ");",
			up:      []string{"ALTER TABLE `child` DROP INDEX `c_fk`, ADD PRIMARY KEY (`pid`, `id`)"},
			down:    []string{takeoverRebuilt, "ALTER TABLE `child` DROP PRIMARY KEY"},
		},
		{
			name:    "a UNIQUE, the key kept, on a table that loses a column",
			held:    takeoverHeld,
			desired: takeoverParent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT u UNIQUE (pid)" + takeoverKey + ");",
			up:      []string{"ALTER TABLE `child` DROP COLUMN `qid`", unique},
			down:    []string{"ALTER TABLE `child` ADD COLUMN `qid` bigint", takeoverRebuilt, dropUnique},
		},
		{
			name: "the column's own UNIQUE over the index of an unnamed key, which holds the key's name",
			held: takeoverParent + takeoverChild + ", CONSTRAINT child_ibfk_1 FOREIGN KEY (pid) REFERENCES parent (id));\n" +
				"CREATE INDEX pid ON child (pid);\n",
			desired: takeoverParent + uniqueChild + ", CONSTRAINT child_ibfk_1 FOREIGN KEY (pid) REFERENCES parent (id));",
			up:      []string{"ALTER TABLE `child` DROP INDEX `pid`, MODIFY COLUMN `pid` bigint UNIQUE"},
			down:    []string{columnPlain, "ALTER TABLE `child` DROP INDEX `pid`, ADD INDEX `pid` (`pid`)"},
		},
		{
			name:    "a UNIQUE and an index, the key kept",
			held:    takeoverHeld,
			desired: takeoverParent + takeoverChild + ", CONSTRAINT u UNIQUE (pid)" + takeoverKey + ");\nCREATE INDEX w ON child (pid, qid);",
			up:      []string{unique, "CREATE INDEX `w` ON `child` (`pid`, `qid`)"},
			down:    []string{takeoverRebuilt, dropUnique, "DROP INDEX `w` ON `child`"},
		},
		{
			name:    "two indexes, the key kept",
			held:    takeoverHeld,
			desired: takeoverParent + takeoverChild + takeoverKey + ");\nCREATE INDEX a ON child (pid);\nCREATE INDEX w ON child (pid, qid);",
			up:      []string{"ALTER TABLE `child` DROP INDEX `c_fk`, ADD INDEX `a` (`pid`)", "CREATE INDEX `w` ON `child` (`pid`, `qid`)"},
			down:    []string{takeoverRebuilt, "DROP INDEX `a` ON `child`", "DROP INDEX `w` ON `child`"},
		},
		{
			name:    "the key moves to another column under its name",
			held:    takeoverHeld,
			desired: takeoverParent + takeoverChild + ", CONSTRAINT u UNIQUE (pid), CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id));",
			up: []string{
				dropKey,
				unique,
				"ALTER TABLE `child` ADD CONSTRAINT `c_fk` FOREIGN KEY (`qid`) REFERENCES `parent`(`id`)",
			},
			down: []string{
				dropKey,
				"ALTER TABLE `child` DROP INDEX `c_fk`, ADD INDEX `c_fk` (`pid`)",
				addKey,
				dropUnique,
			},
		},
	}
}

// TestKeyTakesOverForeignKeyIndex_FailurePath pins the plans that leave the index alone:
// the declaration holds it, or the key the plan adds does not begin with the foreign key's
// columns, so the server keeps the index for the foreign key.
func TestKeyTakesOverForeignKeyIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		up      []string
	}{
		{
			name:    "the declaration holds the index",
			desired: takeoverParent + takeoverChild + ", CONSTRAINT u UNIQUE (pid)" + takeoverKey + ");\n" + takeoverBuilt,
			up:      []string{"ALTER TABLE `child` ADD CONSTRAINT `u` UNIQUE (`pid`)"},
		},
		{
			name:    "a UNIQUE that begins with another column",
			desired: takeoverParent + takeoverChild + ", CONSTRAINT u UNIQUE (qid, pid)" + takeoverKey + ");",
			up:      []string{"ALTER TABLE `child` ADD CONSTRAINT `u` UNIQUE (`qid`, `pid`)"},
		},
		{
			name:    "another column's own UNIQUE",
			desired: takeoverParent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint UNIQUE" + takeoverKey + ");",
			up:      []string{"ALTER TABLE `child` MODIFY COLUMN `qid` bigint UNIQUE"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			up, _ := bothWaysOfPlan(c, "mysql", takeoverHeld, test.desired)

			c.Assert(up, qt.DeepEquals, test.up)
		})
	}
}
