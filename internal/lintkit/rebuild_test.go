package lintkit_test

import (
	"maps"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintscan"
)

// rebuildOf is a SQLite rebuild of users through a copy named copyName whose
// copy step is the statement given: the copy is created, filled, the old
// table dropped and the copy renamed over it. The DROP TABLE is statement 2.
func rebuildOf(copyName, copyStatement string) []lintext.Statement {
	return lintscan.Split("CREATE TABLE "+copyName+" (id int, name text, flag int);\n"+
		copyStatement+";\n"+
		"DROP TABLE users;\n"+
		"ALTER TABLE "+copyName+" RENAME TO users;\n", lintscan.ForDialect("sqlite"))
}

// columnsOf says that users holds the columns given before the file runs, and
// knows no table where none are given.
func columnsOf(columns []string) lintkit.Columns {
	return func(_ int, table string) ([]string, bool) {
		return columns, columns != nil && table == "USERS"
	}
}

// usersColumns are the columns users holds in the rows that know them.
var usersColumns = []string{"id", "name"}

// copyCase is a copy of users and what the matcher knows of users' columns.
type copyCase struct {
	name string
	// copyStatement fills the copy users_tmp.
	copyStatement string
	// columns are users' columns, nil where the old table is not known.
	columns []string
}

// A copy that writes every value of every column of users into the column of
// the same name keeps the table, so its DROP TABLE ends a rebuild. Quotes and
// case do not change which column a name means, a qualifier may name the
// table or its alias, and an item's own alias does not matter, since the
// INSERT pairs items with its columns by position.
func TestRebuilds_KeepsACopyThatCarriesEveryColumn(t *testing.T) {
	tests := []copyCase{
		{name: "the same columns", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, name FROM users", columns: usersColumns},
		{name: "the same columns, old table unknown", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, name FROM users"},
		{name: "SELECT * with no column list", copyStatement: "INSERT INTO users_tmp SELECT * FROM users", columns: usersColumns},
		{name: "SELECT * with no column list, old table unknown", copyStatement: "INSERT INTO users_tmp SELECT * FROM users"},
		{name: "quoted, in another case", copyStatement: `INSERT INTO users_tmp ("Id", "NAME") SELECT "ID", ` + "`name`" + ` FROM users`, columns: usersColumns},
		{name: "qualified by the table", copyStatement: "INSERT INTO users_tmp (id, name) SELECT users.id, main.users.name FROM users", columns: usersColumns},
		{name: "qualified by an alias", copyStatement: "INSERT INTO users_tmp (id, name) SELECT u.id, u.name FROM users u", columns: usersColumns},
		{name: "qualified by an alias after AS", copyStatement: "INSERT INTO users_tmp (id, name) SELECT u.id, name FROM users AS u", columns: usersColumns},
		{name: "items named after their own columns", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id AS id, name AS name FROM users", columns: usersColumns},
		{name: "items named after other columns", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id AS name, name id FROM users", columns: usersColumns},
		{name: "NULL replaced by IFNULL", copyStatement: `INSERT INTO users_tmp (id, name) SELECT id, IFNULL(name, 'x') AS name FROM users`, columns: usersColumns},
		{name: "NULL replaced by COALESCE", copyStatement: `INSERT INTO users_tmp (id, name) SELECT id, COALESCE(users.name, lower('X'), '') FROM users`, columns: usersColumns},
		{name: "a column the old table does not hold, filled with a constant", copyStatement: "INSERT INTO users_tmp (id, name, flag) SELECT id, name, 0 FROM users", columns: usersColumns},
		{name: "a column the old table does not hold, left out", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, name FROM users", columns: usersColumns},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			steps := lintkit.Rebuilds(rebuildOf("users_tmp", test.copyStatement), columnsOf(test.columns))
			c.Assert(steps.Drops, qt.DeepEquals, map[int]bool{2: true})
			c.Assert(steps.Renames, qt.DeepEquals, map[int]bool{3: true})
		})
	}
}

// A copy that does not write some value of users into the column of the same
// name loses that column, so its DROP TABLE is a drop: the rows survive and
// the values do not. Where users' columns are not known, a column the copy
// writes from anything but itself may be one of them.
func TestRebuilds_ReportsACopyThatLosesAColumn(t *testing.T) {
	tests := []copyCase{
		{name: "NULL", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, NULL FROM users", columns: usersColumns},
		{name: "NULL, old table unknown", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, NULL FROM users"},
		{name: "two columns swapped", copyStatement: "INSERT INTO users_tmp (id, name) SELECT name, id FROM users", columns: usersColumns},
		{name: "two columns swapped, old table unknown", copyStatement: "INSERT INTO users_tmp (id, name) SELECT name, id FROM users"},
		{name: "a constant", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, 'x' FROM users", columns: usersColumns},
		{name: "a constant, old table unknown", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, 'x' FROM users"},
		{name: "a column left out", copyStatement: "INSERT INTO users_tmp (id) SELECT id FROM users", columns: usersColumns},
		{name: "one column twice", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, id FROM users", columns: usersColumns},
		{name: "an expression", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, upper(name) FROM users", columns: usersColumns},
		{name: "an expression the column begins", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, name || '' FROM users", columns: usersColumns},
		{name: "a CAST", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, CAST(name AS INTEGER) FROM users", columns: usersColumns},
		{name: "IFNULL of another column", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, IFNULL(id, name) FROM users", columns: usersColumns},
		{name: "a SELECT list longer than the column list", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, name, name FROM users", columns: usersColumns},
		{name: "no column list and no SELECT *", copyStatement: "INSERT INTO users_tmp SELECT id, name FROM users", columns: usersColumns},
		{name: "no column list and no SELECT *, old table unknown", copyStatement: "INSERT INTO users_tmp SELECT id, name FROM users"},
		{name: "a column the old table may not hold, filled with a constant, old table unknown", copyStatement: "INSERT INTO users_tmp (id, name, flag) SELECT id, name, 0 FROM users"},
		{name: "a qualifier naming another table", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, other.name FROM users", columns: usersColumns},
		{name: "the table's name where the SELECT gives it an alias", copyStatement: "INSERT INTO users_tmp (id, name) SELECT id, users.name FROM users u", columns: usersColumns},
		{name: "a bare NULL for a column named null", copyStatement: `INSERT INTO users_tmp (id, "null") SELECT id, NULL FROM users`, columns: []string{"id", "null"}},
		{name: "a number for a column named by it", copyStatement: `INSERT INTO users_tmp (id, "1") SELECT id, 1 FROM users`, columns: []string{"id", "1"}},
		{name: "a bare USER for a column named user", copyStatement: `INSERT INTO users_tmp (id, "user") SELECT id, user FROM users`, columns: []string{"id", "user"}},
		{name: "rows folded by FINAL", copyStatement: "INSERT INTO users_tmp SELECT * FROM users FINAL", columns: usersColumns},
		{name: "rows left behind by a WHERE after an alias", copyStatement: "INSERT INTO users_tmp SELECT * FROM users u WHERE u.id > 1", columns: usersColumns},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			steps := lintkit.Rebuilds(rebuildOf("users_tmp", test.copyStatement), columnsOf(test.columns))
			c.Assert(steps.Drops, qt.HasLen, 0)
			c.Assert(steps.Renames, qt.HasLen, 0)
		})
	}
}

// Every drop that ends a rebuild's steps is a candidate, whatever its copy
// writes, since only the old table's columns decide some of them.
func TestRebuildCandidates_ListsEveryDropThatEndsARebuild(t *testing.T) {
	c := qt.New(t)
	file := append(rebuildOf("users_tmp", "INSERT INTO users_tmp (id, name, flag) SELECT id, name, 0 FROM users"),
		rebuildOf("new_users", "INSERT INTO new_users (id, name) SELECT name, id FROM users")...)
	file = append(file, lintscan.Split("DROP TABLE users;\n", lintscan.ForDialect("sqlite"))...)

	c.Assert(lintkit.RebuildCandidates(file), qt.DeepEquals, []int{2, 6})
}

// A replacement names, in the order the old table reports them, the columns
// whose values its copy does not write into the column of the same name.
func TestRebuildChanges_NamesTheLostColumns(t *testing.T) {
	tests := []struct {
		name          string
		copyStatement string
		want          []string
	}{
		{name: "a full copy", copyStatement: "INSERT INTO new_users (id, name) SELECT id, name FROM users"},
		{name: "SELECT *", copyStatement: "INSERT INTO new_users SELECT * FROM users"},
		{name: "a constant in a column the old table does not hold", copyStatement: "INSERT INTO new_users (id, name, flag) SELECT id, name, 0 FROM users"},
		{name: "NULL", copyStatement: "INSERT INTO new_users (id, name) SELECT id, NULL FROM users", want: []string{"name"}},
		{name: "two columns swapped", copyStatement: "INSERT INTO new_users (id, name) SELECT name, id FROM users", want: []string{"id", "name"}},
		{name: "a column left out", copyStatement: "INSERT INTO new_users (name) SELECT name FROM users", want: []string{"id"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changes := lintkit.RebuildChanges(rebuildOf("new_users", test.copyStatement), columnsOf(usersColumns))
			c.Assert(changes, qt.DeepEquals, map[int]lintkit.RebuildChange{
				2: {Create: 0, LostColumns: test.want, Renames: []int{3}},
			})
		})
	}
}

// Without the old table's columns no replacement says what it lost.
func TestRebuildChanges_LeavesOutAnUnknownTable(t *testing.T) {
	c := qt.New(t)
	file := rebuildOf("new_users", "INSERT INTO new_users (id, name) SELECT id, NULL FROM users")
	c.Assert(lintkit.RebuildChanges(file, nil), qt.HasLen, 0)
	c.Assert(lintkit.RebuildChanges(file, columnsOf(nil)), qt.HasLen, 0)
}

// Whatever a file holds, reading its copies does not panic, and a drop a
// rebuild keeps is always a candidate the old table's columns could decide.
func FuzzRebuilds(f *testing.F) {
	for _, seed := range []string{
		"INSERT INTO users_tmp (id, name) SELECT id, name FROM users",
		"INSERT INTO users_tmp (id, name) SELECT u.id, IFNULL(u.name, 'x') AS name FROM users AS u",
		"INSERT INTO users_tmp (id, name) SELECT COALESCE(id, , FROM users",
		"INSERT INTO users_tmp (id, name) SELECT IFNULL([name FROM users]",
		"INSERT INTO users_tmp (id, name) SELECT id, ( FROM users",
		"INSERT INTO users_tmp SELECT * FROM users u AS",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, copyStatement string) {
		c := qt.New(t)
		file := rebuildOf("users_tmp", copyStatement)

		kept := slices.Sorted(maps.Keys(lintkit.Rebuilds(file, columnsOf(usersColumns)).Drops))
		candidates := lintkit.RebuildCandidates(file)
		lintkit.RebuildChanges(file, columnsOf(usersColumns))

		c.Assert(slices.DeleteFunc(kept, func(drop int) bool { return slices.Contains(candidates, drop) }), qt.HasLen, 0)
	})
}
