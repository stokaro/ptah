package mysqlname_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/mysqlname"
)

// sameIgnoringCase compares index names the way MySQL does.
func sameIgnoringCase(a, b string) bool {
	return strings.EqualFold(a, b)
}

func TestForeignKey(t *testing.T) {
	tests := []struct {
		name  string
		table string
		n     int
		want  string
	}{
		{name: "the first key", table: "c", n: 1, want: "c_ibfk_1"},
		{name: "a later key", table: "a4", n: 6, want: "a4_ibfk_6"},
		{name: "the table keeps its case", table: "MixedChild", n: 1, want: "MixedChild_ibfk_1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.ForeignKey(test.table, test.n), qt.Equals, test.want)
		})
	}
}

// TestForeignKeyNumber_HappyPath covers the names ALTER TABLE counts. Each row
// is a key a table held on MySQL 8.4.11 while an unnamed key was added, and
// the server named the added key one more than want.
func TestForeignKeyNumber_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		table string
		key   string
		want  int
	}{
		{name: "a key the server named", table: "a4", key: "a4_ibfk_1", want: 1},
		{name: "a key written with a number the server skips to", table: "a4", key: "a4_ibfk_5", want: 5},
		{name: "a number past nine", table: "t", key: "t_ibfk_12", want: 12},
		{name: "a table in mixed case", table: "MixedAlt", key: "MixedAlt_ibfk_1", want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := mysqlname.ForeignKeyNumber(test.table, test.key)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(mysqlname.IsForeignKeyName(test.table, test.key), qt.IsTrue)
		})
	}
}

// TestForeignKeyNumber_FailurePath covers the names ALTER TABLE does not
// count. The rows measured on MySQL 8.4.11 left the added key at `_ibfk_1`.
func TestForeignKeyNumber_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		table string
		key   string
	}{
		{name: "a name of the author's", table: "a9", key: "fk_a9"},
		{name: "the table in another case", table: "MixedAlt", key: "mixedalt_ibfk_3"},
		{name: "a suffix that is not a number", table: "f1", key: "f1_ibfk_x"},
		{name: "a number with a leading zero", table: "f2", key: "f2_ibfk_07"},
		{name: "zero", table: "f3", key: "f3_ibfk_0"},
		{name: "no number", table: "f4", key: "f4_ibfk_"},
		{name: "another table's key", table: "c", key: "cc_ibfk_1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := mysqlname.ForeignKeyNumber(test.table, test.key)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, 0)
			c.Assert(mysqlname.IsForeignKeyName(test.table, test.key), qt.IsFalse)
		})
	}
}

// TestIsUnnamedKeyIndexName_HappyPath covers the names MySQL 8.4.11 gave the
// index it built for an unnamed key, read back from information_schema.
func TestIsUnnamedKeyIndexName_HappyPath(t *testing.T) {
	long := strings.Repeat("c", 64)
	tests := []struct {
		name   string
		column string
		index  string
	}{
		{name: "the key's first column", column: "p_id", index: "p_id"},
		{name: "the column's name taken", column: "p_id", index: "p_id_2"},
		{name: "a later number", column: "a", index: "a_10"},
		{name: "another case", column: "ParentId", index: "parentid"},
		{name: "a long column cut to 61 bytes before its number", column: long, index: long[:61] + "_2"},
		{name: "a long column MariaDB does not cut", column: long[:62], index: long[:62] + "_2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.IsUnnamedKeyIndexName(test.column, test.index, sameIgnoringCase), qt.IsTrue)
		})
	}
}

// TestIsUnnamedKeyIndexName_FailurePath covers names the server does not give
// the index of an unnamed key.
func TestIsUnnamedKeyIndexName_FailurePath(t *testing.T) {
	long := strings.Repeat("c", 64)
	tests := []struct {
		name   string
		column string
		index  string
	}{
		{name: "the constraint's name", column: "p_id", index: "c_ibfk_1"},
		{name: "another column", column: "p_id", index: "o"},
		{name: "a first number, which the server never writes", column: "p_id", index: "p_id_1"},
		{name: "a number with a leading zero", column: "p_id", index: "p_id_02"},
		{name: "a suffix that is not a number", column: "p_id", index: "p_id_x"},
		{name: "a longer column that only starts with the name", column: "p_id_extra", index: "p_id_2"},
		{name: "a long column cut in the wrong place", column: long, index: long[:60] + "_2"},
		{name: "no column", column: "", index: "p_id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.IsUnnamedKeyIndexName(test.column, test.index, sameIgnoringCase), qt.IsFalse)
		})
	}
}

// TestNamesForeignKeys_HappyPath covers the dialects the rules were measured
// on: MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 name an unnamed key
// `<table>_ibfk_<n>` and its index after the key's first column alike.
func TestNamesForeignKeys_HappyPath(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB, "MariaDB"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.NamesForeignKeys(dialect), qt.IsTrue)
		})
	}
}

// TestNamesForeignKeys_FailurePath keeps the rules off the engines that name
// an unnamed key another way or keep no name.
func TestNamesForeignKeys_FailurePath(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.SQLite, platform.SQLServer, ""} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.NamesForeignKeys(dialect), qt.IsFalse)
		})
	}
}

// TestClauseIndexNamesKey pins the one rule the engines disagree on. Measured,
// `FOREIGN KEY idx_c_p (p_id) REFERENCES p(id)` names the key `idx_c_p` on
// MariaDB 11.8.9 and `c_ibfk_1` on MySQL 8.4.11 and 26.7.0.
func TestClauseIndexNamesKey(t *testing.T) {
	tests := []struct {
		dialect string
		want    bool
	}{
		{dialect: platform.MariaDB, want: true},
		{dialect: platform.MySQL, want: false},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.ClauseIndexNamesKey(test.dialect), qt.Equals, test.want)
		})
	}
}

// TestNextForeignKeyNumber covers the number ALTER TABLE starts from. Each row
// is what a table held before a statement added an unnamed key, measured on
// MySQL 8.4.11, 26.7.0 and MariaDB 11.8.9.
func TestNextForeignKeyNumber(t *testing.T) {
	tests := []struct {
		name  string
		table string
		held  []string
		want  int
	}{
		{name: "a table with no keys", table: "c", want: 1},
		{name: "one more than the highest number", table: "a4", held: []string{"a4_ibfk_1", "a4_ibfk_5"}, want: 6},
		{name: "a key the statement drops still counts", table: "c", held: []string{"c_ibfk_3"}, want: 4},
		{name: "a name of the author's", table: "a9", held: []string{"fk_a9"}, want: 1},
		{name: "the table in another case", table: "MixedAlt", held: []string{"mixedalt_ibfk_3"}, want: 1},
		{name: "names the rule does not count", table: "c", held: []string{"c_ibfk_07", "c_ibfk_x", ""}, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.NextForeignKeyNumber(test.table, test.held), qt.Equals, test.want)
		})
	}
}

// TestIsNumberedForeignKeyName_HappyPath covers the names MariaDB 12.1.2,
// 12.2.2 and 12.3.3 gave the keys a statement left unnamed.
func TestIsNumberedForeignKeyName_HappyPath(t *testing.T) {
	for _, name := range []string{"1", "4", "12"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.IsNumberedForeignKeyName(platform.MariaDB, name), qt.IsTrue)
		})
	}
}

// TestIsNumberedForeignKeyName_FailurePath covers names the server never
// writes for an unnamed key, and MySQL, which never writes the number alone.
func TestIsNumberedForeignKeyName_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		key     string
	}{
		{name: "a leading zero", dialect: platform.MariaDB, key: "03"},
		{name: "zero", dialect: platform.MariaDB, key: "0"},
		{name: "the older scheme", dialect: platform.MariaDB, key: "c_ibfk_1"},
		{name: "a name of the author's", dialect: platform.MariaDB, key: "fk_c"},
		{name: "MySQL", dialect: platform.MySQL, key: "1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.IsNumberedForeignKeyName(test.dialect, test.key), qt.IsFalse)
		})
	}
}

// TestIsServerForeignKeyName covers the names the comparison reads as the
// server's own for an unnamed key of `c`: either scheme on MariaDB, the older
// one on MySQL, and neither on an engine the package does not speak for.
func TestIsServerForeignKeyName(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		key     string
		want    bool
	}{
		{name: "MySQL, the table's scheme", dialect: platform.MySQL, key: "c_ibfk_1", want: true},
		{name: "MySQL, a number", dialect: platform.MySQL, key: "1", want: false},
		{name: "MariaDB, the table's scheme", dialect: platform.MariaDB, key: "c_ibfk_1", want: true},
		{name: "MariaDB, a number", dialect: platform.MariaDB, key: "1", want: true},
		{name: "MariaDB, a name of the author's", dialect: platform.MariaDB, key: "fk_c", want: false},
		{name: "SQLite", dialect: platform.SQLite, key: "c_ibfk_1", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.IsServerForeignKeyName(test.dialect, "c", test.key), qt.Equals, test.want)
		})
	}
}
