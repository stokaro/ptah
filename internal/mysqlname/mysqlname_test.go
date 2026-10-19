package mysqlname_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/mysqlname"
)

// sameIgnoringCase compares index names the way MySQL does.
func sameIgnoringCase(a, b string) bool {
	return strings.EqualFold(a, b)
}

// describedAs is the built-in descriptor of dialect, whose naming facts the
// rules here are asked with.
func describedAs(c *qt.C, dialect string) descriptor.Dialect {
	described, found := platform.Lookup(dialect)
	c.Assert(found, qt.IsTrue)
	return described
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
// index it built for an unnamed key, read back from information_schema, and
// the whole name MariaDB keeps. The cuts are the ones MySQL's descriptor
// states, beside none.
func TestIsUnnamedKeyIndexName_HappyPath(t *testing.T) {
	long := strings.Repeat("c", 64)
	accented := strings.Repeat("c", 60) + "é" + "ccc"
	tests := []struct {
		name      string
		baseBytes []int
		column    string
		index     string
	}{
		{name: "the key's first column", baseBytes: []int{61}, column: "p_id", index: "p_id"},
		{name: "the column's name taken", baseBytes: []int{61}, column: "p_id", index: "p_id_2"},
		{name: "a later number", baseBytes: []int{61}, column: "a", index: "a_10"},
		{name: "another case", baseBytes: []int{61}, column: "ParentId", index: "parentid"},
		{name: "a long column cut to 61 bytes before its number", baseBytes: []int{61}, column: long, index: long[:61] + "_2"},
		{name: "a long column MariaDB does not cut", baseBytes: []int{61}, column: long[:62], index: long[:62] + "_2"},
		{name: "a whole column where no cut is stated", column: long[:62], index: long[:62] + "_2"},
		{name: "a cut before a character the count would split", baseBytes: []int{61}, column: accented, index: accented[:60] + "_2"},
		{name: "the second of two cuts", baseBytes: []int{50, 61}, column: long, index: long[:61] + "_3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.IsUnnamedKeyIndexName(test.baseBytes, test.column, test.index, sameIgnoringCase), qt.IsTrue)
		})
	}
}

// TestIsUnnamedKeyIndexName_FailurePath covers names the server does not give
// the index of an unnamed key.
func TestIsUnnamedKeyIndexName_FailurePath(t *testing.T) {
	long := strings.Repeat("c", 64)
	tests := []struct {
		name      string
		baseBytes []int
		column    string
		index     string
	}{
		{name: "the constraint's name", baseBytes: []int{61}, column: "p_id", index: "c_ibfk_1"},
		{name: "another column", baseBytes: []int{61}, column: "p_id", index: "o"},
		{name: "a first number, which the server never writes", baseBytes: []int{61}, column: "p_id", index: "p_id_1"},
		{name: "a number with a leading zero", baseBytes: []int{61}, column: "p_id", index: "p_id_02"},
		{name: "a suffix that is not a number", baseBytes: []int{61}, column: "p_id", index: "p_id_x"},
		{name: "a longer column that only starts with the name", baseBytes: []int{61}, column: "p_id_extra", index: "p_id_2"},
		{name: "a long column cut in the wrong place", baseBytes: []int{61}, column: long, index: long[:60] + "_2"},
		{name: "a long column cut where no cut is stated", column: long, index: long[:61] + "_2"},
		{name: "no column", baseBytes: []int{61}, column: "", index: "p_id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.IsUnnamedKeyIndexName(test.baseBytes, test.column, test.index, sameIgnoringCase), qt.IsFalse)
		})
	}
}

// TestIndexName covers the name a server gives an index it names after a
// column, with the cut each built-in descriptor states. Measured on MySQL
// 9.7.2, a 64-character column yields a 63-character `_2` and a 64-character
// `_10`; MariaDB keeps the column whole.
func TestIndexName(t *testing.T) {
	long := strings.Repeat("c", 64)
	accented := strings.Repeat("c", 60) + "é" + "ccc"
	tests := []struct {
		name    string
		dialect string
		base    string
		taken   []string
		want    string
	}{
		{name: "MySQL, a free name", dialect: platform.MySQL, base: long, want: long},
		{name: "MySQL, a long name taken", dialect: platform.MySQL, base: long, taken: []string{long}, want: long[:61] + "_2"},
		{
			name: "MySQL, the tenth", dialect: platform.MySQL, base: long,
			taken: []string{long, long[:61] + "_2", long[:61] + "_3", long[:61] + "_4", long[:61] + "_5",
				long[:61] + "_6", long[:61] + "_7", long[:61] + "_8", long[:61] + "_9"},
			want: long[:61] + "_10",
		},
		{name: "MySQL, a cut before a split character", dialect: platform.MySQL, base: accented, taken: []string{accented}, want: accented[:60] + "_2"},
		{name: "MySQL, a short name taken", dialect: platform.MySQL, base: "p_id", taken: []string{"P_ID"}, want: "p_id_2"},
		{name: "MariaDB, a long name taken", dialect: platform.MariaDB, base: long[:62], taken: []string{long[:62]}, want: long[:62] + "_2"},
		{name: "MariaDB, a name past the cut taken", dialect: platform.MariaDB, base: long, taken: []string{long}, want: long + "_2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			taken := func(name string) bool {
				return slices.ContainsFunc(test.taken, func(held string) bool { return strings.EqualFold(held, name) })
			}

			got := mysqlname.IndexName(describedAs(c, test.dialect).IndexNaming.BaseBytes, test.base, taken)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestForeignKeyNameLimit pins the longest derived name each server keeps as
// derived, from the rules and the identifier limit its descriptor states.
// Measured over a utf8mb4 connection on MySQL 8.4.11 and 26.7.0 and MariaDB
// 11.8.9: MySQL keeps 64 characters in either statement and refuses 65;
// MariaDB refuses 64 in CREATE TABLE and keeps 63; its ALTER TABLE keeps 64
// and cuts 65 to 64.
func TestForeignKeyNameLimit(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement mysqlname.Statement
		want      int
	}{
		{name: "MySQL, CREATE TABLE", dialect: platform.MySQL, statement: mysqlname.CreateTable, want: 64},
		{name: "MySQL, ALTER TABLE", dialect: platform.MySQL, statement: mysqlname.AlterTable, want: 64},
		{name: "MariaDB, CREATE TABLE", dialect: platform.MariaDB, statement: mysqlname.CreateTable, want: 63},
		{name: "MariaDB, ALTER TABLE", dialect: platform.MariaDB, statement: mysqlname.AlterTable, want: 64},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			described := describedAs(c, test.dialect)

			got := mysqlname.ForeignKeyNameLimit(described.ForeignKeyNameRules, described.IdentifierLimit, test.statement)

			c.Assert(got, qt.Equals, test.want)
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

			rules := describedAs(c, platform.MariaDB).ForeignKeyNameRules

			c.Assert(mysqlname.IsNumberedForeignKeyName(rules, name), qt.IsTrue)
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

			rules := describedAs(c, test.dialect).ForeignKeyNameRules

			c.Assert(mysqlname.IsNumberedForeignKeyName(rules, test.key), qt.IsFalse)
		})
	}
}

// TestIsServerForeignKeyName covers the names the comparison reads as the
// server's own for an unnamed key of `c`: either scheme on MariaDB, and the
// older one on MySQL.
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rules := describedAs(c, test.dialect).ForeignKeyNameRules

			c.Assert(mysqlname.IsServerForeignKeyName(rules, "c", test.key), qt.Equals, test.want)
		})
	}
}
