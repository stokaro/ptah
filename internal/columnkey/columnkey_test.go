package columnkey_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/columnkey"
)

// takenNames reports the names given as held.
func takenNames(names ...string) func(string) bool {
	return func(name string) bool { return slices.Contains(names, name) }
}

// Each row is the name the server gave the key of `x int UNIQUE` on table c,
// measured on MySQL 8.4.11, MariaDB 11.8.9 and PostgreSQL 18.6. A name another
// object holds moves the key to the next number: `_2` on MySQL and MariaDB,
// `1` on PostgreSQL.
func TestName_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		table   string
		column  string
		taken   func(string) bool
		want    string
	}{
		{name: "MySQL names the key after the column", dialect: platform.MySQL, table: "c", column: "x", want: "x"},
		{name: "MariaDB names the key after the column", dialect: platform.MariaDB, table: "c", column: "x", want: "x"},
		{
			name: "MySQL takes the next number when the name is held", dialect: platform.MySQL, table: "c", column: "x",
			taken: takenNames("x", "x_2"), want: "x_3",
		},
		{name: "PostgreSQL names the key after the table and the column", dialect: platform.Postgres, table: "c", column: "x", want: "c_x_key"},
		{
			name: "PostgreSQL takes the next number when the name is held", dialect: platform.Postgres, table: "c", column: "x",
			taken: takenNames("c_x_key", "c_x_key1"), want: "c_x_key2",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := columnkey.Name(test.dialect, test.table, test.column, test.taken)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// An engine whose naming is not measured has no answer, and [columnkey.Named]
// says so before a caller asks.
func TestName_FailurePath(t *testing.T) {
	for _, dialect := range []string{platform.SQLServer, platform.SQLite, platform.Oracle, platform.ClickHouse} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			got, ok := columnkey.Name(dialect, "c", "x", nil)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
			c.Assert(columnkey.Named(dialect), qt.IsFalse)
		})
	}
}

// The engines the rule is measured on are named, under any spelling the
// dialect normalization accepts.
func TestNamed(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB, platform.Postgres, "postgresql"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(columnkey.Named(dialect), qt.IsTrue)
		})
	}
}

// MySQL and MariaDB compare index names without case; PostgreSQL compares them
// as written.
func TestSame(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		a, b    string
		want    bool
	}{
		{name: "MySQL, another case", dialect: platform.MySQL, a: "X", b: "x", want: true},
		{name: "MariaDB, another case", dialect: platform.MariaDB, a: "X", b: "x", want: true},
		{name: "MySQL, another name", dialect: platform.MySQL, a: "x_2", b: "x", want: false},
		{name: "PostgreSQL, another case", dialect: platform.Postgres, a: "C_X_KEY", b: "c_x_key", want: false},
		{name: "PostgreSQL, the same name", dialect: platform.Postgres, a: "c_x_key", b: "c_x_key", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(columnkey.Same(test.dialect, test.a, test.b), qt.Equals, test.want)
		})
	}
}

// Measured on MySQL 8.4.11 and PostgreSQL 18.6: a CHECK named `x` leaves the
// MySQL key `x` free, and a CHECK named c_x_key refuses the PostgreSQL key
// c_x_key.
func TestShares(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		kind    string
		want    bool
	}{
		{name: "MySQL UNIQUE", dialect: platform.MySQL, kind: "UNIQUE", want: true},
		{name: "MariaDB UNIQUE", dialect: platform.MariaDB, kind: "unique", want: true},
		{name: "MySQL CHECK", dialect: platform.MySQL, kind: "CHECK", want: false},
		{name: "MySQL FOREIGN KEY", dialect: platform.MySQL, kind: "FOREIGN KEY", want: false},
		{name: "PostgreSQL CHECK", dialect: platform.Postgres, kind: "CHECK", want: true},
		{name: "PostgreSQL UNIQUE", dialect: platform.Postgres, kind: "UNIQUE", want: true},
		{name: "SQL Server UNIQUE", dialect: platform.SQLServer, kind: "UNIQUE", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(columnkey.Shares(test.dialect, test.kind), qt.Equals, test.want)
		})
	}
}
