package columnkey_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
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

// takenDatabase holds table c beside table d, each with a key, a CHECK on c,
// and a relation of another schema.
var takenDatabase = &schemamodel.Database{
	Tables: []schemamodel.Table{
		{StructName: "C", Name: "c"},
		{StructName: "D", Name: "d"},
		{StructName: "E", Name: "e", Schema: "other"},
	},
	Indexes: []schemamodel.Index{
		{Name: "c_idx", StructName: "C", Fields: []string{"y"}},
		{Name: "d_idx", StructName: "D", Fields: []string{"y"}},
	},
	Constraints: []schemamodel.Constraint{
		{Name: "c_uq", StructName: "C", Type: "UNIQUE", Columns: []string{"z"}},
		{Name: "c_ck", StructName: "C", Type: "CHECK", CheckExpression: "z > 0"},
		{Name: "d_uq", StructName: "D", Type: "UNIQUE", Columns: []string{"z"}},
	},
}

// Taken counts the names the key of a column of c cannot take. On MySQL and
// MariaDB those are c's own index names: its indexes, its UNIQUE constraints
// and PRIMARY. On PostgreSQL they are the constraints and relations of c's
// schema, whichever table holds them.
func TestTaken(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		held    string
		want    bool
	}{
		{name: "MySQL, an index of the table", dialect: platform.MySQL, held: "c_idx", want: true},
		{name: "MySQL, in another case", dialect: platform.MySQL, held: "C_IDX", want: true},
		{name: "MySQL, a UNIQUE of the table", dialect: platform.MySQL, held: "c_uq", want: true},
		{name: "MySQL, the primary key", dialect: platform.MySQL, held: "PRIMARY", want: true},
		{name: "MySQL, a CHECK of the table", dialect: platform.MySQL, held: "c_ck", want: false},
		{name: "MySQL, an index of another table", dialect: platform.MySQL, held: "d_idx", want: false},
		{name: "MariaDB, a UNIQUE of the table", dialect: platform.MariaDB, held: "c_uq", want: true},
		{name: "PostgreSQL, a CHECK of the table", dialect: platform.Postgres, held: "c_ck", want: true},
		{name: "PostgreSQL, an index of another table", dialect: platform.Postgres, held: "d_idx", want: true},
		{name: "PostgreSQL, another table", dialect: platform.Postgres, held: "d", want: true},
		{name: "PostgreSQL, a table of another schema", dialect: platform.Postgres, held: "e", want: false},
		{name: "PostgreSQL, a name nothing holds", dialect: platform.Postgres, held: "c_x_key", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			taken := columnkey.Taken(test.dialect, []*schemamodel.Database{takenDatabase}, takenDatabase.Tables[0])

			c.Assert(taken(test.held), qt.Equals, test.want)
		})
	}
}
