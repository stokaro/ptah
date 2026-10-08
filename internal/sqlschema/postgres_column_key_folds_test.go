package sqlschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// keysOfC lists the keys the model declares for table c, the way pg_constraint
// lists them: the primary key as `pkey <name>(<columns>)`, with an empty name
// for one the server names, a column's own UNIQUE as `key column(<column>)`,
// and every UNIQUE constraint as `key <name>(<columns>)`, sorted.
func keysOfC(database schemamodel.Database) []string {
	var keys []string
	for _, table := range database.Tables {
		if table.Name != "c" {
			continue
		}
		columns := table.PrimaryKey
		for _, field := range database.Fields {
			if field.StructName == table.StructName && field.Primary && len(columns) == 0 {
				columns = []string{field.Name}
			}
		}
		if len(columns) > 0 {
			keys = append(keys, "pkey "+table.PrimaryKeyName+"("+strings.Join(columns, ",")+")")
		}
		for _, field := range database.Fields {
			if field.StructName == table.StructName && field.Unique {
				keys = append(keys, "key column("+field.Name+")")
			}
		}
	}
	for _, constraint := range database.Constraints {
		if constraint.Table == "c" && constraint.Type == "UNIQUE" {
			keys = append(keys, "key "+constraint.Name+"("+strings.Join(constraint.Columns, ",")+")")
		}
	}
	slices.Sort(keys)
	return keys
}

// TestRead_PostgresColumnKeyFolds keeps one key where PostgreSQL builds one
// from a column's own UNIQUE and an equal key of the same CREATE TABLE. Every
// row was run on PostgreSQL 18.6, and the keys pg_constraint reports are the
// ones the row expects: a column's own UNIQUE left in the model would be the
// key `<table>_<column>_key`, beside the other one.
func TestRead_PostgresColumnKeyFolds(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "a column's UNIQUE and a named UNIQUE",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT uq_a UNIQUE (a));",
			want: []string{"key uq_a(a)", "pkey (id)"},
		},
		{
			name: "a named UNIQUE written before the column",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, CONSTRAINT uq_a UNIQUE (a), a int UNIQUE);",
			want: []string{"key uq_a(a)", "pkey (id)"},
		},
		{
			name: "a column's UNIQUE and an unnamed UNIQUE",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, UNIQUE (a));",
			want: []string{"key c_a_key(a)", "pkey (id)"},
		},
		{
			name: "a column's UNIQUE, then two names",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT u1 UNIQUE (a), CONSTRAINT u2 UNIQUE (a));",
			want: []string{"key u1(a)", "pkey (id)"},
		},
		{
			name: "a column's UNIQUE on the column that is the key",
			sql:  "CREATE TABLE c (id int PRIMARY KEY UNIQUE, a int);",
			want: []string{"pkey (id)"},
		},
		{
			name: "a column's UNIQUE on the table's key",
			sql:  "CREATE TABLE c (a int UNIQUE, PRIMARY KEY (a));",
			want: []string{"pkey (a)"},
		},
		{
			name: "a column's primary key takes the first of two names",
			sql:  "CREATE TABLE c (id int, a int PRIMARY KEY, CONSTRAINT u1 UNIQUE (a), CONSTRAINT u2 UNIQUE (a));",
			want: []string{"pkey u1(a)"},
		},
		{
			name: "a primary key over two columns",
			sql:  "CREATE TABLE c (id int, a int, b int, PRIMARY KEY (a, b), UNIQUE (a, b));",
			want: []string{"pkey (a,b)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(keysOfC(database), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_PostgresColumnKeysThatStayApart are the controls for the rows
// above: a column's own UNIQUE the server builds apart from the other keys.
// Each row was run on PostgreSQL 18.6, and pg_constraint lists every key the
// row expects.
func TestRead_PostgresColumnKeysThatStayApart(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "INCLUDE columns on the other key",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT uq_a UNIQUE (a) INCLUDE (id));",
			want: []string{"key column(a)", "key uq_a(a)", "pkey (id)"},
		},
		{
			name: "NULLS NOT DISTINCT on the other key",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT uq_a UNIQUE NULLS NOT DISTINCT (a));",
			want: []string{"key column(a)", "key uq_a(a)", "pkey (id)"},
		},
		{
			name: "a key the column only leads",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int, CONSTRAINT uq_ab UNIQUE (a, b));",
			want: []string{"key column(a)", "key uq_ab(a,b)", "pkey (id)"},
		},
		{
			name: "a later statement adds a UNIQUE beside a column's",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE);\n" +
				"ALTER TABLE c ADD CONSTRAINT uq_a UNIQUE (a);",
			want: []string{"key column(a)", "key uq_a(a)", "pkey (id)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(keysOfC(database), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_MySQLColumnKeysDoNotFold keeps the rule to PostgreSQL. Measured on
// MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3, `a int UNIQUE,
// CONSTRAINT uq_a UNIQUE (a)` builds the keys a and uq_a.
func TestRead_MySQLColumnKeysDoNotFold(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read(
				[]byte("CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT uq_a UNIQUE (a));"), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(keysOfC(database), qt.DeepEquals, []string{"key column(a)", "key uq_a(a)", "pkey (id)"})
		})
	}
}

// TestRead_PostgresFoldedKeysRenderAsTheServerBuiltThem renders the model a
// folding CREATE TABLE reads to, which is what `schema apply` sends to an
// empty database: the statement builds the keys the file builds when run by
// hand, under the same names. A column's primary key that takes a UNIQUE's
// name renders as a table constraint, where the name has a place.
func TestRead_PostgresFoldedKeysRenderAsTheServerBuiltThem(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    string
		wantNot string
	}{
		{
			name:    "a column's primary key takes the first name",
			sql:     "CREATE TABLE c (id int, a int PRIMARY KEY, CONSTRAINT u1 UNIQUE (a), CONSTRAINT u2 UNIQUE (a));",
			want:    `CONSTRAINT "u1" PRIMARY KEY ("a")`,
			wantNot: `"u2"`,
		},
		{
			name:    "a column's UNIQUE and a named UNIQUE",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, CONSTRAINT uq_a UNIQUE (a));",
			want:    `CONSTRAINT "uq_a" UNIQUE ("a")`,
			wantNot: `"a" int UNIQUE`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)
			c.Assert(err, qt.IsNil)
			schemamodel.Finalize(&database)

			statements, err := builtin.GetOrderedCreateStatements(&database, platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
			c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), test.wantNot)
		})
	}
}
