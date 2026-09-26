package sqlschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// checkNames lists every CHECK the model carries, on a column or on a table,
// as `<table> <name>: <expression>`, sorted.
func checkNames(database schemamodel.Database) []string {
	tables := make(map[string]string, len(database.Tables))
	for _, table := range database.Tables {
		tables[table.StructName] = table.QualifiedName()
	}
	var names []string
	for _, field := range database.Fields {
		if field.Check != "" {
			names = append(names, tables[field.StructName]+" "+field.CheckName+": "+field.Check)
		}
	}
	for _, constraint := range database.Constraints {
		if strings.EqualFold(constraint.Type, "CHECK") {
			names = append(names, constraint.Table+" "+constraint.Name+": "+constraint.CheckExpression)
		}
	}
	slices.Sort(names)
	return names
}

// TestRead_PostgresUnnamedCheckNames_HappyPath gives an unnamed CHECK the name
// PostgreSQL gives it, so a schema file compares equal to the database its own
// SQL built (stokaro/ptah#3729). Each expected name was read back from
// pg_constraint on PostgreSQL 18.6 after running the row's SQL.
func TestRead_PostgresUnnamedCheckNames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "one column, on the table and on the column",
			sql: "CREATE TABLE a (plan text, CHECK (plan IN ('x','y')));\n" +
				"CREATE TABLE b (plan text CHECK (plan IN ('x','y')));",
			want: []string{"a a_plan_check: plan IN ('x','y')", "b b_plan_check: plan IN ('x','y')"},
		},
		{
			name: "two columns",
			sql:  "CREATE TABLE c (lo int, hi int, CHECK (lo < hi));",
			want: []string{"c c_check: lo < hi"},
		},
		{
			name: "three on one table, one of them on a column",
			sql:  "CREATE TABLE d (lo int CHECK (lo > 0), hi int, CHECK (lo < hi), CHECK (hi > 0));",
			want: []string{"d d_check: lo < hi", "d d_hi_check: hi > 0", "d d_lo_check: lo > 0"},
		},
		{
			name: "two columns on a column",
			sql:  "CREATE TABLE e (a int CHECK (a IS NULL OR b IS NOT NULL), b int);",
			want: []string{"e e_check: a IS NULL OR b IS NOT NULL"},
		},
		{
			name: "no column, twice",
			sql:  "CREATE TABLE f (a int, CHECK (true), CHECK (1 > 0));",
			want: []string{"f f_check1: 1 > 0", "f f_check: true"},
		},
		{
			name: "two on one column",
			sql:  "CREATE TABLE h2 (a int CHECK (a > 0) CHECK (a < 10));",
			want: []string{"h2 h2_a_check1: a < 10", "h2 h2_a_check: a > 0"},
		},
		{
			name: "a named CHECK on the column before an unnamed one",
			sql:  "CREATE TABLE h3 (a int CONSTRAINT h3_named CHECK (a > 0) CHECK (a < 10));",
			want: []string{"h3 h3_a_check: a < 10", "h3 h3_named: a > 0"},
		},
		{
			name: "a named CHECK takes the name first",
			sql:  "CREATE TABLE i2 (a int, CONSTRAINT i2_a_check CHECK (a < 10), CHECK (a > 0));",
			want: []string{"i2 i2_a_check1: a > 0", "i2 i2_a_check: a < 10"},
		},
		{
			name: "a constraint of another table in the schema takes the name",
			sql: "CREATE TABLE j (x int, CONSTRAINT k_a_check CHECK (x > 0));\n" +
				"CREATE TABLE k (a int CHECK (a > 0));",
			want: []string{"j k_a_check: x > 0", "k k_a_check1: a > 0"},
		},
		{
			name: "a constraint in another schema does not",
			sql: "CREATE SCHEMA o;\nCREATE TABLE o.x (a int CONSTRAINT p_a_check CHECK (a > 0));\n" +
				"CREATE TABLE p (a int CHECK (a > 0));",
			want: []string{"o.x p_a_check: a > 0", "p p_a_check: a > 0"},
		},
		{
			name: "an index of the name does not",
			sql: "CREATE TABLE lx (a int);\nCREATE INDEX l_a_check ON lx (a);\n" +
				"CREATE TABLE l (a int CHECK (a > 0));",
			want: []string{"l l_a_check: a > 0"},
		},
		{
			name: "ALTER TABLE adds a CHECK and a column carrying one",
			sql: "CREATE TABLE al (a int, b int);\n" +
				"ALTER TABLE al ADD CHECK (a > 0);\n" +
				"ALTER TABLE al ADD CHECK (a < 9);\n" +
				"ALTER TABLE al ADD COLUMN c int CHECK (c > 0);\n" +
				"ALTER TABLE al ADD CHECK (a < b);\n" +
				"ALTER TABLE al ADD COLUMN d int CHECK (d > a);",
			want: []string{
				"al al_a_check1: a < 9", "al al_a_check: a > 0", "al al_c_check: c > 0",
				"al al_check1: d > a", "al al_check: a < b",
			},
		},
		{
			name: "one ALTER TABLE adds a column and a CHECK on it",
			sql: "CREATE TABLE al (a int);\n" +
				"ALTER TABLE al ADD COLUMN e int CHECK (e > 0), ADD CHECK (e < 9);",
			want: []string{"al al_e_check1: e < 9", "al al_e_check: e > 0"},
		},
		{
			name: "a column restated with IF NOT EXISTS",
			sql: "CREATE TABLE t (a int CHECK (a > 0));\n" +
				"ALTER TABLE t ADD COLUMN IF NOT EXISTS a int CHECK (a > 0);",
			want: []string{"t t_a_check: a > 0"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(checkNames(database), qt.DeepEquals, test.want)
		})
	}
}

// TestReadOnto_PostgresUnnamedCheckNames_ClaimsEarlierFiles numbers a CHECK
// whose name an earlier file of the same document already took, as the server
// does when it runs the files in order.
func TestReadOnto_PostgresUnnamedCheckNames_ClaimsEarlierFiles(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte("CREATE TABLE d (lo int, hi int, CHECK (lo < hi));"), "postgres")
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte("ALTER TABLE d ADD CHECK (lo < hi + 1);"), "postgres", &earlier)

	c.Assert(err, qt.IsNil)
	c.Assert(checkNames(later), qt.DeepEquals, []string{"d d_check1: lo < hi + 1"})
}

// TestRead_UnnamedCheckStaysUnnamedOutsidePostgres keeps the rule to the engine
// it was measured on. SQLite keeps no name for an unnamed CHECK, so a document
// read for it keeps the CHECK unnamed. MySQL and MariaDB have rules of their
// own, in mysql_check_names_test.go.
func TestRead_UnnamedCheckStaysUnnamedOutsidePostgres(t *testing.T) {
	for _, dialect := range []string{"sqlite"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(
				"CREATE TABLE d (lo int CHECK (lo > 0), hi int, CHECK (lo < hi), CHECK (hi > 0));"), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(checkNames(database), qt.DeepEquals, []string{"d : hi > 0", "d : lo < hi", "d : lo > 0"})
		})
	}
}
