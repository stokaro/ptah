package sqlschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// indexConstraints lists the primary key, UNIQUE and EXCLUDE constraints the
// model carries as `<table> <type> <name>`, sorted. A primary key the table
// carries adds its columns; one only a column declares is not listed.
func indexConstraints(database schemamodel.Database) []string {
	var listed []string
	for _, table := range database.Tables {
		if len(table.PrimaryKey) > 0 {
			listed = append(listed, table.Name+" PRIMARY KEY "+table.PrimaryKeyName+" ("+strings.Join(table.PrimaryKey, ", ")+")")
		}
	}
	for _, constraint := range database.Constraints {
		switch strings.ToUpper(constraint.Type) {
		case "UNIQUE", "EXCLUDE":
			listed = append(listed, constraint.Table+" "+constraint.Type+" "+constraint.Name)
		}
	}
	slices.Sort(listed)
	return listed
}

// TestRead_PostgresFoldedIndexConstraints takes out a UNIQUE or EXCLUDE that
// PostgreSQL does not build because an index constraint the same CREATE TABLE
// declares builds its index, and moves its name to that one when it has none.
// Each row was run on PostgreSQL 18.6, and want is what pg_constraint held.
func TestRead_PostgresFoldedIndexConstraints(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "a named UNIQUE over a column's key",
			sql:  "CREATE TABLE t1 (id int PRIMARY KEY, CONSTRAINT u UNIQUE (id));",
			want: []string{"t1 PRIMARY KEY u (id)"},
		},
		{
			name: "a named UNIQUE before the table's key",
			sql:  "CREATE TABLE t2 (id int, CONSTRAINT u2 UNIQUE (id), PRIMARY KEY (id));",
			want: []string{"t2 PRIMARY KEY u2 (id)"},
		},
		{
			name: "an unnamed UNIQUE over a column's key",
			sql:  "CREATE TABLE t21 (id int PRIMARY KEY, UNIQUE (id));",
			want: nil,
		},
		{
			name: "a named UNIQUE beside a named key",
			sql:  "CREATE TABLE t13 (id int, CONSTRAINT pk13 PRIMARY KEY (id), CONSTRAINT u13 UNIQUE (id));",
			want: []string{"t13 PRIMARY KEY pk13 (id)"},
		},
		{
			name: "a named UNIQUE after an unnamed one",
			sql:  "CREATE TABLE t3 (a int, UNIQUE (a), CONSTRAINT n3 UNIQUE (a));",
			want: []string{"t3 UNIQUE n3"},
		},
		{
			name: "two named UNIQUEs",
			sql:  "CREATE TABLE t4 (a int, CONSTRAINT n4a UNIQUE (a), CONSTRAINT n4b UNIQUE (a));",
			want: []string{"t4 UNIQUE n4a"},
		},
		{
			name: "NULLS DISTINCT written out and left out",
			sql:  "CREATE TABLE t20 (a int, UNIQUE NULLS DISTINCT (a), UNIQUE (a));",
			want: []string{"t20 UNIQUE t20_a_key"},
		},
		{
			name: "an EXCLUDE in other spacing",
			sql:  "CREATE TABLE t9 (r int, EXCLUDE USING btree (r WITH =) WHERE (r > 0), EXCLUDE USING btree ( r  WITH = ) WHERE (r>0));",
			want: []string{"t9 EXCLUDE t9_r_excl"},
		},
		{
			name: "an EXCLUDE in other case",
			sql:  "CREATE TABLE t17 (r int, EXCLUDE USING BTREE (R WITH =), EXCLUDE USING btree (r WITH =));",
			want: []string{"t17 EXCLUDE t17_r_excl"},
		},
		{
			name: "a named EXCLUDE after an unnamed one",
			sql:  "CREATE TABLE t19 (r int, EXCLUDE USING btree (r WITH =), CONSTRAINT x19 EXCLUDE USING btree (r WITH =));",
			want: []string{"t19 EXCLUDE x19"},
		},
		{
			name: "the next unnamed EXCLUDE takes the first number",
			sql:  "CREATE TABLE t22 (r int, s int, EXCLUDE USING btree (r WITH =), EXCLUDE USING btree (r WITH =), EXCLUDE USING hash (r WITH =));",
			want: []string{"t22 EXCLUDE t22_r_excl", "t22 EXCLUDE t22_r_excl1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(indexConstraints(database), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_PostgresIndexConstraintsTheServerKeeps is the control: index
// constraints PostgreSQL 18.6 builds an index for each of, because their keys
// differ or because separate statements declare them.
func TestRead_PostgresIndexConstraintsTheServerKeeps(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "NULLS NOT DISTINCT beside a plain UNIQUE",
			sql:  "CREATE TABLE t6 (a int, UNIQUE (a), UNIQUE NULLS NOT DISTINCT (a));",
			want: []string{"t6 UNIQUE t6_a_key", "t6 UNIQUE t6_a_key1"},
		},
		{
			name: "columns in another order",
			sql:  "CREATE TABLE t14 (a int, b int, UNIQUE (a, b), UNIQUE (b, a));",
			want: []string{"t14 UNIQUE t14_a_b_key", "t14 UNIQUE t14_b_a_key"},
		},
		{
			name: "another access method",
			sql:  "CREATE TABLE t10 (r int, EXCLUDE USING btree (r WITH =), EXCLUDE USING hash (r WITH =));",
			want: []string{"t10 EXCLUDE t10_r_excl", "t10 EXCLUDE t10_r_excl1"},
		},
		{
			name: "a UNIQUE and an EXCLUDE",
			sql:  "CREATE TABLE t12 (a int, UNIQUE (a), EXCLUDE USING btree (a WITH =));",
			want: []string{"t12 EXCLUDE t12_a_excl", "t12 UNIQUE t12_a_key"},
		},
		{
			name: "an element in parentheses",
			sql:  "CREATE TABLE t16 (r int, EXCLUDE USING btree ((r) WITH =), EXCLUDE USING btree (r WITH =));",
			want: []string{"t16 EXCLUDE t16_r_excl", "t16 EXCLUDE t16_r_excl1"},
		},
		{
			name: "ALTER TABLE adds a UNIQUE over the key",
			sql:  "CREATE TABLE g1 (id int, CONSTRAINT g1_pkey PRIMARY KEY (id));\nALTER TABLE g1 ADD UNIQUE (id);",
			want: []string{"g1 PRIMARY KEY g1_pkey (id)", "g1 UNIQUE g1_id_key"},
		},
		{
			name: "ALTER TABLE adds a second UNIQUE",
			sql:  "CREATE TABLE g2 (a int, UNIQUE (a));\nALTER TABLE g2 ADD UNIQUE (a);",
			want: []string{"g2 UNIQUE g2_a_key", "g2 UNIQUE g2_a_key1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(indexConstraints(database), qt.DeepEquals, test.want)
		})
	}
}
