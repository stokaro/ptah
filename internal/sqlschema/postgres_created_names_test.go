package sqlschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// namedConstraints lists every constraint the model names, on a table or on a
// column, as `<name>: <definition>`, sorted, so a row can say which of two
// constraints took which name.
func namedConstraints(database schemamodel.Database) []string {
	var named []string
	for _, field := range database.Fields {
		if field.Check != "" {
			named = append(named, field.CheckName+": CHECK ("+field.Check+")")
		}
		if field.Foreign != "" {
			named = append(named, field.ForeignKeyName+": REFERENCES "+field.Foreign)
		}
	}
	for _, constraint := range database.Constraints {
		definition := strings.ToUpper(constraint.Type) + " (" + strings.Join(constraint.Columns, ", ") + ")"
		if constraint.NullsDistinct != nil && !*constraint.NullsDistinct {
			definition = "UNIQUE NULLS NOT DISTINCT (" + strings.Join(constraint.Columns, ", ") + ")"
		}
		if len(constraint.IncludeColumns) > 0 {
			definition += " INCLUDE (" + strings.Join(constraint.IncludeColumns, ", ") + ")"
		}
		if constraint.CheckExpression != "" {
			definition = "CHECK (" + constraint.CheckExpression + ")"
		}
		if constraint.OnDelete != "" {
			definition += " ON DELETE " + constraint.OnDelete
		}
		named = append(named, constraint.Name+": "+definition)
	}
	slices.Sort(named)
	return named
}

// TestRead_PostgresCreatedConstraintNames_HappyPath names the constraints of
// one CREATE TABLE in the order PostgreSQL builds them, and an unnamed UNIQUE
// after every column of its index (stokaro/ptah#3858, stokaro/ptah#3863).
// Every expected name was read back from pg_constraint on PostgreSQL 18.6
// after running the row's SQL.
func TestRead_PostgresCreatedConstraintNames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "a UNIQUE names its INCLUDE columns",
			sql:  "CREATE TABLE i1 (x int, y int, UNIQUE (x) INCLUDE (y));",
			want: []string{"i1_x_y_key: UNIQUE (x) INCLUDE (y)"},
		},
		{
			name: "an INCLUDE column that repeats a key column is numbered",
			sql:  "CREATE TABLE i3 (x int, y int, z int, UNIQUE (x, y) INCLUDE (z, x));",
			want: []string{"i3_x_y_z_x1_key: UNIQUE (x, y) INCLUDE (z, x)"},
		},
		{
			name: "ALTER TABLE adds a UNIQUE with INCLUDE",
			sql:  "CREATE TABLE i8 (x int, y int);\nALTER TABLE i8 ADD UNIQUE (x) INCLUDE (y);",
			want: []string{"i8_x_y_key: UNIQUE (x) INCLUDE (y)"},
		},
		{
			name: "a UNIQUE with INCLUDE and one without over the same column",
			sql:  "CREATE TABLE i9 (x int, y int, UNIQUE (x) INCLUDE (y), UNIQUE (x));",
			want: []string{"i9_x_key: UNIQUE (x)", "i9_x_y_key: UNIQUE (x) INCLUDE (y)"},
		},
		{
			name: "a UNIQUE with INCLUDE beside a written name of its key columns alone",
			sql:  "CREATE TABLE r2 (a int, b int, UNIQUE (a) INCLUDE (b), CONSTRAINT r2_a_key UNIQUE (b));",
			want: []string{"r2_a_b_key: UNIQUE (a) INCLUDE (b)", "r2_a_key: UNIQUE (b)"},
		},
		{
			name: "a table UNIQUE written before a column's own takes the name first",
			sql:  "CREATE TABLE u4 (UNIQUE NULLS NOT DISTINCT (a), a int UNIQUE);",
			want: []string{"u4_a_key1: UNIQUE (a)", "u4_a_key: UNIQUE NULLS NOT DISTINCT (a)"},
		},
		{
			name: "a column's own UNIQUE written before a table UNIQUE takes the name first",
			sql:  "CREATE TABLE u5 (a int UNIQUE, UNIQUE NULLS NOT DISTINCT (a));",
			want: []string{"u5_a_key1: UNIQUE NULLS NOT DISTINCT (a)"},
		},
		{
			name: "a written name before a column's own UNIQUE numbers the column's key, read onto the table",
			sql:  "CREATE TABLE r1 (CONSTRAINT r1_x_key UNIQUE (y), y int, x int UNIQUE, UNIQUE NULLS NOT DISTINCT (x));",
			want: []string{"r1_x_key1: UNIQUE (x)", "r1_x_key2: UNIQUE NULLS NOT DISTINCT (x)", "r1_x_key: UNIQUE (y)"},
		},
		{
			name: "a table CHECK written before a column's takes the name first",
			sql:  "CREATE TABLE h1 (CHECK (a > 0), a int CHECK (a < 10));",
			want: []string{"h1_a_check1: CHECK (a < 10)", "h1_a_check: CHECK (a > 0)"},
		},
		{
			name: "a FOREIGN KEY written before a column's REFERENCES takes the name first",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE f1 (FOREIGN KEY (a) REFERENCES p (id) ON DELETE CASCADE, a int REFERENCES p (id));",
			want: []string{"f1_a_fkey1: REFERENCES p(id)", "f1_a_fkey: FOREIGN KEY (a) ON DELETE CASCADE"},
		},
		{
			name: "a written CHECK name before an unnamed CHECK numbers it",
			sql:  "CREATE TABLE k1b (CONSTRAINT k1b_a_check CHECK (a < 10), a int CHECK (a > 0));",
			want: []string{"k1b_a_check1: CHECK (a > 0)", "k1b_a_check: CHECK (a < 10)"},
		},
		{
			name: "a written UNIQUE name numbers a foreign key written before it",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE k10 (a int, b int, FOREIGN KEY (a) REFERENCES p (id), CONSTRAINT k10_a_fkey UNIQUE (b));",
			want: []string{"k10_a_fkey1: FOREIGN KEY (a)", "k10_a_fkey: UNIQUE (b)"},
		},
		{
			name: "a written CHECK name numbers a column's UNIQUE written before it",
			sql:  "CREATE TABLE c3 (x int UNIQUE, y int, CONSTRAINT c3_x_key CHECK (y > 0), UNIQUE NULLS NOT DISTINCT (x));",
			want: []string{"c3_x_key1: UNIQUE (x)", "c3_x_key2: UNIQUE NULLS NOT DISTINCT (x)", "c3_x_key: CHECK (y > 0)"},
		},
		{
			name: "a written NOT NULL name numbers the keys built after it",
			sql:  "CREATE TABLE n2b (a int CONSTRAINT n2b_a_key NOT NULL UNIQUE, UNIQUE NULLS NOT DISTINCT (a));",
			want: []string{"n2b_a_key1: UNIQUE (a)", "n2b_a_key2: UNIQUE NULLS NOT DISTINCT (a)"},
		},
		{
			name: "the table holds the name its key derives when the name is cut to 63 bytes",
			sql:  "CREATE TABLE " + strings.Repeat("t", 57) + "_a_key (a int, UNIQUE (a));",
			want: []string{strings.Repeat("t", 56) + "_a_key1: UNIQUE (a)"},
		},
		{
			name: "ALTER TABLE adds a column whose UNIQUE an index's name numbers",
			sql: "CREATE TABLE c (id int PRIMARY KEY, y int);\n" +
				"CREATE UNIQUE INDEX c_x_key ON c (y);\n" +
				"ALTER TABLE c ADD COLUMN x int UNIQUE;",
			want: []string{"c_x_key1: UNIQUE (x)"},
		},
		{
			name: "ALTER TABLE adds a column whose UNIQUE takes the first name",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, y int);\nALTER TABLE c ADD COLUMN x int UNIQUE;",
			want: nil,
		},
		{
			name: "two identical written keys fold into one",
			sql:  "CREATE TABLE r3 (a int, CONSTRAINT r3_k UNIQUE (a), CONSTRAINT r3_k UNIQUE (a));",
			want: []string{"r3_k: UNIQUE (a)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(namedConstraints(database), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_PostgresCreatedConstraintNames_FailurePath refuses a CREATE TABLE
// that writes a constraint name another of its constraints holds when the
// server builds the written one, as PostgreSQL does (stokaro/ptah#3858). Each
// row's SQL was refused by PostgreSQL 18.6 with the error the row's comment
// gives.
func TestRead_PostgresCreatedConstraintNames_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			// relation "c1_x_key" already exists
			name:    "a column's UNIQUE takes the name a later UNIQUE writes",
			sql:     "CREATE TABLE c1 (id bigint PRIMARY KEY, x int UNIQUE, y int, CONSTRAINT c1_x_key UNIQUE (y));",
			wantErr: "CREATE TABLE c1 gives the name c1_x_key to the UNIQUE (y), which the UNIQUE of column x already holds",
		},
		{
			// relation "c7_x_key" already exists
			name:    "a column's UNIQUE takes the name a later EXCLUDE writes",
			sql:     "CREATE TABLE c7 (id bigint PRIMARY KEY, x int UNIQUE, y int, CONSTRAINT c7_x_key EXCLUDE USING btree (y WITH =));",
			wantErr: "CREATE TABLE c7 gives the name c7_x_key to the EXCLUDE (y WITH =), which the UNIQUE of column x already holds",
		},
		{
			// relation "c5_x_y_key" already exists
			name:    "an unnamed UNIQUE with INCLUDE takes the name a later UNIQUE writes",
			sql:     "CREATE TABLE c5 (x int, y int, UNIQUE (x) INCLUDE (y), CONSTRAINT c5_x_y_key UNIQUE (y));",
			wantErr: "CREATE TABLE c5 gives the name c5_x_y_key to the UNIQUE (y), which the UNIQUE (x) INCLUDE (y) already holds",
		},
		{
			// relation "k14_b_key" already exists
			name:    "an unnamed table UNIQUE takes the name a later EXCLUDE writes",
			sql:     "CREATE TABLE k14 (a int, b int, UNIQUE (a), UNIQUE (b), CONSTRAINT k14_b_key EXCLUDE USING btree (a WITH =));",
			wantErr: "CREATE TABLE k14 gives the name k14_b_key to the EXCLUDE (a WITH =), which the UNIQUE (b) already holds",
		},
		{
			// relation "w1_a_key" already exists: the key over a is built at
			// the column, where the server keeps the first of the two
			name:    "a column's UNIQUE folded with a later equal one keeps the column's place",
			sql:     "CREATE TABLE w1 (a int UNIQUE, CONSTRAINT w1_a_key UNIQUE (b), b int, UNIQUE (a));",
			wantErr: "CREATE TABLE w1 gives the name w1_a_key to the UNIQUE (b), which the UNIQUE (a) already holds",
		},
		{
			// check constraint "k1_a_check" already exists
			name:    "a column's CHECK takes the name a later CHECK writes",
			sql:     "CREATE TABLE k1 (a int CHECK (a > 0), CONSTRAINT k1_a_check CHECK (a < 10));",
			wantErr: "CREATE TABLE k1 gives the name k1_a_check to the CHECK (a < 10), which the CHECK (a > 0) of column a already holds",
		},
		{
			// constraint "k3_x_check" for relation "k3" already exists: the
			// server adds the CHECK before it builds any index
			name:    "a CHECK takes the name a UNIQUE written before it writes",
			sql:     "CREATE TABLE k3 (CONSTRAINT k3_x_check UNIQUE (id), id int, x int CHECK (x > 0));",
			wantErr: "CREATE TABLE k3 gives the name k3_x_check to the UNIQUE (id), which the CHECK (x > 0) of column x already holds",
		},
		{
			// relation "k5_pkey" already exists
			name:    "the primary key takes the name a UNIQUE writes",
			sql:     "CREATE TABLE k5 (id int PRIMARY KEY, x int, CONSTRAINT k5_pkey UNIQUE (x));",
			wantErr: "CREATE TABLE k5 gives the name k5_pkey to the UNIQUE (x), which the primary key already holds",
		},
		{
			// relation "k5b_pkey" already exists: the server builds the
			// primary key first wherever the statement writes it
			name:    "the primary key takes the name a UNIQUE written before it writes",
			sql:     "CREATE TABLE k5b (x int, CONSTRAINT k5b_pkey UNIQUE (x), id int PRIMARY KEY);",
			wantErr: "CREATE TABLE k5b gives the name k5b_pkey to the UNIQUE (x), which the primary key already holds",
		},
		{
			// constraint "q1_a_check" for relation "q1" already exists
			name:    "a CHECK takes the name the primary key writes",
			sql:     "CREATE TABLE q1 (a int CHECK (a > 0), CONSTRAINT q1_a_check PRIMARY KEY (a));",
			wantErr: "CREATE TABLE q1 gives the name q1_a_check to the primary key, which the CHECK (a > 0) of column a already holds",
		},
		{
			// constraint "k2b_a_key" for relation "k2b" already exists: the
			// server adds the foreign keys after every index
			name: "a column's UNIQUE takes the name a foreign key written before it writes",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE k2b (b int, CONSTRAINT k2b_a_key FOREIGN KEY (b) REFERENCES p (id), a int UNIQUE);",
			wantErr: "CREATE TABLE k2b gives the name k2b_a_key to the FOREIGN KEY (b), which the UNIQUE of column a already holds",
		},
		{
			// constraint "k7_a_fkey" for relation "k7" already exists
			name: "a column's foreign key takes the name a later one writes",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE k7 (a int REFERENCES p (id), CONSTRAINT k7_a_fkey FOREIGN KEY (a) REFERENCES p (id));",
			wantErr: "CREATE TABLE k7 gives the name k7_a_fkey to the FOREIGN KEY (a), which the foreign key of column a already holds",
		},
		{
			// relation "d8" already exists
			name:    "two UNIQUEs written with one name",
			sql:     "CREATE TABLE k8 (a int, b int, CONSTRAINT d8 UNIQUE (a), CONSTRAINT d8 UNIQUE (b));",
			wantErr: "CREATE TABLE k8 gives the name d8 to the UNIQUE (b), which the UNIQUE (a) already holds",
		},
		{
			// constraint "d9" for relation "k9" already exists
			name:    "a CHECK and a UNIQUE written with one name",
			sql:     "CREATE TABLE k9 (a int, b int, CONSTRAINT d9 CHECK (a > 0), CONSTRAINT d9 UNIQUE (b));",
			wantErr: "CREATE TABLE k9 gives the name d9 to the UNIQUE (b), which the CHECK (a > 0) already holds",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateConstraintName)
			c.Assert(err.Error(), qt.Contains, test.wantErr+" when the server builds it, and PostgreSQL refuses the statement")
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRead_PostgresCreatedConstraintNames_OtherDialectsKeepBoth keeps the
// refusal to the engine it was measured on: SQLite keeps no name for a
// column's UNIQUE, and a document read for it is not refused here.
func TestRead_PostgresCreatedConstraintNames_OtherDialectsKeepBoth(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(
		"CREATE TABLE c1 (id bigint PRIMARY KEY, x int UNIQUE, y int, CONSTRAINT c1_x_key UNIQUE (y));"), "sqlite")

	c.Assert(err, qt.IsNil)
	c.Assert(namedConstraints(database), qt.DeepEquals, []string{"c1_x_key: UNIQUE (y)"})
}
