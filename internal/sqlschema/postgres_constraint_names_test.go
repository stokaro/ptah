package sqlschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// derivedConstraintNames lists every UNIQUE and FOREIGN KEY name the model
// carries, as `<kind> <table> <name>`, sorted. A column's foreign key is listed
// under the table whose struct name the column carries.
func derivedConstraintNames(database schemamodel.Database) []string {
	tables := make(map[string]string, len(database.Tables))
	for _, table := range database.Tables {
		tables[table.StructName] = table.Name
	}
	var names []string
	for _, field := range database.Fields {
		if field.Foreign != "" {
			names = append(names, "fkey "+tables[field.StructName]+" "+field.ForeignKeyName)
		}
	}
	for _, constraint := range database.Constraints {
		kind := map[string]string{"FOREIGN KEY": "fkey", "UNIQUE": "key"}[strings.ToUpper(constraint.Type)]
		if kind != "" {
			names = append(names, kind+" "+constraint.Table+" "+constraint.Name)
		}
	}
	slices.Sort(names)
	return names
}

// postgresNamingParent is the table every row's keys reference.
const postgresNamingParent = "CREATE TABLE parent (id bigint PRIMARY KEY, k bigint, UNIQUE (id, k));\n"

// TestRead_PostgresUnnamedConstraintNames_HappyPath gives an unnamed foreign key
// and an unnamed table-level UNIQUE the name PostgreSQL gives it, so the model
// read from a schema file carries the name the catalog of a database built from
// the same file reports (stokaro/ptah#3643). Every expected name was read back
// from PostgreSQL 18.6 after running the row's SQL.
func TestRead_PostgresUnnamedConstraintNames_HappyPath(t *testing.T) {
	longTable := "a_table_name_that_is_quite_long_indeed_for_testing_truncation"
	longColumn := "a_column_name_that_is_also_rather_long_for_truncation"
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "an inline foreign key",
			sql:  "CREATE TABLE child (id bigint PRIMARY KEY, parent_id bigint REFERENCES parent(id));",
			want: []string{"fkey child child_parent_id_fkey"},
		},
		{
			name: "a table-level foreign key over two columns",
			sql:  "CREATE TABLE child (id bigint PRIMARY KEY, a bigint, b bigint, FOREIGN KEY (a, b) REFERENCES parent(id, k));",
			want: []string{"fkey child child_a_b_fkey"},
		},
		{
			name: "an explicitly named CHECK claims the name first",
			sql:  "CREATE TABLE dup (id bigint PRIMARY KEY, parent_id bigint REFERENCES parent(id), CONSTRAINT dup_parent_id_fkey CHECK (id > 0));",
			want: []string{"fkey dup dup_parent_id_fkey1"},
		},
		{
			name: "two keys over one column",
			sql:  "CREATE TABLE twice (id bigint PRIMARY KEY, p bigint REFERENCES parent(id), FOREIGN KEY (p) REFERENCES parent(id));",
			want: []string{"fkey twice twice_p_fkey", "fkey twice twice_p_fkey1"},
		},
		{
			name: "quoted mixed-case names keep their case",
			sql:  `CREATE TABLE "MixedCase" (id bigint PRIMARY KEY, "ParentId" bigint REFERENCES parent(id));`,
			want: []string{"fkey MixedCase MixedCase_ParentId_fkey"},
		},
		{
			name: "a long table and column are cut to 63 bytes",
			sql:  "CREATE TABLE " + longTable + " (id bigint PRIMARY KEY, " + longColumn + " bigint REFERENCES parent(id), UNIQUE (" + longColumn + "));",
			want: []string{
				"fkey " + longTable + " a_table_name_that_is_quite_lo_a_column_name_that_is_also_r_fkey",
				"key " + longTable + " a_table_name_that_is_quite_lo_a_column_name_that_is_also_ra_key",
			},
		},
		{
			name: "a multibyte column cut on a character boundary",
			sql:  "CREATE TABLE ünï (id bigint PRIMARY KEY, ñame_" + strings.Repeat("ß", 28) + " bigint REFERENCES parent(id));",
			want: []string{"fkey ünï ünï_ñame_" + strings.Repeat("ß", 23) + "_fkey"},
		},
		{
			// The cut falls inside a ß, and the name ends one byte short of
			// 63 rather than on half a character.
			name: "a multibyte column cut inside a character",
			sql:  "CREATE TABLE ünï (id bigint PRIMARY KEY, añame_" + strings.Repeat("ß", 28) + " bigint REFERENCES parent(id));",
			want: []string{"fkey ünï ünï_añame_" + strings.Repeat("ß", 22) + "_fkey"},
		},
		{
			name: "a table-level UNIQUE over two columns",
			sql:  "CREATE TABLE p (id bigint PRIMARY KEY, a bigint, b bigint, UNIQUE (a, b));",
			want: []string{"key p p_a_b_key"},
		},
		{
			name: "an index already holds the UNIQUE's name",
			sql:  "CREATE TABLE p (id bigint PRIMARY KEY, a bigint);\nCREATE INDEX q_a_key ON p (a);\nCREATE TABLE q (id bigint PRIMARY KEY, a bigint, UNIQUE (a));",
			want: []string{"key q q_a_key1"},
		},
		{
			name: "a table already holds the UNIQUE's name",
			sql:  "CREATE TABLE r_a_key (id bigint);\nCREATE TABLE r (id bigint PRIMARY KEY, a bigint, UNIQUE (a));",
			want: []string{"key r r_a_key1"},
		},
		{
			name: "a view already holds the UNIQUE's name",
			sql:  "CREATE VIEW v_a_key AS SELECT 1 AS x;\nCREATE TABLE v (id bigint PRIMARY KEY, a bigint, UNIQUE (a));",
			want: []string{"key v v_a_key1"},
		},
		{
			name: "a materialized view already holds the UNIQUE's name",
			sql:  "CREATE MATERIALIZED VIEW w_a_key AS SELECT 1 AS x;\nCREATE TABLE w (id bigint PRIMARY KEY, a bigint, UNIQUE (a));",
			want: []string{"key w w_a_key1"},
		},
		{
			name: "a sequence already holds the UNIQUE's name",
			sql:  "CREATE SEQUENCE x_a_key;\nCREATE TABLE x (id bigint PRIMARY KEY, a bigint, UNIQUE (a));",
			want: []string{"key x x_a_key1"},
		},
		{
			name: "an explicitly named CHECK claims the UNIQUE's name",
			sql:  "CREATE TABLE s (id bigint PRIMARY KEY, a bigint, CONSTRAINT s_a_key CHECK (a > 0), UNIQUE (a));",
			want: []string{"key s s_a_key1"},
		},
		{
			name: "ALTER TABLE adds an unnamed foreign key and UNIQUE",
			sql: "CREATE TABLE late (id bigint PRIMARY KEY, p bigint, q bigint);\n" +
				"ALTER TABLE late ADD FOREIGN KEY (p) REFERENCES parent(id);\n" +
				"ALTER TABLE late ADD UNIQUE (q);\n" +
				"ALTER TABLE late ADD FOREIGN KEY (p) REFERENCES parent(id);",
			want: []string{"fkey late late_p_fkey", "fkey late late_p_fkey1", "key late late_q_key"},
		},
		{
			name: "a constraint of another table in the schema claims the name",
			sql: "CREATE TABLE late (id bigint PRIMARY KEY, p bigint, q bigint);\n" +
				"CREATE TABLE other (id bigint PRIMARY KEY, x bigint, CONSTRAINT late_q_p_fkey CHECK (x > 0));\n" +
				"ALTER TABLE late ADD FOREIGN KEY (q, p) REFERENCES parent(id, k);",
			want: []string{"fkey late late_q_p_fkey1"},
		},
		{
			name: "a named foreign key keeps its name",
			sql:  "CREATE TABLE child (id bigint PRIMARY KEY, parent_id bigint CONSTRAINT my_fk REFERENCES parent(id));",
			want: []string{"fkey child my_fk"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(postgresNamingParent+test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			want := append([]string{"key parent parent_id_k_key"}, test.want...)
			slices.Sort(want)
			c.Assert(derivedConstraintNames(database), qt.DeepEquals, want)
		})
	}
}

// TestReadOnto_PostgresUnnamedConstraintNames_ClaimsEarlierFiles numbers a
// foreign key whose name an earlier file of the same document already took, as
// the server does when it runs the files in order.
func TestReadOnto_PostgresUnnamedConstraintNames_ClaimsEarlierFiles(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte(postgresNamingParent+
		"CREATE TABLE other (id bigint PRIMARY KEY, x bigint, CONSTRAINT child_parent_id_fkey CHECK (x > 0));"), "postgres")
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte(
		"CREATE TABLE child (id bigint PRIMARY KEY, parent_id bigint REFERENCES parent(id));"), "postgres", &earlier)

	c.Assert(err, qt.IsNil)
	c.Assert(derivedConstraintNames(later), qt.DeepEquals, []string{"fkey child child_parent_id_fkey1"})
}

// TestRead_UnnamedForeignKeyStaysUnnamedOutsidePostgres keeps the rule to the
// engine it was measured on: SQLite stores no name for an unnamed key, so the
// model a SQLite document reads keeps none either.
func TestRead_UnnamedForeignKeyStaysUnnamedOutsidePostgres(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(
		"CREATE TABLE parent (id INTEGER PRIMARY KEY);\n"+
			"CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id));"), "sqlite")

	c.Assert(err, qt.IsNil)
	c.Assert(derivedConstraintNames(database), qt.DeepEquals, []string{"fkey child "})
}
