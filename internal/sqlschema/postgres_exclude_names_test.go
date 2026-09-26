package sqlschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// excludeNames lists every EXCLUDE the model carries as `<table> <name>`,
// sorted.
func excludeNames(database schemamodel.Database) []string {
	var names []string
	for _, constraint := range database.Constraints {
		if strings.EqualFold(constraint.Type, "EXCLUDE") {
			names = append(names, constraint.Table+" "+constraint.Name)
		}
	}
	slices.Sort(names)
	return names
}

// TestRead_PostgresUnnamedExcludeNames_HappyPath gives an unnamed EXCLUDE the
// name PostgreSQL gives it, so a schema file compares equal to the database its
// own SQL built (stokaro/ptah#3749). Each expected name was read back from
// pg_constraint on PostgreSQL 18.6 after running the row's SQL.
func TestRead_PostgresUnnamedExcludeNames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "one column",
			sql:  "CREATE TABLE a (r int, EXCLUDE USING btree (r WITH =));",
			want: []string{"a a_r_excl"},
		},
		{
			name: "two on one column",
			sql:  "CREATE TABLE c (r int, EXCLUDE USING btree (r WITH =), EXCLUDE USING gist (r WITH =));",
			want: []string{"c c_r_excl", "c c_r_excl1"},
		},
		{
			name: "an index of the name",
			sql:  "CREATE TABLE k (r int);\nCREATE INDEX kk_r_excl ON k (r);\nCREATE TABLE kk (r int, EXCLUDE USING btree (r WITH =));",
			want: []string{"kk kk_r_excl1"},
		},
		{
			name: "a view of the name",
			sql:  "CREATE VIEW v2_r_excl AS SELECT 1 AS x;\nCREATE TABLE v2 (r int, EXCLUDE USING btree (r WITH =));",
			want: []string{"v2 v2_r_excl1"},
		},
		{
			name: "a CHECK of the name",
			sql:  "CREATE TABLE w (r int, CONSTRAINT w_r_excl CHECK (r > 0), EXCLUDE USING btree (r WITH =));",
			want: []string{"w w_r_excl1"},
		},
		{
			name: "beside a UNIQUE and a named CHECK",
			sql:  "CREATE TABLE l (r int, CONSTRAINT l_x CHECK (r > 0), UNIQUE (r), EXCLUDE USING btree (r WITH =));",
			want: []string{"l l_r_excl"},
		},
		{
			name: "a function",
			sql:  "CREATE TABLE d (t text, EXCLUDE USING btree (lower(t) WITH =));",
			want: []string{"d d_lower_excl"},
		},
		{
			name: "ALTER TABLE adds two",
			sql: "CREATE TABLE q (r int);\n" +
				"ALTER TABLE q ADD EXCLUDE USING btree (r WITH =);\n" +
				"ALTER TABLE q ADD EXCLUDE USING btree (r WITH =);",
			want: []string{"q q_r_excl", "q q_r_excl1"},
		},
		{
			name: "a named EXCLUDE keeps its name",
			sql:  "CREATE TABLE b (r int, CONSTRAINT b_no_overlap EXCLUDE USING gist (r WITH =));",
			want: []string{"b b_no_overlap"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(excludeNames(database), qt.DeepEquals, test.want)
		})
	}
}
