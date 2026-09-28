package sqlschema_test

import (
	"slices"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// enforcementAndMatch lists what the model keeps about enforcement and the
// MATCH type, as `<where>: not_enforced=<bool> match=<type>`, sorted. A column
// is listed where it carries a CHECK or a foreign key, a constraint where it is
// a CHECK or a foreign key.
func enforcementAndMatch(database schemamodel.Database) []string {
	var described []string
	for _, field := range database.Fields {
		if field.Check != "" {
			described = append(described,
				"column "+field.Name+" CHECK: not_enforced="+strconv.FormatBool(field.CheckNotEnforced))
		}
		if field.Foreign != "" {
			described = append(described, "column "+field.Name+" REFERENCES: not_enforced="+
				strconv.FormatBool(field.ForeignKeyNotEnforced)+" match="+field.ForeignKeyMatch)
		}
	}
	for _, constraint := range database.Constraints {
		switch constraint.Type {
		case "CHECK":
			described = append(described, constraint.Name+": not_enforced="+strconv.FormatBool(constraint.NotEnforced))
		case "FOREIGN KEY":
			described = append(described, constraint.Name+": not_enforced="+
				strconv.FormatBool(constraint.NotEnforced)+" match="+constraint.Match)
		}
	}
	slices.Sort(described)
	return described
}

// TestRead_EnforcementAndMatch_HappyPath reads `NOT ENFORCED` and the MATCH
// type onto the model (stokaro/ptah#3853). Each row's SQL was run on its
// server, PostgreSQL 18.6 or MySQL 8.4.11, which recorded what the row
// expects.
func TestRead_EnforcementAndMatch_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{
			name:    "CHECKs on a column and on the table",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int CHECK (a > 0) NOT ENFORCED, b int, CONSTRAINT c_b CHECK (b > 0) NOT ENFORCED);",
			want:    []string{"c_b: not_enforced=true", "column a CHECK: not_enforced=true"},
		},
		{
			name:    "a column's second CHECK alone",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int, b int CHECK (a > 0) CHECK (b > 0) NOT ENFORCED);",
			want:    []string{"c_b_check: not_enforced=true", "column b CHECK: not_enforced=false"},
		},
		{
			name:    "foreign keys on a column and on the table",
			dialect: "postgres",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE c (a int REFERENCES p (id) MATCH FULL NOT ENFORCED, b int, " +
				"CONSTRAINT c_b FOREIGN KEY (b) REFERENCES p (id) MATCH FULL);",
			want: []string{
				"c_b: not_enforced=false match=FULL",
				"column a REFERENCES: not_enforced=true match=FULL",
			},
		},
		{
			name:    "a CHECK ALTER TABLE adds",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ADD CONSTRAINT c_a CHECK (a > 0) NOT ENFORCED;",
			want:    []string{"c_a: not_enforced=true"},
		},
		{
			name:    "MySQL CHECKs and MATCH PARTIAL",
			dialect: "mysql",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE c (a int CHECK (a > 0) NOT ENFORCED, b int, " +
				"CONSTRAINT c_b FOREIGN KEY (b) REFERENCES p (id) MATCH PARTIAL);",
			want: []string{"c_b: not_enforced=false match=PARTIAL", "column a CHECK: not_enforced=true"},
		},
		{
			name:    "a dropped CHECK takes its enforcement with it",
			dialect: "postgres",
			sql: "CREATE TABLE c (a int CONSTRAINT c_a CHECK (a > 0) NOT ENFORCED);\n" +
				"ALTER TABLE c DROP CONSTRAINT c_a;\nALTER TABLE c ADD CONSTRAINT c_a2 CHECK (a < 10);",
			want: []string{"c_a2: not_enforced=false"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(enforcementAndMatch(database), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_DroppedForeignKeyTakesItsClauses drops a column's foreign key and
// adds the column's reference back without the clauses: the model keeps none
// of the dropped key's MATCH type or enforcement.
func TestRead_DroppedForeignKeyTakesItsClauses(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(
		"CREATE TABLE p (id int PRIMARY KEY);\n"+
			"CREATE TABLE c (a int CONSTRAINT c_a_fk REFERENCES p (id) MATCH FULL NOT ENFORCED);\n"+
			"ALTER TABLE c DROP CONSTRAINT c_a_fk;"), "postgres")

	c.Assert(err, qt.IsNil)
	fields := slices.DeleteFunc(slices.Clone(database.Fields), func(field schemamodel.Field) bool {
		return field.Name != "a"
	})
	c.Assert(fields, qt.HasLen, 1)
	c.Assert(fields[0].Foreign, qt.Equals, "")
	c.Assert(fields[0].ForeignKeyMatch, qt.Equals, "")
	c.Assert(fields[0].ForeignKeyNotEnforced, qt.IsFalse)
}
