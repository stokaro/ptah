package sqlschema_test

import (
	"slices"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// validationOf lists each CHECK and foreign key constraint as
// `<name>: not_valid=<bool>`, sorted.
func validationOf(database schemamodel.Database) []string {
	var described []string
	for _, constraint := range database.Constraints {
		if constraint.Type != "CHECK" && constraint.Type != "FOREIGN KEY" {
			continue
		}
		described = append(described, constraint.Name+": not_valid="+strconv.FormatBool(constraint.NotValid))
	}
	slices.Sort(described)
	return described
}

// TestRead_NotValid_HappyPath reads NOT VALID onto a constraint ALTER TABLE
// adds, and VALIDATE CONSTRAINT off it (stokaro/ptah#3853). Each row's SQL was
// run on PostgreSQL 18.6, which recorded what the row expects in
// pg_constraint.convalidated; CockroachDB v26.3.2 and YugabyteDB 2026.1.2
// record the same.
func TestRead_NotValid_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{
			name:    "a CHECK and a foreign key added NOT VALID",
			dialect: "postgres",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (a int, p int);\n" +
				"ALTER TABLE c ADD CONSTRAINT c_a CHECK (a > 0) NOT VALID;\n" +
				"ALTER TABLE c ADD CONSTRAINT c_p FOREIGN KEY (p) REFERENCES p (id) NOT VALID;",
			want: []string{"c_a: not_valid=true", "c_p: not_valid=true"},
		},
		{
			name:    "NOT VALID in CREATE TABLE, which the server records validated",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int, CONSTRAINT c_a CHECK (a > 0) NOT VALID);",
			want:    []string{"c_a: not_valid=false"},
		},
		{
			name:    "a constraint added without the clause",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ADD CONSTRAINT c_a CHECK (a > 0);",
			want:    []string{"c_a: not_valid=false"},
		},
		{
			name:    "VALIDATE CONSTRAINT after the addition",
			dialect: "postgres",
			sql: "CREATE TABLE c (a int);\nALTER TABLE c ADD CONSTRAINT c_a CHECK (a > 0) NOT VALID;\n" +
				"ALTER TABLE c VALIDATE CONSTRAINT c_a;",
			want: []string{"c_a: not_valid=false"},
		},
		{
			name:    "an unnamed CHECK added NOT VALID, under the name the server gives it",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ADD CHECK (a > 0) NOT VALID;",
			want:    []string{"c_a_check: not_valid=true"},
		},
		{
			name:    "CockroachDB",
			dialect: "cockroachdb",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ADD CONSTRAINT c_a CHECK (a > 0) NOT VALID;",
			want:    []string{"c_a: not_valid=true"},
		},
		{
			name:    "YugabyteDB",
			dialect: "yugabytedb",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ADD CONSTRAINT c_a CHECK (a > 0) NOT VALID;",
			want:    []string{"c_a: not_valid=true"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(validationOf(database), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_ValidatesAColumnCheckInPlace validates a CHECK written on a column,
// which is validated already: the server takes the statement and the model is
// unchanged.
func TestRead_ValidatesAColumnCheckInPlace(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(
		"CREATE TABLE c (a int CONSTRAINT c_a CHECK (a > 0));\nALTER TABLE c VALIDATE CONSTRAINT c_a;"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Constraints, qt.HasLen, 0)
	c.Assert(database.Fields, qt.HasLen, 1)
	c.Assert(database.Fields[0].CheckName, qt.Equals, "c_a")
}

// TestRead_NotValid_FailurePath refuses VALIDATE CONSTRAINT where the table has
// no CHECK or foreign key of that name, and where the dialect has no such
// statement.
func TestRead_NotValid_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "a name the table does not hold",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c VALIDATE CONSTRAINT c_missing;",
			wantErr: `.*ALTER TABLE c VALIDATE CONSTRAINT c_missing names no CHECK or foreign key this schema declares by that name`,
		},
		{
			name:    "a UNIQUE constraint",
			dialect: "postgres",
			sql:     "CREATE TABLE c (a int, CONSTRAINT c_a_uq UNIQUE (a));\nALTER TABLE c VALIDATE CONSTRAINT c_a_uq;",
			wantErr: `.*ALTER TABLE c VALIDATE CONSTRAINT c_a_uq names no CHECK or foreign key this schema declares by that name`,
		},
		{
			name:    "MySQL",
			dialect: "mysql",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c VALIDATE CONSTRAINT c_a;",
			wantErr: `(?s).*VALIDATE CONSTRAINT at position \d+: the mysql dialect takes no VALIDATE CONSTRAINT.*`,
		},
		{
			name:    "NOT VALID after ALTER TABLE ADD on MySQL",
			dialect: "mysql",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ADD CONSTRAINT c_a CHECK (a > 0) NOT VALID;",
			wantErr: `(?s).*NOT VALID at position \d+: the mysql dialect takes no NOT VALID clause.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Constraints, qt.HasLen, 0)
		})
	}
}
