package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// useDocument creates two databases and selects each with USE before the
// tables that belong in it, writing the table names and the foreign key's
// targets without a database, and one name already qualified. Both databases
// hold a table t, so a key into t has to find the one its USE selected.
const useDocument = "CREATE DATABASE app;\nUSE app;\n" +
	"CREATE TABLE t (id int PRIMARY KEY);\n" +
	"CREATE TABLE u (id int PRIMARY KEY, t_id int, CONSTRAINT fk FOREIGN KEY (t_id) REFERENCES t (id));\n" +
	"CREATE INDEX u_t ON u (t_id);\n" +
	"ALTER TABLE t ADD COLUMN name varchar(20);\n" +
	"CREATE DATABASE b;\nUSE b;\nCREATE TABLE v (id int PRIMARY KEY);\nCREATE TABLE app.w (id int PRIMARY KEY);\n" +
	"CREATE TABLE t (id int PRIMARY KEY);\n" +
	"CREATE TABLE x (id int PRIMARY KEY, t_id int, CONSTRAINT fx FOREIGN KEY (t_id) REFERENCES t (id));\n" +
	"CREATE TABLE app.y (id int PRIMARY KEY, t_id int, CONSTRAINT fy FOREIGN KEY (t_id) REFERENCES t (id));\n"

// qualifiedDocument is useDocument with every name written with its database.
const qualifiedDocument = "CREATE DATABASE app;\n" +
	"CREATE TABLE app.t (id int PRIMARY KEY);\n" +
	"CREATE TABLE app.u (id int PRIMARY KEY, t_id int, CONSTRAINT fk FOREIGN KEY (t_id) REFERENCES app.t (id));\n" +
	"CREATE INDEX u_t ON app.u (t_id);\n" +
	"ALTER TABLE app.t ADD COLUMN name varchar(20);\n" +
	"CREATE DATABASE b;\nCREATE TABLE b.v (id int PRIMARY KEY);\nCREATE TABLE app.w (id int PRIMARY KEY);\n" +
	"CREATE TABLE b.t (id int PRIMARY KEY);\n" +
	"CREATE TABLE b.x (id int PRIMARY KEY, t_id int, CONSTRAINT fx FOREIGN KEY (t_id) REFERENCES b.t (id));\n" +
	"CREATE TABLE app.y (id int PRIMARY KEY, t_id int, CONSTRAINT fy FOREIGN KEY (t_id) REFERENCES app.t (id));\n"

// TestRead_UseSelectsTheDatabase reads a MySQL-family USE as the database the
// unqualified table names after it are in, as the server reads them: the
// document reads as the one that writes every name with its database.
// Measured on MySQL 8.4.11 with the pinned community binary v1.3.0, such a
// document compared with a whole server plans app.t, app.u with its key into
// app.t, and b.v. Without USE, every unqualified table names no database, and
// the whole-server comparison refuses the document (stokaro/ptah#3926).
func TestRead_UseSelectsTheDatabase(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			want, _, err := sqlschema.Read([]byte(qualifiedDocument), dialect)
			c.Assert(err, qt.IsNil)

			got, _, err := sqlschema.Read([]byte(useDocument), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, want)
			c.Assert(got.Tables, qt.HasLen, 7)
		})
	}
}

// TestRead_UseChangesNothingElsewhere is the control: a dialect whose USE
// selects no database for the schema, and a dialect-neutral document, read a
// USE as nothing.
func TestRead_UseChangesNothingElsewhere(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
	}{
		{name: "sql server", dialect: platform.SQLServer},
		{name: "a dialect-neutral document", dialect: ""},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			want, _, err := sqlschema.Read([]byte("CREATE TABLE t (id int PRIMARY KEY);\n"), row.dialect)
			c.Assert(err, qt.IsNil)

			got, _, err := sqlschema.Read([]byte("USE app;\nCREATE TABLE t (id int PRIMARY KEY);\n"), row.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, want)
		})
	}
}

// TestRead_UseWithoutADatabase_FailurePath refuses a MySQL-family USE that
// names no database, which the server refuses too.
func TestRead_UseWithoutADatabase_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, _, err := sqlschema.Read([]byte("USE ;\nCREATE TABLE t (id int PRIMARY KEY);\n"), platform.MySQL)

	c.Assert(err, qt.ErrorMatches, `(?s).*expected database name after USE.*`)
	c.Assert(got.Tables, qt.HasLen, 0)
}
