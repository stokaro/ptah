package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// A statement that names a table reaches it with or without the dialect's
// default schema, whichever spelling the CREATE TABLE used. On PostgreSQL
// 16.16, 17.11 and 18.6, `ALTER TABLE public.t` changes the table `CREATE
// TABLE t` created, and the reverse.
func TestRead_TableNamedWithOrWithoutTheDefaultSchema_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql string
	}{
		{
			name:    "PostgreSQL, declared bare",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE t (id int);\nALTER TABLE public.t ADD COLUMN x int;",
		},
		{
			name:    "PostgreSQL, declared in public",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE public.t (id int);\nALTER TABLE t ADD COLUMN x int;",
		},
		{
			name:    "PostgreSQL, the schema in capitals, folded",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE t (id int);\nALTER TABLE PUBLIC.t ADD COLUMN x int;",
		},
		{
			name:    "SQL Server, declared bare",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id int);\nALTER TABLE dbo.t ADD x int;",
		},
		{
			name:    "SQL Server, the schema in capitals, compared without case",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE t (id int);\nALTER TABLE DBO.t ADD x int;",
		},
		{
			name:    "SQLite, declared bare",
			dialect: platform.SQLite,
			sql:     "CREATE TABLE t (id int);\nALTER TABLE main.t ADD COLUMN x int;",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(declaredColumnsOf(database), qt.DeepEquals, []string{"id", "x"})
		})
	}
}

// A schema other than the default one names another table, and a dialect
// without a default schema, MySQL, reads a qualified name as another
// database's table.
func TestRead_TableNamedWithOrWithoutTheDefaultSchema_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql, wantErr string
	}{
		{
			name:    "PostgreSQL, another schema",
			dialect: platform.Postgres,
			sql:     "CREATE SCHEMA app;\nCREATE TABLE t (id int);\nALTER TABLE app.t ADD COLUMN x int;",
			wantErr: `.*ALTER TABLE app.t .*names a table this schema does not declare`,
		},
		{
			name:    "PostgreSQL, a quoted name holding the schema's name",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE t (id int);\nALTER TABLE \"public.t\" ADD COLUMN x int;",
			wantErr: `.*ALTER TABLE "public.t" .*names a table this schema does not declare`,
		},
		{
			name:    "MySQL, a database qualifies the name",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE t (id int);\nALTER TABLE shop.t ADD COLUMN x int;",
			wantErr: `.*ALTER TABLE shop.t .*names a table this schema does not declare`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// declaredColumnsOf lists every column the database declares, in order.
func declaredColumnsOf(database schemamodel.Database) []string {
	var names []string
	for _, field := range database.Fields {
		names = append(names, field.Name)
	}
	return names
}
