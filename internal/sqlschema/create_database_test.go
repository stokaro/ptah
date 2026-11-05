package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// TestRead_CreateDatabase reads a CREATE DATABASE as the schema it declares on
// the MySQL family, where CREATE DATABASE and CREATE SCHEMA are synonyms, and on
// ClickHouse, where a database is the only namespace, and as nothing on PostgreSQL,
// where it names the database the model already is.
// Unread on the MySQL family, a schema file for a whole server declares tables
// in databases nothing creates, and materializing it on a dev server fails with
// ERROR 1049, unknown database (stokaro/ptah#3885).
func TestRead_CreateDatabase(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		sql     string
		want    []schemamodel.Schema
	}{
		{
			name: "mysql", dialect: platform.MySQL,
			sql:  "CREATE DATABASE app;\nCREATE TABLE app.t (id int PRIMARY KEY);\n",
			want: []schemamodel.Schema{{Name: "app"}},
		},
		{
			name: "mariadb with IF NOT EXISTS", dialect: platform.MariaDB,
			sql:  "CREATE DATABASE IF NOT EXISTS app;\nCREATE TABLE app.t (id int PRIMARY KEY);\n",
			want: []schemamodel.Schema{{Name: "app"}},
		},
		{
			name: "mysql, the same as CREATE SCHEMA", dialect: platform.MySQL,
			sql:  "CREATE SCHEMA app;\nCREATE TABLE app.t (id int PRIMARY KEY);\n",
			want: []schemamodel.Schema{{Name: "app"}},
		},
		{
			name: "clickhouse", dialect: platform.ClickHouse,
			sql:  "CREATE DATABASE app;\n",
			want: []schemamodel.Schema{{Name: "app"}},
		},
		{
			name: "postgres", dialect: platform.Postgres,
			sql:  "CREATE DATABASE app;\nCREATE TABLE t (id int PRIMARY KEY);\n",
			want: make([]schemamodel.Schema, 0),
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(row.sql), row.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Schemas, qt.DeepEquals, row.want)
		})
	}
}

// TestRead_GuardedCreateSchema_HappyPath reads the statement a description of a SQL
// Server database writes for a schema, so a schema file takes what it printed.
func TestRead_GuardedCreateSchema_HappyPath(t *testing.T) {
	rows := []struct {
		name, sql, want string
	}{
		{name: "as a description writes it", sql: "IF SCHEMA_ID('app') IS NULL\n    EXEC('CREATE SCHEMA [app]');\nGO\n", want: "app"},
		{name: "national strings", sql: "IF SCHEMA_ID(N'app') IS NULL EXEC(N'CREATE SCHEMA [app]');\n", want: "app"},
		{name: "a bracket in the name", sql: "IF SCHEMA_ID('a]b') IS NULL EXEC('CREATE SCHEMA [a]]b]');\n", want: "a]b"},
		{name: "a quote in the name", sql: "IF SCHEMA_ID('a''b') IS NULL EXEC('CREATE SCHEMA [a''b]');\n", want: "a'b"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(row.sql), platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: row.want}})
		})
	}
}

// TestRead_GuardedCreateSchema_FailurePath refuses every other IF. Each could do
// something the model does not hold, so reading it as a schema would lose it.
func TestRead_GuardedCreateSchema_FailurePath(t *testing.T) {
	rows := []struct {
		name, sql string
	}{
		{name: "another guard", sql: "IF OBJECT_ID('t') IS NULL EXEC('CREATE SCHEMA [app]');\n"},
		{name: "a guard that is not on the schema it creates", sql: "IF SCHEMA_ID('other') IS NULL EXEC('CREATE SCHEMA [app]');\n"},
		{name: "a body that is not a creation", sql: "IF SCHEMA_ID('app') IS NULL EXEC('DROP SCHEMA [app]');\n"},
		{name: "a guard on a schema that exists", sql: "IF SCHEMA_ID('app') IS NOT NULL EXEC('CREATE SCHEMA [app]');\n"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, _, err := sqlschema.Read([]byte(row.sql), platform.SQLServer)

			c.Assert(err, qt.ErrorMatches, `.*unsupported SQL statement: IF at position 0.*`)
			c.Assert(got.Schemas, qt.HasLen, 0)
		})
	}
}
