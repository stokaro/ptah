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

// sqlServerSchemaCommentCall is the statement a description writes for a schema's
// comment, with the literals varied.
func sqlServerSchemaCommentCall(name, comment, kind, schema string) string {
	return "EXEC sp_addextendedproperty @name = N'" + name + "', @value = N'" + comment +
		"', @level0type = N'" + kind + "', @level0name = N'" + schema + "'"
}

// TestRead_SchemaCommentCall_HappyPath reads a schema's comment back from the
// statements a description writes it in, guarded where the statement may find the
// schema holding one.
func TestRead_SchemaCommentCall_HappyPath(t *testing.T) {
	const creation = "IF SCHEMA_ID('app') IS NULL\n    EXEC('CREATE SCHEMA [app]');\n"
	const absent = "IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 3 AND major_id = SCHEMA_ID('app') " +
		"AND minor_id = 0 AND name = N'MS_Description')\n    "
	rows := []struct {
		name, sql, want string
	}{
		{name: "guarded", sql: creation + absent + sqlServerSchemaCommentCall("MS_Description", "application", "SCHEMA", "app") + ";\nGO\n", want: "application"},
		{name: "plain", sql: "CREATE SCHEMA [app];\n" + sqlServerSchemaCommentCall("MS_Description", "application", "SCHEMA", "app") + ";\n", want: "application"},
		{name: "a quote in the comment", sql: creation + sqlServerSchemaCommentCall("MS_Description", "it''s", "SCHEMA", "app") + ";\n", want: "it's"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(row.sql), platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: "app", Comment: row.want}})
		})
	}
}

// TestRead_SchemaCommentCall_FailurePath refuses every other EXEC, and any other
// condition on one. Each could do something the model does not hold.
func TestRead_SchemaCommentCall_FailurePath(t *testing.T) {
	rows := []struct {
		name, sql, want string
	}{
		{name: "another procedure", sql: "EXEC sp_rename 'a', 'b';\n", want: "EXEC"},
		{name: "dynamic SQL", sql: "EXEC('CREATE SCHEMA [app]');\n", want: "EXEC"},
		{name: "another property", sql: sqlServerSchemaCommentCall("other", "x", "SCHEMA", "app") + ";\n", want: "EXEC"},
		{name: "a property on something else", sql: sqlServerSchemaCommentCall("MS_Description", "x", "USER", "app") + ";\n", want: "EXEC"},
		{
			name: "a table's property",
			sql:  sqlServerSchemaCommentCall("MS_Description", "x", "SCHEMA", "app") + ", @level1type = N'TABLE', @level1name = N't';\n",
			want: "EXEC",
		},
		{
			name: "another condition",
			sql:  "IF NOT EXISTS (SELECT 1 FROM sys.tables) " + sqlServerSchemaCommentCall("MS_Description", "x", "SCHEMA", "app") + ";\n",
			want: "IF",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, _, err := sqlschema.Read([]byte(row.sql), platform.SQLServer)

			c.Assert(err, qt.ErrorMatches, `.*unsupported SQL statement: `+row.want+` at position 0.*`)
			c.Assert(got.Schemas, qt.HasLen, 0)
		})
	}
}
