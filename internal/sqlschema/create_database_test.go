package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// TestRead_CreateDatabase reads a CREATE DATABASE as the schema it declares on
// the MySQL family, where CREATE DATABASE and CREATE SCHEMA are synonyms, and
// as nothing on PostgreSQL, where it names the database the model already is.
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
