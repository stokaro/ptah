package atlasschema

// White-box testing required: scopeMaterializationToDev runs between the render
// of a document and its execution on the dev database, inside
// withMaterializedDevSchema, and no exported entry point exposes the statements
// between the two. The rewrite and the refusal are string work over those
// statements and are asserted here; that a diff leaves the target untouched is
// covered live in integration/mysql_dev_materialization_scope_e2e_test.go.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
)

// TestScopeMaterializationToDev_HappyPath holds the statements that create a
// document on the dev database to the dev database: one database the document
// names is rewritten to the dev database's own, and what already stays inside
// the dev database is left as rendered.
func TestScopeMaterializationToDev_HappyPath(t *testing.T) {
	dev := catalog.ServerInfo{Dialect: platform.MySQL, Schema: "appdev"}
	tests := []struct {
		name       string
		info       catalog.ServerInfo
		statements []string
		want       []string
	}{
		{
			name: "one database the document names moves to the dev database",
			info: dev,
			statements: []string{
				"CREATE SCHEMA IF NOT EXISTS `app`",
				"CREATE TABLE `app`.`t9` (`id` int PRIMARY KEY)",
				"ALTER TABLE `app`.`t9` ADD CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `app`.`t1` (`id`)",
			},
			want: []string{
				"CREATE SCHEMA IF NOT EXISTS `appdev`",
				"CREATE TABLE `appdev`.`t9` (`id` int PRIMARY KEY)",
				"ALTER TABLE `appdev`.`t9` ADD CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `appdev`.`t1` (`id`)",
			},
		},
		{
			name:       "a document that names no database",
			info:       dev,
			statements: []string{"CREATE TABLE `t9` (`id` int PRIMARY KEY)"},
			want:       []string{"CREATE TABLE `t9` (`id` int PRIMARY KEY)"},
		},
		{
			name:       "a document that names the dev database itself",
			info:       dev,
			statements: []string{"CREATE TABLE `appdev`.`t9` (`id` int PRIMARY KEY)"},
			want:       []string{"CREATE TABLE `appdev`.`t9` (`id` int PRIMARY KEY)"},
		},
		{
			name:       "a whole dev server takes the document as written",
			info:       catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true},
			statements: []string{"CREATE TABLE `app`.`t9` (`id` int)", "CREATE TABLE `other`.`t8` (`id` int)"},
			want:       []string{"CREATE TABLE `app`.`t9` (`id` int)", "CREATE TABLE `other`.`t8` (`id` int)"},
		},
		{
			name:       "a PostgreSQL schema stays inside the connected database",
			info:       catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"},
			statements: []string{`CREATE TABLE "app"."t9" ("id" integer PRIMARY KEY)`},
			want:       []string{`CREATE TABLE "app"."t9" ("id" integer PRIMARY KEY)`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := scopeMaterializationToDev(test.statements, test.info)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestScopeMaterializationToDev_FailurePath refuses a document there is no one
// database to put in: two databases, or a session switch to another one. The
// refusal names the statement and the database it reaches.
func TestScopeMaterializationToDev_FailurePath(t *testing.T) {
	dev := catalog.ServerInfo{Dialect: platform.MariaDB, Schema: "appdev"}
	tests := []struct {
		name       string
		statements []string
		wantSchema string
		wantIndex  int
	}{
		{
			name:       "two databases",
			statements: []string{"CREATE TABLE `app`.`t9` (`id` int)", "CREATE TABLE `other`.`t8` (`id` int)"},
			wantSchema: "app",
			wantIndex:  1,
		},
		{
			name:       "a session switch beside a qualified table",
			statements: []string{"CREATE TABLE `app`.`t9` (`id` int)", "USE `other`"},
			wantSchema: "app",
			wantIndex:  1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := scopeMaterializationToDev(test.statements, dev)

			var scope *DevScopeError
			c.Assert(err, qt.ErrorAs, &scope)
			c.Assert(scope.Schema, qt.Equals, test.wantSchema)
			c.Assert(scope.StatementIndex, qt.Equals, test.wantIndex)
			c.Assert(scope.DevSchema, qt.Equals, "appdev")
			c.Assert(got, qt.IsNil)
		})
	}
}
