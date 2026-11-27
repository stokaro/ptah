package schemaremoval_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/schemaremoval"
)

func TestRefuseStatements_ProtectsExcludedObjectsFromSavedSchemaDrops(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql, schema string
		comparison                 identifier.Comparison
	}{
		{name: "database", dialect: "mysql", sql: "DROP DATABASE app", schema: "app"},
		{name: "schema synonym", dialect: "mariadb", sql: "DROP SCHEMA IF EXISTS `app`", schema: "app"},
		{name: "comments", dialect: "mysql", sql: "/* review */ DROP/* explanation */DATABASE app;", schema: "app"},
		{name: "executable comment", dialect: "mysql", sql: "/*!80000 DROP DATABASE app */", schema: "app"},
		{name: "MariaDB executable comment", dialect: "mariadb", sql: "/*M!100100 DROP DATABASE app */", schema: "app"},
		{name: "escaped backtick", dialect: "mysql", sql: "DROP DATABASE `odd``db`", schema: "odd`db"},
		{name: "literal dot", dialect: "mysql", sql: "DROP DATABASE `app.dot`", schema: "app.dot"},
		{name: "later statement", dialect: "mysql", sql: "SELECT 1; DROP DATABASE app", schema: "app"},
		{name: "folded database", dialect: "mysql", sql: "DROP DATABASE APP", schema: "app", comparison: identifier.ComparisonASCIIInsensitive},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			info := catalog.ServerInfo{Dialect: test.dialect, IdentifierSemantics: identifier.Semantics{TableNames: test.comparison}}

			err := schemaremoval.RefuseStatements([]string{test.sql}, map[string]string{test.schema: "table protected.t"}, info)

			c.Assert(err, qt.ErrorMatches, `(?s).*cannot drop database.*table protected.t.*outside the selection.*`)
		})
	}
}

func TestRefuseStatements_AllowsChangesOutsideProtectedSchemas(t *testing.T) {
	for _, statement := range []string{
		"DROP DATABASE other",
		"DROP DATABASE APP",
		"DROP TABLE app.u",
		"ALTER TABLE app.u ADD COLUMN note int",
		"SELECT 'DROP DATABASE app'",
		"/* DROP DATABASE app */ SELECT 1",
		"CREATE PROCEDURE p() BEGIN SELECT 'DROP DATABASE app'; END",
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			info := catalog.ServerInfo{Dialect: "mysql", IdentifierSemantics: identifier.Semantics{TableNames: identifier.ComparisonExact}}

			c.Assert(schemaremoval.RefuseStatements([]string{statement}, map[string]string{"app": "table app.t"}, info), qt.IsNil)
		})
	}
}

func TestRefuseStatements_AllowsSchemaDropsWithoutExcludedObjects(t *testing.T) {
	c := qt.New(t)
	c.Assert(schemaremoval.RefuseStatements([]string{"DROP DATABASE app"}, nil, catalog.ServerInfo{Dialect: "mysql"}), qt.IsNil)
}
