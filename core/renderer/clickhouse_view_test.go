package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
)

func TestGetOrderedCreateStatements_ClickHouseViewCapabilityEnabled(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{Views: []goschema.View{{
		StructName: "ActiveUsers",
		Name:       "analytics.active_users",
		Body:       "SELECT id\nFROM `analytics`.`users`",
	}}}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.ClickHouse)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE VIEW `analytics`.`active_users` AS\nSELECT id\nFROM `analytics`.`users`\n;\n",
	})
}

func TestGetOrderedCreateStatements_ClickHouseOrdersViewDependencies(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{Views: []goschema.View{
		{
			StructName: "Dependent",
			Name:       "analytics.a_dep",
			Body:       "SELECT n FROM `analytics`.`z_base`",
		},
		{
			StructName: "Base",
			Name:       "analytics.z_base",
			Body:       "SELECT 1 AS n",
		},
	}}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.ClickHouse)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Contains, "CREATE VIEW `analytics`.`z_base`")
	c.Assert(statements[1], qt.Contains, "CREATE VIEW `analytics`.`a_dep`")
}

func TestGetOrderedCreateStatements_ClickHouseViewCapabilityDisabled(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{Views: []goschema.View{{
		StructName: "ActiveUsers",
		Name:       "analytics.active_users",
		Body:       "SELECT 1",
	}}}
	caps := capability.ClickHouse24().
		With(capability.MaterializedViews, false).
		With(capability.Views, false)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		database,
		platform.ClickHouse,
		caps,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"-- CLICKHOUSE: CREATE VIEW \"analytics.active_users\" is not supported\n",
	})
}
