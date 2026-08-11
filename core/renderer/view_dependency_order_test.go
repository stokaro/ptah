package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

func TestGetOrderedCreateStatements_PostgresOrdersViewLikeDependencies(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Views: []goschema.View{{
			Name: "a_report",
			Body: "SELECT id FROM z_base",
		}},
		MaterializedViews: []goschema.MaterializedView{{
			Name: "z_base",
			Body: "SELECT id FROM users",
		}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Contains, `CREATE MATERIALIZED VIEW "z_base"`)
	c.Assert(statements[1], qt.Contains, `CREATE VIEW "a_report"`)
}
