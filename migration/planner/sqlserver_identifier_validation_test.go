package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestGenerateSchemaDiffAST_SQLServerUnknownTableSemantics_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: []string{"dbo.orders", "dbo.users"},
	}
	desired := &schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "Order", Schema: "dbo", Name: "orders"},
		{StructName: "User", Schema: "dbo", Name: "users"},
	}}

	nodes, err := planner.GenerateSchemaDiffAST(
		diff,
		desired,
		platform.SQLServer,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*target tables dbo\.orders and dbo\.users may have the same catalog identity.*`)
	c.Assert(nodes, qt.IsNil)
}

func TestGenerateSchemaDiffAST_SQLServerUnknownColumnSemantics_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: []string{"dbo.users"},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "email", Type: "NVARCHAR(320)"},
			{StructName: "User", Name: "status", Type: "INT"},
		},
	}

	nodes, err := planner.GenerateSchemaDiffAST(
		diff,
		desired,
		platform.SQLServer,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*target columns dbo\.users\.email and dbo\.users\.status may have the same catalog identity.*`,
	)
	c.Assert(nodes, qt.IsNil)
}

func TestGenerateSchemaDiffAST_SQLServerIncompleteSnapshot_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
		})
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		TablesAdded:         []string{"dbo.users"},
	}
	desired := &schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "User", Schema: "dbo", Name: "users"},
	}}

	nodes, err := planner.GenerateSchemaDiffAST(
		diff,
		desired,
		platform.SQLServer,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*snapshot does not resolve "users".*`)
	c.Assert(nodes, qt.IsNil)
}

func TestGenerateSchemaDiffAST_SQLServerInvalidSnapshot_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog(
		"SQL_Latin1_General_CP1_CI_AS",
	)
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
	}

	nodes, err := planner.GenerateSchemaDiffAST(
		diff,
		&schemamodel.Database{},
		platform.SQLServer,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*invalid identifier semantics snapshot.*`)
	c.Assert(nodes, qt.IsNil)
}
