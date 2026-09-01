package fromschema_test

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

func TestWalkDatabase_VisitsNodesInExecutionOrder(t *testing.T) {
	c := qt.New(t)
	database := schemamodel.Database{
		Schemas:    []schemamodel.Schema{{Name: "app"}},
		Extensions: []schemamodel.Extension{{Name: "pgcrypto"}},
		Sequences:  []schemamodel.Sequence{{Name: "order_number_seq"}},
		Enums:      []schemamodel.Enum{{Name: "order_status", Values: []string{"open", "closed"}}},
		Tables: []schemamodel.Table{
			{StructName: "Customer", Name: "customers"},
			{StructName: "Order", Name: "orders"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Customer", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Order", Name: "customer_id", Type: "BIGINT", Foreign: "customers(id)"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Order", Name: "uidx_orders_id", Fields: []string{"id"}, Unique: true},
			{StructName: "Order", Name: "idx_orders_customer", Fields: []string{"customer_id"}},
		},
		Views: []schemamodel.View{{Name: "open_orders", Body: "SELECT id FROM orders"}},
	}

	var visited []ast.Node
	err := fromschema.WalkDatabase(database, platform.Postgres, func(node ast.Node) error {
		visited = append(visited, node)
		return nil
	})
	c.Assert(err, qt.IsNil)

	types := make([]string, 0, len(visited))
	for _, node := range visited {
		types = append(types, fmt.Sprintf("%T", node))
	}
	c.Assert(types, qt.DeepEquals, []string{
		"*ast.CreateSchemaNode",
		"*ast.ExtensionNode",
		"*ast.CreateSequenceNode",
		"*ast.EnumNode",
		"*ast.CreateTableNode",
		"*ast.CreateTableNode",
		"*ast.IndexNode",
		"*ast.AlterTableNode",
		"*ast.CreateViewNode",
		"*ast.IndexNode",
	})
	c.Assert(visited, qt.DeepEquals, fromschema.FromDatabase(database, platform.Postgres).Statements)
}

func TestWalkDatabase_StopsAtVisitorError(t *testing.T) {
	c := qt.New(t)
	database := schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "one"}, {Name: "two"}, {Name: "three"}},
	}
	wantErr := errors.New("stop after two nodes")
	visited := 0

	err := fromschema.WalkDatabase(database, platform.Postgres, func(ast.Node) error {
		visited++
		if visited == 2 {
			return wantErr
		}
		return nil
	})

	c.Assert(err, qt.ErrorIs, wantErr)
	c.Assert(visited, qt.Equals, 2)
}

func TestWalkDatabase_RefusesNilVisitor(t *testing.T) {
	err := fromschema.WalkDatabase(schemamodel.Database{}, platform.Postgres, nil)
	qt.Assert(t, err, qt.ErrorMatches, "walk database schema: nil visitor")
}
