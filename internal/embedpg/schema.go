// Package embedpg is the PostgreSQL half of an inference migration: the tables
// the run state lives in, the keyset scan that reads the source, and the
// transaction that writes vectors and their checkpoint together.
//
// It is where the design's one non-negotiable becomes a single BEGIN..COMMIT
// rather than a promise (stokaro/ptah#2068).
package embedpg

import (
	"fmt"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/embedstore"
)

// Dialect is what this package renders for.
const Dialect = "postgres"

// keyColumns names each store table's primary key.
//
// It is here rather than on the field descriptors because a key is a property
// of the table, and the descriptors are a flat list a renderer walks. The
// event table's key is its sequence, which is why the sequence column exists at
// all: an audit trail whose rows have no order is a set of things that
// happened.
var keyColumns = map[string][]string{
	embedstore.GenerationTable: {"identity"},
	embedstore.RunTable:        {"id"},
	embedstore.EventTable:      {"run_id", "sequence"},
	embedstore.PointerTable:    {"target_table"},
}

// SchemaSQL renders the statements that create the store's tables and indexes.
//
// It goes through Ptah's own renderer rather than writing DDL here, because a
// second DDL path would be a second answer to what these tables are -- and the
// first answer, the field descriptors in embedstore, is the one the persistence
// ratchet checks against embedrun.Run.
func SchemaSQL() ([]string, error) {
	var statements []string
	for _, table := range embedstore.Objects() {
		node, err := tableNode(table)
		if err != nil {
			return nil, err
		}
		rendered, err := renderer.RenderSQL(Dialect, node)
		if err != nil {
			return nil, fmt.Errorf("render %s: %w", table.Name, err)
		}
		statements = append(statements, rendered)
	}
	for _, index := range embedstore.Indexes() {
		node := ast.NewIndex(index.Name, index.StructName, index.Fields...)
		node.IfNotExists = true
		rendered, err := renderer.RenderSQL(Dialect, node)
		if err != nil {
			return nil, fmt.Errorf("render index %s: %w", index.Name, err)
		}
		statements = append(statements, rendered)
	}
	return statements, nil
}

// tableNode builds one table's CREATE statement.
func tableNode(table schemamodel.Table) (*ast.CreateTableNode, error) {
	fields, err := fieldsOf(table.Name)
	if err != nil {
		return nil, err
	}
	node := ast.NewCreateTable(table.Name).SetIfNotExists()
	for _, field := range fields {
		column := ast.NewColumn(field.Name, field.Type)
		if !field.Nullable {
			column.SetNotNull()
		}
		node.AddColumn(column)
	}
	key, found := keyColumns[table.Name]
	if !found {
		// A store table with no key is a store table two writers can both
		// insert into, and the second one does not fail.
		return nil, fmt.Errorf("no primary key declared for %s", table.Name)
	}
	node.AddConstraint(ast.NewPrimaryKeyConstraint(key...))
	return node, nil
}

// fieldsOf returns one store table's columns.
func fieldsOf(table string) ([]schemamodel.Field, error) {
	switch table {
	case embedstore.GenerationTable:
		return embedstore.GenerationFields(), nil
	case embedstore.RunTable:
		return embedstore.RunFields(), nil
	case embedstore.EventTable:
		return embedstore.EventFields(), nil
	case embedstore.PointerTable:
		return embedstore.PointerFields(), nil
	default:
		return nil, fmt.Errorf("no columns declared for %s", table)
	}
}
