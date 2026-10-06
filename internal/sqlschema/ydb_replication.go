package sqlschema

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbreplication"
)

func appendYDBReplication(database *schemamodel.Database, document *Document, statement ast.Node, dialect string) (bool, error) {
	switch node := statement.(type) {
	case *ast.CreateAsyncReplicationNode:
		schema, name := normalizeSQLTableIdentifier(dialect, node.Name)
		if findYDBReplication(database, document.base, schema, name) != nil {
			return true, fmt.Errorf("%w: duplicate async replication declaration", ErrUnmodeledStatement)
		}
		database.AsyncReplications = append(database.AsyncReplications, schemamodel.AsyncReplication{Schema: schema, Name: name, Spec: node.Spec.Clone()})
	case *ast.CreateTransferNode:
		schema, name := normalizeSQLTableIdentifier(dialect, node.Name)
		if findYDBTransfer(database, document.base, schema, name) != nil {
			return true, fmt.Errorf("%w: duplicate transfer declaration", ErrUnmodeledStatement)
		}
		database.Transfers = append(database.Transfers, schemamodel.Transfer{Schema: schema, Name: name, Spec: node.Spec})
	case *ast.AlterAsyncReplicationNode:
		schema, name := normalizeSQLTableIdentifier(dialect, node.Name)
		previous := findYDBReplication(database, document.base, schema, name)
		if previous == nil {
			return true, fmt.Errorf("%w: ALTER ASYNC REPLICATION needs an earlier declaration", ErrUnmodeledStatement)
		}
		spec, err := ydbreplication.ApplyReplicationSettings(previous.Spec, node.SourceSettings)
		if err != nil {
			return true, fmt.Errorf("%w: invalid async replication settings change", ErrUnmodeledStatement)
		}
		previous.Spec = spec
	case *ast.AlterTransferNode:
		schema, name := normalizeSQLTableIdentifier(dialect, node.Name)
		previous := findYDBTransfer(database, document.base, schema, name)
		if previous == nil {
			return true, fmt.Errorf("%w: ALTER TRANSFER needs an earlier declaration", ErrUnmodeledStatement)
		}
		spec, err := ydbreplication.ApplyTransferSettings(previous.Spec, node.SourceSettings)
		if err != nil {
			return true, fmt.Errorf("%w: invalid transfer settings change", ErrUnmodeledStatement)
		}
		previous.Spec = spec
	default:
		return false, nil
	}
	return true, nil
}

func findYDBReplication(current, earlier *schemamodel.Database, schema, name string) *schemamodel.AsyncReplication {
	for _, database := range []*schemamodel.Database{current, earlier} {
		if database == nil {
			continue
		}
		for i := range database.AsyncReplications {
			object := &database.AsyncReplications[i]
			if object.Schema == schema && object.Name == name {
				return object
			}
		}
	}
	return nil
}

func findYDBTransfer(current, earlier *schemamodel.Database, schema, name string) *schemamodel.Transfer {
	for _, database := range []*schemamodel.Database{current, earlier} {
		if database == nil {
			continue
		}
		for i := range database.Transfers {
			object := &database.Transfers[i]
			if object.Schema == schema && object.Name == name {
				return object
			}
		}
	}
	return nil
}
