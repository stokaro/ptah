package sqlschema

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/internal/yqlparse"
)

// appendReplicationDeclaration declares the async replication or the transfer
// a CREATE statement names. Every source format declares one through the
// owner, so a repeated one is refused as it is in Go and YAML.
func appendReplicationDeclaration(database *schemamodel.Database, payload ast.ExtensionPayload) (bool, error) {
	var err error
	switch value := payload.(type) {
	case *ydbast.AsyncReplication:
		if value.Change.Before != nil || value.Change.After == nil {
			return false, nil
		}
		database.FeatureObjects, err = ydbreplication.DeclareReplication(database.FeatureObjects, value.Schema, value.Name, "",
			value.Change.After.Spec)
	case *ydbast.Transfer:
		if value.Change.Before != nil || value.Change.After == nil {
			return false, nil
		}
		database.FeatureObjects, err = ydbreplication.DeclareTransfer(database.FeatureObjects, value.Schema, value.Name, "",
			value.Change.After.Spec)
	default:
		return false, nil
	}
	return true, err
}

// applyReplicationSettings folds an ALTER ASYNC REPLICATION or ALTER TRANSFER
// into the object this schema, or the one it extends, declares at the path.
func applyReplicationSettings(database, base *schemamodel.Database, node *yqlparse.ReplicationSettings) error {
	for _, source := range []*schemamodel.Database{database, base} {
		if source == nil {
			continue
		}
		found, err := applyDeclaredReplicationSettings(source, node)
		if err != nil || found {
			return err
		}
	}
	if node.Kind == ydbreplication.TransferSource {
		return fmt.Errorf("%w: ALTER TRANSFER needs an earlier declaration", ErrUnmodeledStatement)
	}
	return fmt.Errorf("%w: ALTER ASYNC REPLICATION needs an earlier declaration", ErrUnmodeledStatement)
}

// applyDeclaredReplicationSettings folds node into the object database
// declares at its path, and reports whether it declares one.
func applyDeclaredReplicationSettings(database *schemamodel.Database, node *yqlparse.ReplicationSettings) (bool, error) {
	ref := ydbreplication.ReplicationRef(node.Schema, node.Name)
	if node.Kind == ydbreplication.TransferSource {
		ref = ydbreplication.TransferRef(node.Schema, node.Name)
	}
	object, found, err := database.FeatureObjects.Get(ref)
	if err != nil || !found {
		return false, err
	}
	switch value := object.Value.(type) {
	case *ydbreplication.DesiredReplication:
		spec, err := ydbreplication.ApplyReplicationSettings(value.Spec, node.Settings)
		if err != nil {
			return true, fmt.Errorf("%w: invalid async replication settings change", ErrUnmodeledStatement)
		}
		value.Spec = spec
	case *ydbreplication.DesiredTransfer:
		spec, err := ydbreplication.ApplyTransferSettings(value.Spec, node.Settings)
		if err != nil {
			return true, fmt.Errorf("%w: invalid transfer settings change", ErrUnmodeledStatement)
		}
		value.Spec = spec
	default:
		return true, fmt.Errorf("%w: expected a desired %s, got %T", schemaext.ErrInvalidValue, ref.Kind, object.Value)
	}
	database.FeatureObjects, err = database.FeatureObjects.Replace(object)
	return true, err
}
