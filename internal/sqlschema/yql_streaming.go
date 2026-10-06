package sqlschema

import (
	"fmt"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbstream"
)

func appendStreamingQuery(database, base *schemamodel.Database, node *ydbstream.Node) error {
	if node.Operation != ydbstream.CreateOperation {
		return fmt.Errorf("%w: only CREATE declares a streaming query", ErrUnmodeledStatement)
	}
	ref, ok := tableref.Parse(node.Name)
	if !ok {
		return fmt.Errorf("invalid streaming query path %q", node.Name)
	}
	query := schemamodel.StreamingQuery{Name: ref.Name, Schema: ref.Schema, Spec: node.Spec.Clone(), AllowStateReset: node.Creation.ReplacesExisting()}
	for _, source := range []*schemamodel.Database{database, base} {
		if source == nil {
			continue
		}
		for i := range source.StreamingQueries {
			held := &source.StreamingQueries[i]
			if held.QualifiedName() != query.QualifiedName() {
				continue
			}
			switch {
			case node.Creation.IfNotExists:
				return nil
			case node.Creation.OrReplace:
				*held = query
				return nil
			default:
				return fmt.Errorf("streaming query %q is declared twice", node.Name)
			}
		}
	}
	database.StreamingQueries = append(database.StreamingQueries, query)
	return nil
}
