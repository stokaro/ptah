package sqlschema

import (
	"fmt"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
)

func appendStreamingQuery(database, base *schemamodel.Database, node *ydbast.StreamingQuery) error {
	if err := node.Validate(); err != nil {
		return err
	}
	if node.Operation != ydbast.StreamingCreate {
		return fmt.Errorf("%w: only CREATE declares a streaming query", ErrUnmodeledStatement)
	}
	query := schemamodel.StreamingQuery{Name: node.Name, Schema: node.Schema, Spec: node.Spec.Clone(), AllowStateReset: node.Creation.OrReplace && !node.Creation.IfNotExists}
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
				return fmt.Errorf("streaming query %q is declared twice", node.QualifiedName())
			}
		}
	}
	database.StreamingQueries = append(database.StreamingQueries, query)
	return nil
}
