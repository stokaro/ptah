package ydb

import (
	"ptah.run/core/ast"
	"ptah.run/internal/modelast"
	"ptah.run/internal/ydbstream"
	"ptah.run/migration/schemadiff/difftypes"
)

func (p *Planner) streamingQueries(diff *difftypes.SchemaDiff) (before, after []ast.Node, err error) {
	for _, query := range diff.StreamingQueriesRemoved {
		before = append(before, &ydbstream.Node{Operation: ydbstream.DropOperation, Name: query.QualifiedName()})
	}
	for _, query := range diff.StreamingQueriesAdded {
		after = append(after, modelast.FromStreamingQuery(query))
	}
	for _, change := range diff.StreamingQueriesChanged {
		node := &ydbstream.Node{Operation: ydbstream.AlterOperation, Name: change.Desired.QualifiedName(),
			Spec: change.Desired.Spec.Clone(), Previous: change.Current.Spec.Clone(), AllowStateReset: change.Desired.AllowStateReset}
		// Validate before returning any plan: a rejected body change must not
		// follow the mutations of other objects in a partly executed migration.
		if _, err := node.Statement(p.caps); err != nil {
			return nil, nil, err
		}
		after = append(after, node)
	}
	return before, after, nil
}
