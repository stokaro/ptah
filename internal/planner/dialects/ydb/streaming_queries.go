package ydb

import (
	"ptah.run/core/ast"
	"ptah.run/internal/modelast"
	"ptah.run/internal/ydbstream"
	"ptah.run/migration/schemadiff/difftypes"
)

func (p *Planner) streamingQueries(diff *difftypes.SchemaDiff) (before, after []ast.Node, err error) {
	for _, query := range diff.StreamingQueriesRemoved {
		node := &ydbstream.Node{Operation: ydbstream.DropOperation, Name: query.QualifiedName()}
		if _, err := node.Statement(p.caps); err != nil {
			return nil, nil, err
		}
		before = append(before, node)
	}
	for _, query := range diff.StreamingQueriesAdded {
		node := modelast.FromStreamingQuery(query)
		if _, err := node.Statement(p.caps); err != nil {
			return nil, nil, err
		}
		after = append(after, node)
	}
	for _, change := range diff.StreamingQueriesChanged {
		node := &ydbstream.Node{Operation: ydbstream.AlterOperation, Name: change.Desired.QualifiedName(),
			Spec: change.Desired.Spec.Clone(), Previous: change.Current.Spec.Clone(), AllowStateReset: change.Desired.AllowStateReset}
		// Validate before returning any plan: a rejected body change must not
		// follow the mutations of other objects in a partly executed migration.
		if _, err := node.Statement(p.caps); err != nil {
			return nil, nil, err
		}
		if ydbstream.Running(change.Current.Spec) {
			stopped := change.Current.Spec.Clone()
			stopped.Run = new(false)
			before = append(before, &ydbstream.Node{Operation: ydbstream.AlterOperation, Name: node.Name, Spec: stopped, Previous: change.Current.Spec.Clone()})
			if ydbstream.Equal(stopped, change.Desired.Spec) {
				continue
			}
		}
		after = append(after, node)
	}
	return before, after, nil
}
