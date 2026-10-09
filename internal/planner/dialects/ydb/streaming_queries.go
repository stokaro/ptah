package ydb

import (
	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/internal/modelast"
	"ptah.run/internal/ydbstream"
	"ptah.run/migration/schemadiff/difftypes"
)

func (p *Planner) streamingQueries(diff *difftypes.SchemaDiff) (before, after []ast.Node, err error) {
	for _, query := range diff.StreamingQueriesRemoved {
		node := &ydbast.StreamingQuery{Operation: ydbast.StreamingDrop, Schema: query.Schema, Name: query.Name}
		if err := ydbrender.StreamingHandler().Validate(renderer.ExtensionContext{Target: "ydb", Capabilities: p.caps}, node); err != nil {
			return nil, nil, err
		}
		before = append(before, &ast.ExtensionStatement{Payload: node})
	}
	for _, query := range diff.StreamingQueriesAdded {
		node := modelast.FromStreamingQuery(query)
		if err := ydbrender.StreamingHandler().Validate(renderer.ExtensionContext{Target: "ydb", Capabilities: p.caps}, node.Payload); err != nil {
			return nil, nil, err
		}
		after = append(after, node)
	}
	for _, change := range diff.StreamingQueriesChanged {
		node := &ydbast.StreamingQuery{Operation: ydbast.StreamingAlter, Schema: change.Desired.Schema, Name: change.Desired.Name,
			Spec: change.Desired.Spec.Clone(), Previous: change.Current.Spec.Clone(), AllowStateReset: change.Desired.AllowStateReset}
		// Validate before returning any plan: a rejected body change must not
		// follow the mutations of other objects in a partly executed migration.
		if err := ydbrender.StreamingHandler().Validate(renderer.ExtensionContext{Target: "ydb", Capabilities: p.caps}, node); err != nil {
			return nil, nil, err
		}
		if ydbstream.Running(change.Current.Spec) {
			stopped := change.Current.Spec.Clone()
			stopped.Run = new(false)
			before = append(before, &ast.ExtensionStatement{Payload: &ydbast.StreamingQuery{Operation: ydbast.StreamingAlter, Schema: node.Schema, Name: node.Name, Spec: stopped, Previous: change.Current.Spec.Clone()}})
			if ydbstream.Equal(stopped, change.Desired.Spec) {
				continue
			}
		}
		after = append(after, &ast.ExtensionStatement{Payload: node})
	}
	return before, after, nil
}
