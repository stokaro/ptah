package embedydb

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// EnsureSchema creates the run-state tables when they are not there.
//
// They are rendered by Ptah's own YDB renderer from [embedstore]'s field
// descriptors, the answer the PostgreSQL store renders too, and for the
// connected server's capabilities: a timestamp column is Timestamp64 where the
// line has the 64-bit types and Timestamp on 25.1, which does not.
//
// The secondary indexes the PostgreSQL store declares are left out. This
// store reads the registry, the runs and the pointers whole inside each
// lifecycle transaction, which is what lets YDB serialize those decisions, so
// a lookup index would serve no query; and a run's history is read by the
// event table's own key, which begins with the run.
func (b *Backend) EnsureSchema(ctx context.Context, service renderer.Service) error {
	statements, err := SchemaSQL(ctx, service, b.caps)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		if _, err := b.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create store schema: %w", err)
		}
	}
	return nil
}

// SchemaSQL renders, through service, the statements that create the
// run-state tables on a server with caps. Every statement is rendered before
// any is returned.
func SchemaSQL(ctx context.Context, service renderer.Service, caps capability.Capabilities) ([]string, error) {
	var nodes []ast.Node
	for _, store := range storeTables() {
		node := ast.NewCreateTable(store.name).SetIfNotExists()
		for _, field := range store.columns {
			column := ast.NewColumn(field.Name, field.Type)
			if !field.Nullable {
				column.SetNotNull()
			}
			node.AddColumn(column)
		}
		node.AddConstraint(ast.NewPrimaryKeyConstraint(store.key...))
		nodes = append(nodes, node)
	}
	result, err := renderer.Render(ctx, service, renderer.Request{Target: Dialect, Capabilities: caps, Nodes: nodes})
	if err != nil {
		return nil, fmt.Errorf("render inference store schema: %w", err)
	}
	if len(result.Omissions) != 0 {
		return nil, fmt.Errorf("%w: inference store rendering omitted declarations", ptaherr.ErrUnsupportedFeature)
	}
	for index, fragment := range result.Fragments {
		if strings.TrimSpace(fragment) == "" {
			return nil, fmt.Errorf("%w: inference store node %d produced no YQL", renderer.ErrInvalidResult, index)
		}
	}
	return result.Fragments, nil
}
