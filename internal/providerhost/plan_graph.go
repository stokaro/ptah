package providerhost

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/internal/tableref"
)

func (p commonPlanner) schedule(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, nodes []ast.Node, removalStart int) ([]ast.Node, error) {
	request, names, err := p.planningContext(diff)
	if err != nil {
		return nil, err
	}
	metadata, err := commonPlanningMetadata(request, nodes)
	if err != nil {
		return nil, err
	}
	graph, err := featurehost.NewGraph("ptah.run/provider-common", nodes, metadata, featurehost.Windows{
		Creation: removalStart, Dependent: removalStart, Removal: removalStart,
		Prerequisites: &featurehost.PrerequisiteWindows{Creation: 0, Removal: len(nodes)},
		Types:         &featurehost.TypeWindows{Creation: featurehost.Span{}, Removal: featurehost.Span{Start: len(nodes), End: len(nodes)}},
	})
	if err != nil {
		return nil, err
	}
	request.CommonSteps = graph.CommonSteps()
	features, err := featurehost.Plan(ctx, runtime, request, names, featureplan.PhaseDependent, featureplan.PhasePrerequisite, featureplan.PhaseType)
	if err != nil {
		return nil, err
	}
	// The external session currently executes unwrapped units and has no real
	// transaction handle. Retaining metadata alone cannot satisfy a requirement.
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			if step.Transaction == plangraph.TransactionRequired {
				return nil, fmt.Errorf("%w: provider operation %s/%s requires a transaction; provider transaction sessions are not implemented",
					ptaherr.ErrUnsupportedFeature, step.ID.Owner, step.ID.Name)
			}
		}
	}
	return graph.Schedule(ctx, features)
}

// Metadata is read from the accepted common operands, never reconstructed from
// the desired table. In particular, a column rewrite receives the actual ADD
// COLUMN definition, which can differ from the final declaration.
func commonPlanningMetadata(request featureplan.Request, nodes []ast.Node) ([]featureplan.CommonStep, error) {
	builder := objectidentity.NewBuilder(request.Identifiers)
	parents := make(map[objectidentity.Key]objectidentity.ID, len(request.Tables))
	for _, table := range request.Tables {
		parents[table.Subject.Key()] = table.Subject
	}
	metadata := make([]featureplan.CommonStep, len(nodes))
	for i, node := range nodes {
		name, action := "", plangraph.Action("")
		switch typed := node.(type) {
		case *ast.CreateTableNode:
			name, action = typed.Name, plangraph.Create
		case *ast.DropTableNode:
			name, action = typed.Name, plangraph.Drop
		case *ast.AlterTableNode:
			name, action = typed.Name, plangraph.Alter
		default:
			return nil, fmt.Errorf("%w: unsupported common planning node %T", schemaext.ErrInvalidValue, node)
		}
		ref, valid := tableref.Parse(name)
		parent, found := parents[builder.TableParts(ref.Schema, ref.Name).Key()]
		if !valid || !found {
			return nil, fmt.Errorf("%w: common operation has no captured parent", schemaext.ErrInvalidValue)
		}
		step := featureplan.CommonStep{Parent: parent, Effects: []plangraph.Effect{{Subject: parent, Action: action}}}
		if table, ok := node.(*ast.AlterTableNode); ok {
			if len(table.Operations) != 1 {
				return nil, fmt.Errorf("%w: common ALTER must carry one operation", schemaext.ErrInvalidValue)
			}
			var column string
			switch operation := table.Operations[0].(type) {
			case *ast.AddColumnOperation:
				step.AddedColumn, column, action = operation.Column, operation.Column.Name, plangraph.Create
			case *ast.DropColumnOperation:
				column, action = operation.ColumnName, plangraph.Drop
			default:
				return nil, fmt.Errorf("%w: unsupported common ALTER operation", schemaext.ErrInvalidValue)
			}
			subject := planningChild(parent, builder.ColumnParts(parent.Schema.Source, parent.Name.Source, column))
			step.Effects = append(step.Effects, plangraph.Effect{Subject: subject, Action: action})
		}
		metadata[i] = step
	}
	return metadata, nil
}
