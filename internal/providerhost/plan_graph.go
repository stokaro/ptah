package providerhost

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/internal/tableref"
)

// schedule joins the provider's steps with the host's sequence. A feature
// object is created at creationStart, after the tables and columns it may
// read and before the views, grants and comments that may name it, and the
// removals join at removalStart, before the host removes columns and tables.
// Lifecycle operations share the complete common sequence. Their effects and
// explicit dependencies order them around common changes, including a native
// object that replaces a table at the same physical path. Rewrite boundaries
// still keep a claimed common operation in its original position.
func (p commonPlanner) schedule(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, nodes []ast.Node, creationStart, removalStart int) ([]ast.Node, error) {
	request, names, err := p.planningContext(diff)
	if err != nil {
		return nil, err
	}
	metadata, err := commonPlanningMetadata(request, nodes)
	if err != nil {
		return nil, err
	}
	graph, err := featurehost.NewGraph("ptah.run/provider-common", nodes, metadata, featurehost.Windows{
		Creation: creationStart, Dependent: removalStart, Removal: removalStart,
		Lifecycle: &featurehost.Span{Start: 0, End: len(nodes)},
		Prerequisites: &featurehost.PrerequisiteWindows{Creation: 0, Removal: len(nodes)},
		Types:         &featurehost.TypeWindows{Creation: featurehost.Span{}, Removal: featurehost.Span{Start: len(nodes), End: len(nodes)}},
	})
	if err != nil {
		return nil, err
	}
	request.CommonSteps = graph.CommonSteps()
	features, err := featurehost.Plan(ctx, runtime, request, names, featureplan.PhaseDependent, featureplan.PhasePrerequisite, featureplan.PhaseType, featureplan.PhaseLifecycle)
	if err != nil {
		return nil, err
	}
	// A required transaction needs a negotiated engine service as well as
	// established transactional DDL. Metadata alone cannot provide atomicity.
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			if step.Transaction == plangraph.TransactionRequired && (!p.transactions || !p.options.Capabilities.Has(capability.TransactionalDDL)) {
				return nil, fmt.Errorf("%w: provider operation %s/%s requires a transaction; transaction sessions and transactional DDL must be established",
					ptaherr.ErrUnsupportedFeature, step.ID.Owner, step.ID.Name)
			}
		}
	}
	scheduled, err := graph.Schedule(ctx, features)
	if err != nil {
		return nil, err
	}
	if err := rejectUnclaimedChanges(scheduled); err != nil {
		return nil, err
	}
	return scheduled, nil
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
		common, isCommon, err := commonObjectMetadata(builder, parents, node)
		if err != nil {
			return nil, err
		}
		if isCommon {
			metadata[i] = common
			continue
		}
		name, action := "", plangraph.Action("")
		switch typed := node.(type) {
		case *ast.CreateTableNode:
			name, action = typed.Name, plangraph.Create
		case *pendingTableCreation:
			name, action = typed.name, plangraph.Create
		case *ast.DropTableNode:
			name, action = typed.Name, plangraph.Drop
		case *pendingTableDrop:
			name, action = typed.name, plangraph.Drop
		case *ast.AlterTableNode:
			name, action = typed.Name, plangraph.Alter
		case *pendingColumnChange:
			name, action = typed.table, plangraph.Alter
		default:
			return nil, fmt.Errorf("%w: unsupported common planning node %T", schemaext.ErrInvalidValue, node)
		}
		ref, valid := tableref.Parse(name)
		parent, found := parents[builder.TableParts(ref.Schema, ref.Name).Key()]
		if !valid || !found {
			return nil, fmt.Errorf("%w: common operation has no captured parent", schemaext.ErrInvalidValue)
		}
		step := featureplan.CommonStep{Parent: parent, Effects: []plangraph.Effect{{Subject: parent, Action: action}}}
		edit, err := columnChange(node)
		if err != nil {
			return nil, err
		}
		if edit != nil {
			// A column step reads its table and creates or drops one column of
			// it, the footprint a provider that plans columns expects. Altering
			// the table itself would claim a change this step does not make
			// and would clash with the provider's own table operations.
			action = plangraph.Drop
			if edit.added != nil {
				step.AddedColumn, action = edit.added, plangraph.Create
			}
			subject := planningChild(parent, builder.ColumnParts(parent.Schema.Source, parent.Name.Source, edit.name))
			step.Effects = []plangraph.Effect{{Subject: parent, Action: plangraph.Read}, {Subject: subject, Action: action}}
		}
		metadata[i] = step
	}
	return metadata, nil
}

// columnEdit is the one column a step adds or drops. added is nil for a drop.
type columnEdit struct {
	name  string
	added *ast.ColumnNode
}

// columnChange reads the column a common ALTER node or a placeholder adds or
// drops. It returns nil for a node that changes no column.
func columnChange(node ast.Node) (*columnEdit, error) {
	switch typed := node.(type) {
	case *pendingColumnChange:
		return &columnEdit{name: typed.column, added: typed.added}, nil
	case *ast.AlterTableNode:
		if len(typed.Operations) != 1 {
			return nil, fmt.Errorf("%w: common ALTER must carry one operation", schemaext.ErrInvalidValue)
		}
		switch operation := typed.Operations[0].(type) {
		case *ast.AddColumnOperation:
			return &columnEdit{name: operation.Column.Name, added: operation.Column}, nil
		case *ast.DropColumnOperation:
			return &columnEdit{name: operation.ColumnName}, nil
		default:
			return nil, fmt.Errorf("%w: unsupported common ALTER operation", schemaext.ErrInvalidValue)
		}
	}
	return nil, nil
}
