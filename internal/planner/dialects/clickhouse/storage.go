package clickhouse

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/internal/tableref"
)

// storagePhases are the common operations of a plan in the order they run.
// Columns and indexes become graph steps with known effects, so feature owners
// can order their operations around them and see what the host already does
// to an object they own settings of.
//
// drops holds the drops of the materialized views the plan replaces: they run
// after before and ahead of the column and feature steps, and the views are
// recreated in after.
//
// rebuilds are the tables the plan creates again. Each is one step, after the
// materialized-view drops and ahead of every feature step, because the new
// table takes the table's name only when the step swaps the two, so an
// operation that names the table in between would change the old one.
type storagePhases struct {
	before, drops, columns, after, indexes, last []ast.Node
	rebuilds                                     tableRebuilds
}

func (p *Planner) scheduleStorage(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, phases storagePhases) ([]ast.Node, error) {
	request := featureplan.Request{
		Target: platform.ClickHouse, Identifiers: diff.EffectiveIdentifierSemantics(platform.ClickHouse),
		Capabilities: p.capabilities(), Changes: slices.Clone(diff.FeatureChanges), DatabasePath: diff.CurrentDatabasePath,
		Types: featureplan.TypeContext{Desired: schemaext.DefinedTypes(diff.Features.DesiredObjects), Current: schemaext.DefinedTypes(diff.Features.CurrentObjects)},
	}
	names := make(map[objectidentity.Key]string)
	builder := objectidentity.NewBuilder(request.Identifiers)
	for _, table := range diff.TablesModified {
		if len(table.ColumnsAdded)+len(table.ColumnsModified)+len(table.ColumnsRemoved)+len(table.FeatureChanges) == 0 {
			continue
		}
		subject := builder.Table(table.TableName)
		action, changes := featureplan.AlterTable, table.FeatureChanges
		if phases.rebuilds.has(builder, table.TableName) {
			// The new table is made from the declaration, so the changes of
			// the table's own storage and skipping indexes have nothing left
			// to change. A row policy change is still planned: a policy
			// names the table, so it holds for the new one after the swap.
			action, changes = featureplan.RebuildTable, withoutRebuiltChanges(changes)
		}
		request.Tables = append(request.Tables, featureplan.Table{Subject: subject, Action: action, Desired: table.Desired, Current: table.Current})
		request.Changes = append(request.Changes, changes...)
		names[subject.Key()] = table.TableName
	}
	for _, removal := range diff.TablesRemoved {
		subject := builder.Table(removal.Name)
		request.Tables = append(request.Tables, featureplan.Table{Subject: subject, Action: featureplan.DropTable, Current: removal.Current})
		names[subject.Key()] = removal.Name
	}
	common, steps, err := commonColumns(builder, phases.columns)
	if err != nil {
		return nil, err
	}
	first, last := plangraph.StepID{Owner: common.Owner, Name: "before-storage"}, plangraph.StepID{Owner: common.Owner, Name: "after-storage"}
	materializedViewChanges(&request, names, diff)
	drops := plangraph.StepID{Owner: common.Owner, Name: "materialized-view-replacements"}
	dropEffects, createEffects := replacementEffects(builder, phases.drops)
	if len(dropEffects) > 0 {
		steps = append(steps,
			featureplan.CommonStep{ID: drops, Effects: slices.Clone(dropEffects)},
			featureplan.CommonStep{ID: last, Effects: slices.Clone(createEffects)})
	}
	var middle []plangraph.StepID
	for _, step := range common.Steps {
		middle = append(middle, step.ID)
	}
	rebuildSteps := rebuildStepIDs(&common, &steps, builder, phases.rebuilds)
	middle = append(middle, rebuildSteps...)
	indexSteps, err := commonIndexes(builder, request.Tables, phases.indexes, &common)
	if err != nil {
		return nil, err
	}
	steps = append(steps, indexSteps...)
	request.CommonSteps = steps
	features, err := featurehost.Plan(ctx, runtime, request, names)
	if err != nil {
		return nil, err
	}
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			middle = append(middle, step.ID)
		}
	}
	// These surrounding phases retain their order. Their footprints remain
	// unknown until each object family contributes its own graph metadata;
	// the one known so far is the replacement of a materialized view, whose
	// drop runs ahead of every column and feature step and whose recreation
	// is in the phase after them.
	final := plangraph.StepID{Owner: common.Owner, Name: "after-indexes"}
	common.Steps = append(common.Steps,
		plangraph.Step[[]ast.Node]{ID: first, Payload: phases.before},
		plangraph.Step[[]ast.Node]{ID: drops, Payload: phases.drops, Effects: dropEffects},
		plangraph.Step[[]ast.Node]{ID: last, Payload: phases.after, Effects: createEffects},
		plangraph.Step[[]ast.Node]{ID: final, Payload: phases.last})
	common.Dependencies = append(common.Dependencies, plangraph.Dependency{Before: first, After: drops}, plangraph.Dependency{Before: drops, After: last})
	for _, id := range middle {
		common.Dependencies = append(common.Dependencies, plangraph.Dependency{Before: drops, After: id}, plangraph.Dependency{Before: id, After: last})
	}
	// A rebuilt table is the old one until EXCHANGE TABLES swaps in the new
	// one, so no feature step runs before the rebuild has finished.
	for _, rebuild := range rebuildSteps {
		for _, contribution := range features.Contributions {
			for _, step := range contribution.Steps {
				common.Dependencies = append(common.Dependencies, plangraph.Dependency{Before: rebuild, After: step.ID})
			}
		}
	}
	// Index operations keep their sequence after the other objects: a
	// replacement drops before it adds, and removals follow additions.
	previous := last
	for _, step := range indexSteps {
		common.Dependencies = append(common.Dependencies, plangraph.Dependency{Before: previous, After: step.ID})
		previous = step.ID
	}
	common.Dependencies = append(common.Dependencies, plangraph.Dependency{Before: previous, After: final})
	plan, err := plangraph.ScheduleRewritten(ctx, common, features.Rewrites, features.Contributions...)
	if err != nil {
		return nil, err
	}
	var nodes []ast.Node
	for _, step := range plan.Steps {
		nodes = append(nodes, step.Payload...)
	}
	return nodes, nil
}

// commonIndexes adds one step per common index operation, naming the index it
// creates or drops. Parent binds a step to a captured table; an index on a
// table the request does not capture keeps its effects without a parent.
func commonIndexes(builder objectidentity.Builder, tables []featureplan.Table, nodes []ast.Node, common *plangraph.Contribution[[]ast.Node]) ([]featureplan.CommonStep, error) {
	captured := make(map[objectidentity.Key]bool, len(tables))
	for _, table := range tables {
		captured[table.Subject.Key()] = true
	}
	var steps []featureplan.CommonStep
	for i, node := range nodes {
		var table, name string
		var action plangraph.Action
		switch typed := node.(type) {
		case *ast.IndexNode:
			table, name, action = typed.Table, typed.Name, plangraph.Create
		case *ast.DropIndexNode:
			table, name, action = typed.Table, typed.Name, plangraph.Drop
		default:
			return nil, fmt.Errorf("%w: unexpected common index operation %T", schemaext.ErrInvalidValue, node)
		}
		parent := builder.Table(table)
		subject := builder.Index(table, name)
		if ref, valid := tableref.Parse(table); valid {
			parent, subject = builder.TableParts(ref.Schema, ref.Name), builder.IndexParts(ref.Schema, ref.Name, name)
		}
		step := featureplan.CommonStep{
			ID:          plangraph.StepID{Owner: common.Owner, Name: fmt.Sprintf("index/%d", i)},
			Effects:     []plangraph.Effect{{Subject: parent, Action: plangraph.Read}, {Subject: subject, Action: action}},
			Transaction: plangraph.TransactionForbidden,
		}
		if captured[parent.Key()] {
			step.Parent = parent
		}
		steps = append(steps, step)
		common.Steps = append(common.Steps, plangraph.Step[[]ast.Node]{ID: step.ID, Payload: []ast.Node{node}, Effects: slices.Clone(step.Effects), Transaction: step.Transaction})
	}
	return steps, nil
}

func commonColumns(builder objectidentity.Builder, nodes []ast.Node) (plangraph.Contribution[[]ast.Node], []featureplan.CommonStep, error) {
	common := plangraph.Contribution[[]ast.Node]{Owner: "ptah.run/common/clickhouse"}
	var steps []featureplan.CommonStep
	for i, node := range nodes {
		alter, ok := node.(*ast.AlterTableNode)
		if !ok || len(alter.Operations) != 1 {
			return common, nil, fmt.Errorf("%w: expected one common column operation", schemaext.ErrInvalidValue)
		}
		step := featureplan.CommonStep{ID: plangraph.StepID{Owner: common.Owner, Name: fmt.Sprintf("column/%d", i)}, Parent: builder.Table(alter.Name), Transaction: plangraph.TransactionForbidden}
		var name string
		var action plangraph.Action
		switch operation := alter.Operations[0].(type) {
		case *ast.AddColumnOperation:
			name, action, step.AddedColumn = operation.Column.Name, plangraph.Create, operation.Column
		case *ast.ModifyColumnOperation:
			name, action = operation.Column.Name, plangraph.Alter
		case *ast.DropColumnOperation:
			name, action = operation.ColumnName, plangraph.Drop
		case *ast.SetCommentOperation:
			name, action = operation.Column, plangraph.Alter
		default:
			return common, nil, fmt.Errorf("%w: unexpected common column operation %T", schemaext.ErrInvalidValue, operation)
		}
		step.Effects = []plangraph.Effect{{Subject: step.Parent, Action: plangraph.Read}, {Subject: builder.Column(alter.Name, name), Action: action}}
		if name == "" {
			// A table's own comment alters the table, not a column of it.
			step.Effects = []plangraph.Effect{{Subject: step.Parent, Action: plangraph.Alter}}
		}
		steps = append(steps, step)
		common.Steps = append(common.Steps, plangraph.Step[[]ast.Node]{ID: step.ID, Payload: []ast.Node{node}, Effects: slices.Clone(step.Effects), Transaction: step.Transaction})
		if i > 0 {
			common.Dependencies = append(common.Dependencies, plangraph.Dependency{Before: common.Steps[i-1].ID, After: step.ID})
		}
	}
	return common, steps, nil
}

// materializedViewChanges adds the attached-setting changes of materialized
// views to the request and binds each view's name for the operations its
// owner contributes. Whether the common plan replaces a view, its owner learns
// from the effects of the steps that drop and recreate it.
func materializedViewChanges(request *featureplan.Request, names map[objectidentity.Key]string, diff *difftypes.SchemaDiff) {
	for _, view := range diff.MaterializedViewsModified {
		if len(view.FeatureChanges) == 0 {
			continue
		}
		request.Changes = append(request.Changes, view.FeatureChanges...)
		names[view.FeatureChanges[0].Subject.Key()] = view.ViewName
	}
}

// replacementEffects are the effects of the replacement drops the plan
// emitted: each drops its view, and the phase after storage creates it again.
// A view whose drop was not emitted, such as on a target without materialized
// views, is not replaced and has neither effect.
func replacementEffects(builder objectidentity.Builder, drops []ast.Node) (dropEffects, createEffects []plangraph.Effect) {
	for _, node := range drops {
		drop, ok := node.(*ast.DropMaterializedViewNode)
		if !ok {
			continue
		}
		subject := builder.SchemaScoped(objectidentity.KindMatView, drop.Name)
		dropEffects = append(dropEffects, plangraph.Effect{Subject: subject, Action: plangraph.Drop})
		createEffects = append(createEffects, plangraph.Effect{Subject: subject, Action: plangraph.Create})
	}
	return dropEffects, createEffects
}

// withoutRebuiltChanges drops the changes of a table's own storage and of its
// skipping indexes, which a table created again from its declaration already
// carries.
func withoutRebuiltChanges(changes []schemaext.ChangeRecord) []schemaext.ChangeRecord {
	return slices.DeleteFunc(slices.Clone(changes), func(change schemaext.ChangeRecord) bool {
		kind := change.Value.Kind()
		return kind == chdiff.TableKind || kind == chdiff.IndexKind
	})
}

// rebuildStepIDs adds one step to the common plan for each rebuilt table, and
// the common step its owners read, and returns the steps' identities.
func rebuildStepIDs(common *plangraph.Contribution[[]ast.Node], steps *[]featureplan.CommonStep, builder objectidentity.Builder, rebuilds tableRebuilds) []plangraph.StepID {
	var ids []plangraph.StepID
	for i, rebuild := range rebuilds {
		id := plangraph.StepID{Owner: common.Owner, Name: fmt.Sprintf("rebuild/%d", i)}
		effects := []plangraph.Effect{{Subject: builder.Table(rebuild.name), Action: plangraph.Alter}}
		*steps = append(*steps, featureplan.CommonStep{ID: id, Parent: rebuild.subject, Effects: slices.Clone(effects), Transaction: plangraph.TransactionForbidden})
		common.Steps = append(common.Steps, plangraph.Step[[]ast.Node]{
			ID: id, Payload: rebuild.nodes, Effects: effects, Transaction: plangraph.TransactionForbidden, Impact: rebuild.effect,
		})
		ids = append(ids, id)
	}
	return ids
}
