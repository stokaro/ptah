package providerhost

import (
	"context"
	"fmt"
	"slices"

	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/planext"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/modelast"
	"ptah.run/internal/tableref"
)

type commonPlanner struct {
	options       planext.Options
	target        string
	assessParents bool
	transactions  bool
	commonTables  bool
	// commonObjects plans the views, sequences, roles, grants and comments a
	// provider carries on the common-object wire.
	commonObjects bool
	// comments sets the comments of a table the plan creates, and of a column
	// it adds, with steps of their own: the engine carries comments on the
	// common-object wire, and the common table message has no place for one.
	comments bool
	// indexes plans the common indexes of the tables a provider owns: the
	// provider reports the indexes a table holds, so the comparison has a
	// current side, and claims the steps that add and drop them.
	indexes bool
	// renames plans an index the comparison pairs with one declared under
	// another name as one step the owner claims, which keeps the index.
	renames bool
	// observedKeys reads the primary key a provider table holds, in key
	// order, so the comparison reports a change of it.
	observedKeys bool
	// keys hands such a change to the table's owner, which plans it.
	keys       bool
	selections *scopeSelections
}

// commonObjectFields are the diff fields of the common objects beyond
// tables, and the context they bring, which a provider that carries them on
// the common-object wire receives.
var commonObjectFields = []string{"SequencesAdded", "SequencesRemoved", "SequencesModified", "ViewsAdded", "ViewsRemoved", "ViewsModified",
	"MaterializedViewsAdded", "MaterializedViewsRemoved", "MaterializedViewsModified", "ObjectCommentsChanged", "DeclaredViewLikes",
	"CurrentGrants", "RolesAdded", "RolesRemoved", "RolesModified", "RoleMembershipsAdded", "RoleMembershipsRemoved",
	"GrantsAdded", "GrantsRemoved", "GrantOptionsAdded", "GrantOptionsRevoked"}

func (p commonPlanner) GenerateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if diff == nil {
		return nil, fmt.Errorf("provider planner requires a diff")
	}
	// Without the common-table contract the host cannot interpret a table
	// operation itself. A provider that owns the lifecycle declares parent
	// kinds, assesses each captured creation and column change, and claims the
	// host's placeholder.
	if !p.commonTables && !p.assessParents && (len(diff.TablesAdded) > 0 || len(diff.TablesModified) > 0 || len(diff.TablesRemoved) > 0) {
		return nil, unsupported("common table planning without " + provider.CommonTableIdentityFeature)
	}
	// The common statements below carry no comment, existence guard or
	// CASCADE, so the statement style changes nothing here. Owner operations
	// receive the style in their planning request and control their own
	// spelling. Skipped change kinds are not a request a provider can honor:
	// the planning entry in migration/planner removes those changes before
	// this planner is built and passes no kinds, so options that still carry
	// them are refused. The shape is the entry's too: it rewrites what this planner
	// returns, and an engine that states no catalog form keeps it as it is. An enum
	// rebuild is written only by the PostgreSQL-family planner, so the choice about
	// its guards changes nothing here.
	if err := onlyFields(p.options, "Shape", "EnumRebuildKeepsGuards", "TableRebuildRequest", "Capabilities", "Statements", "AllowTableRebuild", "OnlineAlter",
		"ConcurrentIndexes", "ConcurrentIndexRefs", "ConcurrentIndexDrops", "ConcurrentIndexDropRefs", "OmitNullBackfill"); err != nil {
		return nil, err
	}
	// Matched and FeatureCounts count the objects a comparison paired and an
	// owner compared, for the drift report. They are not changes, and a plan
	// that refused them would refuse every diff that keeps a table or that a
	// provider's comparison counted.
	common, err := p.commonObjectSteps(diff)
	if err != nil {
		return nil, err
	}
	indexes, err := p.indexChanges(diff)
	if err != nil {
		return nil, err
	}
	// An index is dropped before the tables and columns change, so a
	// replacement drops the held index before it creates the declared one
	// under the same name, and is added once its columns exist. A rename
	// neither frees nor takes a name another index change uses: the
	// comparison pairs only names the other side does not hold.
	ahead, behind := indexRemovalPlacement(diff.EffectiveIdentifierSemantics(p.target), slices.Concat(indexes.removals, indexes.renames))
	nodes := slices.Concat(common.before, ahead, common.prepared)
	var columnRemovals []ast.Node
	ordered := diff.TablesAdded.InDependencyOrder()
	if len(ordered) != len(diff.TablesAdded) {
		return nil, fmt.Errorf("provider planner received incomplete or duplicate table creations")
	}
	for _, creation := range ordered {
		created, err := p.creationNodes(creation)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, created...)
	}
	nodes = append(nodes, behind...)
	for _, table := range diff.TablesModified {
		additions, removals, err := p.alterations(table)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, additions...)
		columnRemovals = append(columnRemovals, removals...)
	}
	nodes = append(nodes, indexes.additions...)
	creationStart := len(nodes)
	nodes = append(nodes, common.after...)
	removalStart := len(nodes)
	nodes = append(nodes, columnRemovals...)
	for _, removal := range diff.TablesRemoved {
		if !removal.Current.HasTable() {
			return nil, fmt.Errorf("provider planner requires captured table state")
		}
	}
	for _, name := range deporder.TableDropOrderWithDependencies(diff.TablesRemoved.Names(), diff.DeclaredTables, diff.DeclaredTableDependencies) {
		if !p.commonTables {
			nodes = append(nodes, &pendingTableDrop{name: name})
			continue
		}
		nodes = append(nodes, &ast.DropTableNode{Name: name})
	}
	nodes = append(nodes, common.last...)
	return p.schedule(ctx, runtime, diff, nodes, creationStart, removalStart)
}

// indexRemovalPlacement places the index removals and renames of a plan ahead
// of the table creations or behind them, and returns the steps for each
// place; one of the two is empty. Where an engine scopes an index name to its
// table, no table the plan creates can hold the name of an index another
// table drops, so the creations come first, as the YDB engine built into
// Ptah at 74de0b65b planned them: its plan wrote every CREATE TABLE ahead of
// the DROP INDEX statements of the tables it kept. Where names share a
// schema, the index is dropped first, so a new table can take its name.
func indexRemovalPlacement(semantics identifier.Semantics, steps []ast.Node) (ahead, behind []ast.Node) {
	if semantics.IndexNamespace == identifier.IndexNamespaceTable {
		return nil, steps
	}
	return steps, nil
}

// commonObjectSteps plans the common objects beyond tables for a provider
// that carries them. It first refuses a diff field the planner has no step
// for; for a provider without the common-object wire that includes every
// mutation field of those objects. CurrentNotDescribed is observation
// context even when none of those objects is writable; refusing it would
// block table changes merely because access was not inspected.
func (p commonPlanner) commonObjectSteps(diff *difftypes.SchemaDiff) (commonObjectPlan, error) {
	// The declared tables, schemas, foreign keys and constraint hosts are
	// context a planner reads, not changes: DeclaredForeignKeys holds every
	// foreign key the declaration states, changed or not.
	fields := []string{"TablePreparation", "IdentifierSemantics", "TablesAdded", "TablesRemoved", "TablesModified",
		"DeclaredTables", "DeclaredSchemas", "DeclaredForeignKeys", "DeclaredConstraintHosts", "ObservedConstraintHosts",
		"DeclaredTableDependencies", "Features", "FeatureChanges", "CurrentDatabasePath", "CurrentNotDescribed", "Matched", "FeatureCounts"}
	if p.commonObjects {
		fields = append(fields, commonObjectFields...)
	}
	if p.ownsIndexes() {
		// An owner of the table lifecycle receives a table's indexes in the
		// table it creates or alters; planningContext refuses an index of any
		// other table.
		fields = append(fields, "IndexesAdded", "IndexesRemoved", "DeclaredIndexes")
		if p.renames {
			// A paired index travels as a rename step; see indexChanges.
			fields = append(fields, "IndexesRenamed")
		}
		if p.comments {
			// The comment of such an index travels on the common-object
			// wire; see indexCommentNodes.
			fields = append(fields, "IndexCommentsChanged")
		}
	}
	if p.observedKeys {
		// A changed primary key reaches the table's owner as an alteration
		// of the table; see keyChanges. Any other constraint stays refused.
		if err := p.keyChanges(diff); err != nil {
			return commonObjectPlan{}, err
		}
		fields = append(fields, "ConstraintsAdded", "ConstraintsRemoved")
	}
	if err := onlyFields(*diff, fields...); err != nil {
		return commonObjectPlan{}, err
	}
	if !p.commonObjects {
		return commonObjectPlan{}, nil
	}
	return p.planCommonObjects(diff)
}

func (p commonPlanner) alterations(table difftypes.TableDiff) (nodes, removals []ast.Node, err error) {
	fields := []string{"TableName", "ColumnsAdded", "ColumnsRemoved", "Desired", "Current", "ColumnKeyNames", "FeatureChanges"}
	ownedColumns := p.ownsColumns()
	if ownedColumns {
		// The owner receives complete captured columns and must account for
		// the parent change. Shared column spellings cannot preempt that decision.
		fields = append(fields, "ColumnsModified")
	}
	if p.commonObjects {
		// A comment travels on the common-object wire; see commentNodes. A
		// column change other than its comment is not one the wire carries.
		fields = append(fields, "CommentChange", "ColumnsModified")
		for _, column := range table.ColumnsModified {
			if !ownedColumns && !commentOnly(column) {
				return nil, nil, unsupported(fmt.Sprintf("changing column %s of table %s beyond its comment", column.ColumnName, table.TableName))
			}
		}
	}
	if err := onlyFields(table, fields...); err != nil {
		return nil, nil, err
	}
	if !table.Desired.HasTable() || !table.Current.HasTable() {
		return nil, nil, fmt.Errorf("provider planner requires both captured table states")
	}
	name := tableref.CanonicalExact(table.Desired.Table.Schema, table.Desired.Table.Name)
	nodes = make([]ast.Node, 0, len(table.ColumnsAdded))
	for _, field := range table.ColumnsRemoved {
		if !p.commonTables {
			removals = append(removals, &pendingColumnChange{table: name, column: field.Name})
			continue
		}
		removals = append(removals, &ast.AlterTableNode{Name: name, Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: field.Name}}})
	}
	for _, field := range table.ColumnsAdded {
		if !p.commonTables {
			added, err := p.ownedColumnAddition(name, field)
			if err != nil {
				return nil, nil, err
			}
			nodes = append(nodes, added...)
			continue
		}
		comment := ""
		if p.comments {
			comment, field.Comment = field.Comment, ""
		}
		column, err := commonColumn(field)
		if err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, &ast.AlterTableNode{Name: name, Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}}})
		if comment != "" {
			nodes = append(nodes, commentStep(name, field.Name, comment))
		}
	}
	return nodes, removals, nil
}

// ownedColumnAddition is the step that adds field to the table name for an
// owner of the table lifecycle. The owner interprets the whole declaration, so
// the operand keeps every property the field carries. Its comment is also set
// with a step of its own on an engine that carries comments on the
// common-object wire, as for a common column.
func (p commonPlanner) ownedColumnAddition(name string, field schemamodel.Field) ([]ast.Node, error) {
	if field.Name == "" || field.Type == "" {
		return nil, fmt.Errorf("provider column requires a name and type")
	}
	nodes := []ast.Node{&pendingColumnChange{table: name, column: field.Name, added: modelast.FromField(field, nil, p.target)}}
	if p.comments && field.Comment != "" {
		nodes = append(nodes, commentStep(name, field.Name, field.Comment))
	}
	return nodes, nil
}

// commentOnly reports a column change that changes the column's comment and
// nothing else, which the comparison may also record among its changes.
func commentOnly(column difftypes.ColumnDiff) bool {
	if column.CommentChange == nil || column.NotNullConstraintNameChange != nil || column.CurrentSequenceRestart != "" {
		return false
	}
	for attribute := range column.Changes {
		if attribute != "comment" {
			return false
		}
	}
	return true
}
