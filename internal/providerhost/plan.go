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
	comments   bool
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
	// The statement style is accepted and changes nothing: the common
	// statements below carry no comment, existence guard or CASCADE, and an
	// owner operation keeps its own spelling in every style. The rest of the
	// policy reaches the provider as a request (see policy); a provider
	// without the behavior plans as it would without it, as the host's own
	// planners do. Skipped change kinds are not a request a provider can
	// honor, so they stay refused.
	if err := onlyFields(p.options, "TableRebuildRequest", "Capabilities", "Statements", "AllowTableRebuild", "OnlineAlter",
		"ConcurrentIndexes", "ConcurrentIndexRefs", "ConcurrentIndexDrops", "ConcurrentIndexDropRefs", "OmitNullBackfill"); err != nil {
		return nil, err
	}
	// Matched counts the pairs a comparison made, for the drift report. It is
	// not a change, and a plan that refused it would refuse every diff that
	// keeps a table.
	common, err := p.commonObjectSteps(diff)
	if err != nil {
		return nil, err
	}
	nodes := slices.Concat(common.before, common.prepared)
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
	for _, table := range diff.TablesModified {
		additions, removals, err := p.alterations(table)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, additions...)
		columnRemovals = append(columnRemovals, removals...)
	}
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

// commonObjectSteps plans the common objects beyond tables for a provider
// that carries them. It first refuses a diff field the planner has no step
// for; for a provider without the common-object wire that includes every
// mutation field of those objects. CurrentNotDescribed is observation
// context even when none of those objects is writable; refusing it would
// block table changes merely because access was not inspected.
func (p commonPlanner) commonObjectSteps(diff *difftypes.SchemaDiff) (commonObjectPlan, error) {
	fields := []string{"TablePreparation", "IdentifierSemantics", "TablesAdded", "TablesRemoved", "TablesModified",
		"DeclaredTables", "DeclaredSchemas", "DeclaredConstraintHosts", "ObservedConstraintHosts",
		"DeclaredTableDependencies", "Features", "FeatureChanges", "CurrentDatabasePath", "CurrentNotDescribed", "Matched"}
	if p.commonObjects {
		fields = append(fields, commonObjectFields...)
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
	ownedColumns := !p.commonTables && p.assessParents
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
			// The owner interprets the whole declaration, so the operand keeps
			// every property the field carries.
			if field.Name == "" || field.Type == "" {
				return nil, nil, fmt.Errorf("provider column requires a name and type")
			}
			nodes = append(nodes, &pendingColumnChange{table: name, column: field.Name, added: modelast.FromField(field, nil, p.target)})
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
