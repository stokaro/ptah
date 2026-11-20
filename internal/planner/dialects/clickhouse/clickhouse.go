// Package clickhouse implements ClickHouse-specific migration planning.
//
// ClickHouse only honors a subset of the schema features expressible through
// Ptah's annotations: tables, columns, plain views, roles and grants, and a
// narrow set of constraints (CHECK only). Enums, custom types, extensions,
// functions and row-level security policies are PostgreSQL-shaped and have no
// direct equivalent here, so this planner emits no runnable SQL for them.
//
// Roles and grants are not in that list (stokaro/ptah#1025). ClickHouse's
// access control is its own shape rather than PostgreSQL's with different
// keywords -- a role carries no attributes at all,
// and a grant's scope is a two-part pattern with no object-type keyword -- so
// what a declaration may say is narrowed by internal/clickhouserbac before a
// plan is built, and rbac.go plans what survives. See rbac.go for the ordering
// the server forces on those statements.
//
// It does not drop them in silence. Every such object the diff carries is
// emitted as its AST node and reduced by the renderer to a
// `-- CLICKHOUSE: ... is not supported` comment naming the object, which is
// exactly what `ptah schema render --dialect clickhouse` produces for the same
// model -- the two surfaces have to give the same answer. Those comments are
// stripped before execution, so nothing unrunnable reaches the server.
//
// Common column operations join storage operations from the selected feature
// service in one dependency graph before any AST nodes are returned.
package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/modelast"
	"ptah.run/internal/planner/columnchange"
	"ptah.run/internal/planner/guards"
	"ptah.run/internal/planner/schemaprecondition"
)

// Planner implements the migration planner interface for ClickHouse.
type Planner struct {
	caps capability.Capabilities
	// statements is the statement style. This planner reads it for two
	// decisions: whether a dropped table carries the line comment that only
	// describes it, and whether a guard no earlier statement needs stays.
	statements planext.StatementStyle
	// rebuild says whether a change ClickHouse has no ALTER TABLE for creates
	// the table again, or is refused.
	rebuild tableRebuildPolicy
}

// WithStatementStyle returns a copy of the planner that spells its statements
// in style. The receiver is not modified.
func (p *Planner) WithStatementStyle(style planext.StatementStyle) *Planner {
	cp := *p
	cp.statements = style
	return &cp
}

// New returns a ClickHouse planner using the default ClickHouse 24 preset.
func New() *Planner { return NewWithCapabilities(capability.ClickHouse24()) }

// NewWithCapabilities returns a ClickHouse planner for a concrete server
// capability set. The set is cloned so later caller mutations cannot change a
// plan already being assembled. A nil set is conservative; New explicitly
// selects the default ClickHouse 24 preset.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return &Planner{caps: caps}
}

func (p *Planner) capabilities() capability.Capabilities {
	return p.caps
}

// GenerateMigrationAST produces the AST node sequence that, when rendered
// against the ClickHouse renderer, brings the database from its current
// state (described by diff) to the target schema (described by generated).
//
// The output is ordered to satisfy ClickHouse's constraint that tables must
// be created before any subsequent ALTER references them:
//
//  1. Diagnostics for the extensions and sequences ClickHouse cannot host.
//  2. CREATE TABLE for every newly-added table.
//  3. ALTER TABLE for every per-table column add/modify/drop.
//  4. CREATE ROLE; CREATE, CREATE OR REPLACE, or DROP for plain views; REVOKE
//     and GRANT; plus diagnostics for functions, materialized views, RLS and
//     triggers that Ptah's ClickHouse model cannot express.
//  5. ADD INDEX for new data-skipping indexes.
//  6. DROP INDEX for removed indexes.
//  7. DROP TABLE for removed tables.
//
// Step 4 keeps roles ahead of grants because the server refuses a grant to a
// role it does not know (Code 511, UNKNOWN_ROLE), and keeps revokes ahead of
// grants because ClickHouse absorbs a narrower grant into a broader one in
// silence; rbac.go carries the measurements.
//
// Unsupported nodes in steps 1 and 4 emit no runnable SQL: the renderer reduces
// each to a named `-- CLICKHOUSE: ... is not supported` comment, in the order
// `schema render` produces for the same model. Plain-view, role and grant nodes
// are executable and retain what they declare.
func (p *Planner) GenerateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) (plannedNodes []ast.Node, planErr error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	defer func() {
		if err := ctx.Err(); err != nil {
			plannedNodes, planErr = nil, err
		}
	}()

	if err := schemaprecondition.RefuseServerSchemas(platform.ClickHouse, diff); err != nil {
		return nil, err
	}
	if err := schemaprecondition.RefuseIndexChangesInPlace(platform.ClickHouse, diff); err != nil {
		return nil, err
	}
	if err := schemaprecondition.RefuseSerialSequenceChanges(platform.ClickHouse, diff); err != nil {
		return nil, err
	}
	if err := schemaprecondition.RefuseRoleMemberships(platform.ClickHouse, diff); err != nil {
		return nil, err
	}
	if err := schemaprecondition.RefuseSharedRowSecurity(platform.ClickHouse, diff); err != nil {
		return nil, err
	}
	var result []ast.Node

	// The identity check alone. Nothing is resolved: an addition carries its
	// own declaration (stokaro/ptah#2315).
	if err := indexscope.ValidateDiffWithSemantics(
		platform.ClickHouse,
		diff.EffectiveIdentifierSemantics(platform.ClickHouse),
		diff,
	); err != nil {
		return nil, err
	}
	var err error

	if len(diff.EnumsAdded)+len(diff.EnumsRemoved)+len(diff.EnumsModified) > 0 {
		result = append(result, ast.NewComment("CLICKHOUSE: enum changes are ignored; declare ClickHouse Enum8/Enum16 columns inline via platform.clickhouse.type"))
	}

	if err := refuseUnfinishedRebuilds(diff); err != nil {
		return nil, err
	}
	rebuilds, err := p.planRebuilds(diff)
	if err != nil {
		return nil, err
	}
	result = reportUnsupportedObjectsBeforeTables(result, diff)
	result = p.addNewTables(result, diff)
	before := result
	columns, err := p.modifyExistingTables(nil, diff, rebuilds)
	if err != nil {
		return nil, err
	}
	after, replacementDrops, err := planObjectsAfterTables(nil, diff, p.capabilities())
	if err != nil {
		return nil, err
	}
	indexes, err := p.addNewIndexes(nil, diff, rebuilds)
	if err != nil {
		return nil, err
	}
	indexes = p.removeIndexes(indexes, diff, rebuilds)
	last := p.removeTables(nil, diff)

	nodes, err := p.scheduleStorage(ctx, runtime, diff, storagePhases{before: before, drops: replacementDrops, columns: columns, rebuilds: rebuilds, after: after, indexes: indexes, last: last})
	if err != nil {
		return nil, err
	}
	if p.statements == planext.StatementStyleExact {
		// An owner statement here changes a table's storage, a skipping index or
		// a row policy, and none of them removes a table or a view a later drop
		// names.
		guards.Settle(nodes, guards.Options{HarmlessOwner: func(ast.Node) bool { return true }})
	}
	return nodes, nil
}

// addNewTables emits CREATE TABLE for every declared table the diff creates.
//
// Membership is asked by identity rather than as raw map lookup, for the same
// reason the SQLite planner's addTables does: `diff.TablesAdded` carries the
// comparator's spelling while `table.QualifiedName()` carries the declaration's,
// and a table whose two sides disagree got no CREATE TABLE at all -- no
// statement, no comment, and a plan that exits 0 having created nothing.
// addNewTables renders each created table from the creation the comparison
// carried for it, rather than walking the declaration and keeping the tables
// the diff named (stokaro/ptah#2315).
func (p *Planner) addNewTables(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	creations := diff.TablesAdded.Qualified(diff.DeclaredUserTypes, platform.ClickHouse).InDependencyOrder()
	for _, creation := range creations {
		// The captured creation carries prepared storage facets alongside the
		// common columns; the selected renderer owns their SQL spelling.
		tableNode := modelast.FromTableWithConstraints(creation.Table, creation.Fields, creation.Enums, platform.ClickHouse, creation.Constraints)
		// The table's row policies are created with it: the comparison leaves
		// the children of a created table to the table's own transition.
		tableNode.OwnedObjects = creation.OwnedObjects
		result = append(result, tableNode)
	}

	return result
}

func (p *Planner) modifyExistingTables(result []ast.Node, diff *difftypes.SchemaDiff, rebuilds tableRebuilds) ([]ast.Node, error) {
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(platform.ClickHouse))
	for _, td := range diff.TablesModified {
		if rebuilds.has(builder, td.TableName) {
			// The table is created again from its declaration, which holds
			// every column, comment and constraint this change would make.
			continue
		}
		if td.CommentChange != nil {
			result = append(result, &ast.AlterTableNode{
				Name:       td.TableName,
				Operations: []ast.AlterOperation{&ast.SetCommentOperation{Comment: td.CommentChange.Desired, HasCurrent: td.CommentChange.Current != ""}},
			})
		}
		// The column travels WITH the change, on both paths: an addition carries
		// the column it adds and a modification carries the column it renders
		// (stokaro/ptah#2315). There is no lookup left to fail.
		for _, column := range td.ColumnsAdded {
			col := modelast.FromField(column, diff.DeclaredUserTypes.Enums, platform.ClickHouse)
			result = append(result, &ast.AlterTableNode{
				Name:       td.TableName,
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: col}},
			})
		}

		for _, colDiff := range td.ColumnsModified {
			if colDiff.Desired.Name == "" {
				return nil, fmt.Errorf("%w: the diff carries no column definition for %s.%s", ptaherr.ErrInvalidSchemaDiff, td.TableName, colDiff.ColumnName)
			}
			result = appendColumnComment(result, td.TableName, colDiff)
			if colDiff.CommentChange != nil && len(colDiff.Changes) == 0 {
				continue
			}
			col := modelast.FromField(colDiff.Desired, diff.DeclaredUserTypes.Enums, platform.ClickHouse)
			// The previous default is what lets the renderer remove one: a
			// MODIFY COLUMN that names only a type keeps the default the column
			// had (stokaro/ptah#4030).
			previousDefault, defaultChanged := columnchange.PreviousDefault(colDiff)
			result = append(result, &ast.AlterTableNode{
				Name: td.TableName,
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
					Column:                col,
					PreviousType:          columnchange.PreviousType(colDiff),
					TypeNarrows:           colDiff.TypeNarrows,
					TypeNarrowingKnown:    colDiff.PreviousType != "",
					PreviousNullable:      previousColumnNullable(colDiff.Changes["nullable"]),
					HasPreviousNullable:   colDiff.Changes["nullable"] != "",
					PreviousDefault:       previousDefault,
					HasPreviousDefault:    defaultChanged,
					PreviousGeneratedKind: colDiff.PreviousGeneratedKind,
					Changed:               columnchange.Properties(colDiff),
					HasChanged:            true,
				}},
			})
		}

		for _, column := range td.ColumnsRemoved {
			result = append(result, &ast.AlterTableNode{
				Name:       td.TableName,
				Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: column.Name}},
			})
		}
	}
	return result, nil
}

func (p *Planner) addNewIndexes(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	rebuilds tableRebuilds,
) ([]ast.Node, error) {
	if len(diff.IndexesAdded) == 0 {
		return result, nil
	}
	replacements := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(platform.ClickHouse),
		diff.IndexRemovals(),
	)
	// The declaration travels WITH the addition (stokaro/ptah#2315).
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(platform.ClickHouse))
	for _, change := range diff.IndexesAdded {
		if rebuilds.has(builder, change.TableName) {
			// The rebuild creates every index the table declares.
			continue
		}
		ref := difftypes.IndexRef{Name: change.Index.Name, TableName: change.TableName}
		if replacements.Contains(ref) {
			result = append(result, ast.NewDropIndex(ref.Name).SetTable(ref.TableName).SetIfExists())
		}
		result = append(result, indexNode(change.Index, ref.TableName))
	}
	return result, nil
}

// indexNode is the ADD INDEX of a declared skipping index on a table.
func indexNode(index schemamodel.Index, tableName string) *ast.IndexNode {
	node := ast.NewIndex(index.Name, tableName, index.Fields...)
	if index.Unique {
		node.Unique = true
	}
	if index.Comment != "" {
		node.Comment = index.Comment
	}
	if index.Type != "" {
		node.Type = index.Type
	}
	// The owned skipping-index settings travel with the declaration; the
	// renderer resolves the creation defaults for any it leaves out.
	node.Facets = index.Facets
	// ClickHouse has no payload columns. The node carries them anyway, so
	// the renderer refuses the index with the message a render of the same
	// schema gives, instead of writing a skipping index without them
	// (stokaro/ptah#4112).
	node.IncludeColumns = append([]string(nil), index.IncludeColumns...)
	return node
}

func (p *Planner) removeIndexes(result []ast.Node, diff *difftypes.SchemaDiff, rebuilds tableRebuilds) []ast.Node {
	replacements := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(platform.ClickHouse),
		diff.IndexAdditions(),
	)
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(platform.ClickHouse))
	for _, ref := range diff.IndexRemovals() {
		if replacements.Contains(ref) || rebuilds.has(builder, ref.TableName) {
			// A table created again takes its indexes with the old table.
			continue
		}
		result = append(result, ast.NewDropIndex(ref.Name).SetTable(ref.TableName).SetIfExists())
	}
	return result
}

func (p *Planner) removeTables(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, name := range diff.TablesRemoved.Names() {
		drop := ast.NewDropTable(name).SetIfExists()
		if p.statements != planext.StatementStyleExact {
			drop.SetComment("WARNING: dropping table will delete all data")
		}
		result = append(result, drop)
	}
	return result
}

func previousColumnNullable(change string) bool {
	before, _, ok := strings.Cut(change, " -> ")
	return ok && strings.TrimSpace(before) == "true"
}

// appendColumnComment plans a column's comment change as a statement of its
// own. MODIFY COLUMN restates the type and the default and leaves the comment
// the column holds, so a change planned only as one never reaches the server
// and is planned again on every apply.
func appendColumnComment(result []ast.Node, table string, colDiff difftypes.ColumnDiff) []ast.Node {
	if colDiff.CommentChange == nil {
		return result
	}
	return append(result, &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.SetCommentOperation{
			Column:     colDiff.ColumnName,
			Comment:    colDiff.CommentChange.Desired,
			HasCurrent: colDiff.CommentChange.Current != "",
		}},
	})
}
