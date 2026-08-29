// Package clickhouse implements ClickHouse-specific migration planning.
//
// ClickHouse only honors a subset of the schema features expressible through
// Ptah's annotations: tables, columns, plain views, roles and grants, and a
// narrow set of constraints (CHECK only). Enums, custom types, extensions,
// functions and row-level security policies are PostgreSQL-shaped and have no
// direct equivalent here, so this planner emits no runnable SQL for them.
//
// Roles and grants used to be in that list and are not any more
// (stokaro/ptah#1025). ClickHouse's access control is its own shape rather than
// PostgreSQL's with different keywords -- a role carries no attributes at all,
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
// The renderer is therefore both the second line of defense and the single
// place that decides what ClickHouse can express: the planner stays free to
// emit dialect-neutral nodes without needing to know every detail of
// ClickHouse's syntax.
package clickhouse

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// Planner implements the migration planner interface for ClickHouse.
type Planner struct {
	caps capability.Capabilities
}

// New returns a ClickHouse planner using the default ClickHouse 24 preset.
func New() *Planner { return NewWithCapabilities(capability.ClickHouse24()) }

// NewWithCapabilities returns a ClickHouse planner for a concrete server
// capability set. The set is cloned so later caller mutations cannot change a
// plan already being assembled. A nil set is conservative; New explicitly
// selects the default ClickHouse 24 preset.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return &Planner{caps: caps.Clone()}
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
// The declaration is no longer read: every fact this planner needs travels
// on the diff. The parameter stays until it leaves the Planner interface,
// which is one mechanical sweep over 257 call sites (stokaro/ptah#2315).
func (p *Planner) GenerateMigrationAST(diff *difftypes.SchemaDiff, _ *schemamodel.Database) ([]ast.Node, error) {
	var result []ast.Node

	indexes, err := indexscope.NewResolverWithSemantics(
		platform.ClickHouse,
		diff.EffectiveIdentifierSemantics(platform.ClickHouse),
		diff,
	)
	if err != nil {
		return nil, err
	}

	if len(diff.EnumsAdded)+len(diff.EnumsRemoved)+len(diff.EnumsModified) > 0 {
		result = append(result, ast.NewComment("CLICKHOUSE: enum changes are ignored; declare ClickHouse Enum8/Enum16 columns inline via platform.clickhouse.type"))
	}

	result = reportUnsupportedObjectsBeforeTables(result, diff)
	result = p.addNewTables(result, diff)
	result = p.modifyExistingTables(result, diff)
	result, err = planObjectsAfterTables(result, diff, p.capabilities())
	if err != nil {
		return nil, err
	}
	result, err = p.addNewIndexes(result, diff, indexes)
	if err != nil {
		return nil, err
	}
	result = p.removeIndexes(result, diff)
	// Row policies go before the tables they name, so a drop never names an
	// object that is already gone.
	result = removeRowPolicies(result, diff, p.capabilities())

	result = p.removeTables(result, diff)

	return result, nil
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
		// FromTable applies platform.clickhouse.* overrides into the AST
		// node's Options map (uppercased), which the renderer then reads
		// to build the ENGINE clause.
		tableNode := fromschema.FromTable(creation.Table, creation.Fields, creation.Enums, platform.ClickHouse)
		result = append(result, tableNode)
	}

	return result
}

func (p *Planner) modifyExistingTables(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(platform.ClickHouse)
	for _, td := range diff.TablesModified {
		structName := lookupStructName(diff.DeclaredTables, td.TableName, semantics)
		if structName == "" {
			result = append(result, ast.NewComment(fmt.Sprintf("WARNING: ClickHouse planner could not find struct for table %s; skipping modifications", td.TableName)))
			continue
		}

		// The column travels WITH the change, on both paths: an addition carries
		// the column it adds and a modification carries the column it renders
		// (stokaro/ptah#2315). There is no lookup left to fail.
		for _, column := range td.ColumnsAdded {
			col := fromschema.FromField(column, diff.DeclaredUserTypes.Enums, platform.ClickHouse)
			result = append(result, &ast.AlterTableNode{
				Name:       td.TableName,
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: col}},
			})
		}

		for _, colDiff := range td.ColumnsModified {
			if colDiff.Desired.Name == "" {
				result = append(result, ast.NewComment(fmt.Sprintf("WARNING: the diff carries no column definition for %s.%s; skipping MODIFY COLUMN", td.TableName, colDiff.ColumnName)))
				continue
			}
			col := fromschema.FromField(colDiff.Desired, diff.DeclaredUserTypes.Enums, platform.ClickHouse)
			result = append(result, &ast.AlterTableNode{
				Name: td.TableName,
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
					Column:              col,
					PreviousType:        previousColumnType(colDiff.Changes["type"]),
					PreviousNullable:    previousColumnNullable(colDiff.Changes["nullable"]),
					HasPreviousNullable: colDiff.Changes["nullable"] != "",
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
	return result
}

func (p *Planner) addNewIndexes(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	indexes *indexscope.Resolver,
) ([]ast.Node, error) {
	if len(diff.IndexesAdded) == 0 {
		return result, nil
	}
	replacements := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(platform.ClickHouse),
		diff.IndexRemovals(),
	)
	for _, ref := range diff.IndexAdditions() {
		index, err := indexes.Resolve(ref)
		if err != nil {
			return nil, err
		}
		tableName := ref.TableName
		if replacements.Contains(ref) {
			result = append(result, ast.NewDropIndex(ref.Name).SetTable(tableName).SetIfExists())
		}
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
		node.Granularity = index.Granularity
		result = append(result, node)
	}
	return result, nil
}

func (p *Planner) removeIndexes(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	replacements := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(platform.ClickHouse),
		diff.IndexAdditions(),
	)
	for _, ref := range diff.IndexRemovals() {
		if replacements.Contains(ref) {
			continue
		}
		result = append(result, ast.NewDropIndex(ref.Name).SetTable(ref.TableName).SetIfExists())
	}
	return result
}

func (p *Planner) removeTables(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, name := range diff.TablesRemoved {
		result = append(result, ast.NewDropTable(name).SetIfExists().SetComment("WARNING: dropping table will delete all data"))
	}
	return result
}

func lookupStructName(
	declared []schemamodel.Table,
	tableName string,
	semantics identifier.Semantics,
) string {
	table := objectlookup.Qualified(declared, tableName, semantics)
	if table == nil {
		return ""
	}
	return table.StructName
}

func previousColumnType(change string) string {
	before, _, ok := strings.Cut(change, " -> ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(before)
}

func previousColumnNullable(change string) bool {
	before, _, ok := strings.Cut(change, " -> ")
	return ok && strings.TrimSpace(before) == "true"
}
