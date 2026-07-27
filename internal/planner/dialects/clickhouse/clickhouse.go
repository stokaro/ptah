// Package clickhouse implements ClickHouse-specific migration planning.
//
// ClickHouse only honors a subset of the schema features expressible
// through Ptah's annotations: tables, columns and a narrow set of
// constraints (CHECK only). Enums, custom types, extensions, functions,
// row-level security policies and roles are PostgreSQL-shaped and have no
// direct equivalent here, so this planner deliberately drops them from
// the output rather than emitting unrunnable SQL.
//
// The renderer is the second line of defense: any AST node this planner
// did emit that ClickHouse cannot express is rendered as a
// `-- CLICKHOUSE: ... is not supported` comment. Keeping both layers
// honest means the planner stays free to emit dialect-neutral nodes
// without needing to know every detail of ClickHouse's syntax.
package clickhouse

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/convert/fromschema"
	"github.com/stokaro/ptah/internal/indexscope"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// Planner implements the migration planner interface for ClickHouse.
type Planner struct{}

// New returns a ClickHouse planner.
func New() *Planner { return &Planner{} }

// GenerateMigrationASTChecked produces the AST node sequence that, when rendered
// against the ClickHouse renderer, brings the database from its current
// state (described by diff) to the target schema (described by generated).
//
// The output is ordered to satisfy ClickHouse's constraint that tables must
// be created before any subsequent ALTER references them:
//
//  1. CREATE TABLE for every newly-added table.
//  2. ALTER TABLE for every per-table column add/modify/drop.
//  3. ADD INDEX for new data-skipping indexes.
//  4. DROP INDEX for removed indexes.
//  5. DROP TABLE for removed tables.
//
// Diff fields concerning PostgreSQL-only constructs (enums, extensions,
// functions, RLS, roles) are intentionally ignored; the renderer would
// reduce them to comments anyway, and emitting nothing keeps the output
// migration small.
func (p *Planner) GenerateMigrationASTChecked(diff *types.SchemaDiff, generated *goschema.Database) ([]ast.Node, error) {
	var result []ast.Node

	if generated == nil {
		generated = &goschema.Database{}
	}
	indexes, err := indexscope.NewResolverWithSemantics(
		platform.ClickHouse,
		diff.EffectiveIdentifierSemantics(platform.ClickHouse),
		diff,
		generated,
	)
	if err != nil {
		return nil, err
	}

	if len(diff.EnumsAdded)+len(diff.EnumsRemoved)+len(diff.EnumsModified) > 0 {
		result = append(result, ast.NewComment("CLICKHOUSE: enum changes are ignored; declare ClickHouse Enum8/Enum16 columns inline via platform.clickhouse.type"))
	}

	result = p.addNewTables(result, diff, generated)
	result = p.modifyExistingTables(result, diff, generated)
	result, err = p.addNewIndexes(result, diff, indexes)
	if err != nil {
		return nil, err
	}
	result = p.removeIndexes(result, diff)
	result = p.removeTables(result, diff)

	return result, nil
}

func (p *Planner) addNewTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	added := make(map[string]struct{}, len(diff.TablesAdded))
	for _, name := range diff.TablesAdded {
		added[name] = struct{}{}
	}
	if len(added) == 0 {
		return result
	}

	for _, table := range generated.Tables {
		if _, ok := added[table.Name]; !ok {
			continue
		}
		// FromTable applies platform.clickhouse.* overrides into the AST
		// node's Options map (uppercased), which the renderer then reads
		// to build the ENGINE clause.
		tableNode := fromschema.FromTable(table, generated.Fields, generated.Enums, platform.ClickHouse)
		result = append(result, tableNode)
	}

	return result
}

func (p *Planner) modifyExistingTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, td := range diff.TablesModified {
		structName := lookupStructName(generated, td.TableName)
		if structName == "" {
			result = append(result, ast.NewComment(fmt.Sprintf("WARNING: ClickHouse planner could not find struct for table %s; skipping modifications", td.TableName)))
			continue
		}

		for _, colName := range td.ColumnsAdded {
			field := lookupField(generated, structName, colName)
			if field == nil {
				result = append(result, ast.NewComment(fmt.Sprintf("WARNING: ClickHouse planner could not find field %s.%s; skipping ADD COLUMN", td.TableName, colName)))
				continue
			}
			col := fromschema.FromField(*field, generated.Enums, platform.ClickHouse)
			result = append(result, &ast.AlterTableNode{
				Name:       td.TableName,
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: col}},
			})
		}

		for _, colDiff := range td.ColumnsModified {
			field := lookupField(generated, structName, colDiff.ColumnName)
			if field == nil {
				result = append(result, ast.NewComment(fmt.Sprintf("WARNING: ClickHouse planner could not find field %s.%s; skipping MODIFY COLUMN", td.TableName, colDiff.ColumnName)))
				continue
			}
			col := fromschema.FromField(*field, generated.Enums, platform.ClickHouse)
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

		for _, colName := range td.ColumnsRemoved {
			result = append(result, &ast.AlterTableNode{
				Name:       td.TableName,
				Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: colName}},
			})
		}
	}
	return result
}

func (p *Planner) addNewIndexes(
	result []ast.Node,
	diff *types.SchemaDiff,
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

func (p *Planner) removeIndexes(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
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

func (p *Planner) removeTables(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.TablesRemoved {
		result = append(result, ast.NewDropTable(name).SetIfExists().SetComment("WARNING: dropping table will delete all data"))
	}
	return result
}

func lookupStructName(generated *goschema.Database, tableName string) string {
	for _, t := range generated.Tables {
		if t.Name == tableName {
			return t.StructName
		}
	}
	return ""
}

func lookupField(generated *goschema.Database, structName, columnName string) *goschema.Field {
	for i := range generated.Fields {
		if generated.Fields[i].StructName == structName && generated.Fields[i].Name == columnName {
			return &generated.Fields[i]
		}
	}
	return nil
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
