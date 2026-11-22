package clickhouse

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/modelast"
)

// checkChanges are the CHECK constraint statements of the tables a plan keeps.
//
// ClickHouse checks each row an INSERT writes against the CHECKs of the table,
// and ALTER TABLE ... ADD CONSTRAINT checks none of the rows the table already
// holds, measured on 24.10 and 26.10. The server refuses a DROP COLUMN of a
// column a CHECK reads and an ADD CONSTRAINT over a column the table does not
// have, both with code 47, so the drops go before the column changes of the
// plan and the additions after them.
//
// A table the plan creates or rebuilds takes its CHECKs in its CREATE TABLE
// ([withDeclaredChecks]), and a table the plan drops takes its CHECKs with it,
// so neither has a statement here. Only CHECK is planned: ClickHouse's other
// constraint, ASSUME, has no declaration, and the grammar has no UNIQUE or
// FOREIGN KEY.
type checkChanges struct {
	drops, additions []ast.Node
}

// planCheckChanges collects the CHECK constraints the comparison adds and
// removes, and the CHECK of each column the plan adds, which the column's own
// ADD COLUMN cannot carry: ClickHouse has no CHECK on a column.
func planCheckChanges(diff *difftypes.SchemaDiff, rebuilds tableRebuilds) checkChanges {
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(platform.ClickHouse))
	created := tableKeys(builder, diff.TablesAdded.Names())
	removed := tableKeys(builder, diff.TablesRemoved.Names())
	kept := func(table string) bool {
		if strings.TrimSpace(table) == "" {
			// A CHECK of a struct the schema declares no table for has no
			// table to alter, as on every other dialect.
			return false
		}
		key := builder.Table(table).Key()
		return !created[key] && !removed[key] && !rebuilds.has(builder, table)
	}
	var changes checkChanges
	for _, removal := range diff.ConstraintsRemoved {
		if isCheck(removal.Type) && kept(removal.TableName) {
			changes.drops = append(changes.drops, alterTable(removal.TableName, &ast.DropConstraintOperation{ConstraintName: removal.Name}))
		}
	}
	for _, table := range diff.TablesModified {
		if !kept(table.TableName) {
			continue
		}
		for _, column := range table.ColumnsAdded {
			if column.Check == "" {
				continue
			}
			changes.additions = append(changes.additions, alterTable(table.TableName, &ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
				Type: ast.CheckConstraint, Name: chcolumn.CheckName(column.CheckName, table.TableName, column.Name), Expression: column.Check,
			}}))
		}
	}
	for _, addition := range diff.ConstraintsAdded {
		if isCheck(addition.Type) && addition.CheckExpression != "" && kept(addition.TableName) {
			changes.additions = append(changes.additions, alterTable(addition.TableName, &ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
				Type: ast.CheckConstraint, Name: addition.Name, Expression: addition.CheckExpression,
			}}))
		}
	}
	return changes
}

// withDeclaredChecks adds to a CREATE TABLE the CHECK constraints the table
// declares on its own, which [modelast.FromTableWithConstraints] leaves to the
// planner. A table created in one statement holds them from its first row.
func withDeclaredChecks(create *ast.CreateTableNode, constraints []schemamodel.Constraint) {
	for _, constraint := range constraints {
		if isCheck(constraint.Type) {
			create.AddConstraint(modelast.FromConstraint(constraint))
		}
	}
}

func isCheck(constraintType string) bool {
	return strings.EqualFold(strings.TrimSpace(constraintType), "CHECK")
}

func alterTable(table string, operation ast.AlterOperation) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{operation}}
}

func tableKeys(builder objectidentity.Builder, names []string) map[objectidentity.Key]bool {
	keys := make(map[objectidentity.Key]bool, len(names))
	for _, name := range names {
		keys[builder.Table(name).Key()] = true
	}
	return keys
}
