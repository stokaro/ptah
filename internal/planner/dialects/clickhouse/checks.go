package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/modelast"
	"ptah.run/internal/rundiag"
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
// So each added CHECK is scanned for: a SELECT counts the rows the table holds
// that the CHECK refuses, a NULL result among them, as an INSERT refuses one
// with code 469, and stops the plan with code 395 naming the constraint. One
// scan runs before the change and one after the ADD, for a row written between
// the two; a row written after the ADD is the constraint's to refuse. On a
// table with no added column or changed column definition, the first scan goes
// ahead of the drops, so a refused row stops the plan while the CHECK the table
// holds is still there to refuse the rows written after it. A removed column
// or a changed comment does not change the values the new condition reads.
// An added column or a changed definition may produce values the condition
// needs, so the first scan then waits for the ADD, and a refused row stops the
// plan with the table's dropped CHECK gone. The new constraint is absent when
// the scan before it fails, so a retried apply plans it and stops again. A plan
// requested without scans writes none and says so above each ADD.
//
// A table the plan creates or rebuilds takes its CHECKs in its CREATE TABLE
// ([withDeclaredChecks]), and a table the plan drops takes its CHECKs with it,
// so neither has a statement here. Only CHECK is planned: ClickHouse's other
// constraint, ASSUME, has no declaration, and the grammar has no UNIQUE or
// FOREIGN KEY.
type checkChanges struct {
	drops, additions []columnStep
}

// addedCheck is a CHECK the plan adds to a table it keeps.
type addedCheck struct {
	table, name, expression string
}

// planCheckChanges collects the CHECK constraints the comparison adds and
// removes, and the CHECK of each column the plan adds, which the column's own
// ADD COLUMN cannot carry: ClickHouse has no CHECK on a column. A planner
// asked for plans without the scans ([Planner.WithCheckScanSkipped]) plans no
// scan and raises [rundiag.UnscannedChecks] for each addition.
func (p *Planner) planCheckChanges(ctx context.Context, diff *difftypes.SchemaDiff, rebuilds tableRebuilds) checkChanges {
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(platform.ClickHouse))
	created := tableKeys(builder, diff.TablesAdded.Names())
	removed := tableKeys(builder, diff.TablesRemoved.Names())
	kept := func(table string) bool {
		if strings.TrimSpace(table) == "" {
			// A CHECK of a struct the schema declares no table for has no
			// table to alter, as on every other dialect.
			return false
		}
		key := tableID(builder, table).Key()
		return !created[key] && !removed[key] && !rebuilds.has(builder, table)
	}
	var changes checkChanges
	for _, removal := range diff.ConstraintsRemoved {
		if isCheck(removal.Type) && kept(removal.TableName) {
			changes.drops = append(changes.drops, columnStep{
				table: removal.TableName, alter: alterTable(removal.TableName, &ast.DropConstraintOperation{ConstraintName: removal.Name}),
			})
		}
	}
	columnsChange := make(map[objectidentity.Key]bool)
	var additions []addedCheck
	for _, table := range diff.TablesModified {
		if !kept(table.TableName) {
			continue
		}
		if len(table.ColumnsAdded) > 0 || slices.ContainsFunc(table.ColumnsModified, func(column difftypes.ColumnDiff) bool {
			return len(column.Changes) > 0
		}) {
			columnsChange[tableID(builder, table.TableName).Key()] = true
		}
		for _, column := range table.ColumnsAdded {
			if column.Check != "" {
				additions = append(additions, addedCheck{table: table.TableName, name: chcolumn.CheckName(column.CheckName, table.TableName, column.Name), expression: column.Check})
			}
		}
	}
	for _, addition := range diff.ConstraintsAdded {
		if isCheck(addition.Type) && addition.CheckExpression != "" && kept(addition.TableName) {
			additions = append(additions, addedCheck{table: addition.TableName, name: addition.Name, expression: addition.CheckExpression})
		}
	}
	var ahead []columnStep
	for _, added := range additions {
		add := columnStep{table: added.table, alter: alterTable(added.table, &ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
			Type: ast.CheckConstraint, Name: added.name, Expression: added.expression,
		}})}
		if p.skipCheckScan {
			warnUnscanned(ctx, added)
			add.before = []ast.Node{ast.NewComment(unscannedNote(added))}
			changes.additions = append(changes.additions, add)
			continue
		}
		// The note goes above the ADD rather than above the first scan, so
		// the statement a failed scan reports is the scan alone.
		first := ast.NewRawSQL(heldRowScan(added))
		add.before = []ast.Node{ast.NewComment(scannedNote(added))}
		if columnsChange[tableID(builder, added.table).Key()] {
			add.before = append([]ast.Node{first}, add.before...)
		} else {
			ahead = append(ahead, columnStep{table: added.table, before: []ast.Node{first}})
		}
		add.after = []ast.Node{ast.NewRawSQL(heldRowScan(added))}
		changes.additions = append(changes.additions, add)
	}
	changes.drops = append(ahead, changes.drops...)
	rundiag.Settle(ctx, rundiag.UnscannedChecks)
	return changes
}

// heldRowScan is the SELECT that stops the plan when table holds a row the
// added CHECK refuses. throwIf takes only a constant message, which is why the
// message names no count.
func heldRowScan(added addedCheck) string {
	return fmt.Sprintf("SELECT throwIf(count() > 0, %s) FROM %s WHERE NOT ifNull((%s), 0);",
		chcolumn.StringLiteral(fmt.Sprintf("%s is broken by rows %s holds", added.name, added.table)), quoteTable(added.table),
		strings.TrimSpace(added.expression))
}

// scannedNote is the comment above the ADD CONSTRAINT of a CHECK the plan
// scans for.
func scannedNote(added addedCheck) string {
	return fmt.Sprintf("CLICKHOUSE: ADD CONSTRAINT %s checks only the rows written after it; "+
		"a SELECT ahead of it and one after it scan %s for held rows the CHECK refuses", added.name, added.table)
}

// unscannedNote is the comment above an ADD CONSTRAINT planned without the
// scans.
func unscannedNote(added addedCheck) string {
	return fmt.Sprintf("CLICKHOUSE: ADD CONSTRAINT %s checks only the rows written after it, "+
		"and the plan has no scan: the rows %s already holds are not checked", added.name, added.table)
}

// warnUnscanned raises the run warning for a CHECK planned without the scans.
func warnUnscanned(ctx context.Context, added addedCheck) {
	check := added.table + "." + added.name
	_ = rundiag.Warn(ctx, rundiag.Stream(ctx), rundiag.UnscannedChecks, rundiag.Record{
		Key: check,
		Line: fmt.Sprintf("CHECK %s on %s is added without a scan of the rows %s holds: "+
			"the server checks only the rows written after ADD CONSTRAINT, so a held row the CHECK refuses stays", added.name, added.table, added.table),
		Attrs: []slog.Attr{slog.String("check", check)},
	})
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
		keys[tableID(builder, name).Key()] = true
	}
	return keys
}
