// Package chrender validates and renders ClickHouse-owned extension payloads.
// Callers select these handlers explicitly; no global registry is installed.
package chrender

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chsql"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/chkey"
)

// Handlers returns independent descriptors for the supported operation roles.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&chast.AlterStorage{}, ast.AlterExtension, validateStorage, renderStorage),
		renderer.TypedHandler(&chast.AddSkippingIndex{}, ast.AlterExtension, validateIndex, renderIndex),
		renderer.TypedHandler(&chast.DropSkippingIndex{}, ast.AlterExtension, validateDropIndex, renderDropIndex),
		renderer.TypedHandler(&chast.ModifyRefresh{}, ast.AlterExtension, validateRefresh, renderRefresh),
		renderer.TypedHandler(&chast.RowPolicy{}, ast.StatementExtension, validateRowPolicy, renderRowPolicy),
		renderer.TypedHandler(&chast.RowPolicyCommand{}, ast.StatementExtension, validateRowPolicyCommand, renderRowPolicyCommand),
	}
}

// Registry creates a local handler registry. Unknown kinds and roles fail
// closed; an ALTER payload requires its parent before SQL can be rendered.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

func validateStorage(ctx renderer.ExtensionContext, op *chast.AlterStorage) error {
	if ctx.Target != platform.ClickHouse {
		return fmt.Errorf("%w: ClickHouse storage operation on %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	return op.Validate()
}

// renderStorage writes one statement per changed clause. Settings the change
// sets come first and settings it resets come last, so a setting that permits
// another change is in place before it, and one that stops permitting it is
// taken away after.
//
// The sorting key and the columns its new expressions use share one ALTER
// TABLE, which ClickHouse requires. Every other clause has a statement of its
// own: measured on 26.9, a MODIFY TTL followed by a comma reads what follows
// as more of the rule, and MODIFY SETTING followed by a comma reads RESET as
// a setting name.
func renderStorage(ctx renderer.ExtensionContext, op *chast.AlterStorage) ([]string, error) {
	table := quoteTable(ctx.Parent.Name)
	after, err := op.Change.After.Observed()
	if err != nil {
		return nil, err
	}
	changed := chsql.Changed(op.Change.Before, after)
	if err := refuseStorageNot(ctx.Parent.Name, op, after, changed); err != nil {
		return nil, err
	}
	var set []chsql.Setting
	var reset []string
	if slices.Contains(changed, chsql.Settings) {
		if set, reset, err = chsql.SettingsChange(op.Change.Before.Settings, after.Settings); err != nil {
			return nil, err
		}
	}
	var statements []string
	if len(set) > 0 {
		entries := make([]string, len(set))
		for i, setting := range set {
			entries[i] = setting.Name + " = " + setting.Value
		}
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s MODIFY SETTING %s;", table, strings.Join(entries, ", ")))
	}
	if slices.Contains(changed, chsql.OrderBy) {
		var clauses []string
		for _, column := range op.Columns {
			clauses = append(clauses, "ADD COLUMN "+chcolumn.Definition(column.Name, column.Type, column.Value, column.Expression, column.Comment))
		}
		clauses = append(clauses, "MODIFY ORDER BY "+chsql.KeyExpression(after.OrderBy))
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s %s;", table, strings.Join(clauses, ", ")))
	}
	if slices.Contains(changed, chsql.SampleBy) {
		clause := "REMOVE SAMPLE BY"
		if strings.TrimSpace(after.SampleBy) != "" {
			clause = "MODIFY SAMPLE BY " + after.SampleBy
		}
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s %s;", table, clause))
	}
	if slices.Contains(changed, chsql.TTL) {
		clause := "REMOVE TTL"
		if after.TTL != "" {
			clause = "MODIFY TTL " + after.TTL
		}
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s %s;", table, clause))
	}
	if len(reset) > 0 {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s RESET SETTING %s;", table, strings.Join(reset, ", ")))
	}
	return statements, nil
}

// refuseStorageNot refuses a changed clause, or a column added for a new
// sorting key, that names a column called not, which the server stores in a
// form it cannot read back; see [chkey.QuotedNot]. Only a changed clause is
// read, because only a changed clause is written: a TTL change leaves the
// sorting key as the server holds it. Every clause has its text here, the
// engine, PARTITION BY and PRIMARY KEY too, although validation refuses a
// change to those three before rendering, so the set read is the set
// [chsql.Changed] reports.
func refuseStorageNot(table string, op *chast.AlterStorage, after *chschema.ObservedTable, changed []chsql.Clause) error {
	text := map[chsql.Clause]string{
		chsql.Engine: after.Engine, chsql.PartitionBy: after.PartitionBy, chsql.PrimaryKey: after.PrimaryKey,
		chsql.OrderBy: after.OrderBy, chsql.SampleBy: after.SampleBy, chsql.TTL: after.TTL,
	}
	for _, clause := range changed {
		if err := chkey.RefuseQuotedNot(fmt.Sprintf("the %s of table %q", clause, table), text[clause]); err != nil {
			return err
		}
	}
	for _, column := range op.Columns {
		if err := chkey.RefuseQuotedNot(fmt.Sprintf("the %s expression of column %q", column.Value, column.Name), column.Expression); err != nil {
			return err
		}
	}
	return nil
}
