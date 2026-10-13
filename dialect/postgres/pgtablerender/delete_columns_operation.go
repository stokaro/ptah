package pgtablerender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/sqlident"
)

// DeleteColumnsHandlers returns the foreign-key selection handler shared by
// PostgreSQL and YugabyteDB. Server capabilities still govern its use.
func DeleteColumnsHandlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{renderer.TypedHandler(&pgtable.ReplaceDeleteColumns{}, ast.AlterExtension, validateDeleteColumnsOperation, renderDeleteColumnsOperation)}
}
func validateDeleteColumnsOperation(ctx renderer.ExtensionContext, op *pgtable.ReplaceDeleteColumns) error {
	if err := pgtable.ValidateReplaceDeleteColumns(op); err != nil {
		return err
	}
	if ctx.Target != platform.Postgres && ctx.Target != platform.YugabyteDB {
		return fmt.Errorf("%w: ON DELETE column-list replacement requires PostgreSQL support", ptaherr.ErrUnsupportedFeature)
	}
	requirements := []struct {
		needed  bool
		feature capability.Capability
	}{
		{true, capability.ForeignKeys}, {true, capability.ForeignKeyDeleteColumnList},
		{op.Deferrable, capability.DeferrableKeys}, {op.NotEnforced, capability.NotEnforcedForeignKeys},
		{op.NotValid, capability.AddConstraintNotValid}, {op.Comment != "", capability.ConstraintComments},
		{op.Match == "FULL", capability.ForeignKeyMatchFull}, {op.Match == "PARTIAL", capability.ForeignKeyMatchPartial},
	}
	for _, requirement := range requirements {
		if requirement.needed && !ctx.Capabilities.Has(requirement.feature) {
			return fmt.Errorf("%w: foreign-key replacement requires %s on %s", ptaherr.ErrUnsupportedFeature, requirement.feature, ctx.Target)
		}
	}
	return nil
}
func renderDeleteColumnsOperation(ctx renderer.ExtensionContext, op *pgtable.ReplaceDeleteColumns) ([]string, error) {
	table := sqlident.QuotePostgresQualified(ctx.Parent.Name)
	name := sqlident.Quote(platform.Postgres, op.Name)
	clause := "CONSTRAINT " + name + " FOREIGN KEY (" + deleteColumnNames(op.Columns) + ") REFERENCES " + sqlident.QuotePostgresQualified(op.ForeignTable) + " (" + deleteColumnNames(op.ForeignColumns) + ")"
	if op.Match != "" {
		clause += " MATCH " + op.Match
	}
	if op.OnDelete != "" {
		clause += " ON DELETE " + op.OnDelete
	}
	if len(op.DeleteColumns) > 0 {
		clause += " (" + deleteColumnNames(op.DeleteColumns) + ")"
	}
	if op.OnUpdate != "" {
		clause += " ON UPDATE " + op.OnUpdate
	}
	if op.Deferrable {
		clause += " DEFERRABLE"
		if op.Initially != "" {
			clause += " INITIALLY " + strings.ToUpper(op.Initially)
		}
	}
	if op.NotEnforced {
		clause += " NOT ENFORCED"
	}
	if op.NotValid {
		clause += " NOT VALID"
	}
	statements := []string{"ALTER TABLE " + table + " DROP CONSTRAINT " + name + ", ADD " + clause + ";"}
	if op.Comment != "" {
		statements = append(statements, "COMMENT ON CONSTRAINT "+name+" ON "+table+" IS "+sqlident.StringLiteral(op.Comment)+";")
	}
	return statements, nil
}
func deleteColumnNames(columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = sqlident.Quote(platform.Postgres, column)
	}
	return strings.Join(quoted, ", ")
}
