package pgtablerender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// ExclusionHandlers returns the local ALTER TABLE handler for captured
// exclusion changes. A selected rendering service invokes it within its batch.
func ExclusionHandlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{renderer.TypedHandler(&pgtable.AlterExclusion{}, ast.AlterExtension, validateExclusion, renderExclusion)}
}

func validateExclusion(ctx renderer.ExtensionContext, op *pgtable.AlterExclusion) error {
	if err := ValidateExclusionTarget(ctx.Target); err != nil {
		return err
	}
	if err := pgtable.ValidateAlterExclusion(op); err != nil {
		return err
	}
	if op.Change.After != nil && op.Change.After.Deferrable && !ctx.Capabilities.Has(capability.DeferrableKeys) {
		return fmt.Errorf("%w: EXCLUDE constraint %q declares DEFERRABLE, which %s does not support", ptaherr.ErrUnsupportedFeature, op.Name, ctx.Target)
	}
	return nil
}

// ValidateExclusionTarget refuses targets without PostgreSQL exclusion indexes.
// Every refusal wraps ptaherr.ErrUnsupportedFeature and names EXCLUDE.
func ValidateExclusionTarget(target string) error {
	if platform.NormalizeDialect(target) != platform.Postgres {
		return fmt.Errorf("%w: EXCLUDE constraints require PostgreSQL; target is %q", ptaherr.ErrUnsupportedFeature, target)
	}
	return nil
}

// ExclusionSQL renders one CREATE TABLE element without indentation or a
// trailing comma. An empty name leaves naming to PostgreSQL. Validation is
// structural; a server decides expression and operator-class validity.
func ExclusionSQL(name string, value *pgtable.DesiredExclusion) (string, error) {
	if err := pgtable.ValidateDesiredExclusion(value); err != nil {
		return "", err
	}
	result := "EXCLUDE USING " + value.UsingMethod + " (" + value.Elements + ")"
	if name != "" {
		result = "CONSTRAINT " + sqlident.Quote(platform.Postgres, sqlident.UnquoteDoubleQuoted(name)) + " " + result
	}
	if value.WhereCondition != "" {
		result += " WHERE (" + value.WhereCondition + ")"
	}
	if value.Deferrable {
		result += " DEFERRABLE"
		if value.Initially != "" {
			result += " INITIALLY " + strings.ToUpper(value.Initially)
		}
	}
	return result, nil
}

func renderExclusion(ctx renderer.ExtensionContext, op *pgtable.AlterExclusion) ([]string, error) {
	table := sqlident.QuotePostgresQualified(ctx.Parent.Name)
	name := sqlident.Quote(platform.Postgres, sqlident.UnquoteDoubleQuoted(op.Name))
	var statements []string
	if !op.Change.CommentOnly {
		if op.Change.Before != nil {
			statements = append(statements, "ALTER TABLE "+table+" DROP CONSTRAINT "+name+";")
		}
		if op.Change.After != nil {
			clause, err := ExclusionSQL(op.Name, op.Change.After)
			if err != nil {
				return nil, err
			}
			statements = append(statements, "ALTER TABLE "+table+" ADD "+clause+";")
		}
	}
	if op.Change.After != nil && (op.Change.CommentOnly || op.Change.After.Comment != "") {
		literal := "NULL"
		if op.Change.After.Comment != "" {
			literal = sqlident.StringLiteral(op.Change.After.Comment)
		}
		statements = append(statements, "COMMENT ON CONSTRAINT "+name+" ON "+table+" IS "+literal+";")
	}
	return statements, nil
}

// CreateTableExclusionStatements adds each named child after CREATE TABLE.
// Separate additions preserve equivalent constraints with different names. It returns no output for an empty collection. Unknown
// child kinds, observations, mismatched parents and unsupported targets fail.
// A rendering service calls it locally inside its complete node batch.
func CreateTableExclusionStatements(target string, caps capability.Capabilities, table string, objects schemaext.Objects) ([]string, error) {
	var statements []string
	values, err := objects.All()
	if err != nil {
		return nil, err
	}
	for _, object := range values {
		value, ok := object.Value.(*pgtable.DesiredExclusion)
		if !ok {
			return nil, fmt.Errorf("%w: no table child renderer for %s", ptaherr.ErrUnsupportedFeature, object.Ref.Kind)
		}
		if err := ValidateExclusionTarget(target); err != nil {
			return nil, err
		}
		if err := pgtable.ValidateExclusionRef(object.Ref); err != nil {
			return nil, err
		}
		if !exclusionParentMatches(target, table, object.Ref) {
			return nil, fmt.Errorf("%w: EXCLUDE constraint names another table", schemaext.ErrInvalidValue)
		}
		operation := &pgtable.AlterExclusion{Name: object.Ref.Name.Source, Change: pgtable.ExclusionChange{After: value}}
		context := renderer.ExtensionContext{Target: target, Capabilities: caps, Parent: &ast.AlterTableNode{Name: table}}
		if err := validateExclusion(context, operation); err != nil {
			return nil, err
		}
		created, err := renderExclusion(context, operation)
		if err != nil {
			return nil, err
		}
		statements = append(statements, created...)
	}
	return statements, nil
}

func exclusionParentMatches(target, table string, ref objectidentity.ID) bool {
	parent, ok := tableref.Parse(table)
	if !ok {
		return false
	}
	component := func(part objectidentity.Part) string {
		if part.Quoted {
			return sqlident.UnquoteDoubleQuoted(part.Source)
		}
		return part.Source
	}
	builder := objectidentity.NewBuilder(identifier.ForDialect(target))
	expected := builder.TableParts(parent.Schema, parent.Name)
	actual := builder.TableParts(component(ref.Schema), component(ref.Parent))
	return expected.Key() == actual.Key()
}
