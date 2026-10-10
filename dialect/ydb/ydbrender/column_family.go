package ydbrender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbfamily"
)

// ColumnFamiliesHandler renders an in-place change of a row table's column
// families as its own ALTER TABLE.
func ColumnFamiliesHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.AlterColumnFamilies{}, ast.AlterExtension, validateColumnFamilies, renderColumnFamilies)
}

// validateColumnFamilies refuses a change on a target other than YDB, an
// action the target has no key for (see [ydbfamily.ChangeRequirements]), and a
// keep_in_memory no statement writes (see [ydbfamily.ChangeRefusal]).
func validateColumnFamilies(ctx renderer.ExtensionContext, op *ydbast.AlterColumnFamilies) error {
	subject := fmt.Sprintf("table %q", parentName(ctx))
	if platform.NormalizeDialect(ctx.Target) != platform.YDB {
		return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(ydbschema.ColumnFamiliesKind), Err: ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("YDB column families cannot be rendered for %q", ctx.Target)}
	}
	if err := ydbdiff.ValidateColumnFamilies(&op.Change); err != nil {
		return fmt.Errorf("the column families of %s: %w", subject, err)
	}
	before := heldFamilies(op)
	for _, requirement := range ydbfamily.ChangeRequirements(op.Change.After.Families, before) {
		if !ctx.Capabilities.Has(requirement.Key) {
			return refuseKey(requirement.Key, fmt.Sprintf("changing the %s of %s", requirement.Settings, subject))
		}
	}
	if reason := ydbfamily.ChangeRefusal(op.Change.After.Families, before); reason != "" {
		return refuseFact(subject, reason)
	}
	if err := op.Validate(); err != nil {
		return fmt.Errorf("the column families of %s: %w", subject, err)
	}
	return nil
}

// renderColumnFamilies writes the one ALTER TABLE that changes the families:
// the families it adds, the settings it states and the columns it moves, as
// [ydbfamily.AlterActions] lists them. YDB applies the actions of one
// statement together.
func renderColumnFamilies(ctx renderer.ExtensionContext, op *ydbast.AlterColumnFamilies) ([]string, error) {
	prefix := "ALTER TABLE " + sqlident.Quote(platform.YDB, ydbscheme.ObjectPath(parentName(ctx))) + " "
	return []string{prefix + strings.Join(op.Actions(), ", ") + ";"}, nil
}

func heldFamilies(op *ydbast.AlterColumnFamilies) []ydbschema.ColumnFamily {
	if op.Change.Before == nil {
		return nil
	}
	return op.Change.Before.Families
}

// CreateTableFamilies are the families a CREATE TABLE writes for a row
// table's declared column families: where each column sits, and the
// `FAMILY <name> (...)` entries that declare them. A table without the facet
// writes none. columns are the table's columns in order and key its key.
//
// It refuses a family the target has no key for, a declaration YDB refuses
// (a column in no column the table declares, a key column outside the default
// family; see [ydbfamily.Refusal]) and a keep_in_memory no CREATE TABLE writes
// (see [ydbfamily.CreateRefusal]).
func CreateTableFamilies(caps capability.Capabilities, table string, facets schemaext.Facets, columns, key []string) (CreateFamilies, error) {
	value, found, err := schemaext.FacetAs[*ydbschema.DesiredColumnFamilies](facets, ydbschema.ColumnFamiliesKind)
	if err != nil || !found {
		return CreateFamilies{}, err
	}
	subject := fmt.Sprintf("table %q", table)
	if err := ydbschema.ValidateDesiredColumnFamilies(value); err != nil {
		return CreateFamilies{}, fmt.Errorf("the column families of %s: %w", subject, err)
	}
	if err := RefuseFamilies(caps, subject, value.Families, columns, key); err != nil {
		return CreateFamilies{}, err
	}
	return CreateFamilies{families: value.Families, Entries: ydbfamily.CreateEntries(value.Families)}, nil
}

// RefuseFamilies refuses families a CREATE TABLE of subject cannot write: a
// family the target has no key for, a declaration YDB refuses, read against
// the table's columns and key, and a family that keeps its columns in memory.
func RefuseFamilies(caps capability.Capabilities, subject string, families []ydbschema.ColumnFamily, columns, key []string) error {
	for _, requirement := range ydbfamily.Requirements(families) {
		if !caps.Has(requirement.Key) {
			return refuseKey(requirement.Key, fmt.Sprintf("the %s of %s", requirement.Settings, subject))
		}
	}
	if reason := ydbfamily.Refusal(families, columns, key); reason != "" {
		return refuseFact(subject, reason)
	}
	if reason := ydbfamily.CreateRefusal(families); reason != "" {
		return refuseFact(subject, reason)
	}
	return nil
}

// CreateFamilies is what a CREATE TABLE writes for a table's families.
type CreateFamilies struct {
	families []ydbschema.ColumnFamily
	// Entries are the `FAMILY <name> (...)` entries, after the key.
	Entries []string
}

// ColumnClause is the clause column's definition carries: ` FAMILY <name>`,
// or nothing for a column in the default family.
func (f CreateFamilies) ColumnClause(column string) string {
	return ydbfamily.ColumnClause(ydbfamily.FamilyOf(f.families, column))
}
