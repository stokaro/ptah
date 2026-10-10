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
	"ptah.run/internal/ydbpartition"
)

// TablePartitioningHandler renders an in-place change of a row table's
// settings as its own ALTER TABLE ... SET.
func TablePartitioningHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.AlterTablePartitioning{}, ast.AlterExtension, validateTablePartitioning, renderTablePartitioning)
}

// validateTablePartitioning refuses a change on a target other than YDB, a
// setting either side holds that the target has no key for -- a change that
// removes read replicas needs the key as one that adds them does -- a side
// YDB could not hold, and a starting layout, which YDB takes only when it
// creates a table (see [ydbpartition.TableChangeRefusal]).
func validateTablePartitioning(ctx renderer.ExtensionContext, op *ydbast.AlterTablePartitioning) error {
	subject := fmt.Sprintf("table %q", parentName(ctx))
	if platform.NormalizeDialect(ctx.Target) != platform.YDB {
		return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(ydbschema.TablePartitioningKind), Err: ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("YDB table partitioning cannot be rendered for %q", ctx.Target)}
	}
	if err := ydbdiff.ValidateTablePartitioning(&op.Change); err != nil {
		return fmt.Errorf("the settings of %s: %w", subject, err)
	}
	if err := RefuseTablePartitioningChange(ctx.Capabilities, subject, op.Change); err != nil {
		return err
	}
	if err := op.Validate(); err != nil {
		return fmt.Errorf("the settings of %s: %w", subject, err)
	}
	return nil
}

// RefuseTablePartitioningChange refuses a change of subject's settings the
// target cannot make in place: a setting either side states that the target
// has no key for, a side YDB could not hold, and a starting layout the table
// was not created with.
func RefuseTablePartitioningChange(caps capability.Capabilities, subject string, change ydbdiff.TablePartitioning) error {
	sides := []*ydbschema.TablePartitioning{&change.After.TablePartitioning}
	if change.Before != nil {
		sides = append(sides, &change.Before.TablePartitioning)
	}
	for _, side := range sides {
		for _, requirement := range ydbpartition.Requirements(side) {
			if !caps.Has(requirement.Key) {
				return refuseKey(requirement.Key, "changing the "+requirement.Settings+" of "+subject)
			}
		}
	}
	op := &ydbast.AlterTablePartitioning{Change: change}
	held, desired, err := op.Resolve()
	if err != nil {
		return refuseFact(subject, err.Error())
	}
	if reason := ydbpartition.TableChangeRefusal(&change.After.TablePartitioning, desired, held); reason != "" {
		return refuseFact(subject, reason)
	}
	return nil
}

// renderTablePartitioning writes the one ALTER TABLE ... SET that changes the
// settings; see [ydbpartition.TableClause].
func renderTablePartitioning(ctx renderer.ExtensionContext, op *ydbast.AlterTablePartitioning) ([]string, error) {
	settings, err := op.Settings()
	if err != nil {
		return nil, err
	}
	path := sqlident.Quote(platform.YDB, ydbscheme.ObjectPath(parentName(ctx)))
	return []string{fmt.Sprintf("ALTER TABLE %s SET (%s);", path, strings.Join(settings, ", "))}, nil
}

// CreateTablePartitioning writes the settings a CREATE TABLE ... WITH (...)
// carries for a row table's declared settings, each as the declaration names
// it, and its starting layout. A table without the facet writes none.
// keyTypes are the YDB types of the table's key columns, in key order.
//
// It refuses a setting the target has no key for, a declaration YDB refuses,
// with YDB's reason, and a starting layout that does not fit the table's key;
// see [ydbpartition.LayoutClause].
func CreateTablePartitioning(caps capability.Capabilities, table string, facets schemaext.Facets, keyTypes []string) ([]string, error) {
	value, found, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](facets, ydbschema.TablePartitioningKind)
	if err != nil || !found {
		return nil, err
	}
	subject := fmt.Sprintf("table %q", table)
	if err := ydbschema.ValidateDesiredTablePartitioning(value); err != nil {
		return nil, fmt.Errorf("the settings of %s: %w", subject, err)
	}
	spec := &value.TablePartitioning
	if err := RefuseTablePartitioning(caps, subject, spec); err != nil {
		return nil, err
	}
	layout, err := ydbpartition.LayoutClause(spec, keyTypes, caps)
	if err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	settings := ydbpartition.CreateClause(spec)
	if layout != "" {
		settings = append(settings, layout)
	}
	return settings, nil
}

// RefuseTablePartitioning refuses settings a CREATE TABLE of subject cannot
// write: a setting the target has no key for, and a declaration YDB refuses
// whatever the table's columns are.
func RefuseTablePartitioning(caps capability.Capabilities, subject string, spec *ydbschema.TablePartitioning) error {
	for _, requirement := range ydbpartition.Requirements(spec) {
		if !caps.Has(requirement.Key) {
			return refuseKey(requirement.Key, subject+" declares its "+requirement.Settings)
		}
	}
	if _, err := ydbpartition.ResolveTable(spec, ydbpartition.DefaultTableSettings()); err != nil {
		return refuseFact(subject, err.Error())
	}
	return nil
}
