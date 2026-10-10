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
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// IndexPartitioningHandler renders an in-place change of a global index's
// partitioning as its own ALTER TABLE ... ALTER INDEX ... SET.
func IndexPartitioningHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.AlterIndexPartitioning{}, ast.AlterExtension, validateIndexPartitioning, renderIndexPartitioning)
}

func validateIndexPartitioning(ctx renderer.ExtensionContext, op *ydbast.AlterIndexPartitioning) error {
	subject := fmt.Sprintf("index %q of table %q", op.Index, parentName(ctx))
	if platform.NormalizeDialect(ctx.Target) != platform.YDB {
		return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(ydbschema.IndexPartitioningKind), Err: ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("YDB index partitioning cannot be rendered for %q", ctx.Target)}
	}
	if err := ydbdiff.ValidateIndexPartitioning(&op.Change); err != nil {
		return fmt.Errorf("the settings of %s: %w", subject, err)
	}
	if err := RefuseIndexPartitioningChange(ctx.Capabilities, subject, op); err != nil {
		return err
	}
	if err := op.Validate(); err != nil {
		return fmt.Errorf("the settings of %s: %w", subject, err)
	}
	return nil
}

// RefuseIndexPartitioningChange refuses a change of subject's settings the
// target cannot make: one on a target without [capability.IndexPartitioning],
// and one with a side YDB could not hold.
func RefuseIndexPartitioningChange(caps capability.Capabilities, subject string, op *ydbast.AlterIndexPartitioning) error {
	if !caps.Has(capability.IndexPartitioning) {
		return refuseKey(capability.IndexPartitioning, "changing the partitioning of "+subject)
	}
	if _, _, err := op.Resolve(); err != nil {
		return refuseFact(subject, err.Error())
	}
	return nil
}

func renderIndexPartitioning(ctx renderer.ExtensionContext, op *ydbast.AlterIndexPartitioning) ([]string, error) {
	settings, err := op.Settings()
	if err != nil || len(settings) == 0 {
		return nil, err
	}
	path := sqlident.Quote(platform.YDB, ydbscheme.ObjectPath(parentName(ctx)))
	return []string{fmt.Sprintf("ALTER TABLE %s ALTER INDEX %s SET (%s);", path, sqlident.Quote(platform.YDB, op.Index),
		strings.Join(settings, ", "))}, nil
}

// DeclaredIndexPartitioning is the settings an index's facets declare, or nil
// for none.
func DeclaredIndexPartitioning(facets schemaext.Facets) (*ydbschema.IndexPartitioning, error) {
	value, found, err := schemaext.FacetAs[*ydbschema.DesiredIndexPartitioning](facets, ydbschema.IndexPartitioningKind)
	if err != nil || !found {
		return nil, err
	}
	if err := ydbschema.ValidateDesiredIndexPartitioning(value); err != nil {
		return nil, err
	}
	return &value.IndexPartitioning, nil
}

// CreateIndexPartitioning writes the settings a new index's declared
// partitioning names, which a statement after the index's creation sets,
// refusing it on a target without [capability.IndexPartitioning] and refusing
// what YDB would refuse whatever the index holds. An index without the facet
// writes none.
func CreateIndexPartitioning(caps capability.Capabilities, subject string, facets schemaext.Facets) ([]string, error) {
	spec, err := DeclaredIndexPartitioning(facets)
	if err != nil {
		return nil, fmt.Errorf("the settings of %s: %w", subject, err)
	}
	if spec == nil || spec.IsZero() {
		return nil, nil
	}
	if !caps.Has(capability.IndexPartitioning) {
		return nil, refuseKey(capability.IndexPartitioning, subject+" declares its partitioning")
	}
	if _, err := ydbindex.Resolve(spec, ydbpartition.DefaultSettings()); err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	return ydbindex.CreateClause(spec), nil
}
