package ydbrender

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbindex"
)

// ValidateIndexFacets checks the index values the YDB renderer consumes: a
// vector index's declared settings. An empty collection is valid. Any other
// active kind wraps ptaherr.ErrUnsupportedFeature, and a malformed
// declaration wraps schemaext.ErrInvalidValue.
func ValidateIndexFacets(facets schemaext.Facets) error {
	_, err := VectorIndexDeclaration(facets)
	return err
}

// VectorIndexDeclaration returns the vector settings an index declares, or
// nil for an index that declares none. Any other active kind is refused.
func VectorIndexDeclaration(facets schemaext.Facets) (*ydbschema.DesiredVectorIndex, error) {
	for _, kind := range facets.Kinds() {
		if kind != ydbschema.VectorIndexKind {
			return nil, fmt.Errorf("%w: YDB index facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, _, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](facets, ydbschema.VectorIndexKind)
	if err != nil {
		return nil, err
	}
	if value != nil {
		if err := ydbschema.ValidateDesiredVectorIndex(value); err != nil {
			return nil, err
		}
	}
	return value, nil
}

// AddVectorIndexHandler renders the creation of a vector index a settings
// change builds again.
func AddVectorIndexHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.AddVectorIndex{}, ast.AlterExtension, validateAddVectorIndex, renderAddVectorIndex)
}

// DropVectorIndexHandler renders the removal of a vector index a settings
// change builds again.
func DropVectorIndexHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.DropVectorIndex{}, ast.AlterExtension, validateDropVectorIndex, renderDropVectorIndex)
}

// validateAddVectorIndex refuses the operation on a target other than YDB, on
// a target without [capability.VectorIndexes], and over bit vectors on a
// target without [capability.VectorBitType].
func validateAddVectorIndex(ctx renderer.ExtensionContext, op *ydbast.AddVectorIndex) error {
	subject := fmt.Sprintf("index %q", op.Name)
	if err := vectorTarget(ctx); err != nil {
		return err
	}
	if !ctx.Capabilities.Has(capability.VectorIndexes) {
		return refuseKey(capability.VectorIndexes, subject+" is a vector index")
	}
	if op.Settings.VectorType == ydbindex.BitVectorType && !ctx.Capabilities.Has(capability.VectorBitType) {
		return refuseKey(capability.VectorBitType, subject+" stores bit vectors")
	}
	return op.Validate()
}

func renderAddVectorIndex(ctx renderer.ExtensionContext, op *ydbast.AddVectorIndex) ([]string, error) {
	return []string{fmt.Sprintf("ALTER TABLE %s ADD INDEX %s %s;", vectorTable(ctx), quoteIdentifier(op.Name), op.Clause(quoteIdentifier))}, nil
}

func validateDropVectorIndex(ctx renderer.ExtensionContext, op *ydbast.DropVectorIndex) error {
	if err := vectorTarget(ctx); err != nil {
		return err
	}
	return op.Validate()
}

func renderDropVectorIndex(ctx renderer.ExtensionContext, op *ydbast.DropVectorIndex) ([]string, error) {
	return []string{fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", vectorTable(ctx), quoteIdentifier(op.Name))}, nil
}

func vectorTarget(ctx renderer.ExtensionContext) error {
	if platform.NormalizeDialect(ctx.Target) != platform.YDB {
		return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(ydbschema.VectorIndexKind), Err: ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("YDB vector indexes cannot be rendered for %q", ctx.Target)}
	}
	return nil
}

func vectorTable(ctx renderer.ExtensionContext) string {
	return sqlident.Quote(platform.YDB, ydbscheme.ObjectPath(parentName(ctx)))
}

func quoteIdentifier(name string) string { return sqlident.Quote(platform.YDB, name) }
