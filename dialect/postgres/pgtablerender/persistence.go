// Package pgtablerender renders explicitly selected PostgreSQL table features.
package pgtablerender

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/sqlident"
)

// Handlers returns the owned persistence and exclusion operation handlers.
func Handlers() []renderer.ExtensionHandler {
	return append(ExclusionHandlers(), renderer.TypedHandler(&pgtable.AlterPersistence{}, ast.AlterExtension, validatePersistence, renderPersistence))
}

func validatePersistence(ctx renderer.ExtensionContext, op *pgtable.AlterPersistence) error {
	if !pgtable.SupportsPersistence(ctx.Target) {
		return fmt.Errorf("%w: UNLOGGED tables require PostgreSQL, not %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	return pgtable.ValidateAlterPersistence(op)
}

func renderPersistence(ctx renderer.ExtensionContext, op *pgtable.AlterPersistence) ([]string, error) {
	keyword := "LOGGED"
	if op.Change.After != nil && op.Change.After.Unlogged {
		keyword = "UNLOGGED"
	}
	return []string{"ALTER TABLE " + sqlident.QuotePostgresQualified(ctx.Parent.Name) + " SET " + keyword + ";"}, nil
}

// CreateTablePrefix renders declared persistence before TABLE and returns the
// facets for the remaining owners. It does not mutate facets. Missing persistence
// returns no prefix. Observations, invalid values and unsupported targets fail.
func CreateTablePrefix(target string, _ capability.Capabilities, _ string, facets schemaext.Facets) (string, schemaext.Facets, error) {
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPersistence](facets, pgtable.PersistenceKind)
	if err != nil {
		return "", schemaext.Facets{}, err
	}
	if !found {
		return "", facets, nil
	}
	if !pgtable.SupportsPersistence(target) {
		return "", schemaext.Facets{}, fmt.Errorf("%w: UNLOGGED tables require PostgreSQL, not %q", ptaherr.ErrUnsupportedDialect, target)
	}
	if err := pgtable.ValidateDesiredPersistence(value); err != nil {
		return "", schemaext.Facets{}, err
	}
	prefix := ""
	if value.Unlogged {
		prefix = " UNLOGGED"
	}
	return prefix, facets.Without(pgtable.PersistenceKind), nil
}

// PrepareTableFacets validates persistence and keeps only settings the target
// renders. PostgreSQL retains the facet. Other targets omit it; YDB refuses a
// true declaration. An explicit false is a default, not a lost setting.
func PrepareTableFacets(target string, facets schemaext.Facets) (schemaext.Facets, error) {
	var err error
	facets, err = preparePartitionFacets(target, facets)
	if err != nil {
		return schemaext.Facets{}, err
	}
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPersistence](facets, pgtable.PersistenceKind)
	if err != nil {
		return schemaext.Facets{}, err
	}
	if !found {
		return facets, nil
	}
	if err := pgtable.ValidateDesiredPersistence(value); err != nil {
		return schemaext.Facets{}, err
	}
	if !slices.Contains(pgtable.PersistenceTargets(), platform.NormalizeDialect(target)) {
		return schemaext.Facets{}, fmt.Errorf("%w: table persistence has no policy for %q", ptaherr.ErrUnsupportedDialect, target)
	}
	if pgtable.SupportsPersistence(target) {
		return facets, nil
	}
	if platform.NormalizeDialect(target) == platform.YDB && value.Unlogged {
		return schemaext.Facets{}, fmt.Errorf("%w: UNLOGGED is PostgreSQL's; YDB has no unlogged table", ptaherr.ErrUnsupportedFeature)
	}
	return facets.Without(pgtable.PersistenceKind), nil
}

// PersistenceOmission names a true declaration the target leaves out. It
// validates through PrepareTableFacets and returns nil for retained or default
// settings. The caller supplies facets projected onto its selected target.
func PersistenceOmission(target, table string, facets schemaext.Facets) (*renderer.Omission, error) {
	remaining, err := PrepareTableFacets(target, facets)
	if err != nil {
		return nil, err
	}
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPersistence](facets, pgtable.PersistenceKind)
	if err != nil {
		return nil, err
	}
	if !found || !value.Unlogged || remaining.Len() == facets.Len() {
		return nil, nil
	}
	return &renderer.Omission{Dialect: target, Reason: "unsupported", Kind: "table", Name: table, Property: "unlogged"}, nil
}
