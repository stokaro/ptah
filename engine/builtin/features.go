package builtin

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin/internal/dialects/clickhouse"
	"ptah.run/internal/ydbextensions"
)

// This is built-in composition. Feature-specific shape and support decisions
// belong to the selected owner, including refusal of unrelated payload kinds.
func validateNamedFeatures(dialect string, caps capability.Capabilities, objects schemaext.Objects) error {
	if objects.IsZero() {
		return nil
	}
	if platform.NormalizeDialect(dialect) == platform.YDB {
		return ydbextensions.ValidateObjects(dialect, caps, objects)
	}
	return fmt.Errorf("%w: feature objects are not registered for target %q", ptaherr.ErrUnsupportedFeature, dialect)
}

func prepareFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	projected, err := projectFacets(dialect, facets)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return refuseActiveFacets(dialect, projected)
}

func projectFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	if facets.IsZero() {
		return facets, nil
	}
	selected, err := resolveTargetSelection(dialect)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.ForTarget(selected)
}

func refuseActiveFacets(dialect string, projected schemaext.Facets) (schemaext.Facets, error) {
	if projected.Len() == 0 {
		return projected, nil
	}
	return schemaext.Facets{}, fmt.Errorf("%w: feature facet %q is not registered for target %q", ptaherr.ErrUnsupportedFeature, projected.Kinds()[0], dialect)
}

func prepareTableFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	projected, err := projectFacets(dialect, facets)
	if err != nil {
		return schemaext.Facets{}, err
	}
	if platform.NormalizeDialect(dialect) != platform.ClickHouse {
		return refuseActiveFacets(dialect, projected)
	}
	if err := clickhouse.ValidateTableFacets(projected); err != nil {
		return schemaext.Facets{}, err
	}
	return projected, nil
}

func validateDeclaredFacets(dialect string, database *schemamodel.Database) error {
	tables := make(map[*schemaext.Facets]struct{}, len(database.Tables))
	for i := range database.Tables {
		tables[&database.Tables[i].Facets] = struct{}{}
	}
	for _, facets := range database.FacetSlots() {
		prepare := prepareFacets
		if _, table := tables[facets]; table {
			prepare = prepareTableFacets
		}
		if _, err := prepare(dialect, *facets); err != nil {
			return err
		}
	}
	return nil
}
