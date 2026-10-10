package builtin

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/cockroachdb/crdbrender"
	"ptah.run/dialect/timescaledb/tsrender"
	"ptah.run/dialect/timescaledb/tsschema"
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
	if platform.IsPostgresFamily(dialect) {
		return validateTimescaleObjects(dialect, objects)
	}
	return fmt.Errorf("%w: feature objects are not registered for target %q", ptaherr.ErrUnsupportedFeature, dialect)
}

// validateTimescaleObjects accepts the continuous aggregates a PostgreSQL-family
// target's owner plans and refuses every other named object. Capability gating
// stays with rendering, which writes the skip line on a target without the key.
func validateTimescaleObjects(dialect string, objects schemaext.Objects) error {
	all, err := objects.All()
	if err != nil {
		return err
	}
	for _, object := range all {
		value, ok := object.Value.(*tsschema.DesiredContinuousAggregate)
		if !ok {
			return fmt.Errorf("%w: feature objects are not registered for target %q", ptaherr.ErrUnsupportedFeature, dialect)
		}
		if err := tsschema.ValidateContinuousAggregateRef(object.Ref); err != nil {
			return err
		}
		if err := tsschema.ValidateDesiredContinuousAggregate(value); err != nil {
			return err
		}
	}
	return nil
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
	if platform.IsPostgresFamily(dialect) {
		return preparePostgresTableFacets(dialect, projected)
	}
	if platform.NormalizeDialect(dialect) != platform.ClickHouse {
		return refuseActiveFacets(dialect, projected)
	}
	if err := clickhouse.ValidateTableFacets(projected); err != nil {
		return schemaext.Facets{}, err
	}
	return projected, nil
}

func prepareIndexFacets(dialect string, facets schemaext.Facets) (schemaext.Facets, error) {
	projected, err := projectFacets(dialect, facets)
	if err != nil {
		return schemaext.Facets{}, err
	}
	if platform.NormalizeDialect(dialect) != platform.ClickHouse {
		return refuseActiveFacets(dialect, projected)
	}
	if err := clickhouse.ValidateIndexFacets(projected); err != nil {
		return schemaext.Facets{}, err
	}
	return projected, nil
}

// preparePostgresTableFacets accepts the TimescaleDB settings every
// PostgreSQL-family renderer writes after CREATE TABLE, and the row-level TTL a
// CockroachDB CREATE TABLE carries. Every other kind is refused: no owner
// composed for this family renders it.
func preparePostgresTableFacets(dialect string, projected schemaext.Facets) (schemaext.Facets, error) {
	if err := tsrender.ValidateTableFacets(projected); err != nil {
		return schemaext.Facets{}, err
	}
	rest := projected.Without(tsschema.HypertableKind)
	if platform.NormalizeDialect(dialect) == platform.CockroachDB {
		if err := crdbrender.ValidateTableFacets(rest); err != nil {
			return schemaext.Facets{}, err
		}
		return projected, nil
	}
	if _, err := refuseActiveFacets(dialect, rest); err != nil {
		return schemaext.Facets{}, err
	}
	return projected, nil
}

func validateDeclaredFacets(dialect string, database *schemamodel.Database) error {
	owners := make(map[*schemaext.Facets]func(string, schemaext.Facets) (schemaext.Facets, error), len(database.Tables)+len(database.Indexes))
	for i := range database.Tables {
		owners[&database.Tables[i].Facets] = prepareTableFacets
	}
	for i := range database.Indexes {
		owners[&database.Indexes[i].Facets] = prepareIndexFacets
	}
	for _, facets := range database.FacetSlots() {
		prepare := prepareFacets
		if owner, found := owners[facets]; found {
			prepare = owner
		}
		if _, err := prepare(dialect, *facets); err != nil {
			return err
		}
	}
	return nil
}
