package mysqlrender

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// ValidateColumnFacets accepts the column facets a MySQL-family column
// definition writes, [mysqlschema.ColumnSettingsKind] in either
// representation, and refuses every other kind with
// [ptaherr.ErrUnsupportedFeature], so no setting is dropped in silence.
func ValidateColumnFacets(facets schemaext.Facets) error {
	for _, kind := range facets.Kinds() {
		if kind != mysqlschema.ColumnSettingsKind {
			return fmt.Errorf("%w: feature facet %q is not registered for MySQL-family columns", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	_, _, err := ColumnClauses(facets)
	return err
}

// ColumnClauses returns the character set and the ON UPDATE expression a
// column definition writes, each empty when the facets state none. A
// declaration is written as stated; an observation, which a rollback restores,
// is written as read.
func ColumnClauses(facets schemaext.Facets) (charset, onUpdate string, err error) {
	settings, found, err := mysqlschema.Settings(facets)
	if err != nil || !found {
		return "", "", err
	}
	return settings.Charset, settings.OnUpdate, nil
}
