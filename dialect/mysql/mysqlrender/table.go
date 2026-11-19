package mysqlrender

import (
	"fmt"
	"maps"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/mysqlpartition"
)

// ValidateTableFacets checks the table values the MySQL-family renderer
// consumes: a table's declared options. An empty collection is valid. Any
// other active kind wraps ptaherr.ErrUnsupportedFeature, and an invalid
// declaration wraps schemaext.ErrInvalidValue.
func ValidateTableFacets(facets schemaext.Facets) error {
	_, err := TableOptions(facets)
	return err
}

// TableOptions returns the options a table declares, or nil for a table that
// declares none. Any other active kind is refused.
func TableOptions(facets schemaext.Facets) (*mysqlschema.DesiredTable, error) {
	for _, kind := range facets.Kinds() {
		if kind != mysqlschema.TableKind {
			return nil, fmt.Errorf("%w: MySQL table facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, _, err := schemaext.FacetAs[*mysqlschema.DesiredTable](facets, mysqlschema.TableKind)
	if err != nil || value == nil {
		return nil, err
	}
	if err := mysqlschema.ValidateDesiredTable(value); err != nil {
		return nil, err
	}
	return value, nil
}

// CreateTableOptions writes the declared options as the CREATE TABLE options
// they are, ENGINE, AUTO_INCREMENT and CHARSET, over options that already hold
// them: a declared option is the one written. options is not changed, and a
// table without the facet returns a copy of it.
//
// An option named PARTITION_BY, which a SQL document that names no target
// carries for a PARTITION BY clause, is refused with an error wrapping
// [ptaherr.ErrUnsupportedFeature]: Ptah does not model MySQL or MariaDB
// partitioning, and written as an option the clause is one no server parses.
func CreateTableOptions(facets schemaext.Facets, options map[string]string) (map[string]string, error) {
	if _, declared := options[mysqlpartition.Option]; declared {
		return nil, mysqlpartition.Refusal("PARTITION BY")
	}
	result := make(map[string]string, len(options)+3)
	maps.Copy(result, options)
	declared, err := TableOptions(facets)
	if err != nil || declared == nil {
		return result, err
	}
	for _, option := range []struct{ key, value string }{
		{"ENGINE", declared.Engine}, {"AUTO_INCREMENT", declared.AutoIncrement}, {"CHARSET", declared.Charset},
	} {
		if option.value != "" {
			result[option.key] = option.value
		}
	}
	return result, nil
}
