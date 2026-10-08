package clickhouse

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
)

// ValidateTableFacets checks the table values this renderer consumes. An empty
// collection, including retained source exclusions, is valid. Unknown active
// kinds wrap ErrUnsupportedFeature; observations and malformed declarations
// wrap schemaext.ErrInvalidValue. Target selection happens before this call.
func ValidateTableFacets(facets schemaext.Facets) error {
	for _, kind := range facets.Kinds() {
		if kind != chschema.TableKind {
			return fmt.Errorf("%w: ClickHouse table facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, found, err := schemaext.FacetAs[*chschema.DesiredTable](facets, chschema.TableKind)
	if err != nil || !found {
		return err
	}
	return chschema.ValidateDesired(value)
}

func resolveOwnedTableEngineSpec(node *ast.CreateTableNode) (tableEngineSpec, error) {
	if err := ValidateTableFacets(node.Facets); err != nil {
		return tableEngineSpec{}, err
	}
	value, found, err := schemaext.FacetAs[*chschema.DesiredTable](node.Facets, chschema.TableKind)
	if err != nil {
		return tableEngineSpec{}, err
	}
	if !found {
		return resolveTableEngineSpec(node), nil
	}
	for _, key := range tableEngineOptionKeys {
		if _, present := node.Options[key]; present {
			return tableEngineSpec{}, fmt.Errorf("%w: ClickHouse table %q declares %s in both a typed facet and table options", ptaherr.ErrInvalidSchemaDiff, node.Name, key)
		}
	}
	resolved, err := chresolve.Table(chresolve.Request{
		Desired: value, Creating: true, CommonKey: tablePrimaryKeyColumns(node),
	})
	if err != nil {
		return tableEngineSpec{}, fmt.Errorf("clickhouse: table %q: %w", node.Name, err)
	}
	prepared := resolved.Prepared
	spec := tableEngineSpec{
		engine: prepared.Engine.Value, orderBy: prepared.OrderBy.Value, primaryKey: prepared.PrimaryKey.Value,
		partitionBy: prepared.PartitionBy.Value, sampleBy: prepared.SampleBy.Value,
		ttl: prepared.TTL.Value, settings: prepared.Settings.Value,
	}
	// A default primary key is established by ORDER BY. Preserve that syntax
	// while the resolver records its effective value separately from intent.
	if value.PrimaryKey.State != chschema.Explicit {
		spec.primaryKey = ""
	}
	if spec.isMergeTreeFamily() {
		if value.PrimaryKey.State == chschema.Explicit && spec.primaryKey == "" {
			spec.primaryKey = "tuple()"
		}
		if spec.orderBy == "" {
			spec.orderBy = "tuple()"
		}
	}
	return spec, nil
}
