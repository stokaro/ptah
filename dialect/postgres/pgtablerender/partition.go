package pgtablerender

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/sqlident"
)

// PartitionSQL renders the strategy and ordered key, without PARTITION BY.
// It validates the model, quotes column identifiers, and preserves SQL
// expressions. Invalid input returns no SQL.
func PartitionSQL(value *pgtable.DesiredPartition) (string, error) {
	if err := pgtable.ValidateDesiredPartition(value); err != nil {
		return "", err
	}
	parts := make([]string, 0, len(value.Parts))
	for _, part := range value.Parts {
		key := sqlident.Quote(platform.Postgres, part.Name)
		if part.Expr != "" {
			key = "(" + part.Expr + ")"
		}
		if part.Collation != "" {
			key += " COLLATE " + sqlident.QuotePostgresQualified(part.Collation)
		}
		if part.OperatorClass != "" {
			key += " " + sqlident.QuotePostgresQualified(part.OperatorClass)
		}
		parts = append(parts, key)
	}
	return value.Type + " (" + strings.Join(parts, ", ") + ")", nil
}

// CreateTablePartition renders the owned clause after a table's column list.
// Missing partitioning returns an empty clause. Invalid declarations,
// observations, unsupported targets, and unlogged partitioned tables fail.
func CreateTablePartition(target string, _ capability.Capabilities, _ string, facets schemaext.Facets) (string, error) {
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPartition](facets, pgtable.PartitionKind)
	if err != nil || !found {
		return "", err
	}
	if !pgtable.SupportsPartition(target) {
		return "", fmt.Errorf("%w: PostgreSQL PARTITION BY is not supported on %s", ptaherr.ErrUnsupportedFeature, target)
	}
	persistence, _, err := pgtable.PersistenceOf(facets)
	if err != nil {
		return "", err
	}
	if pgtable.SupportsPersistence(target) && persistence.Unlogged {
		return "", fmt.Errorf("%w: PostgreSQL partitioned tables cannot be UNLOGGED", ptaherr.ErrUnsupportedFeature)
	}
	key, err := PartitionSQL(value)
	if err != nil {
		return "", err
	}
	return " PARTITION BY " + key, nil
}

func preparePartitionFacets(target string, facets schemaext.Facets) (schemaext.Facets, error) {
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPartition](facets, pgtable.PartitionKind)
	if err != nil || !found {
		return facets, err
	}
	if err := pgtable.ValidateDesiredPartition(value); err != nil {
		return schemaext.Facets{}, err
	}
	if pgtable.SupportsPartition(target) {
		_, err := CreateTablePartition(target, capability.Capabilities{}, "", facets)
		return facets, err
	}
	if err := pgtable.RefuseForeignPartition(target); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.Without(pgtable.PartitionKind), nil
}

// PartitionOmission describes the partition key omitted by a foreign target.
// Supported targets return nil. Invalid values and refusing targets return an
// error; omitted SQL includes the declared strategy, key and modifiers.
func PartitionOmission(target, table string, facets schemaext.Facets) (*renderer.Omission, error) {
	remaining, err := preparePartitionFacets(target, facets)
	if err != nil {
		return nil, err
	}
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPartition](facets, pgtable.PartitionKind)
	if err != nil || !found || remaining.Len() == facets.Len() {
		return nil, err
	}
	key, err := PartitionSQL(value)
	if err != nil {
		return nil, err
	}
	return &renderer.Omission{Dialect: target, Reason: "unsupported", Kind: "table", Name: table, Property: "partition", Detail: key}, nil
}
