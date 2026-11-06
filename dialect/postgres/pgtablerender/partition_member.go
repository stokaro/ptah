package pgtablerender

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/sqlident"
)

// CreateTablePartitionOf renders the clause that takes the place of a declared
// partition's column list, with its leading space: PARTITION OF, the parent and
// the bound. An ordinary table returns an empty clause. Invalid declarations and
// targets without the statement fail.
func CreateTablePartitionOf(target string, _ capability.Capabilities, _ string, facets schemaext.Facets) (string, error) {
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPartitionMember](facets, pgtable.PartitionMemberKind)
	if err != nil || !found {
		return "", err
	}
	if err := pgtable.ValidateDesiredPartitionMember(value); err != nil {
		return "", err
	}
	if err := pgtable.RefuseForeignPartitionMember(target); err != nil {
		return "", err
	}
	return " PARTITION OF " + sqlident.Qualified(platform.Postgres, value.ParentSchema, value.Parent) + " " + value.Bound, nil
}

// preparePartitionMember refuses a declared partition on a target without the
// statement. Unlike a partition key it has no omission: a table with no column
// list and no parent is nothing a target can create.
func preparePartitionMember(target string, facets schemaext.Facets) error {
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPartitionMember](facets, pgtable.PartitionMemberKind)
	if err != nil || !found {
		return err
	}
	if err := pgtable.ValidateDesiredPartitionMember(value); err != nil {
		return err
	}
	return pgtable.RefuseForeignPartitionMember(target)
}
