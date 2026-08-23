package compare

import (
	"cmp"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// extendedPropertyKey is what makes two extended properties the same property:
// the address SQL Server stores it under, plus the name.
//
// Every part is folded, because SQL Server's default collation is
// case-insensitive and a declaration that spells the table `Docs` while the
// catalog spells it `docs` is naming one table. That is the same normalization
// synonymTargetParts applies for the same reason, and comparing raw strings
// would report an addition and a removal of one property on every run.
type extendedPropertyKey struct {
	schema string
	table  string
	column string
	name   string
}

func newExtendedPropertyKey(schema, table, column, name string) extendedPropertyKey {
	fold := func(part string) string {
		return strings.ToLower(strings.TrimSpace(part))
	}
	return extendedPropertyKey{
		schema: fold(schema),
		table:  fold(table),
		column: fold(column),
		name:   fold(name),
	}
}

// ExtendedProperties compares declared SQL Server extended properties against
// the ones the database reports.
//
// A changed value is a modification rather than a removal plus an addition,
// because SQL Server has a statement for exactly that case and dropping and
// re-adding would take the property away for the length of the script.
//
// A live property whose value SQL Server stores under a base type Ptah cannot
// write back is declined in BOTH directions: nothing is planned to change it
// and nothing is planned to remove it. The renderer emits an N” literal, so
// re-emitting an int or a date would change its type, and a removal would
// destroy a value no declaration could restore. Reporting it and leaving it
// alone is the only answer that neither lies about it nor damages it.
func ExtendedProperties(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	declared := make(map[extendedPropertyKey]goschema.ExtendedProperty, len(generated.ExtendedProperties))
	for _, property := range generated.ExtendedProperties {
		key := newExtendedPropertyKey(property.Schema, property.Table, property.Column, property.Name)
		declared[key] = property
	}

	live := make(map[extendedPropertyKey]types.DBExtendedProperty, len(database.ExtendedProperties))
	for _, property := range database.ExtendedProperties {
		key := newExtendedPropertyKey(property.Schema, property.Table, property.Column, property.Name)
		live[key] = property
	}

	for key, property := range declared {
		existing, exists := live[key]
		switch {
		case !exists:
			diff.ExtendedPropertiesAdded = append(diff.ExtendedPropertiesAdded,
				refFromDeclaredProperty(property))
		case existing.ValueNotRepresentable:
			// The declaration and the live row cannot be compared: nobody read
			// the live value, so "differs" is not a fact this comparison has.
			continue
		case existing.Value != property.Value:
			diff.ExtendedPropertiesModified = append(diff.ExtendedPropertiesModified,
				difftypes.ExtendedPropertyDiff{
					ExtendedPropertyRef: refFromDeclaredProperty(property),
					OldValue:            existing.Value,
				})
		}
	}

	for key, property := range live {
		if _, ok := declared[key]; ok {
			continue
		}
		if property.ValueNotRepresentable {
			continue
		}
		// A desired state that could not have named this property has not
		// withheld it. No document format Ptah reads can express one -- HCL has
		// no block, YAML has no key, and the SQL parser produces none -- so
		// `schema inspect > out.hcl` followed by `schema apply --to out.hcl`
		// planned sp_dropextendedproperty for every property on the server,
		// through Ptah's own output (stokaro/ptah#1031). A Go schema CAN
		// declare one, records nothing here, and still removes.
		if !cov.PlansRemoval(coverage.ExtendedProperty, property.Schema, property.Name) {
			continue
		}
		diff.ExtendedPropertiesRemoved = append(diff.ExtendedPropertiesRemoved,
			refFromLiveProperty(property))
	}

	sortExtendedPropertyRefs(diff.ExtendedPropertiesAdded)
	sortExtendedPropertyRefs(diff.ExtendedPropertiesRemoved)
	slices.SortFunc(diff.ExtendedPropertiesModified,
		func(a, b difftypes.ExtendedPropertyDiff) int {
			return compareExtendedPropertyRefs(a.ExtendedPropertyRef, b.ExtendedPropertyRef)
		})
}

func refFromDeclaredProperty(property goschema.ExtendedProperty) difftypes.ExtendedPropertyRef {
	return difftypes.ExtendedPropertyRef{
		Name:   property.Name,
		Schema: property.Schema,
		Table:  property.Table,
		Column: property.Column,
		Value:  property.Value,
	}
}

func refFromLiveProperty(property types.DBExtendedProperty) difftypes.ExtendedPropertyRef {
	return difftypes.ExtendedPropertyRef{
		Name:   property.Name,
		Schema: property.Schema,
		Table:  property.Table,
		Column: property.Column,
		Value:  property.Value,
	}
}

func sortExtendedPropertyRefs(refs []difftypes.ExtendedPropertyRef) {
	slices.SortFunc(refs, compareExtendedPropertyRefs)
}

func compareExtendedPropertyRefs(a, b difftypes.ExtendedPropertyRef) int {
	return cmp.Or(
		cmp.Compare(a.Schema, b.Schema),
		cmp.Compare(a.Table, b.Table),
		cmp.Compare(a.Column, b.Column),
		cmp.Compare(a.Name, b.Name),
	)
}
