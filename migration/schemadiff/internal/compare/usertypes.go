package compare

import (
	"fmt"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// Domains compares PostgreSQL domain types between the target schema and the
// current database. Only options the target explicitly declares are compared, so
// undeclared attributes (which the catalog always populates) do not churn.
func Domains(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	generatedDomains := make(map[string]goschema.Domain, len(generated.Domains))
	for _, domain := range generated.Domains {
		generatedDomains[domain.QualifiedName()] = domain
	}
	databaseDomains := make(map[string]types.DBDomain, len(database.Domains))
	for _, domain := range database.Domains {
		databaseDomains[domain.QualifiedName()] = domain
	}

	for name, target := range generatedDomains {
		current, exists := databaseDomains[name]
		if !exists {
			diff.DomainsAdded = append(diff.DomainsAdded, name)
			continue
		}
		if changes := domainChanges(target, current); len(changes) > 0 {
			diff.DomainsModified = append(diff.DomainsModified, difftypes.DomainDiff{DomainName: name, Changes: changes})
		}
	}
	for name := range databaseDomains {
		if _, exists := generatedDomains[name]; !exists {
			diff.DomainsRemoved = append(diff.DomainsRemoved, name)
		}
	}

	sort.Strings(diff.DomainsAdded)
	sort.Strings(diff.DomainsRemoved)
	sort.Slice(diff.DomainsModified, func(i, j int) bool {
		return diff.DomainsModified[i].DomainName < diff.DomainsModified[j].DomainName
	})
}

func domainChanges(target goschema.Domain, current types.DBDomain) map[string]string {
	changes := make(map[string]string)
	if target.BaseType != "" && !strings.EqualFold(target.BaseType, current.BaseType) {
		changes["type"] = fmt.Sprintf("%s -> %s", current.BaseType, target.BaseType)
	}
	if target.NotNull != current.NotNull {
		changes["not_null"] = fmt.Sprintf("%t -> %t", current.NotNull, target.NotNull)
	}
	if target.Check != "" && !strings.EqualFold(strings.TrimSpace(target.Check), strings.TrimSpace(current.Check)) {
		changes["check"] = fmt.Sprintf("%s -> %s", current.Check, target.Check)
	}
	return changes
}

// CompositeTypes compares PostgreSQL composite types between the target schema
// and the current database.
func CompositeTypes(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	generatedTypes := make(map[string]goschema.CompositeType, len(generated.CompositeTypes))
	for _, composite := range generated.CompositeTypes {
		generatedTypes[composite.QualifiedName()] = composite
	}
	databaseTypes := make(map[string]types.DBComposite, len(database.Composites))
	for _, composite := range database.Composites {
		databaseTypes[composite.QualifiedName()] = composite
	}

	for name, target := range generatedTypes {
		current, exists := databaseTypes[name]
		if !exists {
			diff.CompositeTypesAdded = append(diff.CompositeTypesAdded, name)
			continue
		}
		if targetFields, currentFields := compositeFieldList(target), dbCompositeFieldList(current); targetFields != currentFields {
			diff.CompositeTypesModified = append(diff.CompositeTypesModified, difftypes.CompositeTypeDiff{
				TypeName: name,
				Changes:  map[string]string{"fields": fmt.Sprintf("%s -> %s", currentFields, targetFields)},
			})
		}
	}
	for name := range databaseTypes {
		if _, exists := generatedTypes[name]; !exists {
			diff.CompositeTypesRemoved = append(diff.CompositeTypesRemoved, name)
		}
	}

	sort.Strings(diff.CompositeTypesAdded)
	sort.Strings(diff.CompositeTypesRemoved)
	sort.Slice(diff.CompositeTypesModified, func(i, j int) bool {
		return diff.CompositeTypesModified[i].TypeName < diff.CompositeTypesModified[j].TypeName
	})
}

func compositeFieldList(composite goschema.CompositeType) string {
	parts := make([]string, len(composite.Fields))
	for i, field := range composite.Fields {
		parts[i] = strings.ToLower(field.Name + " " + field.Type)
	}
	return strings.Join(parts, ", ")
}

func dbCompositeFieldList(composite types.DBComposite) string {
	parts := make([]string, len(composite.Fields))
	for i, field := range composite.Fields {
		parts[i] = strings.ToLower(field.Name + " " + field.Type)
	}
	return strings.Join(parts, ", ")
}

// Ranges compares PostgreSQL range types between the target schema and the
// current database. Ranges have no in-place alter, so only add/remove is
// reported.
func Ranges(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	generatedRanges := make(map[string]struct{}, len(generated.Ranges))
	for _, rangeType := range generated.Ranges {
		generatedRanges[rangeType.QualifiedName()] = struct{}{}
	}
	databaseRanges := make(map[string]struct{}, len(database.Ranges))
	for _, rangeType := range database.Ranges {
		databaseRanges[rangeType.QualifiedName()] = struct{}{}
	}

	for name := range generatedRanges {
		if _, exists := databaseRanges[name]; !exists {
			diff.RangesAdded = append(diff.RangesAdded, name)
		}
	}
	for name := range databaseRanges {
		if _, exists := generatedRanges[name]; !exists {
			diff.RangesRemoved = append(diff.RangesRemoved, name)
		}
	}

	sort.Strings(diff.RangesAdded)
	sort.Strings(diff.RangesRemoved)
}
