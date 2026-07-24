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

	added, removed := compareNamedItems(generatedDomains, databaseDomains)
	diff.DomainsAdded = append(diff.DomainsAdded, added...)
	diff.DomainsRemoved = append(diff.DomainsRemoved, removed...)

	for name, target := range generatedDomains {
		if current, exists := databaseDomains[name]; exists {
			if changes := domainChanges(target, current); len(changes) > 0 {
				diff.DomainsModified = append(diff.DomainsModified, difftypes.DomainDiff{DomainName: name, Changes: changes})
			}
		}
	}

	sort.Strings(diff.DomainsAdded)
	sort.Strings(diff.DomainsRemoved)
	sort.Slice(diff.DomainsModified, func(i, j int) bool {
		return diff.DomainsModified[i].DomainName < diff.DomainsModified[j].DomainName
	})
}

// domainChanges compares the reconcilable options of a domain: its base type
// (canonicalized so alias spellings such as VARCHAR vs character varying do not
// churn) and NOT NULL. CHECK and DEFAULT are intentionally not compared:
// PostgreSQL rewrites CHECK expressions (adding parentheses and ::casts) on
// read-back, so a string comparison would report phantom changes, and a phantom
// change would drive a drop+recreate. They are therefore create-only; changing
// a domain's CHECK/DEFAULT requires a manual migration.
func domainChanges(target goschema.Domain, current types.DBDomain) map[string]string {
	changes := make(map[string]string)
	if target.BaseType != "" && canonicalizePostgresType(target.BaseType) != canonicalizePostgresType(current.BaseType) {
		changes["type"] = fmt.Sprintf("%s -> %s", current.BaseType, target.BaseType)
	}
	if target.NotNull != current.NotNull {
		changes["not_null"] = fmt.Sprintf("%t -> %t", current.NotNull, target.NotNull)
	}
	return changes
}

// pgTypeAliases maps accepted type spellings to the canonical form PostgreSQL's
// format_type reports, so a declared type compares equal to its introspected
// counterpart.
var pgTypeAliases = map[string]string{
	"varchar":     "character varying",
	"char":        "character",
	"int":         "integer",
	"int4":        "integer",
	"int8":        "bigint",
	"int2":        "smallint",
	"serial":      "integer",
	"serial4":     "integer",
	"serial8":     "bigint",
	"bigserial":   "bigint",
	"smallserial": "smallint",
	"serial2":     "smallint",
	"float8":      "double precision",
	"float4":      "real",
	"bool":        "boolean",
	"decimal":     "numeric",
	"timestamptz": "timestamp with time zone",
	"timestamp":   "timestamp without time zone",
	"timetz":      "time with time zone",
}

// canonicalizePostgresType lower-cases a type, normalizes its parameter list,
// and maps common aliases to the spelling format_type emits.
func canonicalizePostgresType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	base, params := t, ""
	if i := strings.IndexByte(t, '('); i >= 0 {
		base = strings.TrimSpace(t[:i])
		params = strings.ReplaceAll(t[i:], " ", "")
	}
	if canonical, ok := pgTypeAliases[base]; ok {
		base = canonical
	}
	return base + params
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

	added, removed := compareNamedItems(generatedTypes, databaseTypes)
	diff.CompositeTypesAdded = append(diff.CompositeTypesAdded, added...)
	diff.CompositeTypesRemoved = append(diff.CompositeTypesRemoved, removed...)

	for name, target := range generatedTypes {
		current, exists := databaseTypes[name]
		if !exists {
			continue
		}
		if targetFields, currentFields := compositeFieldList(target), dbCompositeFieldList(current); targetFields != currentFields {
			diff.CompositeTypesModified = append(diff.CompositeTypesModified, difftypes.CompositeTypeDiff{
				TypeName: name,
				Changes:  map[string]string{"fields": fmt.Sprintf("%s -> %s", currentFields, targetFields)},
			})
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
		parts[i] = strings.ToLower(field.Name) + " " + canonicalizePostgresType(field.Type)
	}
	return strings.Join(parts, ", ")
}

func dbCompositeFieldList(composite types.DBComposite) string {
	parts := make([]string, len(composite.Fields))
	for i, field := range composite.Fields {
		parts[i] = strings.ToLower(field.Name) + " " + canonicalizePostgresType(field.Type)
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

	added, removed := compareNamedItems(generatedRanges, databaseRanges)
	diff.RangesAdded = append(diff.RangesAdded, added...)
	diff.RangesRemoved = append(diff.RangesRemoved, removed...)

	sort.Strings(diff.RangesAdded)
	sort.Strings(diff.RangesRemoved)
}
