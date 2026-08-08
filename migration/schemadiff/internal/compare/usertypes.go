package compare

import (
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// Domains compares PostgreSQL domain types between the target schema and the
// current database. Only options the target explicitly declares are compared, so
// undeclared attributes (which the catalog always populates) do not churn.
func Domains(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
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
				// CurrentBaseType is the from-side of the comparison carried as
				// a type spelling: the recreate path's non-CASCADE DROP runs
				// against this database, so it is these references that can
				// block it, not the target's.
				diff.DomainsModified = append(diff.DomainsModified, difftypes.DomainDiff{
					DomainName:      name,
					Changes:         changes,
					CurrentBaseType: current.BaseType,
				})
			}
		}
	}

	// An extension owns some of the domains, composites and ranges in a schema.
	// A reader that leaves those to their extension has not said the schema
	// lacks them, and a document that omitted the extension block has not said
	// the type is unwanted (stokaro/ptah#1294, stokaro/ptah#1276).
	//
	// `CREATE DOMAIN` and `CREATE TYPE` have no conditional form, so an
	// undecidable addition is recorded on the coverage rather than dropped.
	keptDomains, withheldDomains := cov.keepPlannedAdditions(
		coverage.Domain, diff.DomainsAdded, qualifiedName, unguardedCreations(),
	)
	diff.DomainsAdded = keptDomains
	cov.recordUndecidedAdditions(coverage.Domain, withheldDomains)
	diff.DomainsRemoved = cov.keepPlannedRemovals(coverage.Domain, diff.DomainsRemoved, qualifiedName)

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
func CompositeTypes(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
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
			// CurrentFieldTypes is the from-side of the comparison carried as
			// type spellings, for the reason given on the domain branch above.
			diff.CompositeTypesModified = append(diff.CompositeTypesModified, difftypes.CompositeTypeDiff{
				TypeName:          name,
				Changes:           map[string]string{"fields": fmt.Sprintf("%s -> %s", currentFields, targetFields)},
				CurrentFieldTypes: dbCompositeFieldTypes(current),
			})
		}
	}

	keptComposites, withheldComposites := cov.keepPlannedAdditions(
		coverage.Composite, diff.CompositeTypesAdded, qualifiedName, unguardedCreations(),
	)
	diff.CompositeTypesAdded = keptComposites
	cov.recordUndecidedAdditions(coverage.Composite, withheldComposites)
	diff.CompositeTypesRemoved = cov.keepPlannedRemovals(coverage.Composite, diff.CompositeTypesRemoved, qualifiedName)

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

// dbCompositeFieldTypes returns the field types the database holds for a
// composite, in field order and in the catalog's own spelling. Unlike
// dbCompositeFieldList these are not normalized: they are resolved against
// other type names later, and canonicalization would rewrite a user-defined
// name that happens to collide with a built-in alias.
func dbCompositeFieldTypes(composite types.DBComposite) []string {
	fieldTypes := make([]string, len(composite.Fields))
	for i, field := range composite.Fields {
		fieldTypes[i] = field.Type
	}
	return fieldTypes
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
func Ranges(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	generatedRanges := make(map[string]goschema.Range, len(generated.Ranges))
	for _, rangeType := range generated.Ranges {
		generatedRanges[rangeType.QualifiedName()] = rangeType
	}
	databaseRanges := make(map[string]types.DBRange, len(database.Ranges))
	for _, rangeType := range database.Ranges {
		databaseRanges[rangeType.QualifiedName()] = rangeType
	}

	added, removed := compareNamedItems(generatedRanges, databaseRanges)
	diff.RangesAdded = append(diff.RangesAdded, added...)
	diff.RangesRemoved = append(diff.RangesRemoved, removed...)

	for name, target := range generatedRanges {
		current, exists := databaseRanges[name]
		if !exists {
			continue
		}
		if changes := rangeChanges(target, current); len(changes) > 0 {
			// CurrentSubtype is the from-side of the comparison carried as a
			// type spelling: the recreate path's non-CASCADE DROP runs against
			// this database, so it is these references that can block it.
			diff.RangesModified = append(diff.RangesModified, difftypes.RangeDiff{
				RangeName:      name,
				Changes:        changes,
				CurrentSubtype: current.Subtype,
			})
		}
	}

	keptRanges, withheldRanges := cov.keepPlannedAdditions(
		coverage.Range, diff.RangesAdded, qualifiedName, unguardedCreations(),
	)
	diff.RangesAdded = keptRanges
	cov.recordUndecidedAdditions(coverage.Range, withheldRanges)
	diff.RangesRemoved = cov.keepPlannedRemovals(coverage.Range, diff.RangesRemoved, qualifiedName)

	sort.Strings(diff.RangesAdded)
	sort.Strings(diff.RangesRemoved)
	sort.Slice(diff.RangesModified, func(i, j int) bool {
		return diff.RangesModified[i].RangeName < diff.RangesModified[j].RangeName
	})
}

// rangeChanges reports how an existing range type differs from its declaration.
//
// Only attributes the target explicitly declares are compared, matching
// domainChanges. The catalog always resolves an operator class and (for a
// collatable subtype) a collation even when the author named neither, so
// comparing an undeclared attribute against the resolved default would report a
// difference on every run and never converge.
//
// Comparing nothing at all was the previous behavior: the comparator built name
// sets, so a changed subtype produced an empty plan and `schema apply` answered
// "Schema is synced, no changes to be made." while the old definition was still
// in the database (stokaro/ptah#931 item 2).
func rangeChanges(target goschema.Range, current types.DBRange) map[string]string {
	changes := make(map[string]string)
	if target.Subtype != "" && canonicalizePostgresType(target.Subtype) != canonicalizePostgresType(current.Subtype) {
		changes["subtype"] = fmt.Sprintf("%s -> %s", current.Subtype, target.Subtype)
	}
	addDeclaredRangeChange(changes, "subtype_opclass", target.SubtypeOpClass, current.SubtypeOpClass)
	addDeclaredRangeChange(changes, "collation", target.Collation, current.Collation)
	addDeclaredRangeChange(changes, "canonical", target.Canonical, current.Canonical)
	addDeclaredRangeChange(changes, "subtype_diff", target.SubtypeDiff, current.SubtypeDiff)
	return changes
}

// addDeclaredRangeChange records a difference only when the target declares the
// attribute. PostgreSQL reports these as bare identifiers, so the comparison is
// case-insensitive on trimmed text rather than type canonicalization.
func addDeclaredRangeChange(changes map[string]string, key, declared, current string) {
	declared = strings.TrimSpace(declared)
	if declared == "" {
		return
	}
	if strings.EqualFold(declared, strings.TrimSpace(current)) {
		return
	}
	changes[key] = fmt.Sprintf("%s -> %s", current, declared)
}
