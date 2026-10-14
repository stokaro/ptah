package compare

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/pgtypealias"
	"ptah.run/migration/schemadiff/difftypes"
)

// Domains compares PostgreSQL domain types between the target schema and the
// current database. Only options the target explicitly declares are compared, so
// undeclared attributes (which the catalog always populates) do not churn.
func Domains(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	domainsWithSemantics(desired, database, diff, cov, identifier.ForDialect(""), nil)
}

// DomainsWithSemantics compares domain identity using the target database's
// resolved default schema and identifier rules.
func DomainsWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
	semantics identifier.Semantics,
	expressions map[string]config.DomainExpression,
) {
	domainsWithSemantics(desired, database, diff, cov, semantics.Normalize(""), expressions)
}

func domainsWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
	semantics identifier.Semantics,
	expressions map[string]config.DomainExpression,
) {
	generatedDomains := make(map[objectIdentity]schemamodel.Domain, len(desired.Domains))
	generatedNames := make(map[objectIdentity]string, len(desired.Domains))
	for _, domain := range desired.Domains {
		identity := newObjectIdentity(objectidentity.KindDomain, domain.Schema, domain.Name, semantics)
		generatedDomains[identity] = domain
		generatedNames[identity] = domain.QualifiedName()
	}
	databaseDomains := make(map[objectIdentity]catalog.Domain, len(database.Domains))
	databaseNames := make(map[objectIdentity]string, len(database.Domains))
	for _, domain := range database.Domains {
		identity := newObjectIdentity(objectidentity.KindDomain, domain.Schema, domain.Name, semantics)
		databaseDomains[identity] = domain
		databaseNames[identity] = domain.QualifiedName()
	}

	for identity, target := range generatedDomains {
		if current, exists := databaseDomains[identity]; exists {
			if changes := domainChanges(target, current, expressions[generatedNames[identity]]); len(changes) > 0 {
				// CurrentBaseType is the from-side of the comparison carried as
				// a type spelling: the recreate path's non-CASCADE DROP runs
				// against this database, so it is these references that can
				// block it, not the target's.
				diff.DomainsModified = append(diff.DomainsModified, difftypes.DomainDiff{
					DomainName:              generatedNames[identity],
					Changes:                 changes,
					CurrentBaseType:         current.BaseType,
					CurrentCheckConstraints: domainCheckConstraintNames(current),
					Desired:                 target,
				})
			}
			continue
		}
		diff.DomainsAdded = append(diff.DomainsAdded, target)
	}
	for identity, current := range databaseDomains {
		if _, exists := generatedDomains[identity]; !exists {
			diff.DomainsRemoved = append(diff.DomainsRemoved, domainFromCatalog(current))
		}
	}

	// An extension owns some of the domains, composites and ranges in a schema.
	// A reader that leaves those to their extension has not said the schema
	// lacks them, and a document that omitted the extension block has not said
	// the type is unwanted (stokaro/ptah#1294, stokaro/ptah#1276).
	//
	// `CREATE DOMAIN` and `CREATE TYPE` have no conditional form, so an
	// undecidable addition is recorded on the coverage rather than dropped.
	keptDomains, withheldDomains := keepPlannedAdditions(cov,
		coverage.Domain, diff.DomainsAdded, domainSpelling, domainDisplay, unguardedCreations(),
	)
	diff.DomainsAdded = keptDomains
	cov.recordUndecidedAdditions(withheldDomains)
	diff.DomainsRemoved = keepPlannedRemovals(cov, coverage.Domain, diff.DomainsRemoved, domainSpelling)

	sortDomains(diff.DomainsAdded)
	sortDomains(diff.DomainsRemoved)
	sort.Slice(diff.DomainsModified, func(i, j int) bool {
		return diff.DomainsModified[i].DomainName < diff.DomainsModified[j].DomainName
	})
}

// domainChanges compares the reconcilable options of a domain: its base type
// (canonicalized so alias spellings such as VARCHAR vs character varying do not
// churn), NOT NULL, and -- when a server has normalized the declaration --
// CHECK and DEFAULT.
//
// Leaving CHECK and DEFAULT create-only has a sound reason: PostgreSQL stores a
// parsed CHECK and prints it back from the parse tree, so the declaration
// behind `(VALUE = ANY (ARRAY['x'::text]))` is `VALUE IN ('x')` and the two
// never compare equal. What it costs is silence. A changed constraint then
// produces no diff and no diagnostic, so `schema apply` reports a synced schema
// over a database still enforcing the old rule (stokaro/ptah#1717).
//
// A resolved expression is the declaration after the same round trip through
// the same server, so the two sides are the same language and a difference is a
// real one. Without one -- an offline comparison has no server to ask -- both
// attributes stay uncompared, which is the behavior this replaces.
//
// The comparison is deliberately one-directional: a declaration that states no
// CHECK and no DEFAULT is not read as a request to remove the ones the catalog
// holds. That is this comparator's rule for every attribute a declaration may
// leave out, and inverting it here would make adopting Ptah over an existing
// database drop constraints nobody asked about.
func domainChanges(
	target schemamodel.Domain,
	current catalog.Domain,
	expression config.DomainExpression,
) map[string]string {
	changes := make(map[string]string)
	if target.BaseType != "" && pgtypealias.Canonical(target.BaseType) != pgtypealias.Canonical(current.BaseType) {
		changes["type"] = fmt.Sprintf("%s -> %s", current.BaseType, target.BaseType)
	}
	if target.NotNull != current.NotNull {
		changes["not_null"] = fmt.Sprintf("%t -> %t", current.NotNull, target.NotNull)
	}
	if expression.Resolved {
		if expression.Check != "" && !domainCheckMatches(expression.Check, current) {
			changes["check"] = fmt.Sprintf("%s -> %s", current.Check, expression.Check)
		}
		if expression.Default != "" && expression.Default != current.Default {
			changes["default"] = fmt.Sprintf("%s -> %s", current.Default, expression.Default)
		}
	}
	return changes
}

// domainCheckMatches reports whether the catalog enforces exactly the declared
// constraint and nothing else.
//
// It compares against the constraints one by one rather than against the joined
// expression the reader also carries. A domain holding two constraints joins as
// `(a) AND (b)`, and a declaration of `a AND b` normalizes to `((a) AND (b))`:
// equal in meaning, different by one pair of parentheses, and comparing the
// joined forms would plan a replacement on every run and converge on nothing.
// One declared constraint against one stored constraint has no such seam.
//
// A domain the reader could not enumerate constraints for -- an older server
// without the catalog functions -- falls back to the joined form, where a false
// difference is still better than the silence it replaces.
func domainCheckMatches(declared string, current catalog.Domain) bool {
	if current.CheckConstraints == nil {
		return declared == current.Check
	}
	return len(current.CheckConstraints) == 1 &&
		current.CheckConstraints[0].Expression == declared
}

// domainCheckConstraintNames lists the catalog's names for a domain's CHECK
// constraints, so a planner can remove them by name.
func domainCheckConstraintNames(current catalog.Domain) []string {
	if len(current.CheckConstraints) == 0 {
		return nil
	}
	names := make([]string, 0, len(current.CheckConstraints))
	for _, check := range current.CheckConstraints {
		names = append(names, check.Name)
	}
	return names
}

// CompositeTypes compares PostgreSQL composite types between the target schema
// and the current database.
func CompositeTypes(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	compositeTypesWithSemantics(desired, database, diff, cov, identifier.ForDialect(""))
}

// CompositeTypesWithSemantics compares composite-type identity using the
// target database's resolved default schema and identifier rules.
func CompositeTypesWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
	semantics identifier.Semantics,
) {
	compositeTypesWithSemantics(desired, database, diff, cov, semantics.Normalize(""))
}

func compositeTypesWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
	semantics identifier.Semantics,
) {
	generatedTypes := make(map[objectIdentity]schemamodel.CompositeType, len(desired.CompositeTypes))
	generatedNames := make(map[objectIdentity]string, len(desired.CompositeTypes))
	for _, composite := range desired.CompositeTypes {
		identity := newObjectIdentity(objectidentity.KindComposite, composite.Schema, composite.Name, semantics)
		generatedTypes[identity] = composite
		generatedNames[identity] = composite.QualifiedName()
	}
	databaseTypes := make(map[objectIdentity]catalog.CompositeType, len(database.Composites))
	databaseNames := make(map[objectIdentity]string, len(database.Composites))
	for _, composite := range database.Composites {
		identity := newObjectIdentity(objectidentity.KindComposite, composite.Schema, composite.Name, semantics)
		databaseTypes[identity] = composite
		databaseNames[identity] = composite.QualifiedName()
	}

	for identity, target := range generatedTypes {
		current, exists := databaseTypes[identity]
		if !exists {
			diff.CompositeTypesAdded = append(diff.CompositeTypesAdded, target)
			continue
		}
		if targetFields, currentFields := compositeFieldList(target), dbCompositeFieldList(current); targetFields != currentFields {
			// CurrentFieldTypes is the from-side of the comparison carried as
			// type spellings, for the reason given on the domain branch above.
			added, removed := compositeAttributeDelta(target, current)
			diff.CompositeTypesModified = append(diff.CompositeTypesModified, difftypes.CompositeTypeDiff{
				TypeName:          generatedNames[identity],
				Changes:           map[string]string{"fields": fmt.Sprintf("%s -> %s", currentFields, targetFields)},
				CurrentFieldTypes: dbCompositeFieldTypes(current),
				AttributesAdded:   added,
				AttributesRemoved: removed,
				Desired:           target,
			})
		}
	}
	for identity, current := range databaseTypes {
		if _, exists := generatedTypes[identity]; !exists {
			diff.CompositeTypesRemoved = append(diff.CompositeTypesRemoved, compositeFromCatalog(current))
		}
	}

	keptComposites, withheldComposites := keepPlannedAdditions(cov,
		coverage.Composite, diff.CompositeTypesAdded, compositeSpelling, compositeDisplay, unguardedCreations(),
	)
	diff.CompositeTypesAdded = keptComposites
	cov.recordUndecidedAdditions(withheldComposites)
	diff.CompositeTypesRemoved = keepPlannedRemovals(cov, coverage.Composite, diff.CompositeTypesRemoved, compositeSpelling)

	sortComposites(diff.CompositeTypesAdded)
	sortComposites(diff.CompositeTypesRemoved)
	sort.Slice(diff.CompositeTypesModified, func(i, j int) bool {
		return diff.CompositeTypesModified[i].TypeName < diff.CompositeTypesModified[j].TypeName
	})
}

// compositeAttributeDelta reports the fields to add and remove, or nothing when
// ALTER TYPE cannot reach the declared shape.
//
// PostgreSQL appends: a new attribute always lands last, whatever position the
// declaration gives it. So the delta is only usable when dropping the removed
// fields and appending the added ones produces exactly the declared list. This
// simulates that and returns nothing when it does not match -- a rebuild then
// carries the declared order, which is the only way to reach it.
//
// A field present on both sides with a different type also returns nothing.
// ALTER TYPE ... ALTER ATTRIBUTE is refused on a composite a column uses,
// measured on PostgreSQL 18.4 with CASCADE and without:
// `cannot alter type "addr" because column "uses_addr.a" uses it`
// (stokaro/ptah#1717).
func compositeAttributeDelta(
	target schemamodel.CompositeType,
	current catalog.CompositeType,
) ([]difftypes.CompositeAttribute, []string) {
	declaredTypes := make(map[string]string, len(target.Fields))
	for _, field := range target.Fields {
		declaredTypes[strings.ToLower(field.Name)] = pgtypealias.Canonical(field.Type)
	}
	currentTypes := make(map[string]string, len(current.Fields))
	for _, field := range current.Fields {
		currentTypes[strings.ToLower(field.Name)] = pgtypealias.Canonical(field.Type)
	}

	var removed []string
	survivors := make([]string, 0, len(current.Fields))
	for _, field := range current.Fields {
		name := strings.ToLower(field.Name)
		declaredType, kept := declaredTypes[name]
		if !kept {
			removed = append(removed, field.Name)
			continue
		}
		if declaredType != currentTypes[name] {
			return nil, nil
		}
		survivors = append(survivors, name)
	}

	var added []difftypes.CompositeAttribute
	for _, field := range target.Fields {
		if _, held := currentTypes[strings.ToLower(field.Name)]; held {
			continue
		}
		added = append(added, difftypes.CompositeAttribute{Name: field.Name, Type: field.Type})
	}
	if len(added) == 0 && len(removed) == 0 {
		return nil, nil
	}

	// The shape ALTER TYPE would actually leave behind: survivors in catalog
	// order, then the additions in declaration order.
	reached := make([]string, 0, len(target.Fields))
	reached = append(reached, survivors...)
	for _, field := range added {
		reached = append(reached, strings.ToLower(field.Name))
	}
	declaredOrder := make([]string, 0, len(target.Fields))
	for _, field := range target.Fields {
		declaredOrder = append(declaredOrder, strings.ToLower(field.Name))
	}
	if !slices.Equal(reached, declaredOrder) {
		return nil, nil
	}
	return added, removed
}

func compositeFieldList(composite schemamodel.CompositeType) string {
	parts := make([]string, len(composite.Fields))
	for i, field := range composite.Fields {
		parts[i] = strings.ToLower(field.Name) + " " + pgtypealias.Canonical(field.Type)
	}
	return strings.Join(parts, ", ")
}

// dbCompositeFieldTypes returns the field types the database holds for a
// composite, in field order and in the catalog's own spelling. Unlike
// dbCompositeFieldList these are not normalized: they are resolved against
// other type names later, and canonicalization would rewrite a user-defined
// name that happens to collide with a built-in alias.
func dbCompositeFieldTypes(composite catalog.CompositeType) []string {
	fieldTypes := make([]string, len(composite.Fields))
	for i, field := range composite.Fields {
		fieldTypes[i] = field.Type
	}
	return fieldTypes
}

func dbCompositeFieldList(composite catalog.CompositeType) string {
	parts := make([]string, len(composite.Fields))
	for i, field := range composite.Fields {
		parts[i] = strings.ToLower(field.Name) + " " + pgtypealias.Canonical(field.Type)
	}
	return strings.Join(parts, ", ")
}

// compositeFromCatalog carries a composite type the database reported into the
// shape the diff holds. The properties a read can establish -- name, schema,
// ordered fields and comment -- are exactly the ones the catalog has, and
// CompositeField is the same {Name, Type} pair on both sides.
func compositeFromCatalog(reported catalog.CompositeType) schemamodel.CompositeType {
	fields := make([]schemamodel.CompositeField, 0, len(reported.Fields))
	for _, field := range reported.Fields {
		fields = append(fields, schemamodel.CompositeField{Name: field.Name, Type: field.Type})
	}
	return schemamodel.CompositeType{
		Name:    reported.Name,
		Schema:  reported.Schema,
		Fields:  fields,
		Comment: reported.Comment,
	}
}

// compositeSpelling is qualifiedName for a change that carries its operand.
func compositeSpelling(composite schemamodel.CompositeType) (schema string, spellings []string) {
	return qualifiedName(composite.QualifiedName())
}

// compositeDisplay names one for a record a person reads.
func compositeDisplay(composite schemamodel.CompositeType) string { return composite.QualifiedName() }

// sortComposites orders by the key the name lists were sorted on.
func sortComposites(composites difftypes.CompositeTypeChanges) {
	sort.Slice(composites, func(i, j int) bool {
		return composites[i].QualifiedName() < composites[j].QualifiedName()
	})
}

// domainFromCatalog carries a domain the database reported into the shape the
// diff holds.
//
// The default is the one field a read cannot transcribe: the catalog has one
// string where the model has a literal value and an expression, and which one
// it is decides whether the renderer quotes it. The rule is
// sqlutil.DefaultLooksLikeExpression, shared with the column path rather than
// copied from it -- a second copy would answer differently the first time
// either learned something.
func domainFromCatalog(reported catalog.Domain) schemamodel.Domain {
	domain := schemamodel.Domain{
		Name:     reported.Name,
		Schema:   reported.Schema,
		BaseType: reported.BaseType,
		NotNull:  reported.NotNull,
		Check:    reported.Check,
		Comment:  reported.Comment,
	}
	if strings.TrimSpace(reported.Default) != "" {
		if sqlutil.DefaultLooksLikeExpression(reported.Default) {
			domain.DefaultExpr = reported.Default
		} else {
			domain.Default = reported.Default
		}
	}
	return domain
}

// domainSpelling is qualifiedName for a change that carries its operand.
func domainSpelling(domain schemamodel.Domain) (schema string, spellings []string) {
	return qualifiedName(domain.QualifiedName())
}

// domainDisplay names one for a record a person reads.
func domainDisplay(domain schemamodel.Domain) string { return domain.QualifiedName() }

// sortDomains orders by the key the name lists were sorted on.
func sortDomains(domains difftypes.DomainChanges) {
	sort.Slice(domains, func(i, j int) bool { return domains[i].QualifiedName() < domains[j].QualifiedName() })
}
