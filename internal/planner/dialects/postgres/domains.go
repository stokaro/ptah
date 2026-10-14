package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
	"ptah.run/internal/typeorder"
	"ptah.run/migration/schemadiff/difftypes"
)

// alterableDomainChanges names the domain attributes PostgreSQL changes in
// place. Everything absent from this set is reconciled by dropping the domain
// and creating it again.
//
// The base type is the attribute that is not here and cannot be: PostgreSQL has
// no ALTER DOMAIN ... TYPE, so a domain whose underlying type changed is a
// different domain and has to be rebuilt.
var alterableDomainChanges = map[string]struct{}{
	"check":    {},
	"default":  {},
	"not_null": {},
}

// domainIsAlterableInPlace reports whether every recorded change has an ALTER
// DOMAIN form.
//
// A domain that mixes an alterable change with a base-type change is not
// alterable: the rebuild carries the whole declaration, so emitting an ALTER
// beside it would be a statement against a domain that no longer exists by the
// time it runs.
func domainIsAlterableInPlace(domainDiff difftypes.DomainDiff) bool {
	if len(domainDiff.Changes) == 0 {
		return false
	}
	for change := range domainDiff.Changes {
		if _, alterable := alterableDomainChanges[change]; !alterable {
			return false
		}
	}
	return true
}

// domainIsAlteredInPlace reports whether this domain is changed with ALTER
// DOMAIN rather than rebuilt.
//
// Both halves have to hold, as for a composite: every change has an ALTER
// DOMAIN form, and the domain does not name a type this plan rebuilds. A
// domain over a rebuilt composite has to go too, whatever its own change: the
// composite's DROP is non-CASCADE, and PostgreSQL 18.6 refuses it with
// `cannot drop type c because other objects depend on it / DETAIL: type d
// depends on type c` while the domain still names it.
func domainIsAlteredInPlace(domainDiff difftypes.DomainDiff, rebuilt map[string]struct{}) bool {
	if !domainIsAlterableInPlace(domainDiff) {
		return false
	}
	_, mustRebuild := rebuilt[deporder.NormalizeTypeReference(domainTypeName(domainDiff))]
	return !mustRebuild
}

// compositeIsAlterableInPlace reports whether this composite may be changed
// with ALTER TYPE rather than rebuilt.
//
// Both halves have to hold: the comparator found a delta ALTER can reach, and
// nothing this plan rebuilds is left depending on it.
func compositeIsAlterableInPlace(
	compositeDiff difftypes.CompositeTypeDiff,
	rebuilt map[string]struct{},
) bool {
	if !compositeHasAttributeDelta(compositeDiff) {
		return false
	}
	_, mustRebuild := rebuilt[deporder.NormalizeTypeReference(compositeTypeName(compositeDiff))]
	return !mustRebuild
}

// alterModifiedDomains emits the in-place ALTER DOMAIN statements for every
// modified domain that needs no rebuild.
//
// Before this the only route for a changed domain was drop and recreate, and
// PostgreSQL refuses a non-CASCADE drop of a domain a column uses. A changed
// CHECK was therefore not merely inconvenient to apply -- it could not be
// applied at all on any domain in use, which is every domain that matters
// (stokaro/ptah#1717).
func (p *Planner) alterModifiedDomains(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	rebuilt := p.rebuiltUserTypes(diff)
	for _, domainDiff := range diff.DomainsModified {
		if !domainIsAlteredInPlace(domainDiff, rebuilt) {
			continue
		}
		// The domain travels WITH the change (stokaro/ptah#2315).
		domain := domainDiff.Desired
		if domain.Name == "" {
			// The same rule the rebuild path follows: a definition the change
			// does not carry is not one to plan statements from.
			result = append(result, unrecreatableUserTypeComment("domain", domainDiff.DomainName))
			continue
		}
		if node := alterDomainNode(domainDiff, domain); node != nil {
			result = append(result, node)
		}
	}
	return result
}

// alterDomainNode builds the ALTER DOMAIN node for one modified domain, or nil
// when nothing in the change set produced a statement.
//
// The order of the operations is the order they have to run in. A replaced
// CHECK drops the old constraint before adding the new one, and NOT NULL is set
// after the CHECK it may depend on rather than before it.
func alterDomainNode(domainDiff difftypes.DomainDiff, domain schemamodel.Domain) ast.Node {
	node := ast.NewAlterType(domainDiff.DomainName)
	operations := 0

	if _, changed := domainDiff.Changes["check"]; changed && domain.Check != "" {
		// Every stored constraint goes, not only the first: a domain holding
		// two constraints against a declaration holding one is a domain with
		// one constraint too many, and leaving the extra would make the next
		// comparison ask for the same replacement again.
		for _, name := range domainDiff.CurrentCheckConstraints {
			node.AddOperation(ast.NewDropDomainConstraintOperation(name))
			operations++
		}
		node.AddOperation(ast.NewAddDomainConstraintOperation(domain.Check))
		operations++
	}

	if _, changed := domainDiff.Changes["default"]; changed {
		if expression := declaredDomainDefault(domain); expression != "" {
			node.AddOperation(ast.NewSetDomainDefaultOperation(expression))
			operations++
		}
	}

	if _, changed := domainDiff.Changes["not_null"]; changed {
		node.AddOperation(ast.NewDomainNotNullOperation(domain.NotNull))
		operations++
	}

	if operations == 0 {
		return nil
	}
	return node
}

// declaredDomainDefault renders the declared default as SQL, or empty when the
// declaration carries none.
//
// A domain keeps a literal and an expression apart. Only the expression is
// already SQL; a literal is the value itself and has to be quoted before a
// server can parse the statement carrying it.
func declaredDomainDefault(domain schemamodel.Domain) string {
	if domain.DefaultExpr != "" {
		return domain.DefaultExpr
	}
	if domain.Default == "" {
		return ""
	}
	return sqlident.StringLiteral(domain.Default)
}

// compositeHasAttributeDelta reports whether the comparator found a field-level
// delta ALTER TYPE can reach.
//
// The comparator sets the delta only when applying it lands exactly on the
// declared shape, so this is a presence check rather than a second judgement
// about ordering (stokaro/ptah#1717).
func compositeHasAttributeDelta(compositeDiff difftypes.CompositeTypeDiff) bool {
	return len(compositeDiff.AttributesAdded) > 0 || len(compositeDiff.AttributesRemoved) > 0
}

// rebuiltUserTypes closes replacement requirements across common and owned
// types. Each changed dependent must leave before a non-CASCADE replacement.
func (p *Planner) rebuiltUserTypes(diff *difftypes.SchemaDiff) map[string]struct{} {
	var changed, replacements []string
	for _, domain := range diff.DomainsModified {
		name := domainTypeName(domain)
		changed = append(changed, name)
		if !domainIsAlterableInPlace(domain) {
			replacements = append(replacements, name)
		}
	}
	for _, composite := range diff.CompositeTypesModified {
		name := compositeTypeName(composite)
		changed = append(changed, name)
		if !compositeHasAttributeDelta(composite) {
			replacements = append(replacements, name)
		}
	}
	for _, change := range diff.FeatureChanges {
		creator, creates := change.Value.(schemaext.TypeCreator)
		observer, observes := change.Value.(schemaext.TypeObserver)
		if !creates || !observes {
			continue
		}
		_, after := creator.CreatedType(change.Subject)
		typ, before := observer.ObservedType(change.Subject)
		if !before || !after {
			continue
		}
		name := typeorder.Name(typ.ID)
		changed = append(changed, name)
		if value, ok := change.Value.(schemaext.TypeRebuilder); ok && value.RebuildsType() {
			replacements = append(replacements, name)
		}
	}
	return typeorder.RebuildNames(typeorder.Vocabulary(nil, p.typeContext(diff).Current), changed, replacements)
}

func domainTypeName(change difftypes.DomainDiff) string {
	if change.Desired.Name != "" {
		return tableref.CanonicalExact(change.Desired.Schema, change.Desired.Name)
	}
	return change.DomainName
}

func compositeTypeName(change difftypes.CompositeTypeDiff) string {
	if change.Desired.Name != "" {
		return tableref.CanonicalExact(change.Desired.Schema, change.Desired.Name)
	}
	return change.TypeName
}

// alterModifiedCompositeTypes emits the in-place ALTER TYPE statements for every
// modified composite that needs no rebuild.
//
// PostgreSQL takes ADD ATTRIBUTE and DROP ATTRIBUTE on a composite a table
// column already uses, and refuses to DROP the type itself in exactly that
// case. Before this, a composite gaining a field was reconciled by dropping and
// recreating it, which is the one thing the engine will not do there.
func (p *Planner) alterModifiedCompositeTypes(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	rebuilt := p.rebuiltUserTypes(diff)
	for _, compositeDiff := range diff.CompositeTypesModified {
		if !compositeIsAlterableInPlace(compositeDiff, rebuilt) {
			continue
		}
		if compositeDiff.Desired.Name == "" {
			result = append(result, unrecreatableUserTypeComment("composite type", compositeDiff.TypeName))
			continue
		}
		node := ast.NewAlterType(compositeDiff.TypeName)
		// Removals first: a field can leave and another arrive in one
		// modification, and dropping before adding keeps a reused name from
		// colliding with the one still there.
		for _, name := range compositeDiff.AttributesRemoved {
			node.AddOperation(ast.NewDropCompositeAttributeOperation(name))
		}
		for _, attribute := range compositeDiff.AttributesAdded {
			node.AddOperation(ast.NewAddCompositeAttributeOperation(attribute.Name, attribute.Type))
		}
		result = append(result, node)
	}
	return result
}
