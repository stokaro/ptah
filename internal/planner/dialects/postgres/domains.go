package postgres

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/schemadiff/types"
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
func domainIsAlterableInPlace(domainDiff types.DomainDiff) bool {
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

// alterModifiedDomains emits the in-place ALTER DOMAIN statements for every
// modified domain that needs no rebuild.
//
// Before this the only route for a changed domain was drop and recreate, and
// PostgreSQL refuses a non-CASCADE drop of a domain a column uses. A changed
// CHECK was therefore not merely inconvenient to apply -- it could not be
// applied at all on any domain in use, which is every domain that matters
// (stokaro/ptah#1717).
func (p *Planner) alterModifiedDomains(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, domainDiff := range diff.DomainsModified {
		if !domainIsAlterableInPlace(domainDiff) {
			continue
		}
		domain := findDomain(generated.Domains, domainDiff.DomainName, semantics)
		if domain == nil {
			// The same rule the rebuild path follows: a definition the schema
			// does not hold is not one to plan statements from.
			result = append(result, unrecreatableUserTypeComment("domain", domainDiff.DomainName))
			continue
		}
		if node := alterDomainNode(domainDiff, *domain); node != nil {
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
func alterDomainNode(domainDiff types.DomainDiff, domain goschema.Domain) ast.Node {
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
func declaredDomainDefault(domain goschema.Domain) string {
	if domain.DefaultExpr != "" {
		return domain.DefaultExpr
	}
	if domain.Default == "" {
		return ""
	}
	return quoteDomainDefaultLiteral(domain.Default)
}

// quoteDomainDefaultLiteral renders a literal default as a SQL string literal.
func quoteDomainDefaultLiteral(literal string) string {
	quoted := make([]byte, 0, len(literal)+2)
	quoted = append(quoted, '\'')
	for i := 0; i < len(literal); i++ {
		if literal[i] == '\'' {
			quoted = append(quoted, '\'')
		}
		quoted = append(quoted, literal[i])
	}
	return string(append(quoted, '\''))
}

// compositeIsAlterableInPlace reports whether the comparator found a field-level
// delta ALTER TYPE can reach.
//
// The comparator sets the delta only when applying it lands exactly on the
// declared shape, so this is a presence check rather than a second judgement
// about ordering (stokaro/ptah#1717).
func compositeIsAlterableInPlace(compositeDiff types.CompositeTypeDiff) bool {
	return len(compositeDiff.AttributesAdded) > 0 || len(compositeDiff.AttributesRemoved) > 0
}

// alterModifiedCompositeTypes emits the in-place ALTER TYPE statements for every
// modified composite that needs no rebuild.
//
// PostgreSQL takes ADD ATTRIBUTE and DROP ATTRIBUTE on a composite a table
// column already uses, and refuses to DROP the type itself in exactly that
// case. Before this, a composite gaining a field was reconciled by dropping and
// recreating it, which is the one thing the engine will not do there.
func (p *Planner) alterModifiedCompositeTypes(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, compositeDiff := range diff.CompositeTypesModified {
		if !compositeIsAlterableInPlace(compositeDiff) {
			continue
		}
		if findCompositeType(generated.CompositeTypes, compositeDiff.TypeName, semantics) == nil {
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
