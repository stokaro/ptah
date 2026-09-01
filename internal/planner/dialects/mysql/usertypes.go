package mysql

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/modelast"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// planDomains emits the CREATE DOMAIN statements a diff adds, where the target
// has domains at all.
//
// The gate is the capability rather than the dialect, because the two Oracle
// lines disagree: 23 has CREATE DOMAIN and 21 answers ORA-00901, so the same
// planner has to plan one and not the other (stokaro/ptah#1920).
//
// Domains go before tables for the reason sequences do: a column may be
// declared with the domain as its type, and Oracle resolves that name through
// the catalog when the table is created.
//
// A modified domain is deliberately absent. Oracle has ALTER DOMAIN, and what
// it alters is not what DomainDiff carries -- adding and dropping a named
// constraint, not changing a base type or a default -- so planning a rename of
// the whole shape would emit a statement that changes something else. It stays
// unplanned until there is a measurement to write it from.
func (p *Planner) planDomains(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.DomainTypes) {
		return result
	}
	// The domain travels WITH the change, so this renders what it was handed
	// (stokaro/ptah#2315). It used to index the desired schema by Name and look
	// the entry up by QualifiedName, which are the same string only for a
	// domain that names no schema: a declaration carrying one produced no node
	// at all, and the plan reported success. Measured on the Oracle preset --
	// `zip` planned one statement and `app.zip` planned none.
	for _, domain := range diff.DomainsAdded {
		result = append(result, modelast.FromDomain(domain))
	}
	return result
}

// planCompositeTypes emits the composite types a diff adds or changes, where
// the target has them at all.
//
// One statement covers both halves, and that is the engine's own doing:
// `CREATE OR REPLACE TYPE t AS OBJECT (...)` creates a type that is not there
// and rewrites one that is. Measured on 23.26.2.0.0, replacing a type from one
// attribute to two succeeds while nothing uses it, and answers ORA-02303 the
// moment a table column does -- changing nothing. That refusal is the server
// declining to leave a column naming a shape it no longer has, and it is left
// as the answer rather than worked around.
//
// Composites go before tables for the reason domains do: a column may be
// declared with the type, and Oracle resolves that name through the catalog
// when the table is created.
func (p *Planner) planCompositeTypes(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.CompositeTypes) {
		return result
	}
	// Both halves travel WITH their change (stokaro/ptah#2315), and for the
	// reason planDomains records: the lookup this replaces keyed the desired
	// schema by Name and searched it by qualified name, so a composite
	// declaring a schema was silently left unplanned.
	declarations := make([]schemamodel.CompositeType, 0,
		len(diff.CompositeTypesAdded)+len(diff.CompositeTypesModified))
	declarations = append(declarations, diff.CompositeTypesAdded...)
	for _, changed := range diff.CompositeTypesModified {
		if changed.Desired.Name != "" {
			declarations = append(declarations, changed.Desired)
		}
	}
	for _, composite := range declarations {
		result = append(result, modelast.FromCompositeType(composite))
	}
	return result
}

// removeCompositeTypes drops the composite types a diff removes, after the
// tables whose columns were typed by them.
//
// The position is the same one removeDomains takes and for the same measured
// reason: on 23.26.2.0.0, dropping a type a column still uses answers
// ORA-02303, so a drop emitted with the unsupported-object reports at step 0
// would fail the plan halfway through.
func (p *Planner) removeCompositeTypes(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.CompositeTypes) {
		return result
	}
	for _, composite := range diff.CompositeTypesRemoved {
		result = append(result, ast.NewDropType(composite.QualifiedName()))
	}
	return result
}

// removeDomains drops the domains a diff removes, after the tables that used
// them.
//
// The position is Oracle's own answer rather than a preference. Measured on
// 23.26.2.0.0, dropping a domain a table still uses answers
//
//	ORA-11502: The domain EMAIL_D to be dropped has dependent objects.
//
// and the first plan this issue produced hit exactly that: the drop was
// emitted with the unsupported-object reports at step 0, which was harmless
// while it rendered a comment and is not once it renders a statement.
func (p *Planner) removeDomains(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.DomainTypes) {
		return result
	}
	for _, domain := range diff.DomainsRemoved {
		result = append(result, ast.NewDropType(domain.QualifiedName()).SetDomain())
	}
	return result
}

// reportRemovedUserTypes names the domains, composite types and range types a
// desired schema no longer declares.
//
// The creation half of this is already refused before any SQL is rendered:
// usertypescope.ValidateDeclared stops a schema declaring a type this target
// cannot host, because a named skip would leave the declaration's own table
// behind naming a type the server has no definition of (stokaro/ptah#1717).
// Removal cannot be refused the same way -- there is no declaration to refuse,
// and dropping a type this target never created is not an error -- so it is
// named instead, which is what every other unhostable kind here does.
//
// The path is not reachable today, and saying so is the point of writing it.
// No reader in this family reports a domain, a composite or a range, so the
// diff cannot carry one; the collections were simply unwalked, which is how
// #1628 closed with grants and row-level security fixed and these three still
// dropped in silence (stokaro/ptah#1708). A reader that learns them later gets
// a sentence rather than nothing, without anyone remembering to come back here.
func (p *Planner) reportRemovedUserTypes(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// A target that HOSTS domains drops them late instead, after the tables
	// whose columns are typed by them -- see removeDomains. Naming them here
	// as well would emit the statement twice.
	if !p.capabilities().Has(capability.DomainTypes) {
		for _, domain := range diff.DomainsRemoved {
			result = append(result, ast.NewDropType(domain.QualifiedName()).SetDomain())
		}
	}
	// A target that HOSTS composite types drops them late instead, after the
	// tables whose columns are typed by them -- see removeCompositeTypes.
	if !p.capabilities().Has(capability.CompositeTypes) {
		for _, composite := range diff.CompositeTypesRemoved {
			result = append(result, ast.NewDropType(composite.QualifiedName()))
		}
	}
	for _, rangeType := range diff.RangesRemoved {
		result = append(result, ast.NewDropType(rangeType.QualifiedName()))
	}
	return result
}
