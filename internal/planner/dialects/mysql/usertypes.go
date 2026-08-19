package mysql

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

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
func (p *Planner) reportRemovedUserTypes(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.DomainsRemoved {
		result = append(result, ast.NewDropType(name).SetDomain())
	}
	for _, name := range diff.CompositeTypesRemoved {
		result = append(result, ast.NewDropType(name))
	}
	for _, name := range diff.RangesRemoved {
		result = append(result, ast.NewDropType(name))
	}
	return result
}
