package mysql

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// reportUnsupportedObjects appends the AST nodes for the declared object kinds
// this planner's targets cannot host, so each target's renderer turns them into
// the same named not-supported comment `ptah schema render` already produces.
//
// Without this the two surfaces disagreed: `schema render --dialect mysql` on a
// schema declaring an extension and a standalone sequence emitted
// `-- Extension pg_trgm not supported in MySQL` and
// `-- CREATE SEQUENCE order_number_seq not supported in mysql`, while
// `schema apply --dry-run` against live MySQL 9.7 and live MariaDB 10.11.18
// planned the CREATE TABLE alone and said nothing about either object
// (stokaro/ptah#931 items 5 and 8). Render already moved; the plan path is the
// half that had not.
//
// The nodes carry identity only. Neither renderer emits DDL for these kinds, so
// identity is all they read, and the resulting comments are stripped before
// execution by atlasschema.SplitApplyStatements.
//
// Sequences used to be withheld from SQL Server here, by name, because the SQL
// Server renderer answered a sequence node with a flat "CREATE SEQUENCE is not
// supported" -- false of an engine that has had sequences since 2012. That
// message now names Ptah's generator rather than the engine, so the hold-out
// has nothing left to protect and the target no longer decides whether the
// object is reported (stokaro/ptah#929 item 5).
//
// Roles and functions join sequences because the converter that feeds `render`
// hands both to the renderer for every target now, and a plan that says nothing
// about an object `render` names is the same disagreement between the two
// surfaces that #929 is about, pointing the other way.
func (p *Planner) reportUnsupportedObjects(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.ExtensionsAdded {
		result = append(result, ast.NewExtension(name))
	}
	for _, name := range diff.ExtensionsRemoved {
		result = append(result, ast.NewDropExtension(name))
	}
	for _, extension := range diff.ExtensionsModified {
		result = append(result, ast.NewExtension(extension.Name).SetSchema(extension.ToSchema))
	}
	for _, name := range diff.SequencesAdded {
		result = append(result, ast.NewCreateSequence(name))
	}
	for _, sequence := range diff.SequencesModified {
		result = append(result, ast.NewAlterSequence(sequence.SequenceName))
	}
	for _, name := range diff.SequencesRemoved {
		result = append(result, ast.NewDropSequence(name))
	}
	return p.reportUnsupportedRoutinesAndRoles(result, diff)
}

// reportUnsupportedRoutinesAndRoles appends the identity-only nodes for the role
// and function kinds these targets do not generate. It is split from
// reportUnsupportedObjects so that adding a kind does not push that function
// past the cyclomatic-complexity gate.
//
// Functions are here only for a target whose capability set declines them. A
// target that declares capability.Functions gets real DDL from
// planFunctions instead, which is what makes the key mean what its own doc
// comment says: a preset may claim it only where a path emits, reads back and
// plans the object.
func (p *Planner) reportUnsupportedRoutinesAndRoles(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.RolesAdded {
		result = append(result, ast.NewCreateRole(name))
	}
	for _, name := range diff.RolesRemoved {
		result = append(result, ast.NewDropRole(name))
	}
	if p.capabilities().Has(capability.Functions) {
		return result
	}
	for _, name := range diff.FunctionsAdded {
		result = append(result, ast.NewCreateFunction(name))
	}
	for _, name := range diff.FunctionsRemoved {
		result = append(result, ast.NewDropFunction(name))
	}
	return result
}

// planFunctions plans the create, replace and drop of stored functions for a
// target that hosts them.
//
// A modified function is planned as its full CREATE node, exactly as an added
// one is: the MySQL-family renderer prefixes every CREATE FUNCTION with
// DROP FUNCTION IF EXISTS, because neither engine has the single-statement
// replace form the PostgreSQL planner leans on, so one node covers both cases.
//
// A target that declines capability.Functions plans nothing here; its named
// skips come from reportUnsupportedRoutinesAndRoles.
func (p *Planner) planFunctions(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	if !p.capabilities().Has(capability.Functions) {
		return result
	}
	for _, name := range diff.FunctionsAdded {
		if fn, ok := findGeneratedFunction(generated, name); ok {
			result = append(result, fromschema.FromFunction(fn))
		}
	}
	for _, fnDiff := range diff.FunctionsModified {
		fn, ok := findGeneratedFunction(generated, fnDiff.FunctionName)
		if !ok {
			continue
		}
		node := fromschema.FromFunction(fn)
		node.SetComment(fmt.Sprintf("Modify function %s: %s",
			fn.Name, strings.Join(slices.Sorted(maps.Keys(fnDiff.Changes)), ", ")))
		result = append(result, node)
	}
	for _, name := range diff.FunctionsRemoved {
		result = append(result, ast.NewDropFunction(name).
			SetIfExists().
			SetComment("WARNING: Ensure no other objects depend on this function"))
	}
	return result
}

// findGeneratedFunction returns the desired definition the diff entry names.
// The diff carries names only, so without the definition there is no faithful
// CREATE to emit.
func findGeneratedFunction(generated *goschema.Database, name string) (goschema.Function, bool) {
	for _, fn := range generated.Functions {
		if fn.Name == name {
			return fn, true
		}
	}
	return goschema.Function{}, false
}
