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
	"go.5x5.cz/ptah/internal/mysqlroutine"
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
// A modified function is planned as TWO nodes: a DROP FUNCTION IF EXISTS
// followed by the full CREATE. Neither engine has the single-statement replace
// form the PostgreSQL planner leans on -- `CREATE OR REPLACE FUNCTION` is
// Error 1064 on MySQL 26.7.0 -- so the pair is what a replacement is here.
//
// The drop is planned rather than rendered inside the CREATE. It used to be
// the first line of VisitCreateFunction, which made one node render two
// statements, and an element of GetOrderedCreateStatements holding two
// statements is executed as one string by the compatibility dev-database path,
// where the driver's default DSN refuses it. Emitting the drop as its own node
// keeps every element a single statement without giving up the replacement.
//
// An ADDED function gets no drop. Nothing of that name is there to replace,
// and the IF EXISTS that made the old unconditional prefix safe was hiding
// that distinction rather than expressing it.
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
		changes := strings.Join(slices.Sorted(maps.Keys(fnDiff.Changes)), ", ")
		node := fromschema.FromFunction(fn)
		// The two halves of a replacement travel together or not at all.
		//
		// The renderer answers a CREATE FUNCTION whose language this target
		// cannot run with a named skip comment and no DDL. Planning the DROP
		// anyway made `schema apply` execute the drop, create nothing, and
		// report success -- the operator asked for a change and got a deletion.
		// Measured on MySQL 26.7.0 and MariaDB 12.3.2: zero rows in
		// information_schema.ROUTINES afterwards. The shape needs no exotic
		// schema, because Canonicalize defaults an omitted `language=` to
		// plpgsql, so an ordinary annotation reaches it.
		//
		// The CREATE node is still emitted, and that is deliberate: it renders
		// the skip comment, so the plan says which function was left alone
		// instead of silently omitting it. Nothing executable is produced.
		if !mysqlroutine.RunsLanguage(fn.Language) {
			node.SetComment(fmt.Sprintf(
				"Function %s differs (%s) but its language is not one this target runs; "+
					"left unchanged, and NOT dropped", fn.Name, changes))
			result = append(result, node)
			continue
		}
		result = append(result, ast.NewDropFunction(fn.Name).
			SetIfExists().
			SetComment(fmt.Sprintf("Replace function %s: %s", fn.Name, changes)))
		node.SetComment(fmt.Sprintf("Modify function %s: %s", fn.Name, changes))
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
