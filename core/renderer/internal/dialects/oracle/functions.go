package oracle

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/oracleroutine"
)

// Oracle hosts standalone functions and procedures, and the header it needs is
// close enough to the shared declaration that most of it renders straight
// through. Everything below was measured on Oracle Database 23ai Free
// 23.26.2.0.0 and repeated on Oracle Database 21c Express Edition 21.3.0.0.0,
// which agreed on every answer except the two guards:
//
//   - `CREATE OR REPLACE FUNCTION` is accepted, and it is the whole
//     replacement: a modified routine needs no DROP in front of its CREATE. The
//     planner emits one anyway, and that is fine -- it is guarded, and the
//     create that follows is what puts the routine back.
//   - Plain `CREATE FUNCTION` over an existing name is ORA-00955.
//   - `CREATE FUNCTION IF NOT EXISTS` is accepted on 23 and is a trap on 21:
//     there it creates a routine literally named IF, reporting "Function
//     created with compilation errors". ObjectExistenceGuards is already false
//     for the 21 preset, so the guard is never written there.
//   - `DROP FUNCTION IF EXISTS` is accepted on 23 and is ORA-00933 on 21, which
//     is the same key from the other side.
//   - A PL/SQL body's inner semicolons do not end the statement: the routine
//     travels to the server as one string, and the statement splitter keeps a
//     routine body whole from the header's IS to the closing END.
//
// The body is written through unchanged. ALL_SOURCE stores the statement text
// verbatim, so what is rendered here is what comes back, and a comparison
// against the declaration that produced it is byte for byte.

// VisitCreateFunction renders an Oracle CREATE FUNCTION or CREATE PROCEDURE.
//
// A routine whose declared language this target does not run is named and
// skipped rather than refused, which is the answer the other dialects reached
// for the same situation and for a reason that applies unchanged here: one
// schema is applied across several dialects, and a declaration cannot yet say
// which dialect it belongs to. Refusing would break a workflow that works
// today. The predicate is [oracleroutine.RunsLanguage] rather than a comparison
// written here, because the planner has to reach the same answer -- it must not
// plan a drop whose create this branch is about to skip.
//
// The trap that costs an afternoon is worth naming, and the message names it:
// [schemamodel.Function.Canonicalize] defaults an unset language to plpgsql, so a
// routine annotated without `language=` lands in this branch and is skipped
// when it looks like it should have been generated.
func (r *Renderer) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	if node.IsProcedure() {
		if r.refuses(capability.Procedures, "CREATE PROCEDURE", node.Name) {
			return nil
		}
	} else if r.refuses(capability.Functions, "CREATE FUNCTION", node.Name) {
		return nil
	}
	if !oracleroutine.RunsLanguage(node.Language) {
		language := strings.ToLower(strings.TrimSpace(node.Language))
		r.w.WriteLinef(
			"-- ORACLE: CREATE FUNCTION %s declares language %s, which this target does not run; skipped.",
			escapeIdentifier(node.Name), language)
		r.w.WriteLinef(
			"--   If this body is PL/SQL, declare language=\"plsql\": an annotation that omits the")
		r.w.WriteLinef(
			"--   language is defaulted to plpgsql and is skipped here for the same reason.")
		return nil
	}
	// A parameter default is accepted by the engine and then invisible to
	// everyone. ALL_ARGUMENTS reports DEFAULTED = 'Y' and never the value, so
	// the reader cannot see it, and a routine created with one would be
	// reported as differing from its own declaration on every run. Naming it
	// and creating nothing is the only answer that does not lie.
	if oracleroutine.ParameterCarriesDefault(node.Parameters) {
		r.w.WriteLinef(
			"-- ORACLE: %s %q declares a parameter default, which the catalog does not "+
				"report back, so it would be replanned on every run; it is not created.",
			routineWord(node), node.Name)
		return nil
	}
	determinism, err := oracleroutine.DeterminismClause(node.Volatility)
	if err != nil {
		return unsupportedFeaturef("CREATE FUNCTION %s: %s", node.Name, err)
	}
	security, err := oracleroutine.SecurityClause(node.Security)
	if err != nil {
		return unsupportedFeaturef("CREATE FUNCTION %s: %s", node.Name, err)
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("%s", strings.Join(routineHeader(node, determinism, security), " "))
	r.w.WriteLinef("%s", strings.TrimSpace(node.Body))
	return nil
}

// routineHeader assembles the words that precede the body.
//
// The clause order is the one Oracle accepts and the one it stores: measured on
// 23.26.2.0.0, `RETURN NUMBER DETERMINISTIC AUTHID CURRENT_USER IS` is created
// and read back from ALL_SOURCE in exactly that order. A procedure has no
// RETURN clause -- that is what separates the two statements -- and takes its
// parameters in the same parentheses a function does, unlike T-SQL.
//
// An empty parameter list is written as no parentheses at all, because `FUNCTION
// f() RETURN NUMBER` is PLS-00103: PL/SQL has no empty formal list.
//
// CREATE OR REPLACE is used for both the plain and the replacing form. It
// creates a routine that does not exist yet, so it is the create as well as the
// replace -- and it is also what a declaration asking for IF NOT EXISTS gets,
// because that guard is a trap on one of the two supported lines.
func routineHeader(node *ast.CreateFunctionNode, determinism, security string) []string {
	name := escapeQualifiedIdentifier(node.Name)
	if parameters := strings.TrimSpace(node.Parameters); parameters != "" {
		name += "(" + parameters + ")"
	}
	header := []string{"CREATE OR REPLACE", strings.ToUpper(routineWord(node)), name}
	if !node.IsProcedure() {
		header = append(header, "RETURN", strings.TrimSpace(node.Returns))
	}
	if determinism != "" {
		header = append(header, determinism)
	}
	if security != "" {
		header = append(header, security)
	}
	return append(header, "IS")
}

// VisitDropFunction renders an Oracle DROP FUNCTION or DROP PROCEDURE.
//
// The verb has to match the object: `DROP FUNCTION p` on a procedure is
// ORA-04043, so a drop that guessed would fail and leave the routine in place
// while reporting the plan it did not run.
func (r *Renderer) VisitDropFunction(node *ast.DropFunctionNode) error {
	if node.IsProcedure() {
		if r.refuses(capability.Procedures, "DROP PROCEDURE", node.Name) {
			return nil
		}
	} else if r.refuses(capability.Functions, "DROP FUNCTION", node.Name) {
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists {
		guard = r.dropGuard()
	}
	r.w.WriteLinef("DROP %s%s %s;",
		strings.ToUpper(dropRoutineWord(node)), guard, escapeQualifiedIdentifier(node.Name))
	return nil
}

// routineWord names the object a statement or a diagnostic is about, so a
// message on a procedure does not call it a function.
func routineWord(node *ast.CreateFunctionNode) string {
	if node.IsProcedure() {
		return "procedure"
	}
	return "function"
}

// dropRoutineWord is routineWord for the node the drop half carries.
func dropRoutineWord(node *ast.DropFunctionNode) string {
	if node.IsProcedure() {
		return "procedure"
	}
	return "function"
}
