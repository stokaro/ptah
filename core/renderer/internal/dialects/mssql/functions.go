package mssql

import (
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/mysqlroutine"
)

// SQL Server hosts scalar and table-valued functions, and the header it needs
// is close enough to the shared declaration that most of it renders straight
// through. Everything below was measured on SQL Server 2025 (RTM-CU8),
// 17.0.4075.5:
//
//   - `CREATE OR ALTER FUNCTION` is accepted. That is worth more here than on
//     the MySQL family, which has no such form and plans a replacement as a
//     DROP followed by a CREATE -- a pair whose first half destroys the
//     operator's function if the second is refused. On this target a
//     replacement is one statement, so that hazard does not arise.
//   - `CREATE FUNCTION IF NOT EXISTS` is `Incorrect syntax near the keyword
//     'IF'`, the same refusal CREATE SEQUENCE and CREATE SECURITY POLICY draw.
//     A declaration asking for the guard gets `CREATE OR ALTER` instead, which
//     is idempotent in the way the guard was asking for.
//   - `DROP FUNCTION IF EXISTS` is accepted, so the drop needs no catalog test.
//   - `LANGUAGE SQL` is `Incorrect syntax near 'LANGUAGE'`. T-SQL has no
//     LANGUAGE clause at all; the body's language is the engine's own.
//   - `WITH SCHEMABINDING` is accepted, and the catalog reports it back as
//     sys.sql_modules.is_schema_bound.

// VisitCreateFunction renders a T-SQL CREATE FUNCTION.
//
// A function whose declared language this target does not run is named and
// skipped rather than refused, which is the answer the MySQL family already
// reached for the same situation and for a reason that applies unchanged here:
// one schema is applied across several dialects, and a declaration cannot yet
// say which dialect it belongs to. Refusing would break a workflow that works
// today. The predicate is [mysqlroutine.RunsLanguage] rather than a comparison
// written here, because the planner has to reach the same answer -- it must not
// plan a drop whose create this branch is about to skip.
//
// The trap that costs an afternoon is the same one too, and the message names
// it: [goschema.Function.Canonicalize] defaults an unset language to plpgsql,
// so a function annotated without `language=` lands in this branch and is
// skipped when it looks like it should have been generated.
func (r *Renderer) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	if r.refuses(capability.Functions, "CREATE FUNCTION", node.Name) {
		return nil
	}
	if !mysqlroutine.RunsLanguage(node.Language) {
		language := strings.ToLower(strings.TrimSpace(node.Language))
		r.w.WriteLinef(
			"-- SQLSERVER: CREATE FUNCTION %s declares language %s, which this target does not run; skipped.",
			escapeIdentifier(node.Name), language)
		r.w.WriteLinef(
			"--   If this body is T-SQL, declare language=\"sql\": an annotation that omits the")
		r.w.WriteLinef(
			"--   language is defaulted to plpgsql and is skipped here for the same reason.")
		return nil
	}
	// A parameter default is accepted by the engine and then invisible to
	// everyone. sys.parameters reports has_default_value = 0 for
	// `@b varchar(50) = 'x'` -- SQL Server records defaults for CLR parameters
	// only -- so the reader cannot see it, and a function created with one
	// would be reported as differing from its own declaration on every run.
	// Naming it and creating nothing is the only answer that does not lie.
	if parameterCarriesDefault(node.Parameters) {
		r.w.WriteLinef("-- SQLSERVER: function %q declares a parameter default, which the catalog does not "+
			"report back, so the function would be replanned on every run; it is not created.", node.Name)
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	// Volatility is the one declared attribute with no T-SQL clause, and only a
	// value other than the default is worth a sentence. Determinism here is the
	// engine's verdict on the body rather than something a header states, so a
	// declaration asking for IMMUTABLE is asking for something no clause grants.
	// VOLATILE is what an unset declaration canonicalizes to and what a function
	// written here reports back, so naming that would be noise on every run.
	if volatility := strings.ToUpper(strings.TrimSpace(node.Volatility)); volatility != "" && volatility != "VOLATILE" {
		r.w.WriteLinef("-- SQLSERVER: function %q declares %s, which T-SQL has no clause for; "+
			"determinism is inferred by the engine from the body.", node.Name, volatility)
	}

	// CREATE OR ALTER is used for both the plain and the replacing form. The
	// engine accepts it on a name that does not exist yet, so it is the create
	// as well as the replace, and it is what a declaration asking for
	// IF NOT EXISTS gets -- that clause does not parse here.
	r.w.WriteLinef("CREATE OR ALTER FUNCTION %s(%s)", escapeQualifiedIdentifier(node.Name),
		strings.TrimSpace(node.Parameters))
	r.w.WriteLinef("RETURNS %s", strings.TrimSpace(node.Returns))
	// SECURITY DEFINER does have a T-SQL spelling, and it was nearly written off
	// as absent: `WITH EXECUTE AS OWNER` is what the catalog reports back as a
	// principal id on sys.sql_modules.execute_as_principal_id. CALLER is the
	// default and needs no clause, which is what INVOKER means.
	if strings.EqualFold(strings.TrimSpace(node.Security), "DEFINER") {
		r.w.WriteLinef("WITH EXECUTE AS OWNER")
	}
	r.w.WriteLinef("AS")
	r.w.WriteLinef("%s;", strings.TrimSpace(node.Body))
	return nil
}

// VisitDropFunction renders a T-SQL DROP FUNCTION.
//
// The IF EXISTS clause is accepted, unlike its counterpart on CREATE, so the
// guarded form needs no catalog test.
func (r *Renderer) VisitDropFunction(node *ast.DropFunctionNode) error {
	if r.refuses(capability.Functions, "DROP FUNCTION", node.Name) {
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists {
		guard = "IF EXISTS "
	}
	r.w.WriteLinef("DROP FUNCTION %s%s;", guard, escapeQualifiedIdentifier(node.Name))
	return nil
}

// parameterCarriesDefault reports whether a declared parameter list assigns a
// default to any argument.
//
// It is a shape test, not a parser: an `=` outside quotes is enough, because
// the parameter list has no other use for one.
func parameterCarriesDefault(parameters string) bool {
	inQuote := false
	for i := 0; i < len(parameters); i++ {
		switch parameters[i] {
		case '\'':
			inQuote = !inQuote
		case '=':
			if !inQuote {
				return true
			}
		}
	}
	return false
}
