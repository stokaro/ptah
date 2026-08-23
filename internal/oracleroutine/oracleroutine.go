// Package oracleroutine holds the single description of how Ptah's
// PostgreSQL-shaped stored-function fields are encoded into Oracle's PL/SQL
// routine model, and how they are recovered from its catalog.
//
// It exists for the reason [go.5x5.cz/ptah/internal/mysqlroutine] exists: the
// renderer and the reader are two halves of one round trip, and every property
// whose halves disagree produces the same failure -- the renderer writes a
// value the reader cannot recover, the comparator compares the two spellings,
// and the schema plans the same destructive replacement on every apply,
// forever. Putting both directions in one file makes the round trip checkable
// by reading it.
//
// Everything here was measured on Oracle Database 23ai Free 23.26.2.0.0 and on
// Oracle Database 21c Express Edition 21.3.0.0.0, and the two lines agreed on
// every answer below. They differ only in the existence guards, which are a
// capability key rather than anything in this file.
package oracleroutine

import (
	"fmt"
	"strings"
)

// Language is the routine language this target runs.
//
// It is named rather than left empty because the comparator compares the field:
// a reader that answered "" would differ from any declaration that says what it
// is, and a declaration is the only place the language can come from.
const Language = "plsql"

// Volatility values, spelled as [go.5x5.cz/ptah/core/goschema.Function.Canonicalize] leaves them.
const (
	Immutable = "IMMUTABLE"
	Stable    = "STABLE"
	Volatile  = "VOLATILE"
)

// RunsLanguage reports whether a routine declared in this language becomes real
// DDL on an Oracle target.
//
// Oracle runs exactly one routine language, PL/SQL. An empty value is accepted
// because an [go.5x5.cz/ptah/core/ast.CreateFunctionNode] built directly
// carries no language; a declaration parsed from an annotation never arrives
// empty, because Canonicalize defaults an unset language to plpgsql.
//
// It lives here, beside the rest of Oracle's routine rules, because TWO callers
// must agree on it and they are in different packages. The renderer uses it to
// decide whether to emit DDL or a named skip; the planner uses it to decide
// whether a replacement may emit its DROP. When only the renderer knows, the
// planner emits an executable DROP in front of a CREATE that renders nothing,
// and an apply deletes a live routine and creates nothing in its place.
func RunsLanguage(language string) bool {
	normalized := strings.ToLower(strings.TrimSpace(language))
	return normalized == "" || normalized == Language
}

// SecurityClause returns the AUTHID clause for a security mode, and refuses a
// value it cannot render.
//
// Oracle spells the two modes AUTHID DEFINER and AUTHID CURRENT_USER, and
// DEFINER is what an omitted clause means -- ALL_PROCEDURES.AUTHID reports
// DEFINER for a routine created without one. The clause is written for both
// modes rather than only for the non-default one, because the value is read
// back and compared: leaving DEFINER implicit renders the same statement and is
// no shorter to explain.
//
// Anything that is neither DEFINER nor INVOKER is refused rather than silently
// dropped, for the reason the MySQL family's SecurityClause records: dropping
// it makes the server apply DEFINER, which is broader than an operator asking
// for anything else can have meant.
func SecurityClause(security string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(security)) {
	case "":
		return "", nil
	case "DEFINER":
		return "AUTHID DEFINER", nil
	case "INVOKER":
		return "AUTHID CURRENT_USER", nil
	default:
		return "", fmt.Errorf(
			"unknown function security mode %q: Oracle has AUTHID DEFINER and "+
				"AUTHID CURRENT_USER, and omitting the clause makes the server apply "+
				"DEFINER, which is broader than an operator asking for anything else "+
				"can have meant",
			security)
	}
}

// SecurityFromCatalog recovers the security mode that [SecurityClause] wrote.
//
// ALL_PROCEDURES.AUTHID reports CURRENT_USER or DEFINER, and nothing else.
func SecurityFromCatalog(authID string) string {
	if strings.EqualFold(strings.TrimSpace(authID), "CURRENT_USER") {
		return "INVOKER"
	}
	return "DEFINER"
}

// DeterminismClause returns the header clause that encodes volatility, and
// refuses the one value Oracle cannot report back.
//
// Oracle describes a routine's determinism on ONE axis with TWO states, which
// ALL_PROCEDURES.DETERMINISTIC reports as YES or NO -- for a procedure as well
// as for a function, measured on both lines. PostgreSQL describes it on one
// axis with THREE values. There is no second column to encode the third value
// onto: the neighbours ALL_PROCEDURES carries -- PARALLEL, RESULT_CACHE,
// PIPELINED, AGGREGATE -- are not descriptive fields but clauses the optimizer
// and the executor act on, so writing one to record a value would change how
// the routine runs. The MySQL family could spend SQL_DATA_ACCESS on this
// because that column is advisory; none of Oracle's is.
//
// So IMMUTABLE is DETERMINISTIC, VOLATILE is the absence of the clause, and
// STABLE is refused by name. Mapping STABLE onto DETERMINISTIC would be a lie
// the server acts on -- a DETERMINISTIC function is what a function-based index
// may be built from, and one that reads a table is not that -- and mapping it
// onto the absence would read back as VOLATILE and plan the same replacement on
// every run. Naming it is the only answer that neither corrupts an index nor
// diffs forever.
func DeterminismClause(volatility string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(volatility)) {
	case Immutable:
		return "DETERMINISTIC", nil
	case Volatile, "":
		return "", nil
	case Stable:
		return "", fmt.Errorf(
			"function volatility %s has no Oracle spelling: this target reports "+
				"determinism as YES or NO only, DETERMINISTIC is a promise a "+
				"function-based index may be built on, and the absence of the clause "+
				"reads back as %s -- declare %s or %s",
			Stable, Volatile, Immutable, Volatile)
	default:
		return "", fmt.Errorf(
			"unknown function volatility %q: this target encodes %s and %s onto the "+
				"DETERMINISTIC clause its catalog reports back, and has no cell for "+
				"another value; declare one of them",
			volatility, Immutable, Volatile)
	}
}

// VolatilityFromCatalog recovers the volatility that [DeterminismClause] wrote.
//
// It is the exact inverse of that function over the values it emits, which is
// what closes the round trip. STABLE is never produced, because nothing writes
// a routine this reads that way -- the clause has two states and both are
// spoken for.
func VolatilityFromCatalog(deterministic string) string {
	if strings.EqualFold(strings.TrimSpace(deterministic), "YES") {
		return Immutable
	}
	return Volatile
}

// Argument spells one parameter the way a declaration writes it.
//
// PL/SQL puts the mode AFTER the name -- `p IN NUMBER` -- which is the opposite
// of PostgreSQL's `IN p integer`, and ALL_ARGUMENTS reports IN/OUT for the mode
// PL/SQL spells `IN OUT`. The type carries no facets because a formal parameter
// cannot: `p IN VARCHAR2(50)` is PLS-00103, so the catalog's DATA_LENGTH and
// DATA_PRECISION have nothing to put back.
//
// The result is lower-cased because Canonicalize lower-cases the declared side,
// and the catalog reports every unquoted name folded to upper case.
func Argument(name, mode, dataType string) string {
	parts := []string{strings.ToLower(strings.TrimSpace(name))}
	if spelled := ArgumentMode(mode); spelled != "" {
		parts = append(parts, spelled)
	}
	parts = append(parts, strings.ToLower(strings.TrimSpace(dataType)))
	return strings.Join(parts, " ")
}

// ArgumentMode spells the catalog's IN_OUT value the way PL/SQL writes it.
func ArgumentMode(mode string) string {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "IN":
		return "in"
	case "OUT":
		return "out"
	case "IN/OUT":
		return "in out"
	default:
		return strings.ToLower(strings.TrimSpace(mode))
	}
}

// FoldDefaultArgumentMode drops the IN that PL/SQL applies to a parameter
// written without a mode.
//
// `p NUMBER` and `p IN NUMBER` are the same parameter, and both spellings are
// ordinary in a declaration, while [Argument] always writes the mode the
// catalog reports. Without the fold, a schema declaring `p NUMBER` compared
// unequal to the routine it had just created and planned the same replacement
// forever.
//
// The fold stays in the COMPARISON and is applied to both sides. Rendering the
// folded form instead would write Ptah's normalization into the operator's
// DDL, and OUT and IN OUT are not defaults and are left exactly as written --
// which is why the second word is only dropped when the third is not OUT.
func FoldDefaultArgumentMode(parameters string) string {
	if strings.TrimSpace(parameters) == "" {
		return parameters
	}
	arguments := splitTopLevelArguments(parameters)
	for i, argument := range arguments {
		arguments[i] = withoutDefaultMode(argument)
	}
	return strings.Join(arguments, ", ")
}

// withoutDefaultMode drops a standalone IN from one parameter, leaving IN OUT
// alone.
func withoutDefaultMode(argument string) string {
	fields := strings.Fields(argument)
	if len(fields) < 3 || !strings.EqualFold(fields[1], "in") {
		return strings.TrimSpace(argument)
	}
	if strings.EqualFold(fields[2], "out") {
		return strings.Join(fields, " ")
	}
	return strings.Join(append(fields[:1:1], fields[2:]...), " ")
}

// splitTopLevelArguments splits a parameter list on the commas that separate
// parameters, leaving the ones inside a default expression's own parentheses
// alone.
func splitTopLevelArguments(parameters string) []string {
	var arguments []string
	depth, start := 0, 0
	for i, r := range parameters {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				arguments = append(arguments, parameters[start:i])
				start = i + 1
			}
		}
	}
	return append(arguments, parameters[start:])
}
