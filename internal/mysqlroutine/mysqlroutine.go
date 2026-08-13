// Package mysqlroutine holds the single description of how Ptah's
// PostgreSQL-shaped stored-function fields are encoded into the MySQL family's
// own routine model, and how they are recovered from its catalog.
//
// It exists because the renderer and the reader are two halves of one round
// trip and were written as two independent translations. Every property whose
// two halves disagreed produced the same failure: the renderer wrote a value
// the reader could not recover, [compare.FunctionDefinitions] compared the two
// spellings exactly, and the schema planned the same destructive
// DROP-then-CREATE on every apply, forever. Volatility, routine type aliases
// and the security mode each reached that state separately. Putting both
// directions in one file is what makes the round trip checkable by reading it:
// a change to the write half that is not mirrored in the read half is visible
// here rather than three packages away.
//
// The mapping is constrained by measurement, not by preference. See
// [Characteristic] for the grid of declarations the engines actually accept.
package mysqlroutine

import (
	"fmt"
	"strings"
)

// Volatility values, spelled as [goschema.Function.Canonicalize] leaves them.
const (
	Immutable = "IMMUTABLE"
	Stable    = "STABLE"
	Volatile  = "VOLATILE"
)

// Characteristic returns the routine characteristic clause that encodes
// volatility for a MySQL-family target.
//
// # Why the mapping is forced
//
// MySQL describes a routine on two axes that information_schema.ROUTINES
// reports separately: IS_DETERMINISTIC and SQL_DATA_ACCESS. PostgreSQL
// describes it on one, with three values. Encoding three values needs three
// distinct (IS_DETERMINISTIC, SQL_DATA_ACCESS) cells that the server accepts.
//
// Not every cell is available. With binary logging on and
// log_bin_trust_function_creators off -- the pinned mysql:26.7 image's own
// defaults -- the server refuses any declaration carrying none of
// DETERMINISTIC, NO SQL or READS SQL DATA with Error 1418. Measured on MySQL
// 26.7.0 by creating all fifteen combinations of {unset, DETERMINISTIC, NOT
// DETERMINISTIC} x {unset, NO SQL, CONTAINS SQL, READS SQL DATA, MODIFIES SQL
// DATA} and reading the catalog back, exactly these cells survive:
//
//	IS_DETERMINISTIC  SQL_DATA_ACCESS     accepted
//	YES               CONTAINS SQL        yes
//	YES               NO SQL              yes
//	YES               READS SQL DATA      yes
//	YES               MODIFIES SQL DATA   yes
//	NO                NO SQL              yes
//	NO                READS SQL DATA      yes
//	NO                CONTAINS SQL        NO -- Error 1418
//	NO                MODIFIES SQL DATA   NO -- Error 1418
//
// MariaDB 12.3.2 ships with binary logging off and accepts all fifteen, so the
// MySQL row is the binding one: the family shares one shape, and a shape MySQL
// refuses is not one.
//
// IMMUTABLE must be DETERMINISTIC -- that axis is load-bearing for the
// optimizer and for replication, and misstating it is a lie the server acts
// on. STABLE and VOLATILE must therefore both be NOT DETERMINISTIC, and there
// are exactly two accepted NOT DETERMINISTIC cells. The assignment is forced
// up to which of the two gets which value.
//
// VOLATILE keeps READS SQL DATA, the spelling stokaro/ptah#1461 shipped. It is
// the value [goschema.Function.Canonicalize] gives every function whose
// annotation omits volatility, so it is the common case and every function
// already deployed by that release carries it; moving it would rewrite the
// catalog entry of every existing routine to fix a value almost nobody
// declares. STABLE takes the cell that is left.
//
// SQL_DATA_ACCESS is advisory rather than enforced -- measured on MySQL 26.7.0,
// a routine declared NOT DETERMINISTIC NO SQL whose body is
// `RETURN (SELECT COUNT(*) FROM t)` was created and returned 3 -- so this uses
// a descriptive field as an encoding channel. That is the cost of the fix and
// it is named here rather than hidden: a STABLE routine's catalog row says
// NO SQL whatever its body reads.
func Characteristic(volatility string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(volatility)) {
	case Immutable:
		return "DETERMINISTIC", nil
	case Stable:
		return "NOT DETERMINISTIC NO SQL", nil
	case Volatile, "":
		return "READS SQL DATA", nil
	default:
		return "", fmt.Errorf(
			"unknown function volatility %q: this target encodes %s, %s and %s onto "+
				"the MySQL routine characteristics its catalog can report back, and has "+
				"no cell for another value; declare one of them",
			volatility, Immutable, Stable, Volatile)
	}
}

// VolatilityFromCatalog recovers the volatility that [Characteristic] wrote,
// from the two columns it is visible in.
//
// It is the exact inverse of [Characteristic] over the values that function
// emits, which is what closes the round trip. A routine Ptah did not create
// can land on a cell this mapping reads differently from how its author meant
// it -- a hand-written NOT DETERMINISTIC NO SQL routine reads back as STABLE.
// That is the same trade the encoding makes in the other direction and it
// cannot be avoided while the catalog has no field for the value itself.
func VolatilityFromCatalog(isDeterministic, sqlDataAccess string) string {
	if strings.EqualFold(strings.TrimSpace(isDeterministic), "YES") {
		return Immutable
	}
	if strings.EqualFold(strings.TrimSpace(sqlDataAccess), "NO SQL") {
		return Stable
	}
	return Volatile
}

// SecurityClause returns the SQL SECURITY clause for a security mode, and
// refuses a value it cannot render.
//
// An unset value renders no clause, which leaves the server its own default.
// Anything else that is neither DEFINER nor INVOKER is refused rather than
// silently dropped. Dropping it was worse than it looks: a misspelled
// `security="INVKOER"` emitted no clause at all, so MySQL applied DEFINER --
// the broader of the two rights -- the apply succeeded, and the next
// comparison reported `security: DEFINER -> INVKOER` and planned the same
// replacement forever. An operator who asked for invoker rights got definer
// rights and a permanent diff, which is the opposite of what they wrote.
func SecurityClause(security string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(security)) {
	case "":
		return "", nil
	case "DEFINER":
		return "SQL SECURITY DEFINER", nil
	case "INVOKER":
		return "SQL SECURITY INVOKER", nil
	default:
		return "", fmt.Errorf(
			"unknown function security mode %q: MySQL and MariaDB have DEFINER and "+
				"INVOKER, and omitting the clause makes the server apply DEFINER, "+
				"which is broader than an operator asking for anything else can have meant",
			security)
	}
}

// routineTypeAliases maps a declared type spelling onto the base type the
// catalog reports for it.
//
// The engines canonicalize these themselves; this table only teaches the
// desired side the answer the catalog will give, so the two sides meet. Every
// row was measured by declaring `RETURNS <spelling>` on MySQL 26.7.0 and
// MariaDB 12.3.2 and reading DTD_IDENTIFIER back. Both engines agreed on the
// base type for every row, so one table serves the family; they disagree only
// about the legacy display width, which [NormalizeType] strips separately.
var routineTypeAliases = map[string]string{
	"integer":           "int",
	"int4":              "int",
	"int1":              "tinyint",
	"int2":              "smallint",
	"int3":              "mediumint",
	"middleint":         "mediumint",
	"int8":              "bigint",
	"dec":               "decimal",
	"numeric":           "decimal",
	"fixed":             "decimal",
	"bool":              "tinyint",
	"boolean":           "tinyint",
	"real":              "double",
	"double precision":  "double",
	"character":         "char",
	"character varying": "varchar",
	"national varchar":  "varchar",
	"national char":     "char",
	"long varchar":      "mediumtext",
}

// integerRoutineTypes are the types whose parenthesized argument is a display
// width rather than a size. Everything else -- varchar(20), decimal(10,2) --
// carries meaning in the parentheses and keeps it.
var integerRoutineTypes = map[string]struct{}{
	"tinyint":   {},
	"smallint":  {},
	"mediumint": {},
	"int":       {},
	"bigint":    {},
}

// NormalizeType canonicalizes one routine type so a desired spelling and a
// catalog spelling of the same type are one string.
//
// It runs on BOTH sides. That is the whole point: normalizing only the catalog
// left `returns="INTEGER"` canonicalized to `integer` on the desired side
// against `int` from information_schema, which
// [compare.FunctionDefinitions] compares exactly, so an already-matching
// function planned another destructive drop and create on every inspection.
//
// Two normalizations happen, and they are independent:
//
//   - The alias is resolved, so `integer`, `int4` and `int` are one type. This
//     is the desired side learning what the catalog will say.
//   - The legacy integer display width is dropped, so `int(11)` and `int` are
//     one type. This is the two ENGINES disagreeing with each other: measured
//     on the same declaration, MySQL 26.7.0 reports `int` where MariaDB 12.3.2
//     reports `int(11)`. Without it the identical schema converged on one and
//     reported a permanent `parameters, returns` diff on the other.
//
// The width is dropped, not the rest: `int(11) unsigned` keeps its unsigned.
func NormalizeType(dataType string) string {
	trimmed := strings.TrimSpace(dataType)
	if trimmed == "" {
		return ""
	}

	base, rest := splitTypeBase(trimmed)
	lowered := strings.ToLower(base)
	if alias, ok := routineTypeAliases[lowered]; ok {
		lowered = alias
	}

	if rest == "" {
		return lowered
	}
	// rest begins at the "(" when there is one.
	if !strings.HasPrefix(rest, "(") {
		return strings.TrimSpace(lowered + " " + rest)
	}
	closing := strings.Index(rest, ")")
	if closing < 0 {
		return strings.TrimSpace(lowered + rest)
	}
	width := rest[:closing+1]
	suffix := strings.TrimSpace(rest[closing+1:])
	if _, isInteger := integerRoutineTypes[lowered]; isInteger {
		width = ""
	}
	return strings.TrimSpace(lowered + width + " " + suffix)
}

// splitTypeBase separates a type's name from its parenthesized argument and
// any trailing attributes.
//
// Multi-word base names are why this is not a split on the first space:
// `double precision` and `character varying(20)` are one type name each, and
// cutting at the first space would leave `double` and `character` to be
// resolved as if the second word were an attribute like `unsigned`.
func splitTypeBase(dataType string) (base, rest string) {
	if open := strings.Index(dataType, "("); open >= 0 {
		return strings.TrimSpace(dataType[:open]), dataType[open:]
	}
	fields := strings.Fields(dataType)
	for size := min(len(fields), 2); size >= 1; size-- {
		candidate := strings.ToLower(strings.Join(fields[:size], " "))
		if _, ok := routineTypeAliases[candidate]; ok {
			return strings.Join(fields[:size], " "), strings.Join(fields[size:], " ")
		}
	}
	return fields[0], strings.Join(fields[1:], " ")
}

// NormalizeParameterList applies [NormalizeType] to every declaration in a
// comma-separated routine parameter list, leaving the parameter names alone.
//
// The list is the same shape on both sides -- the reader builds `name type`
// pairs from information_schema.PARAMETERS and the desired side carries the
// operator's `params="a INTEGER"` -- so normalizing it here is what makes
// `a integer` and `a int` one signature.
func NormalizeParameterList(parameters string) string {
	trimmed := strings.TrimSpace(parameters)
	if trimmed == "" {
		return ""
	}
	declarations := strings.Split(trimmed, ",")
	normalized := make([]string, 0, len(declarations))
	for _, declaration := range declarations {
		fields := strings.Fields(declaration)
		if len(fields) == 0 {
			continue
		}
		if len(fields) == 1 {
			normalized = append(normalized, NormalizeType(fields[0]))
			continue
		}
		name := fields[0]
		normalized = append(normalized, name+" "+NormalizeType(strings.Join(fields[1:], " ")))
	}
	return strings.Join(normalized, ", ")
}
