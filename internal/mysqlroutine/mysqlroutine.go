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

// IdentityKey returns the key under which two spellings of one stored routine
// name are the same routine on a MySQL-family target.
//
// Stored-routine names are case-insensitive on both engines, and that is
// independent of the table-name rules [identifier.Semantics] carries: both
// report TableNames as ComparisonExact, and lower_case_table_names does not
// govern routines. Measured on MySQL 26.7.0, with `foo` in the catalog,
// `SELECT Foo(1)` resolves to it and `CREATE FUNCTION BAR` is refused with
// Error 1304 "FUNCTION BAR already exists" while `bar` is present.
//
// It lives here because THREE callers must agree on it: the comparator, which
// matches a desired routine to a live one; the declaration validator, which
// refuses two declarations the target cannot tell apart; and anything that
// later needs the same question answered. When only the comparator folded, two
// declarations differing by case passed validation as two objects and collapsed
// to one in the comparator's map -- an apply against an empty database created
// ONE function from TWO declarations and reported success, with the discarded
// one named nowhere.
func IdentityKey(name string) string {
	return strings.ToLower(name)
}

// RunsLanguage reports whether a routine declared in this language becomes real
// DDL on a MySQL-family target.
//
// MySQL and MariaDB run exactly one routine language, SQL. An empty value is
// accepted because an [ast.CreateFunctionNode] built directly carries no
// language and the renderer has always treated that as SQL; note that a
// declaration parsed from an annotation never arrives empty, because
// [goschema.Function.Canonicalize] defaults an unset language to plpgsql.
//
// It lives here, next to the rest of the family's routine rules, because TWO
// callers must agree on it and they are in different packages. The renderer
// uses it to decide whether to emit DDL or a named skip; the planner uses it to
// decide whether a replacement may emit its DROP. When only the renderer knew,
// the planner emitted an executable `DROP FUNCTION` in front of a CREATE that
// rendered nothing, and `schema apply` deleted a live routine and created
// nothing in its place -- measured on MySQL 26.7.0 and MariaDB 12.3.2, zero
// rows in information_schema.ROUTINES after an apply that reported success.
// One predicate, two consumers, so the two halves cannot drift apart again.
func RunsLanguage(language string) bool {
	normalized := strings.ToLower(strings.TrimSpace(language))
	return normalized == "" || normalized == "sql"
}

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
// A spelling belongs here only if the catalog form is decided by the
// declaration ALONE. Two families are deliberately absent and refused instead;
// see [ambiguousRoutineTypes].
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
	"double precision":  "double",
	"character":         "char",
	"character varying": "varchar",
	"long varchar":      "mediumtext",
}

// ambiguousRoutineTypes are declared spellings whose catalog form is NOT
// decided by the declaration alone, mapped to the reason and the unambiguous
// spellings that replace them.
//
// They are refused rather than folded, and refused rather than merely left out
// of [routineTypeAliases], because leaving them out is not neutral: the desired
// side would keep the declared spelling, the catalog would report something
// else, and the comparator would plan the same destructive replacement on every
// apply -- the very drift this package exists to end. A spelling that cannot
// round-trip has to be refused at the point of declaration or it becomes a
// permanent diff.
//
//   - REAL depends on the connection's SQL mode. Measured on MySQL 26.7.0,
//     `RETURNS REAL` reports DTD_IDENTIFIER `double` under the image's default
//     sql_mode and `float` with REAL_AS_FLOAT added to the same session. Folding
//     it to `double` is therefore right for one deployment and wrong for the
//     other, and nothing in the declaration says which. DOUBLE, DOUBLE PRECISION
//     and FLOAT are all mode-independent -- measured under REAL_AS_FLOAT, they
//     still report `double`, `double` and `float` -- so the operator has an
//     unambiguous way to say either thing.
//
//   - The NATIONAL spellings carry a character set that DTD_IDENTIFIER does not
//     show. Measured on the same server, `NATIONAL VARCHAR(10)` and `VARCHAR(10)`
//     BOTH report `varchar(10)`, and they differ only in a column this reader
//     does not select:
//
//     DTD_IDENTIFIER  CHARACTER_SET_NAME  COLLATION_NAME
//     varchar(10)     utf8mb3             utf8mb3_general_ci   <- NATIONAL
//     varchar(10)     utf8mb4             utf8mb4_0900_ai_ci   <- plain
//
//     NCHAR, NVARCHAR and NATIONAL CHAR behave identically. Treating them as
//     equivalent made a real character-set change invisible: switching a
//     parameter between the two spellings produced no modification at all, so
//     the authored change was never applied. Comparing the charset properly
//     needs it read on the catalog side AND derived from the declaration on the
//     desired side, which is a larger change than this one.
var ambiguousRoutineTypes = map[string]string{
	"real": "REAL is read back as `double` or `float` depending on whether the " +
		"connection's sql_mode includes REAL_AS_FLOAT, so Ptah cannot tell which " +
		"type this declaration means; declare DOUBLE or FLOAT",
	"national char":    nationalCharsetReason,
	"national varchar": nationalCharsetReason,
	"nchar":            nationalCharsetReason,
	"nvarchar":         nationalCharsetReason,
}

const nationalCharsetReason = "the NATIONAL spellings select the server's national " +
	"character set, which information_schema reports in CHARACTER_SET_NAME rather than " +
	"in the type Ptah compares, so a change to or from one would be invisible; declare " +
	"CHAR or VARCHAR and set the character set on the column or the schema"

// ValidateSignature refuses a routine signature Ptah cannot compare faithfully.
//
// It is the type half of the same rule [Characteristic] and [SecurityClause]
// hold for volatility and the security mode: a value this target cannot
// represent is refused at the point of declaration rather than reinterpreted
// into something that reads back as a different value forever.
func ValidateSignature(parameters, returns string) error {
	if err := validateType(returns); err != nil {
		return fmt.Errorf("return type: %w", err)
	}
	for _, declaration := range splitTopLevel(parameters, ',') {
		name, dataType := splitParameterDeclaration(declaration)
		if name == "" {
			continue
		}
		if err := validateType(dataType); err != nil {
			return fmt.Errorf("parameter %s: %w", name, err)
		}
	}
	return nil
}

func validateType(dataType string) error {
	trimmed := strings.TrimSpace(dataType)
	if trimmed == "" {
		return nil
	}
	base, rest := splitTypeBase(trimmed)
	if reason, ambiguous := ambiguousRoutineTypes[strings.ToLower(base)]; ambiguous {
		return fmt.Errorf("%s cannot be compared faithfully on this target: %s", base, reason)
	}
	// ZEROFILL pads to a width, so the width is the point of the declaration --
	// and when it is left out, the server materializes its own default rather
	// than recording "unspecified". Measured on MySQL 26.7.0 and MariaDB
	// 12.3.2, `INT ZEROFILL` is reported as `int(10) unsigned zerofill` on
	// both, which the desired side cannot predict without a per-type table of
	// engine defaults. Written with a width it round-trips exactly, so the
	// refusal asks for the one thing that makes the declaration decidable.
	if strings.Contains(strings.ToLower(rest), "zerofill") && !strings.HasPrefix(rest, "(") {
		return fmt.Errorf(
			"%s declares ZEROFILL without a display width: the width is what ZEROFILL "+
				"pads to, and the server substitutes its own default, which the "+
				"declaration cannot predict; write the width, as in %s(10) ZEROFILL",
			trimmed, base)
	}
	return nil
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
		return strings.TrimSpace(lowered + " " + normalizeTypeSuffix(rest))
	}
	// closeParen, not strings.Index: an argument list may contain a ")" inside a
	// quoted member, as `enum('a)b')` does, and cutting at the first one would
	// split the type in the middle of a literal.
	closing := closeParen(rest)
	if closing < 0 {
		return strings.TrimSpace(lowered + rest)
	}
	width := rest[:closing+1]
	suffix := normalizeTypeSuffix(rest[closing+1:])

	// The display width is dropped for integers because the two engines
	// disagree about it -- EXCEPT under ZEROFILL, where it stops being a
	// display width and becomes the padding target. Measured on MySQL 26.7.0
	// AND MariaDB 12.3.2, which agree exactly here:
	//
	//	INT(5) ZEROFILL           -> int(5) unsigned zerofill
	//	INT(10) ZEROFILL          -> int(10) unsigned zerofill
	//	INT(5) UNSIGNED ZEROFILL  -> int(5) unsigned zerofill
	//
	// Dropping it collapsed `INT(5) ZEROFILL` and `INT(10) ZEROFILL` onto one
	// `int zerofill`, so changing the padding width produced no modification and
	// was never applied -- a real change made invisible, the same failure as the
	// collapsed volatility. ZEROFILL without a width is refused instead; see
	// [validateType].
	_, isInteger := integerRoutineTypes[lowered]
	if isInteger && !strings.Contains(suffix, "zerofill") {
		width = ""
	}
	return strings.TrimSpace(lowered + width + " " + suffix)
}

// normalizeTypeSuffix canonicalizes the attributes trailing a routine type.
//
// ZEROFILL implies UNSIGNED, and both engines write that implication out:
// `INT(5) ZEROFILL` is reported as `int(5) unsigned zerofill`. The desired side
// carries whatever the operator typed, so without this a faithful declaration
// still reported drift against the catalog on every inspection.
func normalizeTypeSuffix(suffix string) string {
	lowered := strings.ToLower(strings.TrimSpace(suffix))
	if lowered == "" {
		return ""
	}
	if !strings.Contains(lowered, "zerofill") {
		return strings.Join(strings.Fields(lowered), " ")
	}
	return "unsigned zerofill"
}

// closeParen returns the index of the ")" closing the "(" at the start of s, or
// -1. Parentheses and quotes inside a quoted literal do not count.
func closeParen(s string) int {
	depth := 0
	for i := 0; i < len(s); i++ {
		if quote := s[i]; quote == '\'' || quote == '"' || quote == '`' {
			i = skipQuoted(s, i)
			continue
		}
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// skipQuoted returns the index of the closing quote of the literal that opens
// at start, or len(s)-1 when the literal is unterminated. Both escape forms the
// engines accept are honored: a doubled quote and a backslash.
func skipQuoted(s string, start int) int {
	quote := s[start]
	for i := start + 1; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			continue
		}
		if s[i] == quote {
			if i+1 < len(s) && s[i+1] == quote {
				i++
				continue
			}
			return i
		}
	}
	return len(s) - 1
}

// splitTopLevel splits s at every separator outside parentheses and outside a
// quoted literal.
//
// A routine parameter list is not a comma-separated list of anything simple:
// `p ENUM('x,y')` holds a comma that belongs to the member, and
// `d DECIMAL(10,2)` holds one that belongs to the type. Splitting on every
// comma turned the first into two parameters and reassembled it as
// `p enum('x, y')`, which made `ENUM('x,y')` and `ENUM('x','y')` -- genuinely
// different member sets, and reported as such by both catalogs -- normalize to
// the same string, so changing one to the other produced no modification.
func splitTopLevel(s string, separator byte) []string {
	var (
		parts []string
		depth int
		start int
	)
	for i := 0; i < len(s); i++ {
		if quote := s[i]; quote == '\'' || quote == '"' || quote == '`' {
			i = skipQuoted(s, i)
			continue
		}
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case separator:
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
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
		_, isAlias := routineTypeAliases[candidate]
		// The ambiguous table is consulted too, or `NATIONAL VARCHAR` written
		// without a length would split at the space and be validated as the
		// base name `national`, which is in neither table -- so the refusal
		// would not fire and the spelling would pass through unnoticed.
		_, isAmbiguous := ambiguousRoutineTypes[candidate]
		if isAlias || isAmbiguous {
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
	declarations := splitTopLevel(trimmed, ',')
	normalized := make([]string, 0, len(declarations))
	for _, declaration := range declarations {
		name, dataType := splitParameterDeclaration(declaration)
		if name == "" && dataType == "" {
			continue
		}
		if name == "" {
			normalized = append(normalized, NormalizeType(dataType))
			continue
		}
		normalized = append(normalized, name+" "+NormalizeType(dataType))
	}
	return strings.Join(normalized, ", ")
}

// splitParameterDeclaration separates `name type` at the first space that is
// not inside a quoted literal or parentheses.
//
// It is not strings.Fields followed by a Join, which is what this replaces:
// that rebuilds the type with single spaces everywhere, so a member value
// written `ENUM('x,  y')` came back as `ENUM('x, y')` and a change to the
// spacing inside a literal became invisible. Splitting once, and keeping the
// remainder verbatim, leaves the type's own bytes alone -- which matters
// because those bytes are the value being compared.
func splitParameterDeclaration(declaration string) (name, dataType string) {
	trimmed := strings.TrimSpace(declaration)
	if trimmed == "" {
		return "", ""
	}
	for i := 0; i < len(trimmed); i++ {
		if quote := trimmed[i]; quote == '\'' || quote == '"' || quote == '`' {
			i = skipQuoted(trimmed, i)
			continue
		}
		if trimmed[i] == '(' {
			break
		}
		if trimmed[i] == ' ' || trimmed[i] == '\t' || trimmed[i] == '\n' {
			return trimmed[:i], strings.TrimSpace(trimmed[i+1:])
		}
	}
	return "", trimmed
}
