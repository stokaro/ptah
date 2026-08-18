package compare

import (
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
)

// normalizeRoutineSignature reduces a routine's argument list to the form both
// sides of a comparison can be matched on.
//
// The two sides describe the same arguments differently. A schema declares what
// the author wrote -- `a int4`, `b varchar(50) DEFAULT 'x'` -- while PostgreSQL
// reports `pg_get_function_identity_arguments`, which keeps the parameter names
// and modes, drops defaults, and canonicalizes each type to its full spelling
// with modifiers stripped: `a integer`, `b character varying`. Measured on
// PostgreSQL 18.
//
// Running BOTH sides through this function is what makes them comparable, so it
// is deliberately not a PostgreSQL emulator: it has to agree with the catalog,
// not reproduce it.
func normalizeRoutineSignature(arguments string) string {
	parts := splitTopLevel(arguments)
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		token := normalizeRoutineArgument(part)
		if token == "" {
			continue
		}
		normalized = append(normalized, token)
	}
	return strings.Join(normalized, ", ")
}

// routineArgumentModes are the argument-mode prefixes a declaration may carry.
//
// Only three survive into an identity, and that is measured rather than
// assumed: PostgreSQL 18 reports `IN a int, OUT b int` as `a integer, OUT b
// integer`, dropping the redundant IN because it is the default mode while
// keeping the others. So IN is recognized in order to be REMOVED, and the rest
// are part of the identity -- `INOUT a integer` and `a integer` are different
// signatures.
var routineArgumentModes = []string{"variadic", "inout", "out", "in"}

// routineModesInIdentity are the modes the catalog keeps.
var routineModesInIdentity = map[string]bool{"variadic": true, "inout": true, "out": true}

// routineTypeAliases maps the spellings a schema may write to the one the
// catalog reports.
//
// Only the aliases PostgreSQL itself canonicalizes are listed, each measured
// against pg_get_function_identity_arguments rather than assumed. A spelling
// absent here normalizes to itself, so an unmapped alias makes two identical
// arguments compare unequal -- which surfaces as an add and a remove rather
// than as a silent mismatch.
var routineTypeAliases = map[string]string{
	"int":         "integer",
	"int4":        "integer",
	"int2":        "smallint",
	"int8":        "bigint",
	"serial":      "integer",
	"bigserial":   "bigint",
	"bool":        "boolean",
	"varchar":     "character varying",
	"char":        "character",
	"timestamptz": "timestamp with time zone",
	"timetz":      "time with time zone",
	"timestamp":   "timestamp without time zone",
	"time":        "time without time zone",
	"decimal":     "numeric",
	"float8":      "double precision",
	"float4":      "real",
}

// normalizeRoutineArgument reduces one argument to `[mode ][name ]type`.
func normalizeRoutineArgument(argument string) string {
	text := strings.TrimSpace(argument)
	if text == "" {
		return ""
	}
	// A default is not part of the identity, and the catalog drops it. The
	// expression may contain anything, including commas already handled by the
	// top-level split, so the whole tail goes.
	if index := indexKeyword(text, "default"); index >= 0 {
		text = strings.TrimSpace(text[:index])
	}
	if index := strings.Index(text, "="); index >= 0 {
		text = strings.TrimSpace(text[:index])
	}
	mode := ""
	lowered := strings.ToLower(text)
	for _, candidate := range routineArgumentModes {
		if !strings.HasPrefix(lowered, candidate+" ") {
			continue
		}
		if routineModesInIdentity[candidate] {
			mode = strings.ToUpper(candidate) + " "
		}
		text = strings.TrimSpace(text[len(candidate):])
		break
	}
	return mode + normalizeRoutineType(text)
}

// normalizeRoutineType strips a type modifier and maps an alias, leaving any
// leading parameter name alone because the catalog keeps it.
func normalizeRoutineType(text string) string {
	dimensions := 0
	for strings.HasSuffix(text, "[]") {
		dimensions++
		text = strings.TrimSpace(strings.TrimSuffix(text, "[]"))
	}
	array := strings.Repeat("[]", dimensions)
	// A type modifier -- varchar(50), numeric(10,2) -- is not part of the
	// identity; the catalog reports the bare type.
	if open := strings.Index(text, "("); open >= 0 && strings.HasSuffix(text, ")") {
		text = strings.TrimSpace(text[:open])
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return array
	}
	// The last word is the type unless the whole argument IS a type, which is
	// how an unnamed argument arrives. Multi-word types such as `double
	// precision` are already canonical and are left whole.
	head := strings.Join(fields[:len(fields)-1], " ")
	tail := strings.ToLower(fields[len(fields)-1])
	if mapped, ok := routineTypeAliases[tail]; ok {
		tail = mapped
	}
	joined := strings.TrimSpace(head + " " + tail)
	return strings.ToLower(joined) + array
}

// indexKeyword finds a whole-word keyword outside quotes, so a DEFAULT inside a
// string literal is not mistaken for the clause.
func indexKeyword(text, keyword string) int {
	lowered := strings.ToLower(text)
	depth := 0
	quoted := false
	for i := 0; i < len(lowered); i++ {
		switch lowered[i] {
		case '\'':
			quoted = !quoted
		case '(':
			if !quoted {
				depth++
			}
		case ')':
			if !quoted {
				depth--
			}
		}
		if quoted || depth != 0 {
			continue
		}
		if !strings.HasPrefix(lowered[i:], keyword) {
			continue
		}
		if i > 0 && lowered[i-1] != ' ' {
			continue
		}
		after := i + len(keyword)
		if after < len(lowered) && lowered[after] != ' ' {
			continue
		}
		return i
	}
	return -1
}

// splitTopLevel splits an argument list on commas that are not inside
// parentheses or a string literal, so `numeric(10,2)` stays one argument.
func splitTopLevel(text string) []string {
	parts := make([]string, 0)
	depth := 0
	quoted := false
	current := strings.Builder{}
	for i := 0; i < len(text); i++ {
		character := text[i]
		switch character {
		case '\'':
			quoted = !quoted
		case '(':
			if !quoted {
				depth++
			}
		case ')':
			if !quoted {
				depth--
			}
		case ',':
			if !quoted && depth == 0 {
				parts = append(parts, current.String())
				current.Reset()
				continue
			}
		}
		current.WriteByte(character)
	}
	if strings.TrimSpace(current.String()) != "" {
		parts = append(parts, current.String())
	}
	return parts
}

// routinePair is one declared routine matched to the recorded routine it is.
type routinePair struct {
	declared goschema.Function
	recorded types.DBFunction
}

// pairRoutineOverloads matches the routines a schema declares under one name to
// the routines a database records under it, and reports how many on each side
// are left over.
//
// The signature is consulted ONLY when a name carries more than one routine on
// either side, and that restriction is the whole safety argument. A name with
// one routine on each side pairs exactly as it always did, so the overwhelmingly
// common case is untouched and cannot regress on a signature this normalizer
// spells differently from the catalog. An overloaded name is the case that is
// already broken: both maps kept one entry per name, so the second overload
// overwrote the first and a dropped overload was reported as a modification of
// the survivor rather than as a removal (stokaro/ptah#1664).
//
// Left-over routines are genuine additions and removals. A normalizer that
// spelled one side differently would surface as an add beside a remove, which
// is visible in a plan, rather than as a silent mispairing.
func pairRoutineOverloads(
	declared []goschema.Function,
	recorded []types.DBFunction,
) (pairs []routinePair, unmatchedDeclared, unmatchedRecorded int) {
	if len(declared) == 0 {
		return nil, 0, len(recorded)
	}
	if len(recorded) == 0 {
		return nil, len(declared), 0
	}
	if len(declared) == 1 && len(recorded) == 1 {
		return []routinePair{{declared: declared[0], recorded: recorded[0]}}, 0, 0
	}

	used := make([]bool, len(recorded))
	pairs = make([]routinePair, 0, len(declared))
	for _, function := range declared {
		index := matchRecordedRoutine(function, recorded, used)
		if index < 0 {
			unmatchedDeclared++
			continue
		}
		used[index] = true
		pairs = append(pairs, routinePair{declared: function, recorded: recorded[index]})
	}
	for _, taken := range used {
		if !taken {
			unmatchedRecorded++
		}
	}
	return pairs, unmatchedDeclared, unmatchedRecorded
}

// matchRecordedRoutine finds the unused recorded routine whose signature equals
// the declared one's, or -1.
func matchRecordedRoutine(declared goschema.Function, recorded []types.DBFunction, used []bool) int {
	want := normalizeRoutineSignature(declared.Parameters)
	for index, candidate := range recorded {
		if used[index] {
			continue
		}
		if normalizeRoutineSignature(recordedRoutineSignature(candidate)) == want {
			return index
		}
	}
	return -1
}

// recordedRoutineSignature returns the argument list to compare a recorded
// routine on.
//
// IdentityArguments is the catalog's own answer and is preferred; only the
// PostgreSQL reader fills it, so every other dialect falls back to the declared
// parameters. That fallback is why the caller consults a signature at all only
// where a name is overloaded: on a dialect with no identity arguments the two
// sides are compared on the same kind of string, which is the best available
// and is still better than keeping one entry per name.
func recordedRoutineSignature(function types.DBFunction) string {
	if function.IdentityArguments != nil {
		return *function.IdentityArguments
	}
	return function.Parameters
}
