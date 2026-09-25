// Package routineargs reduces a PostgreSQL routine's argument list to the
// forms a comparison matches routines on.
//
// A schema declares arguments as its author wrote them, and PostgreSQL reports
// pg_get_function_identity_arguments. [Signature] is the whole identity, with
// parameter names and modes, which is how a CREATE or DROP names an overload.
// [InputTypes] is the input argument types alone, which is how GRANT and REVOKE
// name one. Both are here so that every package matching a declared routine to
// a recorded one, or two declarations to each other, reduces the arguments the
// same way.
package routineargs

import "strings"

// Signature reduces a routine's argument list to the form both sides of a
// comparison can be matched on.
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
func Signature(arguments string) string {
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

// InputTypes reduces a routine grant's argument list to
// the input argument types alone, which is what names a routine in GRANT and
// REVOKE.
//
// The two sides spell it differently. A declaration writes what PostgreSQL
// accepts after ON FUNCTION -- `f(uuid)`, `f(p uuid)`, `f(IN p uuid)` -- and the
// catalog read reports pg_get_function_identity_arguments, which keeps the
// parameter names: `p uuid`. Both pass through here. A mode is dropped, an OUT
// argument is dropped whole because PostgreSQL ignores it when it resolves the
// routine, and a leading word is taken for a parameter name unless it opens a
// multi-word type such as `double precision` or `timestamp with time zone`.
// A spelling this reads wrongly makes one routine two, which surfaces as a
// grant added and another removed rather than as a silent match.
func InputTypes(arguments string) string {
	parts := splitTopLevel(arguments)
	types := make([]string, 0, len(parts))
	for _, part := range parts {
		text := strings.TrimSpace(part)
		if index := indexKeyword(text, "default"); index >= 0 {
			text = strings.TrimSpace(text[:index])
		}
		if index := strings.Index(text, "="); index >= 0 {
			text = strings.TrimSpace(text[:index])
		}
		mode, rest := splitArgumentMode(text)
		if mode == "out" || rest == "" {
			continue
		}
		types = append(types, normalizeRoutineType(withoutParameterName(rest)))
	}
	return strings.Join(types, ", ")
}

// splitArgumentMode takes a leading argument mode off an argument.
func splitArgumentMode(text string) (mode, rest string) {
	lowered := strings.ToLower(text)
	for _, candidate := range routineArgumentModes {
		if strings.HasPrefix(lowered, candidate+" ") {
			return candidate, strings.TrimSpace(text[len(candidate):])
		}
	}
	return "", text
}

// multiWordTypeStarts are the first words of PostgreSQL type names that are
// more than one word, measured against pg_get_function_identity_arguments on
// PostgreSQL 18.
var multiWordTypeStarts = map[string]bool{
	"double": true, "character": true, "char": true, "national": true, "bit": true,
	"timestamp": true, "time": true, "interval": true,
}

// withoutParameterName drops a leading parameter name from an argument that
// has one.
func withoutParameterName(text string) string {
	fields := strings.Fields(text)
	if len(fields) < 2 || multiWordTypeStarts[strings.ToLower(fields[0])] {
		return text
	}
	return strings.Join(fields[1:], " ")
}

// SplitTarget splits a routine named with its argument types, `purge(uuid)`,
// into the name and the argument list between the parentheses. It reports
// false when the value names no argument list, which a grant target must
// have: PostgreSQL tells overloaded routines apart by their argument types.
// Every source that spells a routine target this way reads it here, so they
// cannot come to disagree about where the name ends.
func SplitTarget(value string) (name, arguments string, ok bool) {
	value = strings.TrimSpace(value)
	open := strings.Index(value, "(")
	if open <= 0 || !strings.HasSuffix(value, ")") {
		return "", "", false
	}
	return strings.TrimSpace(value[:open]), strings.TrimSpace(value[open+1 : len(value)-1]), true
}

// Split returns the arguments of an argument list one entry each, split on
// the commas outside parentheses and string literals, so `numeric(10,2)`
// stays one argument. Each entry is trimmed and empty entries are dropped;
// joining the result with ", " gives back an equivalent list.
func Split(arguments string) []string {
	parts := splitTopLevel(arguments)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
