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
	text, _, _ = CutDefault(text)
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

// CutDefault splits one argument at its default: the declaration before a
// top-level DEFAULT or `=`, and the expression after it, both trimmed. found is
// false for an argument that declares no default, and declaration is then the
// whole argument.
//
// The two spellings are one clause, and PostgreSQL prints both as DEFAULT. Only
// a top-level occurrence counts, so `=` or DEFAULT inside a string literal, a
// quoted name or the default's own parentheses is not taken for the clause.
func CutDefault(argument string) (declaration, expression string, found bool) {
	at, width := -1, 0
	topLevel(argument, func(index int) bool {
		switch {
		case argument[index] == '=':
			at, width = index, 1
		case hasWordAt(argument, index, "default"):
			at, width = index, len("default")
		default:
			return true
		}
		return false
	})
	if at < 0 {
		return strings.TrimSpace(argument), "", false
	}
	return strings.TrimSpace(argument[:at]), strings.TrimSpace(argument[at+width:]), true
}

// hasWordAt reports whether word, in any case, stands at text[index] as a
// whole word.
func hasWordAt(text string, index int, word string) bool {
	end := index + len(word)
	return end <= len(text) && strings.EqualFold(text[index:end], word) &&
		(index == 0 || !isWordByte(text[index-1])) &&
		(end == len(text) || !isWordByte(text[end]))
}

// isWordByte reports whether a byte can be part of an unquoted SQL word.
func isWordByte(character byte) bool {
	return character == '_' || character == '$' ||
		character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' || character >= 0x80
}

// topLevel calls visit with the index of every byte of text outside
// parentheses, brackets, string literals and quoted identifiers, in order, and
// stops when visit answers false. A default can hold all of them --
// `ARRAY[1, 2]`, `'a,b'`, `lower('X')` -- and none of what is inside is part of
// the argument list's own structure.
func topLevel(text string, visit func(index int) bool) {
	depth := 0
	var quote byte
	for i := 0; i < len(text); i++ {
		character := text[i]
		switch {
		case quote != 0:
			if character == quote {
				quote = 0
			}
		case character == '\'' || character == '"':
			quote = character
		case character == '(' || character == '[':
			depth++
		case character == ')' || character == ']':
			depth--
		case depth == 0:
			if !visit(i) {
				return
			}
		}
	}
}

// splitTopLevel splits an argument list on its top-level commas, so
// `numeric(10,2)` and `DEFAULT 'a,b'` stay one argument each.
func splitTopLevel(text string) []string {
	parts := make([]string, 0)
	start := 0
	topLevel(text, func(index int) bool {
		if text[index] == ',' {
			parts = append(parts, text[start:index])
			start = index + 1
		}
		return true
	})
	if rest := text[start:]; strings.TrimSpace(rest) != "" {
		parts = append(parts, rest)
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
		text, _, _ := CutDefault(part)
		mode, rest := splitArgumentMode(text)
		if mode == "out" || rest == "" {
			continue
		}
		types = append(types, normalizeRoutineType(withoutParameterName(rest)))
	}
	return strings.Join(types, ", ")
}

// ImpliedResult answers the result type PostgreSQL gives a function that
// declares OUT arguments and no RETURNS clause, spelled the way
// pg_get_function_result prints it, or empty for an argument list with no
// output.
//
// The server derives it from the arguments that return a value, OUT and INOUT
// alike: one such argument is the function's result, and two or more make it a
// record. Measured on PostgreSQL 18.6:
//
//	declared                          pg_get_function_result
//	a integer DEFAULT 1, OUT b text   text
//	INOUT a int4                      integer
//	a int, OUT b varchar(10)          character varying
//	a int, OUT b int, OUT c text      record
//
// A declaration that leaves RETURNS out is compared against that type.
// Compared as an empty clause, it differs from the catalog's on every plan
// (stokaro/ptah#3690).
func ImpliedResult(arguments string) string {
	var types []string
	for _, part := range splitTopLevel(arguments) {
		text, _, _ := CutDefault(part)
		mode, rest := splitArgumentMode(text)
		if (mode == "out" || mode == "inout") && rest != "" {
			types = append(types, normalizeRoutineType(withoutParameterName(rest)))
		}
	}
	switch len(types) {
	case 0:
		return ""
	case 1:
		return types[0]
	default:
		return "record"
	}
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
