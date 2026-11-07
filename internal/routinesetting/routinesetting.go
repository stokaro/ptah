// Package routinesetting holds the one spelling of a routine's own
// configuration settings, so a declaration and a catalog row compare.
//
// A setting is held as `name=value`, its value in SQL: the text a `SET name =
// value` clause writes after the equals sign, quotes included, as a
// declaration spells it. PostgreSQL keeps the setting in `pg_proc.proconfig`
// as `name=value` with the value unquoted, so [Split] quotes what it reads
// the way pg_get_functiondef prints it: a value that was `'8MB'` reads back as
// 8MB, and written back unquoted, `SET work_mem = 8MB`, the server refuses it
// with "trailing junk after numeric literal".
//
// A declaration writes `SET name = value`, `SET name TO value`, a literal or a
// bare word, and a comma-separated list with spaces the catalog does not keep.
// Comparing those spellings as text would plan a routine rebuild on every run
// for a routine nobody changed -- the failure mode #2044 fixed for CHECK
// constraints, arriving here through a different door (stokaro/ptah#2356) --
// so [Comparable] reads each value down to what the server stores.
//
// A document that holds the catalog's spelling, as the Atlas HCL `set` does,
// crosses to the held spelling through [ParseCatalog] and back through
// [ToCatalog].
package routinesetting

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/sqlident"
)

// listSettings are the settings PostgreSQL keeps as a list of names, each
// element of which pg_get_functiondef quotes on its own:
// `SET search_path TO 'a', 'B', 'pg_catalog'`. Every other setting's value is
// one literal, `SET "DateStyle" TO 'ISO, MDY'`.
var listSettings = map[string]bool{
	"search_path":               true,
	"temp_tablespaces":          true,
	"local_preload_libraries":   true,
	"session_preload_libraries": true,
}

// Normalize folds one setting onto the spelling a declaration is kept in:
// `name=value`, with the white space between the parts of the value removed.
// White space inside a quoted part is the value's own and is kept.
//
// `FROM CURRENT` is left alone. The server resolves it when the routine is
// defined, so no declared form can equal what the catalog reports back, and
// folding it would claim the two are comparable.
func Normalize(setting string) string {
	setting = strings.TrimSpace(setting)
	name, value, found := strings.Cut(setting, "=")
	if !found {
		return setting
	}
	return strings.TrimSpace(name) + "=" + withoutSpaceOutsideQuotes(value)
}

// NormalizeAll folds every setting in a list, dropping the empty ones a catalog
// or an author may leave behind.
func NormalizeAll(settings []string) []string {
	return foldAll(settings, Normalize)
}

// Comparable is setting read down to what the server stores, for comparing a
// declaration with a catalog row: the name without quotes and in lower case,
// since a setting's name is not case-sensitive, and the value's parts, each
// literal and quoted name by its contents and each bare word in lower case, as
// the server folds it. `work_mem='8MB'` and the catalog's `work_mem=8MB`
// compare equal, so do `search_path=pg_catalog,pg_temp` and
// `search_path='pg_catalog', 'pg_temp'`, and so do `"TimeZone"='UTC'` and the
// catalog's `TimeZone=UTC`.
//
// The parts are joined as the server joins them. A list setting keeps each
// element apart, so `search_path='a, b'`, one schema named "a, b", is not
// `search_path=a, b`, two schemas; each element is written as a literal. Any
// other setting stores its parts as one value joined by a comma and a space,
// so `DateStyle=ISO, MDY` and `DateStyle='iso, mdy'` are one value.
func Comparable(setting string) string {
	setting = strings.TrimSpace(setting)
	name, value, found := strings.Cut(setting, "=")
	if !found {
		return strings.ToLower(setting)
	}
	name = settingName(name)
	parts := valueParts(value)
	if !listSettings[name] {
		return name + "=" + strings.Join(parts, ", ")
	}
	literals := make([]string, 0, len(parts))
	for _, part := range parts {
		literals = append(literals, sqlident.StringLiteral(part))
	}
	return name + "=" + strings.Join(literals, ", ")
}

// ComparableAll is [Comparable] for every setting in a list, dropping the
// empty ones, in the order of the names. The server applies every setting
// before the body, so two declarations that list the same settings in another
// order are one routine, and a document whose reader sorts its settings, as
// the Atlas HCL reader does, compares equal to the catalog's order. A name
// set twice keeps its last value, which is the one the server stores.
func ComparableAll(settings []string) []string {
	folded := foldAll(settings, Comparable)
	last := make(map[string]int, len(folded))
	for i, setting := range folded {
		last[comparableName(setting)] = i
	}
	kept := make([]string, 0, len(last))
	for i, setting := range folded {
		if last[comparableName(setting)] == i {
			kept = append(kept, setting)
		}
	}
	slices.Sort(kept)
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// comparableName is the name part of a setting [Comparable] folded, or the
// whole setting when it has no value.
func comparableName(setting string) string {
	name, _, _ := strings.Cut(setting, "=")
	return name
}

// settingName is a setting's name as the server looks it up: without the
// double quotes of a quoted name, `"TimeZone"`, or of each quoted part of a
// dotted one, and in lower case, since the lookup ignores case.
func settingName(name string) string {
	return strings.ToLower(unquotedName(name))
}

// Split reads the newline-joined form the PostgreSQL reader asks the catalog
// for, and spells each entry's value in SQL; see [FromCatalog].
//
// Newline-joined rather than comma-joined because a value is itself
// comma-separated: `search_path=pg_catalog, pg_temp` is one setting, and
// joining on a comma would make it two.
func Split(joined string) []string {
	if strings.TrimSpace(joined) == "" {
		return nil
	}
	entries := strings.Split(joined, "\n")
	spelled := make([]string, 0, len(entries))
	for _, entry := range entries {
		if strings.TrimSpace(entry) != "" {
			spelled = append(spelled, FromCatalog(entry))
		}
	}
	if len(spelled) == 0 {
		return nil
	}
	return spelled
}

// ToCatalog spells setting, a `name=value` whose value is written in SQL, as
// the server keeps it in proconfig: the name without its quotes, a list
// setting's elements separated by a comma and a space, each in double quotes
// unless it is a plain lower-case name, and any other value's parts joined by
// a comma and a space. It is the spelling [FromCatalog] reads, so the two
// round-trip: `work_mem='8MB'` is `work_mem=8MB`, and
// `search_path='$user', public` is `search_path="$user", public`. A setting
// without a value, such as `search_path FROM CURRENT`, is returned as it is.
func ToCatalog(setting string) string {
	setting = strings.TrimSpace(setting)
	name, value, found := strings.Cut(setting, "=")
	if !found {
		return setting
	}
	unquoted := unquotedName(name)
	parts := valueParts(value)
	if !listSettings[strings.ToLower(unquoted)] {
		return unquoted + "=" + strings.Join(parts, ", ")
	}
	elements := make([]string, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, catalogListElement(part))
	}
	return unquoted + "=" + strings.Join(elements, ", ")
}

// unquotedName is name without the double quotes of a quoted name or of each
// quoted part of a dotted one, in the case it is written in.
func unquotedName(name string) string {
	var b strings.Builder
	for rest := strings.TrimSpace(name); rest != ""; {
		if strings.HasPrefix(rest, `"`) {
			var part string
			part, rest, _ = cutQuoted(rest, '"')
			b.WriteString(part)
			continue
		}
		b.WriteByte(rest[0])
		rest = rest[1:]
	}
	return strings.TrimSpace(b.String())
}

// catalogListElement is one element of a list setting as the server keeps
// it: a name of lower-case letters, digits and underscores that does not
// start with a digit as it is, and anything else in double quotes, a quote
// inside doubled, as quote_identifier writes it. A keyword stays bare where
// quote_identifier would quote it, and the server reads both as one name.
func catalogListElement(element string) string {
	plain := element != "" && (element[0] < '0' || element[0] > '9')
	for i := 0; plain && i < len(element); i++ {
		char := element[i]
		plain = char == '_' || (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9')
	}
	if plain {
		return element
	}
	return `"` + strings.ReplaceAll(element, `"`, `""`) + `"`
}

// FromCatalog spells one proconfig entry, `name=value` with the value as the
// server keeps it, in SQL, as pg_get_functiondef does. A list setting's
// elements are separated by commas, an element the server quoted as a name,
// `"B"`, among them; each becomes a literal of its contents. Any other value
// becomes one literal, its own quotes doubled: `8MB` is `'8MB'`.
//
// It takes what a server wrote, so it does not refuse anything: a list the
// server could not have kept stays one literal, which compares as written. A
// value an author wrote goes through [ParseCatalog].
func FromCatalog(entry string) string {
	spelled, err := ParseCatalog(entry)
	if err != nil {
		name, value, _ := strings.Cut(entry, "=")
		return strings.TrimSpace(name) + "=" + sqlident.StringLiteral(value)
	}
	return spelled
}

// ParseCatalog is [FromCatalog] for an entry an author wrote in the catalog's
// spelling, as the Atlas HCL `set` is. It refuses a list setting's value the
// server would not keep, rather than guess at the names in it.
//
// A list is read as the server reads it back, so a bare element is folded to
// lower case: `MySchema, public` names the schema myschema. A name that needs
// its case or a comma kept goes in double quotes, `"MySchema", public`. A single
// quote is SQL's spelling and not the catalog's: the server writes a name
// holding one in double quotes, so a bare `'public'` is refused rather than
// taken for a schema whose name has quotes in it. Any other setting's value is
// one string, white space around it included, as the server keeps it.
func ParseCatalog(entry string) (string, error) {
	name, value, found := strings.Cut(entry, "=")
	if !found {
		return strings.TrimSpace(entry), nil
	}
	name = strings.TrimSpace(name)
	if !listSettings[settingName(name)] {
		return name + "=" + sqlident.StringLiteral(value), nil
	}
	elements, ok := catalogListElements(value)
	if !ok {
		return "", fmt.Errorf(`%q is not a list as pg_proc.proconfig keeps it: `+
			`write names separated by commas, in double quotes where a name needs them, such as %q`,
			value, `"$user", public`)
	}
	literals := make([]string, 0, len(elements))
	for _, element := range elements {
		literals = append(literals, sqlident.StringLiteral(element))
	}
	return name + "=" + strings.Join(literals, ", "), nil
}

// catalogListElements splits a list setting's value by the rules the server
// reads one back with: elements separated by commas, white space around each
// dropped, an element in double quotes read without them and a doubled quote
// inside as one, and any other element running to the next comma or white space
// and folded to lower case. An empty value is the one empty element the server
// stores for an empty literal. It reports false for a value those rules do not
// read, and for a bare element holding a single quote, which the server never
// writes.
func catalogListElements(value string) ([]string, bool) {
	rest := strings.TrimSpace(value)
	if rest == "" {
		return []string{""}, true
	}
	var elements []string
	for {
		var element string
		if strings.HasPrefix(rest, `"`) {
			var closed bool
			if element, rest, closed = cutQuoted(rest, '"'); !closed {
				return nil, false
			}
		} else {
			end := strings.IndexAny(rest, ", \t\r\n")
			if end < 0 {
				end = len(rest)
			}
			element, rest = strings.ToLower(rest[:end]), rest[end:]
			if element == "" || strings.ContainsRune(element, '\'') {
				return nil, false
			}
		}
		elements = append(elements, element)
		rest = strings.TrimSpace(rest)
		if rest == "" {
			return elements, true
		}
		if rest[0] != ',' {
			return nil, false
		}
		rest = strings.TrimSpace(rest[1:])
	}
}

// valueParts reads a value written in SQL into the parts the server stores: a
// literal's contents, a quoted name's contents, and a bare word folded to lower
// case, one part per comma-separated element.
func valueParts(value string) []string {
	var parts []string
	for rest := strings.TrimSpace(value); rest != ""; {
		var part string
		switch {
		case strings.HasPrefix(rest, "'"):
			part, rest, _ = cutQuoted(rest, '\'')
		case strings.HasPrefix(rest, `"`):
			part, rest, _ = cutQuoted(rest, '"')
		default:
			end := strings.IndexByte(rest, ',')
			if end < 0 {
				end = len(rest)
			}
			part, rest = strings.ToLower(strings.TrimSpace(rest[:end])), rest[end:]
		}
		parts = append(parts, part)
		rest = strings.TrimSpace(rest)
		if !strings.HasPrefix(rest, ",") {
			if rest != "" {
				// Text after a quoted part that is not another element is
				// kept, so a value this cannot read still compares as
				// written.
				parts = append(parts, rest)
			}
			return parts
		}
		rest = strings.TrimSpace(rest[1:])
	}
	return parts
}

// cutQuoted reads the part of text quoted by quote, which text starts with,
// and returns its contents, a doubled quote inside read as one, what follows
// the closing quote, and whether there was one. An unclosed part runs to the
// end of text.
func cutQuoted(text string, quote byte) (contents, rest string, closed bool) {
	var b strings.Builder
	for i := 1; i < len(text); i++ {
		if text[i] != quote {
			b.WriteByte(text[i])
			continue
		}
		if i+1 < len(text) && text[i+1] == quote {
			b.WriteByte(quote)
			i++
			continue
		}
		return b.String(), text[i+1:], true
	}
	return b.String(), "", false
}

// withoutSpaceOutsideQuotes is value with the white space between its parts
// removed, the white space inside a single- or double-quoted part kept.
func withoutSpaceOutsideQuotes(value string) string {
	var b strings.Builder
	var quote byte
	for i := 0; i < len(value); i++ {
		char := value[i]
		switch {
		case quote != 0:
			if char == quote {
				quote = 0
			}
		case char == '\'' || char == '"':
			quote = char
		case char == ' ' || char == '\t' || char == '\n' || char == '\r':
			continue
		}
		b.WriteByte(char)
	}
	return b.String()
}

// foldAll applies fold to each setting, dropping the empty ones.
func foldAll(settings []string, fold func(string) string) []string {
	folded := make([]string, 0, len(settings))
	for _, setting := range settings {
		if normalized := fold(setting); normalized != "" {
			folded = append(folded, normalized)
		}
	}
	if len(folded) == 0 {
		return nil
	}
	return folded
}
