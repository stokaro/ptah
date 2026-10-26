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
package routinesetting

import (
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
// declaration with a catalog row: the name in lower case, since a setting's
// name is not case-sensitive, and the value's parts, each literal and quoted
// name by its contents and each bare word in lower case, as the server folds
// it, joined by a comma and a space. `work_mem='8MB'` and the catalog's
// `work_mem=8MB` compare equal, and so do `search_path=pg_catalog,pg_temp` and
// `search_path='pg_catalog', 'pg_temp'`.
func Comparable(setting string) string {
	setting = strings.TrimSpace(setting)
	name, value, found := strings.Cut(setting, "=")
	if !found {
		return strings.ToLower(setting)
	}
	return strings.ToLower(strings.TrimSpace(name)) + "=" + strings.Join(valueParts(value), ", ")
}

// ComparableAll is [Comparable] for every setting in a list, dropping the
// empty ones.
func ComparableAll(settings []string) []string {
	return foldAll(settings, Comparable)
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

// FromCatalog spells one proconfig entry, `name=value` with the value as the
// server keeps it, in SQL, as pg_get_functiondef does. A list setting's
// elements are separated by commas, an element the server quoted as a name,
// `"B"`, among them; each becomes a literal of its contents. Any other value
// becomes one literal, its own quotes doubled: `8MB` is `'8MB'`.
func FromCatalog(entry string) string {
	name, value, found := strings.Cut(strings.TrimSpace(entry), "=")
	if !found {
		return strings.TrimSpace(entry)
	}
	name = strings.TrimSpace(name)
	if !listSettings[strings.ToLower(name)] {
		return name + "=" + sqlident.StringLiteral(value)
	}
	elements := catalogListElements(value)
	literals := make([]string, 0, len(elements))
	for _, element := range elements {
		literals = append(literals, sqlident.StringLiteral(element))
	}
	return name + "=" + strings.Join(literals, ", ")
}

// catalogListElements splits a list setting's value as the server keeps it:
// elements separated by commas, white space around each dropped, and an
// element in double quotes read without them, a doubled quote inside as one.
func catalogListElements(value string) []string {
	var elements []string
	for rest := strings.TrimSpace(value); ; {
		var element string
		if strings.HasPrefix(rest, `"`) {
			element, rest = cutQuoted(rest, '"')
		} else {
			end := strings.IndexByte(rest, ',')
			if end < 0 {
				end = len(rest)
			}
			element, rest = strings.TrimSpace(rest[:end]), rest[end:]
		}
		elements = append(elements, element)
		rest = strings.TrimSpace(rest)
		if !strings.HasPrefix(rest, ",") {
			return elements
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
			part, rest = cutQuoted(rest, '\'')
		case strings.HasPrefix(rest, `"`):
			part, rest = cutQuoted(rest, '"')
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
// and returns its contents, a doubled quote inside read as one, and what
// follows the closing quote. An unclosed part runs to the end of text.
func cutQuoted(text string, quote byte) (contents, rest string) {
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
		return b.String(), text[i+1:]
	}
	return b.String(), ""
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
