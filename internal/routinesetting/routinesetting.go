// Package routinesetting holds the one spelling of a routine's own
// configuration settings, so a declaration and a catalog row compare.
//
// PostgreSQL keeps them in `pg_proc.proconfig` as `name=value` entries. A
// declaration writes `SET name = value`, `SET name TO value`, or a
// comma-separated list with spaces the catalog does not keep. Comparing those
// two spellings without folding them would plan a routine rebuild on every run
// for a routine nobody changed -- the failure mode #2044 fixed for CHECK
// constraints, arriving here through a different door (stokaro/ptah#2356).
package routinesetting

import "strings"

// Normalize folds one setting onto the spelling both sides compare in:
// `name=value`, with the whitespace a value may be written with removed.
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
	return strings.TrimSpace(name) + "=" + strings.Join(strings.Fields(value), "")
}

// NormalizeAll folds every setting in a list, dropping the empty ones a catalog
// or an author may leave behind.
func NormalizeAll(settings []string) []string {
	folded := make([]string, 0, len(settings))
	for _, setting := range settings {
		if normalized := Normalize(setting); normalized != "" {
			folded = append(folded, normalized)
		}
	}
	if len(folded) == 0 {
		return nil
	}
	return folded
}

// Split reads the newline-joined form the PostgreSQL reader asks the catalog
// for, and folds each entry.
//
// Newline-joined rather than comma-joined because a value is itself
// comma-separated: `search_path=pg_catalog, pg_temp` is one setting, and
// joining on a comma would make it two.
func Split(joined string) []string {
	if strings.TrimSpace(joined) == "" {
		return nil
	}
	return NormalizeAll(strings.Split(joined, "\n"))
}
