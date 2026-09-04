package dbschema

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
)

// mysqlProbeKeyLimit is how many keys one table may carry, so it is how many
// names one probe statement can ask about.
//
// Measured on mysql:8.4.11 and mariadb:11.8.9: the sixty-fifth key answers
// `ERROR 1069 (42000): Too many keys specified; max 64 keys allowed`. One
// column of the probe table is spent per name, and the limit is on keys rather
// than columns, so it is the keys that bound a chunk.
const mysqlProbeKeyLimit = 64

// resolveMySQLFamilyIndexNames answers which of the given names the connected
// MySQL-family server treats as one index name.
//
// The two engines fold index names differently and neither fold is reachable
// through SQL, so nothing offline can partition these names -- measured on
// mysql:8.4.11 and mariadb:11.8.9, MySQL collides `İ`/`i` and `K`(U+212A)/`K`
// while MariaDB collides `I`/`ı` and `Σ`/`ς`, each accepting what the other
// refuses. What is reachable is the collision itself: a table carrying both
// names as keys either exists or answers `Duplicate key name`.
//
// A name whose runes are all ASCII is not asked about. Both engines fold ASCII
// identically and Ptah folds it the same way, so those names are already
// answered and a schema carrying no other kind reaches no server at all --
// which is every ordinary schema, and the reason this costs nothing in the
// common case (stokaro/ptah#2768).
func (dc *DatabaseConnection) resolveMySQLFamilyIndexNames(
	ctx context.Context,
	names []string,
) (identifier.Semantics, error) {
	info := dc.Info()
	offline := info.IdentifierSemantics.Normalize(info.Dialect)

	candidates := normalizedIdentifierNames(names)
	beyondASCII := beyondASCIINames(candidates)
	if len(beyondASCII) == 0 {
		return offline, nil
	}
	if len(beyondASCII) > mysqlProbeKeyLimit {
		return identifier.Semantics{}, fmt.Errorf(
			"resolve MySQL-family index names: %d names carry a non-ASCII rune and one probe "+
				"can ask about %d, so the server cannot be asked about all of them at once",
			len(beyondASCII), mysqlProbeKeyLimit)
	}

	classes := newNameClasses(candidates)
	if err := dc.probeMySQLIndexNameChunks(ctx, candidates, beyondASCII, classes); err != nil {
		return identifier.Semantics{}, err
	}
	return identifier.ForMySQLFamilyResolvedIndexNames(info.Dialect, classes.resolved()), nil
}

// probeMySQLIndexNameChunks asks the server about every pairing the offline
// rules cannot answer.
//
// Each chunk carries every non-ASCII name together with a slice of the rest, so
// all three kinds of pairing are covered: two non-ASCII names always share a
// chunk, a non-ASCII name shares one with every other name in turn, and two
// ASCII names need no server because both engines fold ASCII the way Ptah does.
// Sixty-four keys is the per-table ceiling, which is what makes the chunking
// necessary rather than a preference.
func (dc *DatabaseConnection) probeMySQLIndexNameChunks(
	ctx context.Context,
	candidates, beyondASCII []string,
	classes *nameClasses,
) error {
	rest := slices.DeleteFunc(slices.Clone(candidates), func(name string) bool {
		return slices.Contains(beyondASCII, name)
	})
	room := mysqlProbeKeyLimit - len(beyondASCII)
	if room < 1 {
		// Every slot is spent on the names that have to be in every chunk, so
		// the non-ASCII names are asked about among themselves and nothing else
		// fits. The refusal above keeps this from meaning silently less.
		return dc.probeMySQLIndexNameSet(ctx, beyondASCII, classes)
	}
	for start := 0; start < len(rest) || start == 0; start += room {
		end := min(start+room, len(rest))
		chunk := append(slices.Clone(beyondASCII), rest[start:end]...)
		if err := dc.probeMySQLIndexNameSet(ctx, chunk, classes); err != nil {
			return err
		}
		if end >= len(rest) {
			break
		}
	}
	return nil
}

// probeMySQLIndexNameSet asks whether one set of names can coexist as keys, and
// narrows to the colliding pair when it cannot.
//
// The whole set goes in one statement first, because that is the answer for
// every schema that has no collision at all: it either creates and every name
// is its own class, or it names a duplicate and the pairing has to be found.
// Pairwise narrowing is only reached by a schema that already carries a
// collision, which is a schema the comparison is about to report on anyway.
func (dc *DatabaseConnection) probeMySQLIndexNameSet(
	ctx context.Context,
	chunk []string,
	classes *nameClasses,
) error {
	created, err := dc.probeMySQLKeysCoexist(ctx, chunk)
	if err != nil {
		return err
	}
	if created {
		return nil
	}
	for left := range chunk {
		for right := left + 1; right < len(chunk); right++ {
			pair := []string{chunk[left], chunk[right]}
			coexist, err := dc.probeMySQLKeysCoexist(ctx, pair)
			if err != nil {
				return err
			}
			if !coexist {
				classes.merge(chunk[left], chunk[right])
			}
		}
	}
	return nil
}

// probeMySQLKeysCoexist reports whether the server accepts every name as a key
// on one table.
//
// The table is TEMPORARY, so it is this session's alone and gone when the
// connection closes, and it gives the probe exactly the namespace the question
// is about: a MySQL-family index name is unique per table, not per schema.
//
// A refusal that is not the duplicate-name answer is returned rather than read
// as "they collide". Losing the connection, or lacking the privilege to create
// a temporary table, must not look like an equivalence the server reported.
func (dc *DatabaseConnection) probeMySQLKeysCoexist(
	ctx context.Context,
	chunk []string,
) (bool, error) {
	var columns, keys strings.Builder
	for position, name := range chunk {
		if position > 0 {
			columns.WriteString(", ")
			keys.WriteString(", ")
		}
		fmt.Fprintf(&columns, "c%d INT", position)
		fmt.Fprintf(&keys, "KEY %s (c%d)", quoteMySQLIdentifier(name), position)
	}
	table := "ptah_index_name_probe"
	if _, err := dc.ExecContext(ctx, "DROP TEMPORARY TABLE IF EXISTS `"+table+"`"); err != nil {
		return false, fmt.Errorf("resolve MySQL-family index names: clear the probe table: %w", err)
	}
	statement := fmt.Sprintf("CREATE TEMPORARY TABLE `%s` (%s, %s)", table, columns.String(), keys.String())
	if _, err := dc.ExecContext(ctx, statement); err != nil {
		if isMySQLDuplicateKeyName(err) {
			return false, nil
		}
		return false, fmt.Errorf("resolve MySQL-family index names: ask the server: %w", err)
	}
	if _, err := dc.ExecContext(ctx, "DROP TEMPORARY TABLE IF EXISTS `"+table+"`"); err != nil {
		return false, fmt.Errorf("resolve MySQL-family index names: clear the probe table: %w", err)
	}
	return true, nil
}

// isMySQLDuplicateKeyName reports whether an error is the server saying two of
// the names are one name.
//
// Matched on the vendor code rather than the sentence, because the sentence
// carries the offending identifier and is localized; 1061 is the code both
// engines answer.
func isMySQLDuplicateKeyName(err error) bool {
	return strings.Contains(err.Error(), "Error 1061") ||
		strings.Contains(err.Error(), "1061 (42000)")
}

// quoteMySQLIdentifier renders a name as a MySQL-family quoted identifier.
func quoteMySQLIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// beyondASCIINames is the subset a server has to be asked about.
func beyondASCIINames(names []string) []string {
	beyond := make([]string, 0, len(names))
	for _, name := range names {
		if !isASCIIName(name) {
			beyond = append(beyond, name)
		}
	}
	return beyond
}

// isASCIIName reports whether every byte is ASCII, which for UTF-8 is the same
// question as whether every rune is.
func isASCIIName(name string) bool {
	for index := range len(name) {
		if name[index] >= 0x80 {
			return false
		}
	}
	return true
}

// nameClasses accumulates the equivalence classes a probe reports.
//
// It starts with every name in its own class and merges on each collision the
// server names, which is the order the probe discovers them in. The key of a
// class is its smallest member, which is the canonical form
// [identifier.Semantics] requires: a key must itself be one of the names, and
// must be the least of its class.
type nameClasses struct {
	names []string
	keys  map[string]string
}

func newNameClasses(names []string) *nameClasses {
	classes := &nameClasses{names: slices.Clone(names), keys: make(map[string]string, len(names))}
	for _, name := range names {
		classes.keys[name] = name
	}
	return classes
}

// merge records that the server treats two names as one, re-keying the whole
// joined class onto its smallest member.
func (n *nameClasses) merge(left, right string) {
	leftKey, rightKey := n.keys[left], n.keys[right]
	if leftKey == rightKey {
		return
	}
	winner := min(leftKey, rightKey)
	loser := max(leftKey, rightKey)
	for name, key := range n.keys {
		if key == loser {
			n.keys[name] = winner
		}
	}
}

// resolved renders the classes as the sorted mapping the semantics take.
func (n *nameClasses) resolved() []identifier.ResolvedName {
	resolved := make([]identifier.ResolvedName, 0, len(n.names))
	for _, name := range n.names {
		resolved = append(resolved, identifier.ResolvedName{Name: name, Key: n.keys[name]})
	}
	slices.SortFunc(resolved, func(a, b identifier.ResolvedName) int {
		return strings.Compare(a.Name, b.Name)
	})
	return resolved
}

// isMySQLFamilyDialect reports whether the dialect is one whose index-name
// equivalence has to be asked of the server.
func isMySQLFamilyDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}
