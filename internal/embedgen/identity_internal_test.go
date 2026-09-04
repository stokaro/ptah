package embedgen

// White-box testing required: the classification ratchet enumerates Spec's own
// fields against identityComponents and excludedFromIdentity, and neither list
// is reachable from outside the package.

import (
	"reflect"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestIdentity_EveryFieldIsClassified is the ratchet.
//
// The epic names both halves: the properties that MUST change a generation's
// identity, and the ones that must NOT. A field added to Spec without a
// decision joins neither list, and both mistakes are silent -- a field wrongly
// inside the identity makes every run a new generation, and one wrongly outside
// makes two incomparable corpora look like one.
//
// So the struct is enumerated and each leaf field has to appear in
// identityComponents' key list or in excludedFromIdentity, with a reason
// (stokaro/ptah#2068).
func TestIdentity_EveryFieldIsClassified(t *testing.T) {
	c := qt.New(t)

	c.Assert(unclassifiedFields(), qt.HasLen, 0, qt.Commentf(
		"each of these Spec fields must either appear in identityComponents or be listed in "+
			"excludedFromIdentity with the reason changing it leaves existing vectors comparable"))
}

// TestIdentity_TheExclusionsAreDeliberate keeps the other list honest.
//
// An exclusion with no reason is a field somebody wanted out of the digest
// without saying why, and the next reader cannot tell that from an oversight.
func TestIdentity_TheExclusionsAreDeliberate(t *testing.T) {
	c := qt.New(t)

	for _, field := range sortedKeys(excludedFromIdentity) {
		c.Assert(len(strings.TrimSpace(excludedFromIdentity[field])) > 20, qt.IsTrue,
			qt.Commentf("%s is excluded from the identity with no reason worth reading", field))
	}
}

// unclassifiedFields lists the Spec fields that are in neither list.
//
// The walk lives here rather than in the test body because a test asserts and
// does not branch, which is the rule scripts/check-test-style.sh enforces.
func unclassifiedFields() []string {
	included := includedComponentKeys()
	var unclassified []string
	for _, field := range leafFieldPaths(reflect.TypeFor[Spec](), "") {
		if _, excluded := excludedFromIdentity[field]; excluded {
			continue
		}
		if included[componentKeyFor(field)] {
			continue
		}
		unclassified = append(unclassified, field)
	}
	return unclassified
}

// includedComponentKeys lists the component labels identityComponents writes.
//
// The labels are the odd positions of a flat key/value list, plus the two
// ordered field lists whose label is followed by a count rather than a value.
func includedComponentKeys() map[string]bool {
	keys := make(map[string]bool)
	// Both branches, because one component is conditional: the overlap joins
	// the digest only for a specification that chunks, so a zero Spec's list
	// does not contain it and the field would read as unclassified. Reading
	// one branch would make the gate report a decision that was made as an
	// omission.
	branches := [][]string{
		(Spec{}).identityComponents(),
		Spec{Preprocessing: Preprocessing{Truncate: TruncateChunk}}.identityComponents(),
	}
	for _, branch := range branches {
		for _, component := range branch {
			if strings.Contains(component, ".") || component == "spec" || component == "contract" {
				keys[component] = true
			}
		}
	}
	return keys
}

// componentKeyFor maps a Go field path such as Source.InputFields onto the
// component label identityComponents uses for it.
func componentKeyFor(fieldPath string) string {
	replacer := strings.NewReplacer(
		"Preprocessing.", "pre.",
		"Source.", "source.",
		"Model.", "model.",
		"Target.", "target.",
	)
	lowered := replacer.Replace(fieldPath)
	parts := strings.SplitN(lowered, ".", 2)
	if len(parts) != 2 {
		return snakeCase(lowered)
	}
	return parts[0] + "." + snakeCase(parts[1])
}

// snakeCase renders a Go field name the way the component labels spell it.
func snakeCase(name string) string {
	var b strings.Builder
	for index, symbol := range name {
		if symbol >= 'A' && symbol <= 'Z' {
			if index > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(symbol - 'A' + 'a')
			continue
		}
		b.WriteRune(symbol)
	}
	return b.String()
}

// leafFieldPaths lists the dotted paths of every leaf field, descending into
// the struct-valued ones so a field added to Source or Model is enumerated too.
func leafFieldPaths(typ reflect.Type, prefix string) []string {
	var paths []string
	for field := range typ.Fields() {
		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}
		if field.Type.Kind() == reflect.Struct {
			paths = append(paths, leafFieldPaths(field.Type, path)...)
			continue
		}
		paths = append(paths, path)
	}
	return paths
}
