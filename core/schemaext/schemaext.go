// Package schemaext defines immutable feature values, positive source coverage,
// versioned codecs, and conservative operation effects without importing database
// implementations or pipeline stages.
package schemaext

import (
	"strings"
	"unicode"
)

// Kind identifies a feature-owned semantic type independently of a database
// target or payload encoding version. It uses a namespaced path such as
// "example.org/feature/operation".
type Kind string

// Valid reports whether k is a nonempty namespaced path with no empty, dot,
// whitespace, or backslash components. Kind spelling is case-sensitive.
func (k Kind) Valid() bool {
	parts := strings.Split(string(k), "/")
	if len(parts) < 2 || !strings.Contains(parts[0], ".") {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsRune(part, '\\') || strings.ContainsFunc(part, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return false
		}
	}
	return true
}

// Impact is an owner's conservative classification of an operation. The zero
// value is unknown and must not be interpreted as safe.
type Impact string

const (
	// Additive adds state without removing data or protections.
	Additive Impact = "additive"
	// Behavioral may affect workloads, readers, or retention.
	Behavioral Impact = "behavioral"
	// Destructive removes data, objects, or protections.
	Destructive Impact = "destructive"
)

// Effect records an operation's known consequences. An empty or unrecognized
// Impact means the owner has not established the effect. Reason is a human
// explanation, not a serialization identity or comparison key.
type Effect struct {
	Impact Impact
	Reason string
}

// EffectSource provides already-computed operation metadata. It must be pure
// and local: no database access, transport call, or target discovery. An adapter
// for an external provider carries the metadata from its batched plan response.
// Payloads without this contract have unknown safety effects.
type EffectSource interface {
	Effect() Effect
}
