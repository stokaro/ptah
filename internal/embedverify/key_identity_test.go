package embedverify_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedverify"
)

// TestKeyIdentity_ComponentsCannotForgeABoundaryFailurePath is
// stokaro/ptah#2744, stated as the property rather than as the one pair.
//
// The identity was a join on U+001F, chosen for rarity. Rarity is not the
// property a key comparison needs: a TEXT column may hold that byte, and with
// `key_fields: [tenant, id]` these two rows joined to the same string. Coverage
// then compared one source row against the other's target row, and the layer
// that exists to answer key by key answered about the wrong key.
func TestKeyIdentity_ComponentsCannotForgeABoundaryFailurePath(t *testing.T) {
	tests := []struct {
		name  string
		left  []string
		right []string
	}{
		{
			name:  "the reported pair, across the unit separator",
			left:  []string{"a\x1fb", "c"},
			right: []string{"a", "b\x1fc"},
		},
		{
			name:  "across the colon the encoding itself uses",
			left:  []string{"1:a", "b"},
			right: []string{"1", ":ab"},
		},
		{
			name:  "a component holding an encoded list",
			left:  []string{"1:a1:b", ""},
			right: []string{"1:a", "1:b"},
		},
		{
			name:  "an empty component is a component",
			left:  []string{"", "ab"},
			right: []string{"a", "b"},
		},
		{
			name:  "one component against two that concatenate to it",
			left:  []string{"ab"},
			right: []string{"a", "b"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(embedverify.KeyIdentity(test.left...), qt.Not(qt.Equals),
				embedverify.KeyIdentity(test.right...))
		})
	}
}

// TestKeyIdentity_EqualKeysAreOneIdentityHappyPath is the control the failure
// path needs.
//
// An encoder that answered a fresh value every call would satisfy every row
// above and stop the walk matching a source row to its own target row, which is
// the whole of what verification does.
func TestKeyIdentity_EqualKeysAreOneIdentityHappyPath(t *testing.T) {
	tests := []struct {
		name       string
		components []string
	}{
		{name: "one component", components: []string{"4"}},
		{name: "two components", components: []string{"acme", "2"}},
		{name: "a component holding the old separator", components: []string{"a\x1fb", "c"}},
		{name: "an empty component", components: []string{"", ""}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(embedverify.KeyIdentity(test.components...), qt.Equals,
				embedverify.KeyIdentity(test.components...))
			c.Assert(embedverify.RenderKey(embedverify.KeyIdentity(test.components...)),
				qt.Not(qt.Equals), "")
		})
	}
}
