package atlasreport_test

import (
	"encoding/base64"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasreport"
)

// fingerprintWithSeparator is a digest whose standard Base64 encoding contains
// "/", which is the case the documented --name-format example cannot survive.
// It is a fixed value rather than a search, so the test measures the encoding
// rather than the luck of a hash.
const fingerprintWithSeparator = "sha256:" +
	"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

func TestNewSchemaPlanName_SafeFieldsCarryTheSameBytes(t *testing.T) {
	c := qt.New(t)

	name, err := atlasreport.NewSchemaPlanName(fingerprintWithSeparator, fingerprintWithSeparator)

	c.Assert(err, qt.IsNil)
	standard, err := base64.StdEncoding.DecodeString(name.ToHash)
	c.Assert(err, qt.IsNil)
	safe, err := base64.RawURLEncoding.DecodeString(name.ToHashSafe)
	c.Assert(err, qt.IsNil)
	c.Assert(safe, qt.DeepEquals, standard,
		qt.Commentf("the safe rendering must be the same digest, not a different one"))
}

// TestNewSchemaPlanName_SafeFieldsHoldNoSeparator is the property the fields
// exist for. A name built from them is always a legal file name, so the
// command cannot refuse the name its own template produced.
func TestNewSchemaPlanName_SafeFieldsHoldNoSeparator(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fingerprint string
	}{
		{name: "all ones", fingerprint: fingerprintWithSeparator},
		{
			name: "a value whose standard encoding holds a plus",
			fingerprint: "sha256:" +
				"fbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfb",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasreport.NewSchemaPlanName(tc.fingerprint, tc.fingerprint)

			c.Assert(err, qt.IsNil)
			// "/" is the character that breaks a file name. "+" is checked
			// too because the URL-safe alphabet replaces both, so its presence
			// would mean the wrong encoder ran -- not that the name is unusable.
			c.Assert(strings.ContainsAny(got.ToHashSafe, "/+"), qt.IsFalse)
			c.Assert(strings.ContainsAny(got.FromHashSafe, "/+"), qt.IsFalse)
		})
	}
}

// TestNewSchemaPlanName_PlusIsNotAHazard records the half of the alphabet that
// turned out not to matter.
//
// Standard Base64 adds two characters the URL-safe one does not, and only one
// of them breaks a file name. Pinning "+" as harmless keeps a later reader from
// widening the refusal to cover it, which would reject names that work.
func TestNewSchemaPlanName_PlusIsNotAHazard(t *testing.T) {
	c := qt.New(t)

	got, err := atlasreport.NewSchemaPlanName(
		"sha256:"+strings.Repeat("fa", 32), "sha256:"+strings.Repeat("fa", 32))

	c.Assert(err, qt.IsNil)
	c.Assert(got.ToHash, qt.Contains, "+")
	c.Assert(got.ToHash, qt.Not(qt.Contains), "/",
		qt.Commentf("this fixture isolates the plus so the assertion below is about it alone"))
}

// TestNewSchemaPlanName_AtlasShapedFieldsAreUnchanged pins that the fix adds a
// rendering rather than replacing one. The standard-Base64 fields are what make
// Atlas's documented example carry the same value, and that is their whole
// purpose.
func TestNewSchemaPlanName_AtlasShapedFieldsAreUnchanged(t *testing.T) {
	c := qt.New(t)

	got, err := atlasreport.NewSchemaPlanName(fingerprintWithSeparator, fingerprintWithSeparator)

	c.Assert(err, qt.IsNil)
	c.Assert(got.ToHash, qt.Equals, "//////////////////////////////////////////8=")
	c.Assert(got.ToHash, qt.Contains, "/",
		qt.Commentf("this is the hazard, and it stays: the field mirrors Atlas"))
}
