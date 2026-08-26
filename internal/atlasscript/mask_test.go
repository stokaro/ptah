package atlasscript_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasscript"
)

// TestMask_RewritesEachMethodAsDocumented pins the four methods.
func TestMask_RewritesEachMethodAsDocumented(t *testing.T) {
	tests := []struct {
		name  string
		mask  atlasscript.Mask
		value string
		want  string
	}{
		{
			name:  "REDACT uses the default token",
			mask:  atlasscript.Mask{Method: atlasscript.MaskRedact},
			value: "ada@example.com", want: "***",
		},
		{
			name:  "REDACT honors a token",
			mask:  atlasscript.Mask{Method: atlasscript.MaskRedact, Token: "<hidden>"},
			value: "ada@example.com", want: "<hidden>",
		},
		{
			name:  "PARTIAL keeps the right",
			mask:  atlasscript.Mask{Method: atlasscript.MaskPartial, KeepRight: 4},
			value: "4111111111111234", want: "***1234",
		},
		{
			name:  "PARTIAL counts characters, not bytes",
			mask:  atlasscript.Mask{Method: atlasscript.MaskPartial, KeepRight: 2},
			value: "naïve", want: "***ve",
		},
		{
			name:  "REPLACE rewrites what matches",
			mask:  atlasscript.Mask{Method: atlasscript.MaskReplace, Match: `\d`, With: "#"},
			value: "id-2026", want: "id-####",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			mask := test.mask
			c.Assert(mask.Compile(), qt.IsNil)

			c.Assert(mask.Apply(test.value), qt.Equals, test.want)
		})
	}
}

// A partial mask that would keep the whole value redacts it instead.
//
// A short value is the case where a partial mask protects least and matters
// most: `keep_right = 4` over a four-character value would otherwise return the
// value unchanged, which is a mask that ran and did nothing.
func TestMask_PartialRefusesToReturnTheValueWhole(t *testing.T) {
	tests := []struct {
		name      string
		keepRight int
		value     string
	}{
		{name: "keeping exactly the length", keepRight: 4, value: "1234"},
		{name: "keeping more than the length", keepRight: 9, value: "1234"},
		{name: "keeping nothing", keepRight: 0, value: "1234"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			mask := atlasscript.Mask{Method: atlasscript.MaskPartial, KeepRight: test.keepRight}
			c.Assert(mask.Compile(), qt.IsNil)

			got := mask.Apply(test.value)

			c.Assert(got, qt.Equals, "***")
			c.Assert(got, qt.Not(qt.Contains), test.value)
		})
	}
}

// HASH is deterministic under one salt and uncorrelated across two.
//
// Both halves matter and neither implies the other. Determinism is what makes a
// hashed column joinable within a report; the salt is what stops two
// deployments' reports being joined to each other.
func TestMask_HashIsDeterministicAndSalted(t *testing.T) {
	c := qt.New(t)
	one := atlasscript.Mask{Method: atlasscript.MaskHash, Salt: "salt-a"}
	same := atlasscript.Mask{Method: atlasscript.MaskHash, Salt: "salt-a"}
	other := atlasscript.Mask{Method: atlasscript.MaskHash, Salt: "salt-b"}
	for _, mask := range []*atlasscript.Mask{&one, &same, &other} {
		c.Assert(mask.Compile(), qt.IsNil)
	}

	c.Assert(one.Apply("ada@example.com"), qt.Equals, same.Apply("ada@example.com"))
	c.Assert(one.Apply("ada@example.com"), qt.Not(qt.Equals), other.Apply("ada@example.com"))
	c.Assert(one.Apply("ada@example.com"), qt.Not(qt.Equals), one.Apply("grace@example.com"))
	// The digest never carries the value it replaced.
	c.Assert(one.Apply("ada@example.com"), qt.Not(qt.Contains), "ada")
}

// An unkeyed hash is still a hash, rather than an error or a passthrough.
//
// A script that omits the salt has weaker protection, not none — and returning
// the value would be the one outcome that reads as a mask working.
func TestMask_HashWithNoSaltStillHashes(t *testing.T) {
	c := qt.New(t)
	mask := atlasscript.Mask{Method: atlasscript.MaskHash}
	c.Assert(mask.Compile(), qt.IsNil)

	got := mask.Apply("ada@example.com")

	c.Assert(got, qt.Not(qt.Equals), "ada@example.com")
	c.Assert([]byte(got), qt.HasLen, 64)
}

// A broken mask is refused before a database is touched.
//
// A REPLACE whose pattern does not compile is a broken script, and finding that
// out on row 400,000 is finding it out too late.
func TestMask_CompileRefusesABrokenRule(t *testing.T) {
	tests := []struct {
		name string
		mask atlasscript.Mask
		says string
	}{
		{
			name: "an unknown method",
			mask: atlasscript.Mask{Method: "SHRED"},
			says: "unknown method",
		},
		{
			name: "REPLACE with no pattern",
			mask: atlasscript.Mask{Method: atlasscript.MaskReplace, With: "x"},
			says: "no match pattern",
		},
		{
			name: "REPLACE with a pattern that does not compile",
			mask: atlasscript.Mask{Method: atlasscript.MaskReplace, Match: "([", With: "x"},
			says: "match pattern",
		},
		{
			name: "PARTIAL with a negative keep",
			mask: atlasscript.Mask{Method: atlasscript.MaskPartial, KeepRight: -1},
			says: "keep_right is negative",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			mask := test.mask

			err := mask.Compile()

			c.Assert(err, qt.ErrorMatches, ".*"+test.says+".*")
		})
	}
}

// A mask names the columns it covers, and an unqualified one covers all.
func TestMask_CoversTheColumnsItNames(t *testing.T) {
	tests := []struct {
		name    string
		columns []string
		column  string
		want    bool
	}{
		{name: "no columns covers everything", columns: nil, column: "email", want: true},
		{name: "a named column", columns: []string{"email"}, column: "email", want: true},
		{name: "case does not separate them", columns: []string{"Email"}, column: "email", want: true},
		{name: "another column is not covered", columns: []string{"email"}, column: "name", want: false},
		{name: "one of several", columns: []string{"a", "email", "b"}, column: "email", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			mask := atlasscript.Mask{Method: atlasscript.MaskRedact, Columns: test.columns}

			c.Assert(mask.Covers(test.column), qt.Equals, test.want)
		})
	}
}

// Declaration order decides, and the first match wins.
//
// Not interchangeable with last-match: a script that redacts one column and
// then declares a broad rule expects the narrow one to hold. Applying both in
// turn would be a third, worse answer — hashing a redacted value publishes a
// digest of the token rather than of the datum, which looks like a mask
// working.
func TestMaskSet_FirstDeclaredMatchWins(t *testing.T) {
	c := qt.New(t)
	set := atlasscript.MaskSet{
		{Method: atlasscript.MaskRedact, Columns: []string{"email"}, Token: "<narrow>"},
		{Method: atlasscript.MaskPartial, KeepRight: 3},
	}
	c.Assert(set.Compile(), qt.IsNil)

	c.Assert(set.Apply("email", "ada@example.com"), qt.Equals, "<narrow>")
	// The broad rule still reaches a column the narrow one does not name.
	c.Assert(set.Apply("phone", "5550001234"), qt.Equals, "***234")
	// A column no mask covers is returned as it was.
	set = atlasscript.MaskSet{{Method: atlasscript.MaskRedact, Columns: []string{"email"}}}
	c.Assert(set.Compile(), qt.IsNil)
	c.Assert(set.Apply("id", "7"), qt.Equals, "7")
}

// The set reports the first broken rule rather than the last, so the message
// names the mask the author has to fix.
func TestMaskSet_CompileReportsTheBrokenRule(t *testing.T) {
	c := qt.New(t)
	set := atlasscript.MaskSet{
		{Name: "ok", Method: atlasscript.MaskRedact},
		{Name: "broken", Method: atlasscript.MaskReplace, Match: "(["},
		{Name: "also-broken", Method: "SHRED"},
	}

	err := set.Compile()

	c.Assert(err, qt.ErrorMatches, `.*"broken".*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "also-broken")
}
