package dbschema

// White-box testing required: the note is assembled inside
// getDatabaseInfoWithCapabilities, which needs a live server. The assembly it
// performs -- resolve the version, then render the sentence -- is reachable
// here without one, and it is the part that can be wrong.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/servertarget"
)

// liveNote is what getDatabaseInfoWithCapabilities writes into
// ServerInfo.CapabilityNote, without the connection it needs to read a version.
func liveNote(dialect, version string) string {
	info := catalog.ServerInfo{Dialect: dialect, Version: version}
	return servertarget.VersionNote(dialect, version, resolveDatabaseCapabilities(info))
}

// TestCapabilityNote_TheLiveAndTypedPathsSayTheSameThing holds the two halves
// of one answer to each other.
//
// A version reaches Ptah two ways -- read from a server, or typed as
// --server-version -- and both ask the same question: what was actually
// planned. The typed path has said so on stderr since it shipped; the live path
// said it at DEBUG, so connecting to a server newer than anything measured
// planned with an older line's preset and reported nothing (stokaro/ptah#916).
//
// Asserting the two strings are equal is what stops a second wording from
// appearing here. Asserting only that the live one is non-empty would let this
// drift into its own sentence, which is the defect one layer up.
func TestCapabilityNote_TheLiveAndTypedPathsSayTheSameThing(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
		wantAny bool
	}{
		{
			// The row #916 is about: a server newer than anything this
			// repository has measured, planned with the top line's preset.
			name: "newer than the newest measured line", dialect: platform.Postgres,
			version: "9999.0", wantAny: true,
		},
		{
			// A version the ladder has, between measured lines.
			name: "a version that selected no measured line", dialect: platform.MySQL,
			version: "17.0", wantAny: true,
		},
		{
			// A dialect whose version refines nothing, so the version was read
			// and then discarded.
			name: "a dialect with no measured ladder", dialect: platform.SQLServer,
			version: "17.0", wantAny: true,
		},
		{
			// The control that keeps this from being a line on every run: a
			// measured release line resolves version-specific and says nothing.
			name: "a measured release line", dialect: platform.Postgres,
			version: "17.0", wantAny: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			live := liveNote(test.dialect, test.version)
			c.Assert(live != "", qt.Equals, test.wantAny)

			typed, err := servertarget.Resolve(test.dialect, test.version)
			c.Assert(err, qt.IsNil)
			c.Assert(live, qt.Equals, typed.Note,
				qt.Commentf("the same facts must produce the same sentence on both paths"))
		})
	}
}

// TestCapabilityNote_AnUnreadableBannerIsReportedRatherThanRefused pins the one
// place the two paths are allowed to differ, and why.
//
// A typed --server-version that names no server is a typo, and Resolve refuses
// it. A banner is not a typo -- a server does not misspell its own name -- so
// refusing there would take away a working connection over a string Ptah cannot
// read. The live path reports instead, and this is what keeps that difference
// deliberate rather than an accident of which function was called.
func TestCapabilityNote_AnUnreadableBannerIsReportedRatherThanRefused(t *testing.T) {
	c := qt.New(t)

	_, err := servertarget.Resolve(platform.Postgres, "not-a-version")
	c.Assert(err, qt.IsNotNil, qt.Commentf("a typed version that names no server is refused"))

	c.Assert(liveNote(platform.Postgres, "not-a-version"), qt.Not(qt.Equals), "",
		qt.Commentf("a banner Ptah cannot read is reported, and the connection stands"))
}
