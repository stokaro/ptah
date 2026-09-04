package identifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
)

// The four pairs the two engines answer oppositely, plus the pair they agree
// on. Every non-ASCII rune is written as an escape rather than pasted: the
// Kelvin sign in particular does not survive an ordinary editor or shell round
// trip, and written literally it arrives as ASCII "K", which turns the row into
// a name compared with itself and passes against any implementation at all.
const (
	dottedCapitalI = "\u0130" // LATIN CAPITAL LETTER I WITH DOT ABOVE
	dotlessI       = "\u0131" // LATIN SMALL LETTER DOTLESS I
	capitalSigma   = "\u03A3" // GREEK CAPITAL LETTER SIGMA
	finalSigma     = "\u03C2" // GREEK SMALL LETTER FINAL SIGMA
	kelvinSign     = "\u212A" // KELVIN SIGN
	aWithDiaeresis = "\u00E4" // LATIN SMALL LETTER A WITH DIAERESIS
)

// TestFixtureRunesAreTheCodePointsTheyName is the control on this file's own
// inputs, and it is not ceremony: the Kelvin sign does not survive a paste.
//
// Written literally it arrives as ASCII "K", which turns every row naming it
// into a name compared with itself -- an assertion that passes against any
// implementation, including one that does nothing. That happened while this
// file was being written, and the live test caught it only because the other
// engine disagreed. Asserting the code points is what makes a green row mean
// the rune it says.
func TestFixtureRunesAreTheCodePointsTheyName(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  rune
	}{
		{name: "dotted capital I", value: dottedCapitalI, want: 0x0130},
		{name: "dotless i", value: dotlessI, want: 0x0131},
		{name: "capital sigma", value: capitalSigma, want: 0x03A3},
		{name: "final sigma", value: finalSigma, want: 0x03C2},
		{name: "kelvin sign", value: kelvinSign, want: 0x212A},
		{name: "a with diaeresis", value: aWithDiaeresis, want: 0x00E4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert([]rune(test.value), qt.DeepEquals, []rune{test.want})
		})
	}
}

// TestForMySQLFamilyResolvedIndexNames_CollisionsTheServerReported is
// stokaro/ptah#2768: the equivalence the two engines disagree about, taken from
// the server instead of guessed.
//
// Measured on mysql:8.4.11 and mariadb:11.8.9 by creating both names as keys on
// one table, with the identifiers built server-side from UTF-8 bytes and read
// back as HEX so no client transcoding could forge the answer. MySQL refuses
// `İ`/`i` and `K`(U+212A)/`K`; MariaDB refuses `I`/`ı` and `Σ`/`ς`. Each engine
// accepts what the other refuses.
//
// The rows are the resolution a probe of that shape produces, and what the
// model must do with it: a name in another name's class carries that name's
// key, so the two share a conflict key and collide.
func TestForMySQLFamilyResolvedIndexNames_CollisionsTheServerReported(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		resolved []identifier.ResolvedName
		left     string
		right    string
	}{
		{
			name:    "mysql folds the dotted capital onto i",
			dialect: platform.MySQL,
			resolved: []identifier.ResolvedName{
				{Name: "i", Key: "i"},
				{Name: dottedCapitalI, Key: "i"},
			},
			left:  dottedCapitalI,
			right: "i",
		},
		{
			name:    "mysql folds the kelvin sign onto k",
			dialect: platform.MySQL,
			resolved: []identifier.ResolvedName{
				{Name: "K", Key: "K"},
				{Name: kelvinSign, Key: "K"},
			},
			left:  kelvinSign,
			right: "K",
		},
		{
			name:    "mariadb folds the dotless i onto i",
			dialect: platform.MariaDB,
			resolved: []identifier.ResolvedName{
				{Name: "I", Key: "I"},
				{Name: dotlessI, Key: "I"},
			},
			left:  dotlessI,
			right: "I",
		},
		{
			name:    "mariadb folds the final sigma onto the capital",
			dialect: platform.MariaDB,
			resolved: []identifier.ResolvedName{
				{Name: capitalSigma, Key: capitalSigma},
				{Name: finalSigma, Key: capitalSigma},
			},
			left:  finalSigma,
			right: capitalSigma,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			semantics := identifier.ForMySQLFamilyResolvedIndexNames(test.dialect, test.resolved)

			c.Assert(semantics.IndexConflictKey(test.left), qt.Equals,
				semantics.IndexConflictKey(test.right))
			c.Assert(semantics.IndexConflictUnresolved(test.left), qt.IsFalse,
				qt.Commentf("the server answered for this name, so nothing is unresolved"))
		})
	}
}

// TestForMySQLFamilyResolvedIndexNames_NamesTheServerKeptApart is the other
// half, and without it the mode could be satisfied by folding everything
// together.
//
// Each row is a pair the engine under test ACCEPTS as two index names on one
// table. The `a`/`ä` row is the one that motivates the whole change: both
// engines accept it, and the unresolved answer had to report it as a possible
// conflict because nothing offline can tell it from `İ`/`i`.
func TestForMySQLFamilyResolvedIndexNames_NamesTheServerKeptApart(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		resolved []identifier.ResolvedName
		left     string
		right    string
	}{
		{
			name:    "mysql keeps the dotless i apart from I",
			dialect: platform.MySQL,
			resolved: []identifier.ResolvedName{
				{Name: "I", Key: "I"},
				{Name: dotlessI, Key: dotlessI},
			},
			left:  dotlessI,
			right: "I",
		},
		{
			name:    "mysql keeps the sigmas apart",
			dialect: platform.MySQL,
			resolved: []identifier.ResolvedName{
				{Name: capitalSigma, Key: capitalSigma},
				{Name: finalSigma, Key: finalSigma},
			},
			left:  finalSigma,
			right: capitalSigma,
		},
		{
			name:     "mariadb keeps the dotted capital apart from i",
			dialect:  platform.MariaDB,
			resolved: []identifier.ResolvedName{{Name: dottedCapitalI, Key: dottedCapitalI}},
			left:     dottedCapitalI,
			right:    "i",
		},
		{
			name:    "mariadb keeps the kelvin sign apart from K",
			dialect: platform.MariaDB,
			resolved: []identifier.ResolvedName{
				{Name: "K", Key: "K"},
				{Name: kelvinSign, Key: kelvinSign},
			},
			left:  kelvinSign,
			right: "K",
		},
		{
			name:    "both engines keep a apart from a-diaeresis",
			dialect: platform.MySQL,
			resolved: []identifier.ResolvedName{
				{Name: "a", Key: "a"},
				{Name: aWithDiaeresis, Key: aWithDiaeresis},
			},
			left:  aWithDiaeresis,
			right: "a",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			semantics := identifier.ForMySQLFamilyResolvedIndexNames(test.dialect, test.resolved)

			c.Assert(semantics.IndexConflictKey(test.left), qt.Not(qt.Equals),
				semantics.IndexConflictKey(test.right))
			c.Assert(semantics.IndexConflictUnresolved(test.left), qt.IsFalse)
		})
	}
}

// TestForMySQLFamilyResolvedIndexNames_AnUnaskedNameStaysUnresolved keeps the
// upgrade honest about its own coverage.
//
// The mode answers for the names a server was asked about. A name outside that
// set has no equivalence class, and the conservative unknown answer is what it
// must fall back to -- reporting it as resolved would be the guess this issue
// exists to stop, and reporting it as its own class would be the same guess
// wearing a different key.
func TestForMySQLFamilyResolvedIndexNames_AnUnaskedNameStaysUnresolved(t *testing.T) {
	c := qt.New(t)

	semantics := identifier.ForMySQLFamilyResolvedIndexNames(platform.MySQL,
		[]identifier.ResolvedName{
			{Name: "i", Key: "i"},
			{Name: dottedCapitalI, Key: "i"},
		})

	c.Assert(semantics.IndexConflictUnresolved(finalSigma), qt.IsTrue)
	c.Assert(semantics.IndexConflictUnresolved(dottedCapitalI), qt.IsFalse)
}

// TestForMySQLFamilyResolvedIndexNames_ASCIINamesAreNeverAsked pins the reason
// a schema carrying no non-ASCII index name reaches no server.
//
// ASCII folding is shared by both engines and exact, so those names carry the
// same keys resolved or not. That is what makes the probe cost nothing on every
// ordinary schema, and it is why the resolved keys have to live in the same
// space as the ASCII folds -- a non-ASCII name in `i`'s class carries the key
// `i`, which is what the first test above asserts.
func TestForMySQLFamilyResolvedIndexNames_ASCIINamesAreNeverAsked(t *testing.T) {
	c := qt.New(t)

	offline := identifier.ForDialect(platform.MySQL)
	resolved := identifier.ForMySQLFamilyResolvedIndexNames(platform.MySQL,
		[]identifier.ResolvedName{{Name: aWithDiaeresis, Key: aWithDiaeresis}})

	c.Assert(resolved.IndexConflictKey("Users_IDX"), qt.Equals, offline.IndexConflictKey("Users_IDX"))
	c.Assert(resolved.IndexIdentityKey("Users_IDX"), qt.Equals, offline.IndexIdentityKey("Users_IDX"))
}

// TestForMySQLFamilyResolvedIndexNames_SurvivesNormalize is the control on the
// validity rules, and it is not ceremony.
//
// Normalize discards a semantics it considers invalid and silently returns the
// offline default, so a mode that fails `valid` would be dropped on the way to
// the comparison and every assertion above would still pass while the product
// behaved as though nothing had been resolved.
func TestForMySQLFamilyResolvedIndexNames_SurvivesNormalize(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			resolved := identifier.ForMySQLFamilyResolvedIndexNames(test.dialect,
				[]identifier.ResolvedName{
					{Name: "i", Key: "i"},
					{Name: dottedCapitalI, Key: "i"},
				})

			normalized := resolved.Normalize(test.dialect)

			c.Assert(normalized.IndexConflictKey(dottedCapitalI), qt.Equals, "i")
			c.Assert(normalized.IndexConflictUnresolved(dottedCapitalI), qt.IsFalse)
		})
	}
}

// TestForMySQLFamilyResolvedIndexNames_LeavesOtherDialectsAlone keeps the
// upgrade inside the family whose disagreement it is about.
func TestForMySQLFamilyResolvedIndexNames_LeavesOtherDialectsAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sqlserver", dialect: platform.SQLServer},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			resolved := identifier.ForMySQLFamilyResolvedIndexNames(test.dialect,
				[]identifier.ResolvedName{
					{Name: "i", Key: "i"},
					{Name: dottedCapitalI, Key: "i"},
				})

			c.Assert(resolved.Equal(identifier.ForDialect(test.dialect)), qt.IsTrue)
		})
	}
}
