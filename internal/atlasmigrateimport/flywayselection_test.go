package atlasmigrateimport_test

import (
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// flywaySource builds a Flyway directory in which every migration announces its
// own path, so a converted entry can be traced back to the source file it came
// from without depending on the converted file name.
func flywaySource(names ...string) fstest.MapFS {
	source := make(fstest.MapFS, len(names))
	for _, name := range names {
		source[name] = &fstest.MapFile{Data: fmt.Appendf(nil, "SELECT '%s';\n", name)}
	}
	return source
}

// flywayConsumed returns the source files a Flyway directory actually executes,
// in execution order, recovered from the converted SQL bodies.
func flywayConsumed(c *qt.C, fsys fs.FS) []string {
	c.Helper()
	loaded, err := atlasmigrateimport.LoadFS(fsys, "migrations", atlasmigrateimport.FormatFlyway)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(loaded.Entries))
	for _, entry := range loaded.Entries {
		body := strings.TrimSuffix(strings.TrimPrefix(string(entry.Data), "SELECT '"), "';\n")
		names = append(names, body)
	}
	return names
}

// flywayCovered returns the source files the directory's atlas.sum covers, in
// the order Atlas CE hashes them.
func flywayCovered(c *qt.C, fsys fs.FS) []string {
	c.Helper()
	covered, err := atlasmigrateimport.SumFileNames(fsys, atlasmigrateimport.FormatFlyway)
	c.Assert(err, qt.IsNil)
	return covered
}

// TestLoadFSFlywayConsumesExactlyTheCoveredSet is the property #982 exists for:
// for a Flyway directory, the files that EXECUTE are the files the atlas.sum
// the apply-time gate verified COVERS — same members, same order.
//
// It is asserted by construction rather than by listing expected names on both
// sides, because a fixture that spells out both answers only proves the fixture
// was written twice. Every shape here was measured against the pinned Atlas CE
// v1.3.0 oracle, which executes exactly its covered set in exactly sum order;
// scripts/probe-atlas-apply-gate.sh section 9a reproduces the comparison
// against the binary.
func TestLoadFSFlywayConsumesExactlyTheCoveredSet(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		files []string
	}{{
		name: "superseded baseline is neither covered nor executed",
		// #982 exploit 1. B1 is outside atlas.sum, so before the convergence it
		// executed unprotected on a directory both tools called clean.
		files: []string{"B1__one.sql", "B2__two.sql", "V3__three.sql"},
	}, {
		name: "lowercase prefix is neither covered nor executed",
		// #982 exploit 2. The importer's regexp was (?i); CE's match is not.
		files: []string{"v1__one.sql", "V2__two.sql"},
	}, {
		name:  "lowercase baseline prefix",
		files: []string{"b2__y.sql", "V3__three.sql"},
	}, {
		name:  "lowercase repeatable and undo prefixes",
		files: []string{"r__z.sql", "u1__w.sql", "V1__init.sql"},
	}, {
		name:  "uppercase extension is not covered",
		files: []string{"V1__x.SQL", "V2__two.sql"},
	}, {
		name:  "undo files are not covered",
		files: []string{"U1__undo.sql", "V2__two.sql"},
	}, {
		name: "repeatables are covered and executed, last",
		// The loud half of #982: this was a hard refusal, and CE runs it.
		files: []string{"R__repeat.sql", "V1__init.sql"},
	}, {
		name:  "a repeatable carrying a version token still runs last",
		files: []string{"R1__a.sql", "V1__b.sql"},
	}, {
		name: "nested migrations are covered and executed",
		// Flyway is the one layout CE recurses into.
		files: []string{"V1__init.sql", "sub/V2__nested.sql"},
	}, {
		name: "a nested baseline still executes first",
		// Walk order visits sub/B2 last; CE emits and runs it first.
		files: []string{"sub/B2__base.sql", "V1__a.sql", "V3__c.sql"},
	}, {
		name:  "hidden directories are not covered",
		files: []string{"V1__a.sql", ".archive/V2__old.sql"},
	}, {
		name: "the baseline squash compares version tokens as strings",
		// "10" < "2", so B2 squashes V10 — the operand that separates CE's rule
		// from the numeric comparison the importer used to make.
		files: []string{"B2__base.sql", "V10__x.sql", "V3__y.sql"},
	}, {
		name: "a baseline runs first even when its version is numerically larger",
		// The discriminator for the band: B10 outranks V2 numerically, and CE
		// still runs B10 first because a surviving baseline is emitted first.
		files: []string{"B10__base.sql", "V2__x.sql"},
	}, {
		name:  "a baseline runs first even when a survivor scores to zero",
		files: []string{"B9__base.sql", "Va__x.sql"},
	}, {
		name:  "the later of two equal-versioned baselines wins",
		files: []string{"B2__first.sql", "B2.0__second.sql", "V3__c.sql"},
	}, {
		name: "a version token needs no separator, digits or description",
		// V1.sql, Video.sql and V.sql are all ordinary migrations to CE. The
		// importer used to ignore them while hashing them.
		files: []string{"V1.sql", "Video.sql", "V2__two.sql"},
	}, {
		name:  "a zero version is a version",
		files: []string{"V0__zero.sql", "V2__two.sql"},
	}, {
		name: "tokens that score identically are ordered by walk position",
		// Vault and Video both score {0}; CE keeps them apart and runs them in
		// walk order, which is the tie the encoding has to reproduce.
		files: []string{"Video.sql", "Vault.sql", "V1__a.sql"},
	}, {
		name: "V1 orders strictly before V1.0",
		// Component COUNT is significant: zero-extending the shorter token
		// would collapse this pair, which CE keeps distinct and ordered.
		files: []string{"V1.0__b.sql", "V1__a.sql"},
	}, {
		name: "distinct tokens scoring to the same components both run",
		// "1" and "01" are different versions to CE and it runs both.
		files: []string{"V1__a.sql", "V01__b.sql"},
	}, {
		name:  "dotted and underscore separators are interchangeable but distinct",
		files: []string{"V1.5__a.sql", "V1_5__b.sql"},
	}, {
		name:  "a major.minor version orders before the next major",
		files: []string{"V2__major.sql", "V1.5__minor.sql", "V10__later.sql"},
	}, {
		name:  "a 14-digit timestamp version is representable",
		files: []string{"V20250731120000__ts.sql", "V1__a.sql"},
	}, {
		name:  "a baseline may itself be a timestamp",
		files: []string{"B20230101120000__base.sql", "V99999999999999__x.sql"},
	}, {
		name:  "non-Flyway siblings are neither covered nor executed",
		files: []string{"plain.sql", "notes.txt", "V1__a.sql"},
	}, {
		name:  "a baseline, survivors and a repeatable together",
		files: []string{"B2__base.sql", "R__r.sql", "V3__c.sql"},
	}}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			source := flywaySource(tt.files...)

			consumed := flywayConsumed(c, source)

			c.Assert(consumed, qt.DeepEquals, flywayCovered(c, source))
		})
	}
}

// TestLoadFSFlywayCoveredSetIsNotEmptyForTheseShapes guards the test above from
// passing vacuously. LoadFS reports an error on an empty result, so a shape
// whose covered set were empty would fail rather than silently compare two empty
// slices — but a shape covering exactly ONE file would compare two one-element
// slices and hold almost nothing. This asserts the fixtures above exercise
// selection rather than agreeing trivially.
func TestLoadFSFlywayCoveredSetIsNotEmptyForTheseShapes(t *testing.T) {
	c := qt.New(t)
	source := flywaySource("B1__one.sql", "B2__two.sql", "V3__three.sql", "U1__undo.sql", "v4__lower.sql")

	covered := flywayCovered(c, source)

	// Three of the five files are outside the covered set, so the equality above
	// is a claim about selection and not about "everything is kept".
	c.Assert(covered, qt.DeepEquals, []string{"B2__two.sql", "V3__three.sql"})
}

// TestLoadFSFlywayStopsExecutingUncoveredFiles pins the two shapes #982 opened
// with, from the execution side rather than the coverage side. Both directories
// verify clean under `migrate validate` on both tools, so a coverage assertion
// alone would not have caught either.
func TestLoadFSFlywayStopsExecutingUncoveredFiles(t *testing.T) {
	c := qt.New(t)

	c.Run("a superseded baseline does not execute", func(c *qt.C) {
		source := flywaySource("B1__one.sql", "B2__two.sql", "V3__three.sql")

		consumed := flywayConsumed(c, source)

		c.Assert(consumed, qt.DeepEquals, []string{"B2__two.sql", "V3__three.sql"})
		c.Assert(consumed, qt.Not(qt.Contains), "B1__one.sql")
	})

	c.Run("a lowercase-prefixed file does not execute", func(c *qt.C) {
		source := flywaySource("V1__init.sql", "v2__evil.sql")

		consumed := flywayConsumed(c, source)

		c.Assert(consumed, qt.DeepEquals, []string{"V1__init.sql"})
		c.Assert(consumed, qt.Not(qt.Contains), "v2__evil.sql")
	})
}

// TestLoadFSFlywayAtlasVersions pins the projection from Atlas CE's version
// STRING onto the int64 Ptah's migrator orders and identifies migrations by.
//
// The numbers are large because the guarantees are: a surviving baseline sorts
// below every survivor whatever its own version, so the versioned band has to
// begin above the whole baseline range, and the leading component still has to
// hold a 14-digit timestamp. They are asserted literally because they are a
// contract — they end up in the file names `migrate import` writes and in the
// revisions `migrate apply` records.
func TestLoadFSFlywayAtlasVersions(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		files []string
		want  []string
	}{{
		name:  "single major",
		files: []string{"V1__x.sql"},
		want:  []string{"4611686018427469511_x.sql"},
	}, {
		name:  "major.minor",
		files: []string{"V1.5__x.sql"},
		want:  []string{"4611686018427471935_x.sql"},
	}, {
		name:  "major.minor.patch",
		files: []string{"V1.10.3__x.sql"},
		want:  []string{"4611686018427473971_x.sql"},
	}, {
		name: "a trailing zero component is kept, not trimmed",
		// V2 and V2.0 are different versions to CE; the old encoding merged
		// them by trimming trailing zeros and then refused the pair.
		files: []string{"V2__a.sql", "V2.0__b.sql"},
		want:  []string{"4611686018427510315_a.sql", "4611686018427510719_b.sql"},
	}, {
		name:  "a 14-digit timestamp",
		files: []string{"V20230101120000__x.sql"},
		want:  []string{"5437155064527908707_x.sql"},
	}, {
		name: "leading components above MaxInt32 keep their order",
		// Scoring a component with strconv.Atoi takes the PLATFORM int width, so
		// a 32-bit build clamps both of these to the same ceiling and swaps
		// them — writing an atlas.sum the oracle rejects and converting each
		// file to the other's version. testdata/ce-sums/flyway/wide-components
		// holds the oracle's own sum for the pair.
		files: []string{"V9000000000__a.sql", "V10000000000__b.sql"},
		want:  []string{"4612053254427428707_a.sql", "4612094058427428707_b.sql"},
	}, {
		name: "a baseline lands in the low band",
		// 122412 is below the versioned band start, which is what makes the
		// baseline execute first without depending on the files beside it.
		files: []string{"B2__base.sql", "V3__c.sql"},
		want:  []string{"122412_base.sql", "4611686018427551119_c.sql"},
	}, {
		name:  "a repeatable lands on the reserved top slot",
		files: []string{"V1__a.sql", "R__r.sql"},
		want:  []string{"4611686018427469511_a.sql", "9223372036854775807_r.sql"},
	}, {
		name:  "a file with no description keeps a bare version name",
		files: []string{"V1.sql"},
		want:  []string{"4611686018427469511.sql"},
	}, {
		name: "tokens scoring identically take successive tie slots",
		// Vault precedes Video in walk order, so it takes the lower slot.
		files: []string{"Video.sql", "Vault.sql"},
		want:  []string{"4611686018427428707.sql", "4611686018427428708.sql"},
	}}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			loaded, err := atlasmigrateimport.LoadFS(flywaySource(tt.files...), "migrations", atlasmigrateimport.FormatFlyway)

			c.Assert(err, qt.IsNil)
			c.Assert(entryNames(loaded), qt.DeepEquals, tt.want)
		})
	}
}

// TestLoadFSFlywayBaselineDoesNotShiftSurvivorVersions pins that the tie index
// is counted within one kind, so a baseline never consumes a slot the survivors
// are numbered from.
//
// It needs a directory where the baseline and the survivors score to the SAME
// ordering components, which is rarer than it sounds: a survivor must carry a
// version token that is string-greater than the baseline's, and equal
// components usually means an equal token, which the baseline squashes. Bx/Vy/Vz
// gets there — all three score {0}, "y" and "z" both outrank "x" as strings, and
// the baseline is visited first so its backward reach has nothing to drop. The
// oracle covers and executes all three.
//
// Without the kind guard the survivors still order correctly, because the
// baseline lives in a different band, so ordering alone cannot separate the two
// rules. What separates them is stability: adding a baseline to a directory
// would renumber every survivor that scored like it, and a renumbered migration
// is one the migrator no longer recognizes as already applied.
func TestLoadFSFlywayBaselineDoesNotShiftSurvivorVersions(t *testing.T) {
	c := qt.New(t)

	withBaseline, err := atlasmigrateimport.LoadFS(
		flywaySource("Bx__a.sql", "Vy__b.sql", "Vz__c.sql"), "migrations", atlasmigrateimport.FormatFlyway)
	c.Assert(err, qt.IsNil)
	c.Assert(entryNames(withBaseline), qt.DeepEquals, []string{
		"40804_a.sql",
		"4611686018427428707_b.sql",
		"4611686018427428708_c.sql",
	})

	withoutBaseline, err := atlasmigrateimport.LoadFS(
		flywaySource("Vy__b.sql", "Vz__c.sql"), "migrations", atlasmigrateimport.FormatFlyway)
	c.Assert(err, qt.IsNil)
	// The survivors keep the versions they had before the baseline was added.
	c.Assert(entryNames(withoutBaseline), qt.DeepEquals, []string{
		"4611686018427428707_b.sql",
		"4611686018427428708_c.sql",
	})
}

// TestLoadFSFlywayRefusesVersionsAtlasCECannotExecute covers the directories
// Atlas CE itself cannot run: it panics with an index-out-of-range on a
// duplicate version rather than reporting anything, so refusing is a refusal
// toward the oracle rather than past it.
func TestLoadFSFlywayRefusesVersionsAtlasCECannotExecute(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		files []string
		want  string
	}{{
		name:  "two files carrying the same version token",
		files: []string{"V1__a.sql", "V1__b.sql"},
		want:  `Flyway migrations V1__a\.sql and V1__b\.sql both carry the Atlas version "1"`,
	}, {
		name: "two repeatables",
		// Every repeatable is version "" to CE whatever its own token, so these
		// collide with each other even though their tokens differ.
		files: []string{"R1__a.sql", "R2__b.sql", "V1__i.sql"},
		want:  `Flyway migrations R1__a\.sql and R2__b\.sql both carry the empty Atlas version and cannot be executed together`,
	}, {
		name:  "a repeatable beside a file whose own token is empty",
		files: []string{"V.sql", "R__x.sql"},
		want:  `Flyway migrations V\.sql and R__x\.sql both carry the empty Atlas version and cannot be executed together`,
	}}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			loaded, err := atlasmigrateimport.LoadFS(flywaySource(tt.files...), "migrations", atlasmigrateimport.FormatFlyway)

			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(loaded, qt.IsNil)
		})
	}
}

// TestLoadFSFlywayRefusesUnrepresentableVersions_KnownDivergence pins the
// versions Atlas CE executes and Ptah cannot, because CE keys a migration on an
// opaque string while Ptah's migrator keys on an int64 and the two spaces do not
// embed. Every row here is refused on master too, with different wording; the
// refusal is pre-existing and the convergence neither widened nor narrowed it.
//
// Closing these means giving the migrator a version that is not an int64, which
// is a change to revision tracking rather than to this importer.
func TestLoadFSFlywayRefusesUnrepresentableVersions_KnownDivergence(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		files []string
		want  string
	}{{
		name:  "more components than the ordering key carries",
		files: []string{"V1.2.3.4__x.sql", "V1__a.sql"},
		want:  `Flyway migration V1\.2\.3\.4__x\.sql has version "1\.2\.3\.4" with more than 3 components.*`,
	}, {
		name:  "a trailing component outside one slot",
		files: []string{"V1.100__x.sql", "V1__a.sql"},
		want:  `Flyway migration V1\.100__x\.sql has version "1\.100" whose component 2 \(100\) is outside 0\.\.99.*`,
	}, {
		name:  "a leading component past the band",
		files: []string{"V9999999999999999__x.sql", "V1__a.sql"},
		want:  `Flyway migration V9999999999999999__x\.sql has version "9999999999999999" that is too large.*`,
	}, {
		name: "a negative component",
		// CE runs V-5__neg.sql before V2__two.sql; a positive fixed-width slot
		// has no room below zero.
		files: []string{"V-5__neg.sql", "V2__two.sql"},
		want:  `Flyway migration V-5__neg\.sql has version "-5" with a negative component.*`,
	}}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			loaded, err := atlasmigrateimport.LoadFS(flywaySource(tt.files...), "migrations", atlasmigrateimport.FormatFlyway)

			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(loaded, qt.IsNil)
		})
	}
}

// TestLoadFSFlywayLeadingComponentBound pins the edge of the representable
// range from both sides.
//
// The largest accepted version is asserted with the other COMPONENT slots at
// their maximum — three components, both trailing ones at 99 — which is the
// shape that reaches the top of the band. The tie slot stays 0 here because one
// file cannot tie with anything; the tie term of the bound is what the
// "bound ignores the trailing slots and the tie budget" mutation covers. A bound derived from the band width
// alone accepts this leading component and then overflows int64 into a negative
// Atlas version, which nothing downstream range-checks: the migrator would order
// it before every other migration instead of after. Asserting the accepted
// version is positive and below the reserved repeatable slot is what makes the
// row a bound rather than a spot check.
func TestLoadFSFlywayLeadingComponentBound(t *testing.T) {
	c := qt.New(t)

	c.Run("the largest representable version converts", func(c *qt.C) {
		loaded, err := atlasmigrateimport.LoadFS(
			flywaySource("V113020439624235.99.99__x.sql"), "migrations", atlasmigrateimport.FormatFlyway)

		c.Assert(err, qt.IsNil)
		c.Assert(entryNames(loaded), qt.DeepEquals, []string{"9223372036854754447_x.sql"})
	})

	c.Run("one past it is refused", func(c *qt.C) {
		loaded, err := atlasmigrateimport.LoadFS(
			flywaySource("V113020439624236__x.sql"), "migrations", atlasmigrateimport.FormatFlyway)

		c.Assert(err, qt.ErrorMatches, `Flyway migration V113020439624236__x\.sql has version "113020439624236" that is too large.*`)
		c.Assert(loaded, qt.IsNil)
	})
}

// TestLoadFSFlywayTieBudgetExhausted_KnownDivergence pins the one input class
// this change made Ptah refuse where Atlas CE applies.
//
// The fixture is five ordinary helper files that happen to start with a capital
// V. Flyway itself ignores them — it needs the V<version>__ shape — but Atlas
// CE's parse is loose enough to treat any V-prefixed .sql as a migration, so
// their version tokens become "iews", "endors", "acuum", "alidation" and
// "ersion". All five score to the same ordering components, and CE separates
// them only by walk position, which no per-file projection can reproduce; the
// int64 carries four tie slots, which is what is left once a 14-digit leading
// component, three components and two bands are accounted for.
//
// Measured on the oracle: CE applies all six. On master the five are silently
// ignored and V1__init.sql applies alone, so this replaces a silent partial
// execution rather than a correct one — but it is a refusal where CE exits 0.
func TestLoadFSFlywayTieBudgetExhausted_KnownDivergence(t *testing.T) {
	c := qt.New(t)
	source := flywaySource(
		"Views.sql", "Vendors.sql", "Vacuum.sql", "Validation.sql", "Version.sql", "V1__init.sql")

	loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatFlyway)

	c.Assert(err, qt.ErrorMatches,
		`Flyway migration V\w+\.sql shares its version ordering key with more than the 4 files Ptah can tell apart \(version "\w+"\)`)
	c.Assert(loaded, qt.IsNil)
	// Four in one score class still convert, so the budget is a budget and not
	// a ban on ties.
	c.Assert(flywayConsumed(c, flywaySource(
		"Vendors.sql", "Vacuum.sql", "Validation.sql", "Version.sql", "V1__init.sql")), qt.HasLen, 5)
}

// TestLoadFSFlywayDescriptionEndingInDown_KnownDivergence pins a refusal that
// predates this change and that flywayRefusalClasses structurally cannot see.
//
// A Flyway description may end in ".down". The converted name then ends in
// ".down.sql", which Ptah's Atlas migrator reads as the DOWN half of a pair with
// no up half, so registration fails — after LoadFS has already succeeded, which
// is why the fuzz's refusal enumeration never observes it. Atlas CE applies both
// files. Measured on master and on this build: both refuse, identically.
func TestLoadFSFlywayDescriptionEndingInDown_KnownDivergence(t *testing.T) {
	c := qt.New(t)

	loaded, err := atlasmigrateimport.LoadFS(
		flywaySource("V1__a.down.sql", "V10__c.sql"), "migrations", atlasmigrateimport.FormatFlyway)

	// Conversion itself succeeds; the converted name is what the migrator later
	// rejects, so the divergence lives one layer below this package.
	c.Assert(err, qt.IsNil)
	c.Assert(entryNames(loaded), qt.DeepEquals, []string{
		"4611686018427469511_a.down.sql",
		"4611686018427836747_c.sql",
	})
}

// TestFlywaySourceVersionsCarriesSquashedTokens covers the operand the linear
// comparison compares AGAINST: the highest version token a database has
// applied.
//
// A surviving baseline retires the files it squashes from the covered set, and
// with them their tokens. The comparison then has no mark for any applied
// version, reports "none", and passes every pending file — which is how
// `V2__a.sql` applied plus `B3__base.sql` and `R__r.sql` added came to exit 0
// here while the pinned community binary v1.3.0 exits 1 with `migration files
// B3__base.sql, R__r.sql were added out of order`. The tokens of the squashed
// files are therefore recovered from the directory as it stood before the
// baseline, which is the only shape their revision rows can have come from.
//
// The assertion is on the token SET rather than on int64 keys, because the keys
// are this build's projection and the tokens are Atlas CE's own strings; a row
// spelling out both would only prove the fixture was written twice.
func TestFlywaySourceVersionsCarriesSquashedTokens(t *testing.T) {
	tests := []struct {
		name   string
		files  []string
		tokens []string
	}{{
		// Reverted: ["3"] — the applied V2's token is gone with its file, so
		// the comparison has no mark and lets everything through.
		name:   "a baseline that squashes the only applied migration",
		files:  []string{"V2__a.sql", "B3__base.sql"},
		tokens: []string{"2", "3"},
	}, {
		// The shape the pinned binary refuses. Reverted: ["", "3"], and
		// `migrate apply` exits 0 executing both added files.
		name:   "a repeatable beside a baseline that squashes the applied migration",
		files:  []string{"V2__a.sql", "B3__base.sql", "R__r.sql"},
		tokens: []string{"", "2", "3"},
	}, {
		// The recovered token is the SQUASHED file's, not the baseline's: a
		// fill keyed on the baseline would report ["1", "1", "3"] and make the
		// mark "1", passing files an applied "02" outranks.
		name:   "a zero-padded migration squashed by an unpadded baseline",
		files:  []string{"V02__a.sql", "V3__c.sql", "B1__base.sql"},
		tokens: []string{"02", "1", "3"},
	}, {
		// The executed mapping wins where the two selections land on the same
		// slot. B02 squashes V02__b.sql and leaves V2__a.sql, which then takes
		// the tie slot V02 held before it — so the recovered V02 entry and the
		// executed V2 entry are the SAME key, carrying different tokens. Both
		// "2"s are real: one is what this directory executes at that slot, the
		// other is what a pre-baseline database recorded one slot up.
		// Recovering by overwrite rather than by filling gaps prints
		// ["02", "02", "2", "3"] and hands the comparison a mark of "02" for a
		// migration whose token is "2".
		name:   "a padded and an unpadded migration on one ordering slot",
		files:  []string{"V2__a.sql", "V02__b.sql", "V3__c.sql", "B02__base.sql"},
		tokens: []string{"02", "2", "2", "3"},
	}, {
		// Control: a baseline that squashes nothing adds nothing. Green either
		// way; it goes red if the recovery starts inventing tokens.
		name:   "a baseline below every applied migration",
		files:  []string{"V2__a.sql", "B1__base.sql"},
		tokens: []string{"1", "2"},
	}, {
		// Control: no baseline, so there is nothing to recover and the map is
		// exactly the executed one. Green either way.
		name:   "a directory with no baseline at all",
		files:  []string{"V2__a.sql", "V10__b.sql"},
		tokens: []string{"10", "2"},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sources, err := atlasmigrateimport.FlywaySourceVersions(
				flywaySource(test.files...), atlasmigrateimport.FormatFlyway)

			c.Assert(err, qt.IsNil)
			c.Assert(slices.Sorted(maps.Values(sources)), qt.DeepEquals, test.tokens)
		})
	}
}
