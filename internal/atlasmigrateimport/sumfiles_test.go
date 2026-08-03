package atlasmigrateimport_test

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/migratesum"
)

// ceSumCorpusCases is the number of directory shapes in testdata/ce-sums the
// oracle SEALED with an atlas.sum. The corpus test walks the tree, so a corpus
// that failed to load would otherwise pass vacuously; asserting the count makes
// an empty or partial walk fail. Adding a shape is expected to change this
// number.
const ceSumCorpusCases = 92

// ceRefusedCorpusCases is the number of shapes the oracle DECLINED to hash,
// recorded as an atlas.refused marker instead of an atlas.sum.
//
// The two counts are separate on purpose. Until #991 the corpus had no way to
// represent a refusal at all — regenerate.sh ran under `set -e` and any
// non-zero oracle exit aborted the whole regeneration — so the only outcome it
// could hold was agreement, and the shape Atlas CE refuses was structurally
// unrecordable.
const ceRefusedCorpusCases = 5

const sqlBody = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"

func sourceFS(names ...string) fstest.MapFS {
	fsys := make(fstest.MapFS, len(names))
	for _, name := range names {
		fsys[name] = &fstest.MapFile{Data: []byte(sqlBody)}
	}
	return fsys
}

// TestSumFileNamesMatchesAtlasCE replays the pinned oracle's own atlas.sum for
// every captured directory shape. It is the test that makes the per-format file
// set a measured fact rather than a transcribed rule: each case directory holds
// the source files and the atlas.sum Atlas CE v1.3.0 wrote for them, produced
// by testdata/ce-sums/regenerate.sh.
func TestSumFileNamesMatchesAtlasCE(t *testing.T) {
	c := qt.New(t)

	root := filepath.Join("testdata", "ce-sums")
	// Every case directory is <format>/<case>/atlas.sum, so globbing for the
	// recorded sums enumerates exactly the cases the oracle sealed.
	recorded, err := fs.Glob(os.DirFS(root), "*/*/"+migratesum.AtlasFileName)
	c.Assert(err, qt.IsNil)
	c.Assert(recorded, qt.HasLen, ceSumCorpusCases)

	for _, sumPath := range recorded {
		caseDir := path.Dir(sumPath)
		formatName, _, _ := strings.Cut(caseDir, "/")

		c.Run(caseDir, func(c *qt.C) {
			fsys := os.DirFS(filepath.Join(root, filepath.FromSlash(caseDir)))

			names, err := atlasmigrateimport.SumFileNames(
				fsys,
				atlasmigrateimport.Format(formatName),
			)
			c.Assert(err, qt.IsNil)

			sum, err := migratesum.ComputeAtlasFiles(fsys, names)
			c.Assert(err, qt.IsNil)

			want, err := fs.ReadFile(os.DirFS(root), sumPath)
			c.Assert(err, qt.IsNil)
			c.Assert(string(sum.Bytes()), qt.Equals, string(want))
		})
	}
}

// oracleRefusedEntry returns the entry name the oracle's recorded refusal
// blames, so the assertion below compares the two tools' answers rather than
// merely observing that both said no.
func oracleRefusedEntry(c *qt.C, marker string) string {
	c.Helper()
	matches := regexp.MustCompile(`read file "([^"]+)"`).FindStringSubmatch(marker)
	c.Assert(matches, qt.HasLen, 2, qt.Commentf("unparsed oracle refusal: %s", marker))
	return matches[1]
}

// TestSumFileNamesMatchesAtlasCERefusals replays the shapes the pinned oracle
// DECLINED to hash: a directory whose name the layout's glob matches
// (stokaro/ptah#991).
//
// Membership and readability are separate questions, and this test keeps them
// that way. SumFileNames must still return the directory's name — dropping it,
// which is what Ptah did before #991, is precisely the defect: it produced a sum
// over the remainder that the community binary then refused to read. The
// refusal has to come from the READ, so the assertion is that selection
// succeeds and hashing fails, and that the entry Ptah blames is the one the
// oracle blamed.
func TestSumFileNamesMatchesAtlasCERefusals(t *testing.T) {
	c := qt.New(t)

	root := filepath.Join("testdata", "ce-sums")
	refused, err := fs.Glob(os.DirFS(root), "*/*/atlas.refused")
	c.Assert(err, qt.IsNil)
	c.Assert(refused, qt.HasLen, ceRefusedCorpusCases)

	for _, markerPath := range refused {
		caseDir := path.Dir(markerPath)
		formatName, _, _ := strings.Cut(caseDir, "/")

		c.Run(caseDir, func(c *qt.C) {
			fsys := os.DirFS(filepath.Join(root, filepath.FromSlash(caseDir)))

			names, err := atlasmigrateimport.SumFileNames(
				fsys,
				atlasmigrateimport.Format(formatName),
			)
			c.Assert(err, qt.IsNil)

			marker, err := fs.ReadFile(os.DirFS(root), markerPath)
			c.Assert(err, qt.IsNil)
			blamed := oracleRefusedEntry(c, string(marker))
			c.Assert(names, qt.Contains, blamed)

			_, err = migratesum.ComputeAtlasFiles(fsys, names)
			c.Assert(err, qt.ErrorIs, migratesum.ErrCoveredEntryUnreadable)
			c.Assert(err, qt.ErrorMatches, `read file "`+regexp.QuoteMeta(blamed)+`": is a directory.*`)

			// The oracle wrote no atlas.sum, and neither may the corpus hold one.
			_, statErr := fs.Stat(fsys, migratesum.AtlasFileName)
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

func TestSumFileNamesPerFormat(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		format atlasmigrateimport.Format
		files  []string
		want   []string
	}{{
		name:   "atlas covers every sql file by name",
		format: atlasmigrateimport.FormatAtlas,
		files:  []string{"2_more.sql", "1_init.sql", "notes.txt"},
		want:   []string{"1_init.sql", "2_more.sql"},
	}, {
		name:   "goose covers non-versioned sql and ignores go files",
		format: atlasmigrateimport.FormatGoose,
		files:  []string{"1_init.sql", "2_seed.go", "foo.sql"},
		want:   []string{"1_init.sql", "foo.sql"},
	}, {
		name:   "dbmate covers every sql file",
		format: atlasmigrateimport.FormatDBMate,
		files:  []string{"1_init.sql", "foo.sql"},
		want:   []string{"1_init.sql", "foo.sql"},
	}, {
		name:   "liquibase ignores xml changelogs",
		format: atlasmigrateimport.FormatLiquibase,
		files:  []string{"1_init.sql", "changelog.xml"},
		want:   []string{"1_init.sql"},
	}, {
		name:   "golang-migrate covers up files only",
		format: atlasmigrateimport.FormatGolangMigrate,
		files:  []string{"1_init.up.sql", "1_init.down.sql", "2_bare.sql"},
		want:   []string{"1_init.up.sql"},
	}, {
		name:   "golang-migrate matches the up suffix, not a version pattern",
		format: atlasmigrateimport.FormatGolangMigrate,
		files:  []string{"foo.up.sql", "1_init.up.sql", "2_x.up.up.sql"},
		want:   []string{"1_init.up.sql", "2_x.up.up.sql", "foo.up.sql"},
	}, {
		name:   "golang-migrate up suffix is case sensitive",
		format: atlasmigrateimport.FormatGolangMigrate,
		files:  []string{"1_init.up.sql", "2_x.UP.sql"},
		want:   []string{"1_init.up.sql"},
	}, {
		name:   "sql suffix is case sensitive",
		format: atlasmigrateimport.FormatGoose,
		files:  []string{"1_init.SQL"},
		want:   []string{},
	}, {
		name:   "goose does not recurse",
		format: atlasmigrateimport.FormatGoose,
		files:  []string{"1_init.sql", "sub/2_nested.sql"},
		want:   []string{"1_init.sql"},
	}, {
		name:   "dbmate does not recurse",
		format: atlasmigrateimport.FormatDBMate,
		files:  []string{"1_init.sql", "sub/2_nested.sql"},
		want:   []string{"1_init.sql"},
	}, {
		name:   "liquibase does not recurse",
		format: atlasmigrateimport.FormatLiquibase,
		files:  []string{"1_init.sql", "sub/2_nested.sql"},
		want:   []string{"1_init.sql"},
	}, {
		name:   "golang-migrate does not recurse",
		format: atlasmigrateimport.FormatGolangMigrate,
		files:  []string{"1_top.up.sql", "sub/2_nested.up.sql"},
		want:   []string{"1_top.up.sql"},
	}, {
		name:   "atlas does not recurse",
		format: atlasmigrateimport.FormatAtlas,
		files:  []string{"1_top.sql", "sub/2_nested.sql"},
		want:   []string{"1_top.sql"},
	}, {
		name:   "flyway alone recurses, covering the slash path",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1__top.sql", "sub/V2__nested.sql", "a/b/V3__deep.sql"},
		want:   []string{"V1__top.sql", "sub/V2__nested.sql", "a/b/V3__deep.sql"},
	}, {
		name:   "flyway orders nested files by version, not by path",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V9__top.sql", "zzz/V1__nested.sql"},
		want:   []string{"zzz/V1__nested.sql", "V9__top.sql"},
	}, {
		name:   "flyway drops a nested undo file",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1__one.sql", "sub/U1__one.sql"},
		want:   []string{"V1__one.sql"},
	}, {
		name:   "flyway keeps a nested repeatable last",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1__one.sql", "sub/R__view.sql"},
		want:   []string{"V1__one.sql", "sub/R__view.sql"},
	}, {
		name:   "a bare .sql name is not a flyway file",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{".sql", "V1__ok.sql"},
		want:   []string{"V1__ok.sql"},
	}, {
		name:   "flyway drops undo files",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1__init.sql", "U1__init.sql"},
		want:   []string{"V1__init.sql"},
	}, {
		name:   "flyway needs no separator, description or parseable version",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1__ok.sql", "V2.sql", "V3__.sql", "V.sql", "plain.sql"},
		want:   []string{"V.sql", "V1__ok.sql", "V2.sql", "V3__.sql"},
	}, {
		name:   "flyway covers ordinary words with a prefix initial",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"Video.sql", "Users.sql", "Reports.sql", "V1__ok.sql"},
		want:   []string{"Video.sql", "V1__ok.sql", "Reports.sql"},
	}, {
		name:   "flyway orders versions numerically, not lexically",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1__a.sql", "V1.5__b.sql", "V2__c.sql", "V10__d.sql"},
		want:   []string{"V1__a.sql", "V1.5__b.sql", "V2__c.sql", "V10__d.sql"},
	}, {
		name:   "flyway treats underscore and dot as the same separator",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1_5__a.sql", "V2__b.sql"},
		want:   []string{"V1_5__a.sql", "V2__b.sql"},
	}, {
		name:   "flyway puts repeatables last, ordered by name",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"R__zeta.sql", "R__alpha.sql", "V1__init.sql"},
		want:   []string{"V1__init.sql", "R__alpha.sql", "R__zeta.sql"},
	}, {
		name:   "flyway treats a versioned repeatable as repeatable",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"R1__a.sql", "V1__b.sql"},
		want:   []string{"V1__b.sql", "R1__a.sql"},
	}, {
		name:   "flyway splits the version at the first double separator",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1__a__b.sql", "V0__z.sql"},
		want:   []string{"V0__z.sql", "V1__a__b.sql"},
	}, {
		name:   "flyway sorts an unparseable version before every parseable one",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V__z.sql", "Vx__y.sql", "V1__ok.sql"},
		want:   []string{"V__z.sql", "Vx__y.sql", "V1__ok.sql"},
	}, {
		name:   "flyway emits the baseline first and squashes what it covers",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1__init.sql", "U1__init.sql", "B2__baseline.sql", "V3__third.sql"},
		want:   []string{"B2__baseline.sql", "V3__third.sql"},
	}, {
		name:   "flyway squashes a file at exactly the baseline version",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"B2__base.sql", "V2__same.sql", "V3__later.sql"},
		want:   []string{"B2__base.sql", "V3__later.sql"},
	}, {
		name:   "flyway keeps repeatables through a baseline",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"V1__one.sql", "B2__base.sql", "R__view.sql", "U3__undo.sql", "V4__four.sql"},
		want:   []string{"B2__base.sql", "V4__four.sql", "R__view.sql"},
	}, {
		name:   "flyway exempts an unparseable version from the baseline cut",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"B2__base.sql", "V3__three.sql", "Vx__y.sql"},
		want:   []string{"B2__base.sql", "Vx__y.sql", "V3__three.sql"},
	}, {
		name:   "flyway baseline with an unparseable version squashes everything",
		format: atlasmigrateimport.FormatFlyway,
		files:  []string{"Bx__y.sql", "V1__a.sql", "V2__b.sql"},
		want:   []string{"Bx__y.sql"},
	}}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := atlasmigrateimport.SumFileNames(sourceFS(tt.files...), tt.format)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestSumFileNamesFlywayPrefixIsCaseSensitive pins the first of two traps that
// only the oracle reveals. The importer's flywayFileRe is (?i), so reusing it
// here would cover v1__one.sql — a file Atlas CE ignores. The resulting sum
// would differ from the oracle's on any directory holding a lowercase-prefixed
// file, and a verification path built on it would refuse a directory Atlas CE
// hashed and applies.
func TestSumFileNamesFlywayPrefixIsCaseSensitive(t *testing.T) {
	c := qt.New(t)

	for _, prefix := range []string{"v", "b", "r", "u"} {
		c.Run("lowercase "+prefix+" is not a flyway file", func(c *qt.C) {
			lower := prefix + "1__one.sql"

			got, err := atlasmigrateimport.SumFileNames(
				sourceFS(lower, "V2__two.sql"),
				atlasmigrateimport.FormatFlyway,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []string{"V2__two.sql"})
		})
	}
}

// TestSumFileNamesFlywayBaselineDropsLowerBaselines pins the second trap: the
// importer exempts every baseline from its cut (`if !entry.baseline && ...`),
// so a rule borrowed from it would keep B1__one.sql. Atlas CE keeps only the
// highest baseline.
func TestSumFileNamesFlywayBaselineDropsLowerBaselines(t *testing.T) {
	c := qt.New(t)

	c.Run("lower baseline is squashed", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("B1__one.sql", "B2__two.sql", "V3__three.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"B2__two.sql", "V3__three.sql"})
	})

	c.Run("last baseline wins a version tie", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("B2__a.sql", "B2__b.sql", "V3__c.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"B2__b.sql", "V3__c.sql"})
	})
}

// TestSumFileNamesFlywayBaselineCutComparesVersionsAsStrings pins the sharpest
// divergence between the two comparisons Atlas CE uses. Output order is
// numeric, so V10 sorts after V2 — but the baseline cut compares the raw
// version tokens as strings, where "10" < "2", so a baseline at V2 squashes
// V10. Implementing the cut numerically leaves V10 in the sum and produces a
// checksum the oracle never wrote, on an entirely ordinary directory.
func TestSumFileNamesFlywayBaselineCutComparesVersionsAsStrings(t *testing.T) {
	c := qt.New(t)

	c.Run("baseline at 2 squashes version 10", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("B2__base.sql", "V10__ten.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"B2__base.sql"})
	})

	c.Run("version 10 outranks version 2 in output order", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("V2__b.sql", "V10__c.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"V2__b.sql", "V10__c.sql"})
	})

	c.Run("dotted baseline cut also compares as strings", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("B2__base.sql", "V1.5__a.sql", "V2.5__b.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"B2__base.sql", "V2.5__b.sql"})
	})
}

// TestSumFileNamesFlywayBaselineIsAWalkOrderStateMachine pins the rule that
// replaced an earlier refusal.
//
// The baseline cut looked unfittable while it was read as a filter over a set:
// a top-level B2 squashes a nested V1, yet a nested B9 squashes neither of two
// top-level files. It resolves once selection is read as a state machine over
// the walk — a file is squashed only by a baseline already in force when that
// file is VISITED, and is never reconsidered afterwards. At the top level
// baselines always sort before versioned files (B < V), so only a subdirectory
// can reveal the difference.
//
// Every expectation below was measured against the pinned oracle.
func TestSumFileNamesFlywayBaselineIsAWalkOrderStateMachine(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		files []string
		want  []string
	}{{
		name:  "a baseline squashes a nested file visited after it",
		files: []string{"B2__base.sql", "V3__three.sql", "sub/V1__one.sql"},
		want:  []string{"B2__base.sql", "V3__three.sql"},
	}, {
		name:  "a nested baseline squashes nothing visited before it",
		files: []string{"V1__one.sql", "V2__two.sql", "sub/B9__base.sql"},
		want:  []string{"sub/B9__base.sql", "V1__one.sql", "V2__two.sql"},
	}, {
		name:  "a nested baseline squashes its own siblings",
		files: []string{"sub/V1__one.sql", "sub/B2__base.sql", "sub/V3__three.sql"},
		want:  []string{"sub/B2__base.sql", "sub/V3__three.sql"},
	}, {
		// V3__b.sql survives because its PATH ("V3__b.sql", 0x56) sorts above
		// the new baseline's token ("5", 0x35) — not because survivors are
		// immune. See TestSumFileNamesFlywayBaselineReachesBackwards.
		name:  "a superseding baseline spares a survivor whose path outranks its token",
		files: []string{"B2__a.sql", "V3__b.sql", "sub/B5__c.sql"},
		want:  []string{"sub/B5__c.sql", "V3__b.sql"},
	}, {
		name:  "a superseded baseline vanishes from the sum",
		files: []string{"B2__top.sql", "V1__one.sql", "V7__seven.sql", "sub/B5__nested.sql"},
		want:  []string{"sub/B5__nested.sql", "V7__seven.sql"},
	}, {
		name:  "an ordinary project with a baseline and a subfolder",
		files: []string{"B1__baseline.sql", "V2__init.sql", "views/V3__view.sql"},
		want:  []string{"B1__baseline.sql", "V2__init.sql", "views/V3__view.sql"},
	}, {
		// Sorting the paths puts "B3.sql" before "B3/V2__a.sql" ('.' sorts
		// below '/'), which would squash V2. A walk descends B3 first, so V2 is
		// visited before the baseline exists and survives. Measured.
		name:  "selection follows the walk, not a sort of the paths",
		files: []string{"B3/V2__a.sql", "B3.sql"},
		want:  []string{"B3.sql", "B3/V2__a.sql"},
	}, {
		name:  "the squash test compares versions as strings even across directories",
		files: []string{"B2__a.sql", "V4__b.sql", "V10__d.sql", "sub/B9__c.sql"},
		want:  []string{"sub/B9__c.sql", "V4__b.sql"},
	}}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := atlasmigrateimport.SumFileNames(
				sourceFS(tt.files...),
				atlasmigrateimport.FormatFlyway,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestSumFileNamesFlywayBaselineReachesBackwards is a boundary sweep, kept as a
// test because it establishes the rule from observation alone.
//
// A baseline taking force drops files the walk had ALREADY accepted. The test
// is not a version comparison: it compares each survivor's full
// slash-separated path against the baseline's version token, as strings.
//
// The layout is held at <X>dir/V9__old.sql + B5__base.sql and only the
// directory's first byte varies. The nested file's version (9) outranks the
// baseline's (5) in every row, so every version-number model predicts one
// answer for all of them. The oracle instead cuts exactly at the token's first
// byte, which is what rules those models out and what nobody should later
// "simplify" into a numeric comparison.
func TestSumFileNamesFlywayBaselineReachesBackwards(t *testing.T) {
	c := qt.New(t)

	c.Run("sweep: the directory name decides, at the token boundary", func(c *qt.C) {
		// '5' is 0x35. Directories sorting below it lose their file.
		squashed := []string{"0dir", "1dir", "4dir"}
		kept := []string{"5dir", "6dir", "9dir", "Adir", "Vdir", "sdir", "zdir"}

		for _, dir := range squashed {
			got, err := atlasmigrateimport.SumFileNames(
				sourceFS(dir+"/V9__old.sql", "B5__base.sql"),
				atlasmigrateimport.FormatFlyway,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []string{"B5__base.sql"},
				qt.Commentf("%s should be squashed by B5", dir))
		}
		for _, dir := range kept {
			got, err := atlasmigrateimport.SumFileNames(
				sourceFS(dir+"/V9__old.sql", "B5__base.sql"),
				atlasmigrateimport.FormatFlyway,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []string{"B5__base.sql", dir + "/V9__old.sql"},
				qt.Commentf("%s should survive B5", dir))
		}
	})

	c.Run("converse sweep: the token decides, for a fixed directory", func(c *qt.C) {
		// "5d" and "5e" are the same version number and land on opposite
		// sides, which is what rules out every numeric interpretation.
		kept := []string{"1", "4", "5", "50", "5d"}
		squashed := []string{"5e", "6", "8"}

		for _, token := range kept {
			got, err := atlasmigrateimport.SumFileNames(
				sourceFS("5dir/V9__old.sql", "B"+token+"__base.sql"),
				atlasmigrateimport.FormatFlyway,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals,
				[]string{"B" + token + "__base.sql", "5dir/V9__old.sql"},
				qt.Commentf("token %s should spare 5dir", token))
		}
		for _, token := range squashed {
			got, err := atlasmigrateimport.SumFileNames(
				sourceFS("5dir/V9__old.sql", "B"+token+"__base.sql"),
				atlasmigrateimport.FormatFlyway,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []string{"B" + token + "__base.sql"},
				qt.Commentf("token %s should squash 5dir", token))
		}
	})

	c.Run("the forward squash still compares version tokens, not paths", func(c *qt.C) {
		// zdir sorts after B5, so V1 is reached with the baseline in force.
		// Its version (1) loses to the token (5) even though its path (zdir/)
		// would win. The two directions genuinely use different comparisons.
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("B5__base.sql", "zdir/V1__old.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"B5__base.sql"})
	})

	c.Run("repeatables are exempt from the backwards reach", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("0dir/R__x.sql", "B5__base.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"B5__base.sql", "0dir/R__x.sql"})
	})

	c.Run("a superseding baseline reaches backwards again", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("0dir/V9__a.sql", "3dir/V9__b.sql", "B2__base.sql", "zdir/B4__later.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"zdir/B4__later.sql"})
	})

	c.Run("a non-numeric token is the same rule, not a special case", func(c *qt.C) {
		// "x" is 0x78, above nearly every path's first byte, so Bx reaches
		// back over essentially everything. This is what used to be refused.
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("V1__a.sql", "sub/Bx__base.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"sub/Bx__base.sql"})
	})

	c.Run("a huge numeric token reaches back just as far", func(c *qt.C) {
		// Numeric, so no refusal keyed on non-numeric tokens could have
		// covered this one.
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("0archive/V5__old.sql", "B99999999999999999999__base.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"B99999999999999999999__base.sql"})
	})
}

// TestSumFileNamesFlywayComparisonOperands pins WHICH operands each comparison
// uses, and how each breaks a tie.
//
// The three comparisons are easy to collapse into one another by inspection —
// two of them even share operand types — and every earlier round of this work
// got caught by exactly that. These layouts are the ones where a plausible
// substitution changes the answer, so they are what stops a future reader from
// merging them.
func TestSumFileNamesFlywayComparisonOperands(t *testing.T) {
	c := qt.New(t)

	c.Run("supersede compares version tokens, not names", func(c *qt.C) {
		// 1dir/B9 installs first. The name "B5__base.sql" sorts above "9"
		// (0x42 > 0x39) while the token "5" does not, so a name-based
		// supersede would replace B9 with B5 here.
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("B5__base.sql", "1dir/B9__base.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"1dir/B9__base.sql"})
	})

	c.Run("the backward reach squashes an exactly equal path", func(c *qt.C) {
		// Contrived but reachable: the baseline's version token is literally
		// "V1.sql", and a survivor is named "V1.sql". Nothing else in the
		// corpus or the fuzz distinguishes <= from <, so without this the
		// operator is an arbitrary choice.
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("V1.sql", "sub/BV1.sql.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"sub/BV1.sql.sql"})
	})

	c.Run("a skipped baseline does not reach backwards", func(c *qt.C) {
		// zz/B1 loses the supersede test against the installed B5 and is
		// dropped. It must not reach back on the way out.
		//
		// This is NOT equivalent to the installed-baseline rule by
		// transitivity, though it looks it: transitivity would only cover
		// survivors admitted by the BACKWARD test, whose paths already outrank
		// the installed token. 0b/V9__x.sql was admitted by the FORWARD test
		// instead — its version (9) beat the token (5) even though its path
		// ("0b/...") sorts below both "5" and the skipped "1". So a skipped
		// baseline that reached backwards would squash it, and no other rule
		// would notice.
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("0a/B5__base.sql", "0b/V9__x.sql", "zz/B1__base.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"0a/B5__base.sql", "0b/V9__x.sql"})
	})

	c.Run("ties resolve oppositely for a baseline and a file", func(c *qt.C) {
		// An equal-version baseline WINS its comparison and replaces the
		// incumbent; an equal-version file LOSES its comparison and is
		// squashed. Same operand types, opposite tie-breaks — which is why
		// three comparisons is the minimum, not an over-fit.
		baselineWins, err := atlasmigrateimport.SumFileNames(
			sourceFS("B2__a.sql", "zdir/B2__b.sql"),
			atlasmigrateimport.FormatFlyway,
		)
		c.Assert(err, qt.IsNil)
		c.Assert(baselineWins, qt.DeepEquals, []string{"zdir/B2__b.sql"})

		fileLoses, err := atlasmigrateimport.SumFileNames(
			sourceFS("B2__base.sql", "zdir/V2__same.sql"),
			atlasmigrateimport.FormatFlyway,
		)
		c.Assert(err, qt.IsNil)
		c.Assert(fileLoses, qt.DeepEquals, []string{"B2__base.sql"})
	})
}

// TestSumFileNamesFlywayVersionComponents pins the version scoring and
// comparison, the axis a corpus of one-integer versions could never test.
//
// Each case is a pair or triple whose oracle order is decided purely by the
// comparator, measured against the pinned binary.
func TestSumFileNamesFlywayVersionComponents(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		files []string
		want  []string
	}{{
		name:  "a shorter version ranks before its zero-extended form",
		files: []string{"V1.0__seed.sql", "V1__seed.sql"},
		want:  []string{"V1__seed.sql", "V1.0__seed.sql"},
	}, {
		name:  "two trailing zero components rank after none",
		files: []string{"V2.0.0__a.sql", "V2__b.sql"},
		want:  []string{"V2__b.sql", "V2.0.0__a.sql"},
	}, {
		name:  "a trailing separator adds an empty component",
		files: []string{"V1.__a.sql", "V1__b.sql"},
		want:  []string{"V1__b.sql", "V1.__a.sql"},
	}, {
		name:  "a leading separator adds a leading zero component",
		files: []string{"V.1__a.sql", "V0.5__b.sql"},
		want:  []string{"V.1__a.sql", "V0.5__b.sql"},
	}, {
		name:  "a non-numeric component scores zero without poisoning its siblings",
		files: []string{"Vx.5__a.sql", "V0.3__b.sql"},
		want:  []string{"V0.3__b.sql", "Vx.5__a.sql"},
	}, {
		name:  "a non-numeric component ties with an explicit zero",
		files: []string{"V1.x__a.sql", "V1.0__b.sql", "V1.5__c.sql"},
		want:  []string{"V1.0__b.sql", "V1.x__a.sql", "V1.5__c.sql"},
	}, {
		name:  "negative components are kept and ordered",
		files: []string{"V-5__a.sql", "V-1__b.sql"},
		want:  []string{"V-5__a.sql", "V-1__b.sql"},
	}, {
		name:  "an overflowing component clamps rather than being rejected",
		files: []string{"V20240101120000000000__a.sql", "V2__b.sql"},
		want:  []string{"V2__b.sql", "V20240101120000000000__a.sql"},
	}, {
		// Both score {1, 5}, so the tie falls through to walk order, where
		// "V1.5__b.sql" wins on '.' sorting below '_'. Measured, not assumed:
		// the first draft of this case asserted the opposite and the oracle
		// settled it.
		name:  "underscore and dot separators are interchangeable",
		files: []string{"V1_5__a.sql", "V1.5__b.sql", "V2__c.sql"},
		want:  []string{"V1.5__b.sql", "V1_5__a.sql", "V2__c.sql"},
	}}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := atlasmigrateimport.SumFileNames(
				sourceFS(tt.files...),
				atlasmigrateimport.FormatFlyway,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestSumFileNamesFlywaySkipsHiddenDirectories pins that Atlas CE does not
// descend into dot-directories. Covering .archive/V1__old.sql would make Ptah
// refuse a directory the oracle still considers clean, which is worse than a
// plain false refusal: the offending file is one a user deliberately archived.
func TestSumFileNamesFlywaySkipsHiddenDirectories(t *testing.T) {
	c := qt.New(t)

	c.Run("a hidden directory is not covered", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS(".archive/V1__old.sql", "V2__new.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"V2__new.sql"})
	})

	c.Run("a hidden directory nested deeper is not covered either", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("sub/.old/V1__old.sql", "sub/V2__new.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"sub/V2__new.sql"})
	})

	c.Run("an ordinary directory is still covered", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("archive/V1__old.sql", "V2__new.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"archive/V1__old.sql", "V2__new.sql"})
	})
}

// TestSumFileNamesEmptyFileSetIsNotAnError pins the seam the apply-time gate
// will be built on (#973 PR 3).
//
// A directory can hold files while covering none of them: golang-migrate with
// only a down file, Flyway with only undo files, any format with only an
// uppercase .SQL extension. Atlas CE exits 0 with "No migration files to
// execute" on all of them and raises no checksum error, so the gate's exemption
// predicate is "the per-format file set is empty" — which is only expressible
// if an empty set comes back as an empty slice rather than an error. Every case
// here is also in the oracle corpus, where CE recorded an atlas.sum holding
// nothing but the empty-set directory hash.
func TestSumFileNamesEmptyFileSetIsNotAnError(t *testing.T) {
	c := qt.New(t)

	// The sum over no files at all: sha256 of the empty input, base64-encoded.
	const emptySetSum = "h1:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\n"

	tests := []struct {
		corpusCase string
		format     atlasmigrateimport.Format
	}{
		{"golang-migrate/down-only", atlasmigrateimport.FormatGolangMigrate},
		{"flyway/undo-only", atlasmigrateimport.FormatFlyway},
		{"flyway/uppercase-extension", atlasmigrateimport.FormatFlyway},
		{"goose/uppercase-extension", atlasmigrateimport.FormatGoose},
		{"goose/no-sql", atlasmigrateimport.FormatGoose},
	}

	for _, tt := range tests {
		c.Run(tt.corpusCase, func(c *qt.C) {
			fsys := os.DirFS(filepath.Join(
				"testdata", "ce-sums", filepath.FromSlash(tt.corpusCase),
			))

			names, err := atlasmigrateimport.SumFileNames(fsys, tt.format)

			c.Assert(err, qt.IsNil)
			// HasLen 0 would accept nil; the contract is an empty slice.
			c.Assert(names, qt.DeepEquals, []string{})

			// The directory is not empty on disk — only its file set is. Each
			// corpus case holds its source file plus the recorded atlas.sum.
			entries, err := fs.ReadDir(fsys, ".")
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 2)

			sum, err := migratesum.ComputeAtlasFiles(fsys, names)
			c.Assert(err, qt.IsNil)
			c.Assert(string(sum.Bytes()), qt.Equals, emptySetSum)
		})
	}
}

func TestSumFileNamesRejectsBadInput(t *testing.T) {
	c := qt.New(t)

	c.Run("nil filesystem", func(c *qt.C) {
		_, err := atlasmigrateimport.SumFileNames(nil, atlasmigrateimport.FormatGoose)

		c.Assert(err, qt.ErrorMatches, "source migration filesystem is required")
	})

	c.Run("unknown format", func(c *qt.C) {
		_, err := atlasmigrateimport.SumFileNames(sourceFS("1_init.sql"), "sqitch")

		c.Assert(err, qt.ErrorMatches, `unknown migration import format "sqitch"`)
	})
}
