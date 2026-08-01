package atlasmigrateimport_test

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasmigrateimport"
	"github.com/stokaro/ptah/internal/migratesum"
)

// ceSumCorpusCases is the number of directory shapes in testdata/ce-sums. The
// corpus test walks the tree, so a corpus that failed to load would otherwise
// pass vacuously; asserting the count makes an empty or partial walk fail.
// Adding a shape is expected to change this number.
const ceSumCorpusCases = 60

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
// the source files and the atlas.sum Atlas CE v1.2.0 wrote for them, produced
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

// TestSumFileNamesFlywayBaselineWithSubdirectories pins the one layout this
// package declines to answer for. Atlas CE's baseline cut behaves differently
// depending on where the baseline sits relative to the files it would squash,
// and the measured cases admit no single rule, so a computed sum could silently
// disagree with the oracle. Refusing is loud and cannot mis-verify.
func TestSumFileNamesFlywayBaselineWithSubdirectories(t *testing.T) {
	c := qt.New(t)

	c.Run("baseline plus a nested migration is refused", func(c *qt.C) {
		_, err := atlasmigrateimport.SumFileNames(
			sourceFS("B2__base.sql", "sub/V3__three.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.ErrorIs, atlasmigrateimport.ErrFlywayBaselineWithSubdirectories)
		c.Assert(err, qt.ErrorMatches, ".*baseline B2__base.sql with sub/V3__three.sql")
	})

	c.Run("a nested baseline is refused too", func(c *qt.C) {
		_, err := atlasmigrateimport.SumFileNames(
			sourceFS("V1__one.sql", "sub/B2__base.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.ErrorIs, atlasmigrateimport.ErrFlywayBaselineWithSubdirectories)
	})

	c.Run("a nested repeatable alongside a baseline is refused", func(c *qt.C) {
		_, err := atlasmigrateimport.SumFileNames(
			sourceFS("B2__base.sql", "V3__three.sql", "sub/R__view.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.ErrorIs, atlasmigrateimport.ErrFlywayBaselineWithSubdirectories)
	})

	c.Run("subdirectories without a baseline are computed normally", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("V1__top.sql", "sub/V2__nested.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"V1__top.sql", "sub/V2__nested.sql"})
	})

	c.Run("a baseline without subdirectories is computed normally", func(c *qt.C) {
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("B2__base.sql", "V1__one.sql", "V3__three.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"B2__base.sql", "V3__three.sql"})
	})

	c.Run("a nested undo file does not trigger the refusal", func(c *qt.C) {
		// Undo files are dropped before the check, so they cannot make an
		// otherwise answerable directory unanswerable.
		got, err := atlasmigrateimport.SumFileNames(
			sourceFS("B2__base.sql", "V3__three.sql", "sub/U1__one.sql"),
			atlasmigrateimport.FormatFlyway,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"B2__base.sql", "V3__three.sql"})
	})
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
