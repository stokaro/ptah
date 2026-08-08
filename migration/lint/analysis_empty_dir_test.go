package lint_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// An empty migration directory is nothing to analyze on the Atlas-compatible
// surface and a usage failure on the native one.
//
// Measured on the pinned Atlas community binary v1.3.0, 2026-08-08:
// `migrate lint --dir file://<empty> --dev-url … --latest 1` exits 0 and writes
// zero bytes to both streams. ptah-compat exited 1 with
// `no *.sql migration files found` (stokaro/ptah#1241, adjacent to item 7).
//
// The native profile keeps the refusal deliberately: `ptah migrations lint`
// names a directory to analyze, and reporting "no findings" for a directory
// with nothing in it reports success for work that never happened. Only the
// compatibility surface, whose verb runs in CI before the first migration
// exists, treats it as a no-op.

func TestAnalyzeFS_EmptyDirectoryHappyPathOnAtlasProfile(t *testing.T) {
	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(fstest.MapFS{}, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.Files(), qt.HasLen, 0)
	c.Assert(analysis.Findings(), qt.HasLen, 0)
}

func TestAnalyzeFS_EmptyDirectoryFailurePathOnNativeProfile(t *testing.T) {
	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(fstest.MapFS{}, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.ErrorMatches, `no \*\.sql migration files found`)
	c.Assert(analysis.Files(), qt.HasLen, 0)
}

// A directory that is NOT empty still reaches the analyzer on the Atlas
// profile, so the relaxation above cannot be widened into "the Atlas profile
// never fails".
func TestAnalyzeFS_NonEmptyDirectoryHappyPathOnAtlasProfile(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_drop.sql": "DROP TABLE users;",
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.Files(), qt.HasLen, 1)
	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DS101"})
}
