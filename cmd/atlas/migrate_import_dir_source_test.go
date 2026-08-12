package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// flywayImportSource is a source directory only the Flyway layout can read.
// Plain SQL with no directive header is not a Goose or dbmate migration, so a
// run that succeeds on this fixture succeeded BECAUSE the flyway layout was
// selected — which is what makes the precedence rows below separable rather
// than four spellings of "some format was accepted". The companion control
// TestCompatMigrateImportRejectsTheFixtureUnderGoose measures the other half.
const flywayImportSource = "CREATE TABLE t1 (id integer primary key);\n"

// importFixture is a readable Flyway source and a target path that does not
// exist yet, so "the target was written" and "the target was not written" are
// both observable.
type importFixture struct {
	source string
	target string
	// missing is a path nothing was written to, for the rows about a source
	// directory that does not exist.
	missing string
}

func newImportFixture(c *qt.C) importFixture {
	c.Helper()
	root := c.TempDir()
	source := filepath.Join(root, "source")
	writeMigrateImportFixture(c, source, "V1__t1.sql", flywayImportSource)
	return importFixture{
		source:  source,
		target:  filepath.Join(root, "target"),
		missing: filepath.Join(root, "nope"),
	}
}

// importTargetEntryCount returns how many entries the target directory holds,
// and -1 when it does not exist at all.
//
// The count is deliberately not the imported FILE NAME. Ptah and the pinned
// community binary disagree on the version a Flyway migration maps to
// (stokaro/ptah#1235 cell 8.3), and asserting the name here would write that
// separate, still-open divergence into this file as though it were desired.
// What these rows are about is whether anything was written at all.
func importTargetEntryCount(c *qt.C, target string) int {
	c.Helper()
	entries, err := os.ReadDir(target)
	if os.IsNotExist(err) {
		return -1
	}
	c.Assert(err, qt.IsNil)
	return len(entries)
}

// TestCompatMigrateImportRejectsTheFixtureUnderGoose is the control for every
// exit-0 row below. Without it, "exit 0 and two files written" would be
// satisfied by any layout the importer happens to accept, and the precedence
// rows would prove nothing about WHICH layout won.
func TestCompatMigrateImportRejectsTheFixtureUnderGoose(t *testing.T) {
	c := qt.New(t)
	fx := newImportFixture(c)

	_, _, err := runCompatExit(
		"migrate", "import",
		"--from", "file://"+fx.source,
		"--to", "file://"+fx.target,
		"--dir-format", "goose",
	)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(importTargetEntryCount(c, fx.target), qt.Equals, -1)
}

// TestCompatMigrateImportSourceResolutionMatchesTheOracle pins the order
// `migrate import` answers its refusals in, and the values it accepts.
//
// Two things are measured here, and they are one change because they are one
// resolution. Measured on the pinned community binary v1.3.0 on 2026-08-12,
// every exit code read from an unpiped invocation:
//
//	--from src                             1  missing scheme for dir url. Did you mean "file://src"?_
//	--from src --dir-format bogus          1  missing scheme … (scheme outranks the format value)
//	--from file://nope --dir-format bogus  1  unknown dir format "bogus" (format outranks existence)
//	--from file://nope --dir-format flyway 1  sql/migrate: stat nope: no such file or directory
//	--from file://nope                     1  sql/migrate: stat nope: … (NOT the atlas-format refusal)
//	--from file://src (atlas layout)       1  cannot import a migration directory already in "atlas" format
//	--from file://src --to dst             1  missing scheme … (after the atlas-format refusal)
//
// (The trailing underscore marks the space the community binary really writes
// before the newline; see stokaro/ptah#1235 cells 9.1-9.2.)
//
// The middle three rows are the anti-swap set. Ptah answered
// `cannot import a migration directory already in "atlas" format` for a source
// directory that does not exist, because the format-sameness refusal ran ahead
// of any read of the source. Moving a stat in front of that refusal fixes the
// report and can just as easily break the row above it, where the format value
// still has to win — so both directions are rows, not one.
func TestCompatMigrateImportSourceResolutionMatchesTheOracle(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		// want is derived from the fixture because the community binary echoes
		// the directory in the form it was GIVEN, and these rows give it an
		// absolute one. Measured with both spellings:
		// `--from file://nope` prints `stat nope`, and
		// `--from file:///tmp/definitely-not-here-xyz` prints that path in
		// full, byte-identical on both binaries.
		want func(fx importFixture) string
		args func(fx importFixture) []string
	}{
		{
			name: "an unknown format outranks a missing source",
			want: func(importFixture) string { return `unknown dir format "bogus"` },
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.missing,
					"--to", "file://" + fx.target,
					"--dir-format", "bogus",
				}
			},
		},
		{
			name: "a missing source is reported as a missing source",
			want: func(fx importFixture) string {
				return "sql/migrate: stat " + fx.missing + ": no such file or directory"
			},
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.missing,
					"--to", "file://" + fx.target,
					"--dir-format", "flyway",
				}
			},
		},
		{
			name: "a missing source under the default layout is still a missing source",
			want: func(fx importFixture) string {
				return "sql/migrate: stat " + fx.missing + ": no such file or directory"
			},
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.missing,
					"--to", "file://" + fx.target,
				}
			},
		},
		{
			name: "an existing source still reports the target-layout refusal",
			want: func(importFixture) string {
				return `cannot import a migration directory already in "atlas" format`
			},
			args: func(fx importFixture) []string {
				return []string{"--from", "file://" + fx.source, "--to", "file://" + fx.target}
			},
		},
		{
			name: "a scheme-less source outranks everything",
			want: func(importFixture) string {
				return `missing scheme for dir url. Did you mean "file://src"? `
			},
			args: func(fx importFixture) []string {
				return []string{"--from", "src", "--to", "file://" + fx.target, "--dir-format", "bogus"}
			},
		},
		{
			name: "a scheme-less target is refused after the target-layout refusal",
			want: func(importFixture) string {
				return `missing scheme for dir url. Did you mean "file://target"? `
			},
			args: func(fx importFixture) []string {
				return []string{"--from", "file://" + fx.source, "--to", "target", "--dir-format", "flyway"}
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			fx := newImportFixture(c)
			want := tt.want(fx)

			stdout, stderr, err := runCompatExit(append([]string{"migrate", "import"}, tt.args(fx)...)...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(err.Error(), qt.Equals, want)
			c.Assert(stderr, qt.Equals, "Error: "+want+"\n")
			c.Assert(importTargetEntryCount(c, fx.target), qt.Equals, -1)
		})
	}
}

// TestCompatMigrateImportRefusesWhatTheOracleRefuses covers the direction
// compatibility policy (a) forbids outright: `ptah-compat` exiting 0 where the
// pinned community binary exits 1.
//
// `migrate import` kept a private format resolver that lowercased and trimmed
// its input and read a present-but-empty `?format=` as no selection, and it
// never required a scheme on either directory URL. Measured on the pinned
// community binary v1.3.0 on 2026-08-12 against master `dfd1ba6aecfe`, on a
// Flyway source, every one of these rows was exit 0 HERE and exit 1 THERE —
// and every one of them wrote a converted directory and a fresh atlas.sum
// before returning.
//
// The entry count is asserted, not only the exit code. A refusal that still
// wrote the target would be the laundering half of the same defect: the caller
// sees a failure and the directory is on disk anyway.
func TestCompatMigrateImportRefusesWhatTheOracleRefuses(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		want string
		args func(fx importFixture) []string
	}{
		{
			name: "an uppercase --dir-format is not the lowercase layout",
			want: `unknown dir format "FLYWAY"`,
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.source, "--to", "file://" + fx.target,
					"--dir-format", "FLYWAY",
				}
			},
		},
		{
			name: "a padded --dir-format is not the trimmed layout",
			want: `unknown dir format " flyway "`,
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.source, "--to", "file://" + fx.target,
					"--dir-format", " flyway ",
				}
			},
		},
		{
			name: "an uppercase query format is not the lowercase layout",
			want: `unknown dir format "FLYWAY"`,
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.source + "?format=FLYWAY",
					"--to", "file://" + fx.target,
				}
			},
		},
		{
			name: "an empty query format selects atlas and outranks --dir-format",
			want: `cannot import a migration directory already in "atlas" format`,
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.source + "?format=",
					"--to", "file://" + fx.target,
					"--dir-format", "flyway",
				}
			},
		},
		{
			name: "a scheme-less source is refused",
			want: `missing scheme for dir url. Did you mean "file://src"? `,
			args: func(fx importFixture) []string {
				return []string{"--from", "src", "--to", "file://" + fx.target, "--dir-format", "flyway"}
			},
		},
		{
			name: "a scheme-less target is refused",
			want: `missing scheme for dir url. Did you mean "file://target"? `,
			args: func(fx importFixture) []string {
				return []string{"--from", "file://" + fx.source, "--to", "target", "--dir-format", "flyway"}
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			fx := newImportFixture(c)

			stdout, stderr, err := runCompatExit(append([]string{"migrate", "import"}, tt.args(fx)...)...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(err.Error(), qt.Equals, tt.want)
			c.Assert(stderr, qt.Equals, "Error: "+tt.want+"\n")
			c.Assert(importTargetEntryCount(c, fx.target), qt.Equals, -1)
		})
	}
}

// TestCompatMigrateImportKeepsAcceptingWhatTheOracleAccepts is the other half
// of the row above it, and the reason the resolution was hoisted rather than
// simply tightened. Each spelling below is exit 0 on the pinned community
// binary v1.3.0 with the Flyway source read as Flyway, and each stays exit 0
// here.
//
// The last two rows are the ones a blunt "any query disables the flag" or
// "last value wins" rewrite would break, and they are the rules
// [atlasmigrate.ResolveApplyDirFormat] already carried for every other verb:
// an unrecognized key selects nothing and leaves --dir-format deciding, and a
// repeated format key takes the FIRST value. Goose is named second in that row
// precisely because the fixture is unreadable as Goose, so "first wins" is
// what the exit code measures.
func TestCompatMigrateImportKeepsAcceptingWhatTheOracleAccepts(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		args func(fx importFixture) []string
	}{
		{
			name: "the layout on --dir-format",
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.source, "--to", "file://" + fx.target,
					"--dir-format", "flyway",
				}
			},
		},
		{
			name: "the layout in the source query",
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.source + "?format=flyway",
					"--to", "file://" + fx.target,
				}
			},
		},
		{
			name: "an unrecognized query key leaves --dir-format deciding",
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.source + "?nonsense=1",
					"--to", "file://" + fx.target,
					"--dir-format", "flyway",
				}
			},
		},
		{
			name: "a repeated query format takes the first value",
			args: func(fx importFixture) []string {
				return []string{
					"--from", "file://" + fx.source + "?format=flyway&format=goose",
					"--to", "file://" + fx.target,
				}
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			fx := newImportFixture(c)

			stdout, stderr, err := runCompatExit(append([]string{"migrate", "import"}, tt.args(fx)...)...)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
			// One converted migration plus atlas.sum.
			c.Assert(importTargetEntryCount(c, fx.target), qt.Equals, 2)
		})
	}
}
