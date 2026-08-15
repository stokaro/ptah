package atlas_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// Cell 9.14 of the output-shape register (stokaro/ptah#1235): what this surface
// prints when `--url` is missing or names no driver.
//
// Every expectation below was measured against the pinned community binary
// v1.3.0, through ptah-atlas-conformance/bin/atlas, on 2026-08-13, with
// standard output and standard error captured to separate files and each exit
// status read from an unpiped invocation. All eighteen cells put their message
// on standard error, leave standard output empty, and exit 1.
//
// The rows are not one message repeated. Three independent facts are encoded:
//
//   - `migrate apply` answers the singular `required flag "url" not set`; the
//     three schema verbs answer the plural `required flag(s) "url" not set`.
//     A build that emits one spelling everywhere passes three of those four
//     rows, which is exactly why the singular row exists here.
//   - `schema clean` and `schema inspect` refuse only an ABSENT flag; an
//     explicitly empty value passes their check and is answered by a resolver
//     instead. `migrate apply` and `schema apply` answer the value, so empty
//     and absent read alike.
//   - `migrate set` and `migrate status` have no required check at all. Their
//     absent `--url` is opened as the empty string and answered
//     `sql/sqlclient: missing driver` — the row an adapter that refused an
//     empty value up front could never produce.
//
// Native Ptah is deliberately not held to any of this. `ptah migrations status`
// still says `database URL is required`, which names the flag the caller forgot
// rather than reporting a driver problem, and measured before and after this
// change it is byte-identical. Matching is this surface's contract, not an
// improvement (AGENTS.md compatibility rule (b)).

const (
	compatURLSingularRefusal = `Error: required flag "url" not set` + "\n"
	compatURLPluralRefusal   = `Error: required flag(s) "url" not set` + "\n"
	compatURLMissingDriver   = "Error: sql/sqlclient: missing driver. See: https://atlasgo.io/url\n"
	compatURLMissingScheme   = "Error: missing scheme. See: https://atlasgo.io/url\n"
	compatURLUnknownDriver   = "Error: sql/sqlclient: unknown driver \"notadriver\". See: https://atlasgo.io/url\n"
)

// compatURLFixtureVersion is the one migration the hashed fixture directory
// holds, and the version `migrate set` is pointed at.
const compatURLFixtureVersion = "20240101000000"

// compatURLFixture is the file state the rows share: a hashed Atlas migration
// directory and a desired-state schema file.
//
// The directory is hashed so the integrity gate is never what answers. That
// matters for more than tidiness: measured on the pinned binary, an UNHASHED
// directory answers `migrate apply` with `checksum file not found` and the
// required-flag row is not reachable at all, so a fixture that skipped the sum
// would quietly test a different cell.
type compatURLFixture struct {
	dir string
	// unhashedDir holds the same migration with no integrity file. It exists
	// so the rows can pin WHERE the URL is answered and not merely what it
	// says: measured on the pinned binary, `migrate set` and `migrate status`
	// against an unhashed directory print `checksum file not found` even with
	// no --url at all, so the URL must be settled after the integrity gate and
	// not in front of the command body.
	unhashedDir string
	desiredURL  string
}

func newCompatURLFixture(c *qt.C) compatURLFixture {
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, compatURLFixtureVersion+"_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	unhashed := filepath.Join(root, "unhashed")
	c.Assert(os.MkdirAll(unhashed, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(unhashed, compatURLFixtureVersion+"_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)

	desired := filepath.Join(root, "desired.hcl")
	c.Assert(os.WriteFile(desired, []byte("schema \"main\" {\n}\n"), 0o600), qt.IsNil)
	return compatURLFixture{dir: dir, unhashedDir: unhashed, desiredURL: "file://" + desired}
}

// compatURLRow is one measured cell.
type compatURLRow struct {
	name string
	// verb is the compat command path the row exercises. The coverage guard
	// below reads it, so a verb that grows a --url without gaining rows here
	// fails rather than passing unnoticed.
	verb string
	// args builds the invocation. Each verb needs different companion flags,
	// so this is per-row wiring rather than a branch in the test body.
	args func(fx compatURLFixture) []string
	// wantStderr is the whole standard-error stream, byte for byte.
	wantStderr string
	// wantStdout is the whole standard-output stream, byte for byte. It is
	// empty for every URL diagnostic, and asserted rather than ignored so a
	// message that leaked onto the wrong stream is still a divergence. The two
	// integrity rows are the exception: that refusal prints guidance on
	// standard output, on the pinned binary and here alike.
	wantStdout string
}

// compatChecksumGuidance is the standard-output block the integrity refusal
// prints. Measured at 143 bytes on the pinned community binary v1.3.0 and
// byte-identical here.
const compatChecksumGuidance = "You have a checksum error in your migration directory.\n" +
	"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n"

func compatURLRows() []compatURLRow {
	return []compatURLRow{
		{
			name: "migrate apply refuses an absent url in the singular",
			verb: "migrate apply",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "apply", "--dir", "file://" + fx.dir}
			},
			wantStderr: compatURLSingularRefusal,
		},
		{
			name: "migrate apply refuses an empty url in the singular",
			verb: "migrate apply",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "apply", "--dir", "file://" + fx.dir, "--url", ""}
			},
			wantStderr: compatURLSingularRefusal,
		},
		{
			name: "migrate apply names the unknown driver",
			verb: "migrate apply",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "apply", "--dir", "file://" + fx.dir, "--url", "notadriver://x"}
			},
			wantStderr: compatURLUnknownDriver,
		},
		{
			name: "migrate set opens an absent url and reports the missing driver",
			verb: "migrate set",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "set", compatURLFixtureVersion, "--dir", "file://" + fx.dir}
			},
			wantStderr: compatURLMissingDriver,
		},
		{
			name: "migrate set opens an empty url and reports the missing driver",
			verb: "migrate set",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "set", compatURLFixtureVersion, "--dir", "file://" + fx.dir, "--url", ""}
			},
			wantStderr: compatURLMissingDriver,
		},
		{
			name: "migrate set names the unknown driver",
			verb: "migrate set",
			args: func(fx compatURLFixture) []string {
				return []string{
					"migrate", "set", compatURLFixtureVersion,
					"--dir", "file://" + fx.dir, "--url", "notadriver://x",
				}
			},
			wantStderr: compatURLUnknownDriver,
		},
		{
			name: "migrate status opens an absent url and reports the missing driver",
			verb: "migrate status",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "status", "--dir", "file://" + fx.dir}
			},
			wantStderr: compatURLMissingDriver,
		},
		{
			name: "migrate status opens an empty url and reports the missing driver",
			verb: "migrate status",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "status", "--dir", "file://" + fx.dir, "--url", ""}
			},
			wantStderr: compatURLMissingDriver,
		},
		{
			name: "migrate status names the unknown driver",
			verb: "migrate status",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "status", "--dir", "file://" + fx.dir, "--url", "notadriver://x"}
			},
			wantStderr: compatURLUnknownDriver,
		},
		{
			// A bare word carries no scheme to select a driver with, and the
			// pinned binary answers it exactly as it answers an absent value.
			name: "migrate status reports a missing driver for a url with no scheme",
			verb: "migrate status",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "status", "--dir", "file://" + fx.dir, "--url", "noscheme"}
			},
			wantStderr: compatURLMissingDriver,
		},
		{
			// Placement, not wording. If the URL were settled in front of the
			// command body this row would print a missing driver instead.
			name: "migrate status lets the integrity gate outrank an absent url",
			verb: "migrate status",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "status", "--dir", "file://" + fx.unhashedDir}
			},
			wantStderr: "Error: checksum file not found\n",
			wantStdout: compatChecksumGuidance,
		},
		{
			name: "migrate set lets the integrity gate outrank an absent url",
			verb: "migrate set",
			args: func(fx compatURLFixture) []string {
				return []string{"migrate", "set", compatURLFixtureVersion, "--dir", "file://" + fx.unhashedDir}
			},
			wantStderr: "Error: checksum file not found\n",
			wantStdout: compatChecksumGuidance,
		},
		{
			name: "schema apply refuses an absent url in the plural",
			verb: "schema apply",
			args: func(fx compatURLFixture) []string {
				return []string{"schema", "apply", "--to", fx.desiredURL}
			},
			wantStderr: compatURLPluralRefusal,
		},
		{
			name: "schema apply refuses an empty url in the plural",
			verb: "schema apply",
			args: func(fx compatURLFixture) []string {
				return []string{"schema", "apply", "--to", fx.desiredURL, "--url", ""}
			},
			wantStderr: compatURLPluralRefusal,
		},
		{
			name: "schema apply names the unknown driver",
			verb: "schema apply",
			args: func(fx compatURLFixture) []string {
				return []string{"schema", "apply", "--to", fx.desiredURL, "--url", "notadriver://x"}
			},
			wantStderr: compatURLUnknownDriver,
		},
		{
			name: "schema clean refuses an absent url in the plural",
			verb: "schema clean",
			args: func(compatURLFixture) []string {
				return []string{"schema", "clean"}
			},
			wantStderr: compatURLPluralRefusal,
		},
		{
			// The absent/empty split: this verb's refusal asks whether the flag
			// was given, so an empty value travels on to the client layer.
			name: "schema clean opens an empty url and reports the missing driver",
			verb: "schema clean",
			args: func(compatURLFixture) []string {
				return []string{"schema", "clean", "--url", ""}
			},
			wantStderr: compatURLMissingDriver,
		},
		{
			name: "schema clean names the unknown driver",
			verb: "schema clean",
			args: func(compatURLFixture) []string {
				return []string{"schema", "clean", "--url", "notadriver://x"}
			},
			wantStderr: compatURLUnknownDriver,
		},
		{
			name: "schema inspect refuses an absent url in the plural",
			verb: "schema inspect",
			args: func(compatURLFixture) []string {
				return []string{"schema", "inspect"}
			},
			wantStderr: compatURLPluralRefusal,
		},
		{
			// This verb's --url names a desired-state source rather than a
			// connection, so an empty one is answered by the source layer and
			// carries no sql/sqlclient prefix.
			name: "schema inspect reports a missing scheme for an empty url",
			verb: "schema inspect",
			args: func(compatURLFixture) []string {
				return []string{"schema", "inspect", "--url", ""}
			},
			wantStderr: compatURLMissingScheme,
		},
		{
			name: "schema inspect names the unknown driver",
			verb: "schema inspect",
			args: func(compatURLFixture) []string {
				return []string{"schema", "inspect", "--url", "notadriver://x"}
			},
			wantStderr: compatURLUnknownDriver,
		},
	}
}

// TestCompatURLDiagnostics_MatchThePinnedBinary pins every measured cell.
//
// Mutated so the singular spelling on `migrate apply` is replaced by the plural
// one, this test fails on the two `migrate apply` refusal rows and passes
// everywhere else — which is the whole point of keeping the spelling as
// per-verb data. Mutated so the gate in front of the open refuses an empty
// value early, the four `migrate set` and `migrate status` missing-driver rows
// fail instead.
func TestCompatURLDiagnostics_MatchThePinnedBinary(t *testing.T) {
	t.Chdir(t.TempDir())
	c := qt.New(t)
	fx := newCompatURLFixture(c)

	for _, tt := range compatURLRows() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stdout, stderr, err := runCompat(tt.args(fx)...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
			c.Assert(stderr, qt.Equals, tt.wantStderr)
			c.Assert(stdout, qt.Equals, tt.wantStdout)
		})
	}
}

// compatURLVerbsWithoutOracleRow are the compat verbs that register --url with
// no row on the pinned community binary to match.
//
// Both were measured on 2026-08-13: `atlas migrate down --url ...` and
// `atlas schema test --url ...` both answer `Error: unknown flag: --url`, and
// bare `atlas migrate down` reports the verb itself as unavailable in the
// community version. Ptah registers --url on both as capabilities that binary
// does not have, so there is no wording to copy and their own diagnostics are
// left alone. Rewording them would be inventing an oracle rather than matching
// one; deleting the flags would remove a capability (AGENTS.md rule (c)).
func compatURLVerbsWithoutOracleRow() []string {
	return []string{"migrate down", "schema test"}
}

// TestCompatURLDiagnostics_CoverEveryVerbRegisteringTheFlag closes the fixture.
//
// The rows above are a hand-written list, and a hand-written list of what a
// surface exposes goes stale the moment a verb is added. This walks the built
// command tree instead and requires every command that registers --url to be
// either pinned by a row or named as having no oracle row, so a seventh verb
// cannot land carrying the old wording unnoticed.
func TestCompatURLDiagnostics_CoverEveryVerbRegisteringTheFlag(t *testing.T) {
	c := qt.New(t)

	registered := compatVerbsRegisteringFlag(atlas.NewCompatCommand("atlas"), "url")
	c.Assert(len(registered) > 0, qt.IsTrue,
		qt.Commentf("the walk found no --url at all, so it is measuring nothing"))

	accounted := slices.Clone(compatURLVerbsWithoutOracleRow())
	for _, row := range compatURLRows() {
		accounted = append(accounted, row.verb)
	}
	slices.Sort(accounted)
	accounted = slices.Compact(accounted)

	for _, verb := range registered {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(accounted, qt.Contains, verb,
				qt.Commentf("%q registers --url but no row pins its diagnostics"+
					" and it is not named as having no oracle row", verb))
		})
	}

	// The converse: a name that no longer registers --url must not sit here
	// claiming coverage it cannot have.
	for _, verb := range accounted {
		t.Run("still registered: "+verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(registered, qt.Contains, verb,
				qt.Commentf("%q is accounted for but no longer registers --url", verb))
		})
	}
}

// compatVerbsRegisteringFlag returns the space-joined path of every runnable
// command below root that registers the named flag, sorted.
//
// The flag name is a parameter rather than a constant because two coverage
// gates now walk this tree for two different URL-shaped flags, and a second
// copy of the walk is how the two would drift into disagreeing about what
// "runnable command" means.
func compatVerbsRegisteringFlag(root *cobra.Command, flag string) []string {
	var found []string
	var walk func(cmd *cobra.Command, path []string)
	walk = func(cmd *cobra.Command, path []string) {
		children := cmd.Commands()
		for _, child := range children {
			walk(child, append(slices.Clone(path), child.Name()))
		}
		if len(children) > 0 || cmd.Flags().Lookup(flag) == nil {
			return
		}
		found = append(found, strings.Join(path, " "))
	}
	for _, child := range root.Commands() {
		walk(child, []string{child.Name()})
	}
	slices.Sort(found)
	return found
}
