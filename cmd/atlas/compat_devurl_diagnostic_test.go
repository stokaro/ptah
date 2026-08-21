package atlas_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// Cell 9.15 of the output-shape register (stokaro/ptah#1235): what this surface
// prints for `--dev-url`. It is the sibling #1451 measured and named when it
// closed 9.14 for `--url`, and it is a different set of call sites: `--dev-url`
// lands on twelve compat verbs, several of which forward to a native command
// rather than running here.
//
// Every expectation below was measured against the pinned community binary
// v1.3.0, through ptah-atlas-conformance/bin/atlas, on 2026-08-13, with standard
// output and standard error captured to separate files and each exit status read
// from an unpiped invocation. All rows put their message on standard error,
// leave standard output empty, and exit 1, except the two `migrate validate`
// rows that exit 0 with both streams empty and the integrity rows noted inline.
//
// The rows are not one message repeated. Three independent facts are encoded:
//
//   - The unknown-driver row is the SAME string `--url` produces, and before
//     this change not one of the six verbs printed it: five answered
//     `unsupported --dev-url dialect "notadriver://x"` and `migrate validate`
//     wrapped a connector failure into a 130-byte sentence naming an internal
//     replay step. It is asserted on every verb rather than once, because the
//     six reach it through four different code paths.
//   - The absent/empty split. `migrate diff` and `migrate lint` refuse an ABSENT
//     flag with cobra's required-flag wording; `--dev-url ""` passes that check
//     and is answered `sql/sqlclient: missing driver`. A build that refused an
//     empty value up front matches the absent rows and un-matches the empty
//     ones, which is exactly why both are here.
//   - Placement. On `migrate diff` and `migrate validate` the directory's
//     integrity gate outranks the URL; on `migrate lint` the URL outranks it.
//     That is cobra's own order — ValidateRequiredFlags runs after the pre-run
//     hooks — and the three rows at the bottom pin it, because a refusal that
//     said the right words in the wrong place would still be a divergence.
//
// Native Ptah is deliberately not held to any of this. `ptah schema inspect`
// still says `unsupported --dev-url dialect "notadriver://x"`, which names the
// flag the operator has to change and quotes the whole value they typed, where
// the community wording reports a driver problem and shows only `"notadriver"`.
// Measured before and after this change on all seven native verbs registering
// the flag, native is byte-identical on both streams. Matching is this surface's
// contract, not an improvement (AGENTS.md compatibility rule (b)).

const (
	compatDevURLRequiredRefusal = `Error: required flag(s) "dev-url" not set` + "\n"
	compatDevURLMissingDriver   = "Error: sql/sqlclient: missing driver. See: https://atlasgo.io/url\n"
	compatDevURLUnknownDriver   = "Error: sql/sqlclient: unknown driver \"notadriver\". See: https://atlasgo.io/url\n"
	compatDevURLCannotBeEmpty   = "Error: --dev-url cannot be empty\n"
)

// compatDevURLBadValue is the value every unknown-driver row is given. Its
// scheme is what both binaries quote; the `://x` remainder is dropped by the
// community wording, which is one of the reasons native keeps its own.
const compatDevURLBadValue = "notadriver://x"

// compatDevURLFixtureVersion is the one migration the hashed fixture holds.
const compatDevURLFixtureVersion = "20240101000000"

// compatDevURLFixture is the file state the rows share.
type compatDevURLFixture struct {
	dir string
	// unhashedDir holds the same migration with no integrity file. The rows that
	// use it pin WHERE the URL is answered rather than what it says: measured on
	// the pinned binary, `migrate diff` and `migrate validate` print `checksum
	// file not found` on an unhashed directory even when --dev-url is also
	// unusable, while `migrate lint` answers the URL there instead.
	unhashedDir string
	// desiredURL is a local schema file, the desired state the schema verbs are
	// pointed at. It keeps every row below database-free: measured on both
	// binaries, the `--dev-url` verdict is reached before an unreachable --url is
	// contacted, so no row needs a live server.
	desiredURL string
}

func newCompatDevURLFixture(c *qt.C) compatDevURLFixture {
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, compatDevURLFixtureVersion+"_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	unhashed := filepath.Join(root, "unhashed")
	c.Assert(os.MkdirAll(unhashed, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(unhashed, compatDevURLFixtureVersion+"_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)

	desired := filepath.Join(root, "desired.hcl")
	c.Assert(os.WriteFile(desired, []byte("schema \"main\" {\n}\n"), 0o600), qt.IsNil)
	return compatDevURLFixture{dir: dir, unhashedDir: unhashed, desiredURL: "file://" + desired}
}

// compatDevURLUnreachableTarget is a --url no server answers. The rows that
// carry it prove the dev-url verdict lands in front of the target connection,
// which both binaries were measured to do; a row that connected first would
// report the connection instead and stop measuring this cell.
const compatDevURLUnreachableTarget = "postgres://127.0.0.1:1/nope?sslmode=disable"

// compatDevURLRow is one measured cell.
type compatDevURLRow struct {
	name string
	// verb is the compat command path the row exercises. The coverage guard
	// below reads it, so a verb that grows a --dev-url without gaining rows here
	// fails rather than passing unnoticed.
	verb string
	// args builds the invocation. Each verb needs different companion flags, so
	// this is per-row wiring rather than a branch in the test body.
	args func(fx compatDevURLFixture) []string
	// wantStderr is the whole standard-error stream, byte for byte.
	wantStderr string
	// wantStdout is the whole standard-output stream, byte for byte. It is
	// asserted rather than ignored so a message that leaked onto the wrong
	// stream is still a divergence.
	wantStdout string
	// wantErr is whether the invocation fails. The two `migrate validate` rows
	// that accept a missing dev database are the reason this is a field: they
	// exit 0 on the pinned binary and must exit 0 here.
	wantErr bool
}

// compatDevURLChecksumGuidance is the standard-output block the integrity
// refusal prints, byte-identical on the pinned community binary v1.3.0.
const compatDevURLChecksumGuidance = "You have a checksum error in your migration directory.\n" +
	"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n"

func compatDevURLRows() []compatDevURLRow {
	return slices.Concat(
		compatDevURLMigrateDiffRows(),
		compatDevURLMigrateLintRows(),
		compatDevURLMigrateValidateRows(),
		compatDevURLSchemaRows(),
		compatDevURLPlacementRows(),
	)
}

func compatDevURLMigrateDiffRows() []compatDevURLRow {
	return []compatDevURLRow{
		{
			name: "migrate diff refuses an absent dev-url as a required flag",
			verb: "migrate diff",
			args: func(fx compatDevURLFixture) []string {
				return []string{"migrate", "diff", "demo", "--dir", "file://" + fx.dir, "--to", fx.desiredURL}
			},
			wantStderr: compatDevURLRequiredRefusal,
			wantErr:    true,
		},
		{
			// The absent/empty split: the refusal above asks whether the flag was
			// given, so an explicitly empty value travels on to the client layer.
			name: "migrate diff opens an empty dev-url and reports the missing driver",
			verb: "migrate diff",
			args: func(fx compatDevURLFixture) []string {
				return []string{
					"migrate", "diff", "demo", "--dir", "file://" + fx.dir,
					"--to", fx.desiredURL, "--dev-url", "",
				}
			},
			wantStderr: compatDevURLMissingDriver,
			wantErr:    true,
		},
		{
			name: "migrate diff names the unknown driver",
			verb: "migrate diff",
			args: func(fx compatDevURLFixture) []string {
				return []string{
					"migrate", "diff", "demo", "--dir", "file://" + fx.dir,
					"--to", fx.desiredURL, "--dev-url", compatDevURLBadValue,
				}
			},
			wantStderr: compatDevURLUnknownDriver,
			wantErr:    true,
		},
	}
}

func compatDevURLMigrateLintRows() []compatDevURLRow {
	return []compatDevURLRow{
		{
			name: "migrate lint refuses an absent dev-url as a required flag",
			verb: "migrate lint",
			args: func(fx compatDevURLFixture) []string {
				return []string{"migrate", "lint", "--dir", "file://" + fx.dir, "--latest", "1"}
			},
			wantStderr: compatDevURLRequiredRefusal,
			wantErr:    true,
		},
		{
			name: "migrate lint opens an empty dev-url and reports the missing driver",
			verb: "migrate lint",
			args: func(fx compatDevURLFixture) []string {
				return []string{
					"migrate", "lint", "--dir", "file://" + fx.dir,
					"--latest", "1", "--dev-url", "",
				}
			},
			wantStderr: compatDevURLMissingDriver,
			wantErr:    true,
		},
		{
			name: "migrate lint names the unknown driver",
			verb: "migrate lint",
			args: func(fx compatDevURLFixture) []string {
				return []string{
					"migrate", "lint", "--dir", "file://" + fx.dir,
					"--latest", "1", "--dev-url", compatDevURLBadValue,
				}
			},
			wantStderr: compatDevURLUnknownDriver,
			wantErr:    true,
		},
	}
}

func compatDevURLMigrateValidateRows() []compatDevURLRow {
	return []compatDevURLRow{
		{
			// This verb marks nothing required: with no dev database it verifies
			// integrity alone and exits 0. The row is here because a gate that
			// refused an absent value would break it, and because it is the
			// control that keeps the unknown-driver row below non-vacuous.
			name: "migrate validate accepts an absent dev-url",
			verb: "migrate validate",
			args: func(fx compatDevURLFixture) []string {
				return []string{"migrate", "validate", "--dir", "file://" + fx.dir}
			},
		},
		{
			name: "migrate validate accepts an empty dev-url",
			verb: "migrate validate",
			args: func(fx compatDevURLFixture) []string {
				return []string{"migrate", "validate", "--dir", "file://" + fx.dir, "--dev-url", ""}
			},
		},
		{
			name: "migrate validate names the unknown driver",
			verb: "migrate validate",
			args: func(fx compatDevURLFixture) []string {
				return []string{
					"migrate", "validate", "--dir", "file://" + fx.dir,
					"--dev-url", compatDevURLBadValue,
				}
			},
			wantStderr: compatDevURLUnknownDriver,
			wantErr:    true,
		},
	}
}

func compatDevURLSchemaRows() []compatDevURLRow {
	return []compatDevURLRow{
		{
			name: "schema inspect names the unknown driver",
			verb: "schema inspect",
			args: func(fx compatDevURLFixture) []string {
				return []string{"schema", "inspect", "--url", fx.desiredURL, "--dev-url", compatDevURLBadValue}
			},
			wantStderr: compatDevURLUnknownDriver,
			wantErr:    true,
		},
		{
			// The empty value is NOT a driver question on this verb: both
			// binaries answer it with a sentence about the dev database. The row
			// is here so a gate widened to refuse empty values as missing drivers
			// is caught.
			name: "schema inspect keeps its own refusal for an empty dev-url",
			verb: "schema inspect",
			args: func(fx compatDevURLFixture) []string {
				return []string{"schema", "inspect", "--url", fx.desiredURL, "--dev-url", ""}
			},
			wantStderr: compatDevURLCannotBeEmpty,
			wantErr:    true,
		},
		{
			name: "schema diff names the unknown driver",
			verb: "schema diff",
			args: func(fx compatDevURLFixture) []string {
				return []string{
					"schema", "diff", "--from", fx.desiredURL, "--to", fx.desiredURL,
					"--dev-url", compatDevURLBadValue,
				}
			},
			wantStderr: compatDevURLUnknownDriver,
			wantErr:    true,
		},
		{
			// The target is unreachable and never contacted: on both binaries the
			// dev-url verdict lands first. Without this row the schema apply cell
			// could only be measured against a live server.
			name: "schema apply names the unknown driver before it contacts the target",
			verb: "schema apply",
			args: func(fx compatDevURLFixture) []string {
				return []string{
					"schema", "apply", "--url", compatDevURLUnreachableTarget,
					"--to", fx.desiredURL, "--auto-approve", "--dev-url", compatDevURLBadValue,
				}
			},
			wantStderr: compatDevURLUnknownDriver,
			wantErr:    true,
		},
	}
}

// compatDevURLPlacementRows pin WHERE the URL is answered, not what it says.
//
// They are the rows a build that said the right words in the wrong order would
// still fail, and the order is measured rather than chosen: cobra runs
// ValidateRequiredFlags after the pre-run hooks, so a verb gating integrity in a
// hook answers the checksum first and a verb that does not answers the URL
// first. The pinned binary shows exactly that split across these three.
func compatDevURLPlacementRows() []compatDevURLRow {
	return []compatDevURLRow{
		{
			name: "migrate diff lets the integrity gate outrank an absent dev-url",
			verb: "migrate diff",
			args: func(fx compatDevURLFixture) []string {
				return []string{
					"migrate", "diff", "demo", "--dir", "file://" + fx.unhashedDir,
					"--to", fx.desiredURL,
				}
			},
			wantStderr: "Error: checksum file not found\n",
			wantStdout: compatDevURLChecksumGuidance,
			wantErr:    true,
		},
		{
			name: "migrate validate lets the integrity gate outrank an unusable dev-url",
			verb: "migrate validate",
			args: func(fx compatDevURLFixture) []string {
				return []string{
					"migrate", "validate", "--dir", "file://" + fx.unhashedDir,
					"--dev-url", compatDevURLBadValue,
				}
			},
			wantStderr: "Error: checksum file not found\n",
			wantStdout: compatDevURLChecksumGuidance,
			wantErr:    true,
		},
		{
			// The other side of the line: here the required-flag refusal wins
			// over the very same unhashed directory.
			name: "migrate lint lets an absent dev-url outrank the integrity gate",
			verb: "migrate lint",
			args: func(fx compatDevURLFixture) []string {
				return []string{"migrate", "lint", "--dir", "file://" + fx.unhashedDir, "--latest", "1"}
			},
			wantStderr: compatDevURLRequiredRefusal,
			wantErr:    true,
		},
	}
}

// TestCompatDevURLDiagnostics_MatchThePinnedBinary pins every measured cell.
//
// Mutated so the unknown-driver verdict is dropped and the value is passed
// through, the six unknown-driver rows fail and every other row passes.
// Mutated so the refusal asks whether the value is empty instead of whether the
// flag was given, the two missing-driver rows fail and the two required-flag
// rows still pass — which is the whole point of keeping both.
func TestCompatDevURLDiagnostics_MatchThePinnedBinary(t *testing.T) {
	t.Chdir(t.TempDir())
	c := qt.New(t)
	fx := newCompatDevURLFixture(c)

	for _, tt := range compatDevURLRows() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stdout, stderr, err := runCompat(tt.args(fx)...)

			c.Check(err != nil, qt.Equals, tt.wantErr,
				qt.Commentf("stdout=%q stderr=%q err=%v", stdout, stderr, err))
			c.Check(stderr, qt.Equals, tt.wantStderr)
			c.Check(stdout, qt.Equals, tt.wantStdout)
		})
	}
}

// TestCompatDevURLDiagnostics_LeaveDockerToTheProvisioner is the
// non-interference control for the gate.
//
// The gate in this file answers a `--dev-url` whose scheme names no driver, and
// `docker` is such a scheme. Without its explicit exemption every docker dev
// database would be answered `unknown driver "docker"` — the exact trade the
// compatibility policy forbids, a specific diagnostic replaced by a vague one,
// and since stokaro/ptah#844 it would also refuse a URL this build can
// provision.
//
// What the exemption protects changed with that work but the control did not:
// the value must still reach the layer that owns it and be answered in that
// layer's words. `docker://sqlite/3/dev` is used because it is answered without
// starting anything — measured, the pinned community binary v1.3.0 refuses it
// `unsupported docker image "sqlite"` and exits 1 — so this control cannot
// become an image pull.
func TestCompatDevURLDiagnostics_LeaveDockerToTheProvisioner(t *testing.T) {
	t.Chdir(t.TempDir())
	c := qt.New(t)
	fx := newCompatDevURLFixture(c)

	_, stderr, err := runCompat(
		"migrate", "lint", "--dir", "file://"+fx.dir, "--latest", "1",
		"--dev-url", "docker://sqlite/3/dev",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, `unsupported docker image "sqlite"`)
	c.Assert(stderr, qt.Not(qt.Contains), "unknown driver")
}

// compatDevURLVerbsWithoutOracleRow are the compat verbs that register
// --dev-url with no row on the pinned community binary to match.
//
// Six of the eight were measured on 2026-08-13: each answers `Error: unknown
// flag: --dev-url` there, because the community version does not carry the flag
// on them at all, and `migrate checkpoint` additionally reports the whole verb
// as unavailable. `schema plan lint` and `schema plan test` are the other two
// and need no separate measurement: both are sub-verbs of `schema plan`, which
// that binary refuses as a whole (stokaro/ptah#1211). Ptah registers --dev-url
// on all eight as capabilities that binary does not have, so there is no
// wording to copy and their own diagnostics are left alone. Rewording them would be inventing an oracle rather than matching
// one; deleting the flags would remove a capability (AGENTS.md rule (c)).
func compatDevURLVerbsWithoutOracleRow() []string {
	return []string{
		"migrate checkpoint",
		"migrate down",
		"migrate test",
		"schema plan lint",
		"schema plan new",
		"schema plan test",
		"schema plan validate",
		"schema test",
	}
}

// TestCompatDevURLDiagnostics_CoverEveryVerbRegisteringTheFlag closes the
// fixture.
//
// The rows above are a hand-written list, and a hand-written list of what a
// surface exposes goes stale the moment a verb is added. This walks the built
// command tree instead and requires every command registering --dev-url to be
// either pinned by a row or named as having no oracle row, so a thirteenth verb
// cannot land carrying the old wording unnoticed.
func TestCompatDevURLDiagnostics_CoverEveryVerbRegisteringTheFlag(t *testing.T) {
	c := qt.New(t)

	registered := compatVerbsRegisteringFlag(atlas.NewCompatCommand("atlas"), "dev-url")
	c.Assert(len(registered) > 0, qt.IsTrue,
		qt.Commentf("the walk found no --dev-url at all, so it is measuring nothing"))

	accounted := slices.Clone(compatDevURLVerbsWithoutOracleRow())
	for _, row := range compatDevURLRows() {
		accounted = append(accounted, row.verb)
	}
	slices.Sort(accounted)
	accounted = slices.Compact(accounted)

	for _, verb := range registered {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(accounted, qt.Contains, verb,
				qt.Commentf("%q registers --dev-url but no row pins its diagnostics"+
					" and it is not named as having no oracle row", verb))
		})
	}

	// The converse: a name that no longer registers --dev-url must not sit here
	// claiming coverage it cannot have.
	for _, verb := range accounted {
		t.Run("still registered: "+verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(registered, qt.Contains, verb,
				qt.Commentf("%q is accounted for but no longer registers --dev-url", verb))
		})
	}
}
