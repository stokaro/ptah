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

// This file pins WHERE a `docker://` dev database URL is answered on the
// Atlas-compatible surface, verb by verb, and closes the set by walking the
// built command tree (stokaro/ptah#844).
//
// Before this change every consumer of a dev database refused a `docker://`
// value with its own sentence, and `ptah-compat` exited 1 on four verbs where
// the pinned community binary v1.3.0 exits 0. Measured on 2026-08-13 against
// ptah-atlas-conformance/bin/atlas, each exit status read from an unpiped
// invocation, on a hashed migration directory and a schema file:
//
//	verb              pinned binary            ptah-compat before   ptah-compat after
//	migrate diff      exit 0, provisions       exit 1, refused      exit 0, provisions
//	migrate lint      exit 0, provisions       exit 1, refused      exit 0, provisions
//	migrate validate  exit 0, provisions       exit 1, refused      exit 0, provisions
//	schema inspect    exit 0, provisions       exit 1, refused      exit 0, provisions
//	schema diff       exit 0, provisions       exit 0, no container exit 0, see below
//
// # Why the probe URL is docker://sqlite/dev
//
// Every row below is driven with `--dev-url docker://sqlite/dev`, which starts
// nothing. That is not a way of avoiding Docker in a unit test -- it is the
// sharpest available probe, for two reasons.
//
// It is a value the pinned binary REFUSES: measured, `docker://sqlite/dev`
// answers `unsupported docker image "sqlite"` and exits 1, because sqlite is a
// dialect Ptah has and an image that binary will not run. So a build that grew
// a container for it would be exiting 0 where the pinned binary exits 1, the
// one direction AGENTS.md compatibility rule (a) forbids. Ptah's own dialect
// parser answers `sqlite` for this URL quite happily, so the refusal can only
// come from the provisioning layer -- which makes the message proof that the
// value reached that layer and was not answered by something in front of it.
//
// And it separates the wired verbs from the unwired ones by their words. A verb
// that still hands a docker URL to the database connector answers `unsupported
// database dialect: docker`, an internal classification; a verb that reaches
// the provisioner answers in the pinned binary's own wording. The rows record
// which verbs say which, so a verb losing its wiring is a failure here rather
// than a slow refusal in production.
//
// # schema diff is a partial row on purpose
//
// `schema diff` between two local schema FILES never opens a dev database at
// all: it uses `--dev-url` only to pin the SQL dialect, and it does that
// identically for `docker://postgres/16/dev` and for a directly connectable
// `postgres://` URL. Measured after this change, both produce the same
// statement, and neither starts a container. Its migration-directory source
// does provision, and that is the row below. The remaining divergence -- the
// pinned binary normalizes local files through the dev database and so writes
// `ALTER TABLE "public"."users" ADD COLUMN "email" text NULL` where Ptah writes
// `ALTER TABLE "users" ADD COLUMN "email" text` -- is not docker-specific and
// is recorded in docs/conformance.md rather than papered over here.

// compatDockerImageRefusal is the pinned binary's own wording for a docker URL
// naming an image it will not run, spelled once so every row that expects the
// provisioning layer to have answered agrees on it byte for byte.
const compatDockerImageRefusal = `unsupported docker image "sqlite"`

// compatDockerRow is one verb's measured answer to a docker dev database URL.
type compatDockerRow struct {
	name string
	// verb is the compat command path, in the spelling the tree walk below
	// produces. The coverage guard reads it, so a verb that grows a --dev-url
	// without gaining a row here fails rather than passing unnoticed.
	verb string
	// args builds the invocation without --dev-url, which the runner appends.
	args func(fx compatDockerFixture) []string
	// check names which layer answers the verb. The rows genuinely differ --
	// some are refused by the provisioner, some never consult the dev URL at
	// all -- but the difference is one of three outcomes, so the row names one
	// and [assertDevURLOutcome] knows each. Holding the assertion itself put
	// the checker in a table row; see AGENTS.md, "A Table Row Carries Data,
	// Not A Checker".
	check devURLOutcome
	// checkWhitespace names the outcome for the same verb when the identical
	// probe URL is written with ONE LEADING SPACE. See
	// TestCompatDockerDevURL_DoesNotProvisionAValueTheBinaryCannotParse.
	checkWhitespace devURLOutcome
}

// devURLOutcome is which layer answered a row. The values are sentences so a
// failure names the outcome that was expected rather than an index.
type devURLOutcome string

const (
	refusedByProvisioner devURLOutcome = "refused by the provisioner"
	devURLNotConsulted   devURLOutcome = "dev URL never consulted"
	notADockerURL        devURLOutcome = "refused before the provisioner"
)

// assertDevURLOutcome checks one run against the outcome its row named.
func assertDevURLOutcome(c *qt.C, want devURLOutcome, stdout, stderr string, err error) {
	c.Helper()
	switch want {
	case refusedByProvisioner:
		assertRefusedByProvisioner(c, stdout, stderr, err)
	case devURLNotConsulted:
		assertDevURLNotConsulted(c, stdout, stderr, err)
	case notADockerURL:
		assertNotADockerURL(c, stdout, stderr, err)
	default:
		c.Fatalf("no assertion for outcome %q", want)
	}
}

// compatDockerFixture is the file state the rows share.
type compatDockerFixture struct {
	// dir is a hashed Atlas migration directory, so the integrity gate is never
	// what answers a row.
	dir string
	// schema is a local desired-state schema file.
	schema string
	// root holds both, and doubles as an (empty) test-case directory.
	root string
}

func newCompatDockerFixture(c *qt.C) compatDockerFixture {
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "20240101000000_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	schema := filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return compatDockerFixture{dir: dir, schema: schema, root: root}
}

// refusedByProvisioner asserts the verb answered in the provisioning layer's
// words, wherever in its own diagnostic it chose to place them.
func assertRefusedByProvisioner(c *qt.C, stdout, stderr string, err error) {
	c.Helper()
	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Check(stderr, qt.Contains, compatDockerImageRefusal)
	// The old wording must be gone: a verb that still reports the URL as an
	// unknown dialect never reached the provisioner, whatever else it printed.
	c.Check(stderr, qt.Not(qt.Contains), "unsupported database dialect: docker")
}

// devURLNotConsulted asserts the verb completed without ever opening a dev
// database, which is what `schema diff` between two local files does.
func assertDevURLNotConsulted(c *qt.C, stdout, stderr string, err error) {
	c.Helper()
	c.Assert(err, qt.IsNil, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Check(stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
}

// notADockerURL asserts the verb refused a dev URL written with a leading
// space, and refused it WITHOUT having reached the provisioner.
//
// The absence of the provisioner's wording is the whole assertion. A build that
// normalizes before deciding answers these rows `unsupported docker image
// "sqlite"` -- proof that it recognized a docker URL, resolved an image and was
// one valid engine name away from starting a container for a value the pinned
// binary cannot parse at all.
func assertNotADockerURL(c *qt.C, stdout, stderr string, err error) {
	c.Helper()
	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Check(stderr, qt.Not(qt.Contains), compatDockerImageRefusal,
		qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
}

func compatDockerRows() []compatDockerRow {
	return []compatDockerRow{
		{
			name: "migrate diff reaches the provisioner",
			verb: "migrate diff",
			args: func(fx compatDockerFixture) []string {
				return []string{"migrate", "diff", "m2", "--dir", "file://" + fx.dir, "--to", "file://" + fx.schema}
			},
			check:           refusedByProvisioner,
			checkWhitespace: notADockerURL,
		},
		{
			name: "migrate lint reaches the provisioner",
			verb: "migrate lint",
			args: func(fx compatDockerFixture) []string {
				return []string{"migrate", "lint", "--dir", "file://" + fx.dir, "--latest", "1"}
			},
			check:           refusedByProvisioner,
			checkWhitespace: notADockerURL,
		},
		{
			// The forwarded verb. It has no RunE of its own on this surface, so
			// it proves the wiring landed in the shared replay layer and not in
			// one command body: nothing in cmd/atlas was edited for this row.
			name: "migrate validate reaches the provisioner through the replay layer",
			verb: "migrate validate",
			args: func(fx compatDockerFixture) []string {
				return []string{"migrate", "validate", "--dir", "file://" + fx.dir}
			},
			check:           refusedByProvisioner,
			checkWhitespace: notADockerURL,
		},
		{
			name: "schema inspect reaches the provisioner",
			verb: "schema inspect",
			args: func(fx compatDockerFixture) []string {
				return []string{"schema", "inspect", "-u", "file://" + fx.schema}
			},
			check:           refusedByProvisioner,
			checkWhitespace: notADockerURL,
		},
		{
			// The apply rehearsal. Its two alias checks -- is the dev database
			// the target, is it the desired state -- used to answer a docker URL
			// `unsupported database URL dialect` and refuse it before the
			// provisioner ever saw it. This row is what keeps them skipped.
			name: "schema apply rehearsal reaches the provisioner",
			verb: "schema apply",
			args: func(fx compatDockerFixture) []string {
				return []string{
					"schema", "apply", "--url", "sqlite://" + filepath.Join(fx.root, "target.db"),
					"--to", "file://" + fx.schema, "--dry-run",
				}
			},
			check:           refusedByProvisioner,
			checkWhitespace: notADockerURL,
		},
		{
			name: "schema diff from a migration directory reaches the provisioner",
			verb: "schema diff",
			args: func(fx compatDockerFixture) []string {
				return []string{"schema", "diff", "--from", "file://" + fx.dir, "--to", "file://" + fx.schema}
			},
			check:           refusedByProvisioner,
			checkWhitespace: notADockerURL,
		},
		{
			name: "schema diff between two local files never opens a dev database",
			verb: "schema diff",
			args: func(fx compatDockerFixture) []string {
				return []string{"schema", "diff", "--from", "file://" + fx.schema, "--to", "file://" + fx.schema}
			},
			check: devURLNotConsulted,
			// Unchanged: this row never opens a dev database, so the
			// spelling of the URL cannot reach anything that would.
			checkWhitespace: devURLNotConsulted,
		},
	}
}

func TestCompatDockerDevURL_ReachesTheProvisioner(t *testing.T) {
	c := qt.New(t)
	fx := newCompatDockerFixture(c)

	for _, tt := range compatDockerRows() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Chdir(c.TempDir())
			args := append(slices.Clone(tt.args(fx)), "--dev-url", "docker://sqlite/dev")
			stdout, stderr, err := runCompat(args...)
			assertDevURLOutcome(c, tt.check, stdout, stderr, err)
		})
	}
}

// TestCompatDockerDevURL_DoesNotProvisionAValueTheBinaryCannotParse drives every
// row above with the identical probe URL written with ONE LEADING SPACE.
//
// A leading space is not whitespace around a docker URL. Measured on the pinned
// community binary v1.3.0, exit statuses read from unpiped `schema inspect -u
// file://schema.sql --dev-url <value>` invocations:
//
//	<value>                        exit  what it says
//	"docker://postgres/16/dev"        0  provisions and inspects
//	" docker://postgres/16/dev"       1  sql/sqlclient: parse open url: first
//	                                     path segment in URL cannot contain colon
//
// The binary parses the second as a relative path whose first segment is
// `docker:`, so there is no scheme and no docker URL, and nothing is started.
//
// Ptah normalized it into one. `internal/devdocker` trimmed the value before
// deciding, and four of the five consumers trimmed again before handing it over,
// so ` docker://postgres/16/dev` reached the provisioner as a docker URL and
// `ptah-compat schema inspect` exited **0** with a container started, where the
// pinned binary exits 1. That is the one direction compatibility rule (a)
// forbids outright, and it was reachable on every verb wired here.
//
// The rows deliberately reuse the same `docker://sqlite/dev` probe rather than a
// valid engine: the assertion is about which LAYER answers, and the probe's
// refusal is a sentence only the provisioner produces.
func TestCompatDockerDevURL_DoesNotProvisionAValueTheBinaryCannotParse(t *testing.T) {
	c := qt.New(t)
	fx := newCompatDockerFixture(c)

	for _, tt := range compatDockerRows() {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Chdir(c.TempDir())
			args := append(slices.Clone(tt.args(fx)), "--dev-url", " docker://sqlite/dev")
			stdout, stderr, err := runCompat(args...)
			assertDevURLOutcome(c, tt.checkWhitespace, stdout, stderr, err)
		})
	}
}

// compatDockerVerbsNotWired are the compat verbs that register --dev-url and
// still hand a docker value to the database connector.
//
// None of them has a row on the pinned community binary: measured on
// 2026-08-13, `atlas migrate checkpoint`, `atlas migrate down`, `atlas migrate
// test`, `atlas schema test` and `atlas schema plan` all answer `unknown flag:
// --dev-url`, because the community version does not carry the flag there.
// There is therefore no parity claim to keep on them and no wording to copy,
// which is why they are named here rather than wired in the same change. Each
// needs the provisioner threaded through a native runner that currently takes a
// URL it opens directly; that is tracked on #844 rather than guessed at here.
func compatDockerVerbsNotWired() []string {
	return []string{
		"migrate checkpoint",
		"migrate down",
		"migrate test",
		"schema plan new",
		"schema plan validate",
		"schema test",
	}
}

// TestCompatDockerDevURL_CoversEveryVerbRegisteringTheFlag closes the fixture.
//
// The rows above are a hand-written list, and a hand-written list of what a
// surface exposes goes stale the moment a verb is added. This walks the built
// command tree instead and requires every command registering --dev-url to be
// either pinned by a row or named as not yet wired, so a thirteenth verb cannot
// land silently carrying the old refusal.
func TestCompatDockerDevURL_CoversEveryVerbRegisteringTheFlag(t *testing.T) {
	c := qt.New(t)

	registered := compatVerbsRegisteringFlag(atlas.NewCompatCommand("atlas"), "dev-url")
	c.Assert(len(registered) > 0, qt.IsTrue,
		qt.Commentf("the walk found no --dev-url at all, so it is measuring nothing"))

	accounted := slices.Clone(compatDockerVerbsNotWired())
	for _, row := range compatDockerRows() {
		accounted = append(accounted, row.verb)
	}
	slices.Sort(accounted)
	accounted = slices.Compact(accounted)

	for _, verb := range registered {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(accounted, qt.Contains, verb,
				qt.Commentf("%q registers --dev-url but no row pins how it answers a"+
					" docker:// value and it is not named as unwired", verb))
		})
	}

	for _, verb := range accounted {
		t.Run("still registered: "+verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(registered, qt.Contains, verb,
				qt.Commentf("%q is accounted for but no longer registers --dev-url", verb))
		})
	}
}
