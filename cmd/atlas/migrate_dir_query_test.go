package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests pin the migration directory URL query against the pinned
// community binary v1.3.0, measured through
// ptah-atlas-conformance/bin/atlas on 2026-08-03 (stokaro/ptah#1013).
//
// The community binary registers --dir on exactly eight migrate verbs — apply,
// hash, validate, lint, new, diff, status and set — and ignores an unrecognized
// query key on every one of them, reading the directory exactly as it would with
// no query at all. Ptah refused any non-empty query on all eight, through two
// different chokepoints and with two different messages.

// writeQueryFixtureDir writes a hashed native Atlas migration directory and
// returns its path. The sum is written with `migrate hash` so the directory is
// one the integrity gate accepts, which is what lets the query rows below turn
// on the query rather than on a checksum refusal.
func writeQueryFixtureDir(c *qt.C) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "20240101000000_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, _, err := runCompat("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)
	return dir
}

// TestCompatMigrateDirQuery_IgnoresUnknownKeysOnTheVerbsThatVerifyFirst closes
// stokaro/ptah#1013 section 2 on six of the eight verbs that register --dir.
//
// Six, not eight, and the name says so: `migrate new` and `migrate diff` still
// refuse every query and are pinned separately by
// TestCompatMigrateDirQuery_NewAndDiffStillRefuseAQuery. This test used to be
// called ...OnEveryVerb over the same six rows, which claimed a coverage it
// never had — the two verbs the relaxation deliberately skips were exactly the
// two a reader would have assumed were checked here.
//
// Each row runs the verb twice on identical directories, once with
// `?nonsense=1` and once without, and asserts the two runs agree. The control
// run is what makes the assertion mean something: an exit-0 assertion alone
// would also pass if the verb had started ignoring the whole --dir value, and a
// verb that is broken for an unrelated reason fails both runs instead of
// reporting a false pass.
func TestCompatMigrateDirQuery_IgnoresUnknownKeysOnTheVerbsThatVerifyFirst(t *testing.T) {
	tests := []struct {
		name string
		// run invokes the verb against dir, with query appended to the --dir
		// URL. Each verb needs different companion flags, so the invocation is
		// per-row wiring rather than a branch in the body.
		run func(c *qt.C, dir, query string) error
	}{
		{
			name: "apply",
			run: func(c *qt.C, dir, query string) error {
				_, _, err := runCompat("migrate", "apply",
					"--dir", "file://"+dir+query,
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "apply.db"))
				return err
			},
		},
		{
			name: "hash",
			run: func(_ *qt.C, dir, query string) error {
				_, _, err := runCompat("migrate", "hash", "--dir", "file://"+dir+query)
				return err
			},
		},
		{
			name: "validate",
			run: func(_ *qt.C, dir, query string) error {
				_, _, err := runCompat("migrate", "validate", "--dir", "file://"+dir+query)
				return err
			},
		},
		{
			name: "lint",
			run: func(c *qt.C, dir, query string) error {
				_, _, err := runCompat("migrate", "lint",
					"--dir", "file://"+dir+query,
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--latest", "1")
				return err
			},
		},
		{
			name: "status",
			run: func(c *qt.C, dir, query string) error {
				_, _, err := runCompat("migrate", "status",
					"--dir", "file://"+dir+query,
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))
				return err
			},
		},
		{
			name: "set",
			run: func(c *qt.C, dir, query string) error {
				_, _, err := runCompat("migrate", "set", "20240101000000",
					"--dir", "file://"+dir+query,
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "set.db"))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			withQuery := tt.run(c, writeQueryFixtureDir(c), "?nonsense=1")
			control := tt.run(c, writeQueryFixtureDir(c), "")

			c.Assert(control, qt.IsNil, qt.Commentf("control run without a query must succeed"))
			c.Assert(withQuery, qt.IsNil)
		})
	}
}

// TestCompatMigrateDirQuery_RejectsUnknownFormatValue pins the part of the
// query that is NOT ignored: the `format` key's value.
//
// It is the guard on the relaxation above. Once an unrecognized KEY is dropped,
// nothing else stops a misspelled `?format=atals` from being dropped with it and
// the directory being read as native Atlas — which is exactly what the community
// binary does NOT do: measured on v1.3.0, `?format=totally-bogus` exits 1 with
// `unknown dir format "totally-bogus"` on every verb that reads the query.
func TestCompatMigrateDirQuery_RejectsUnknownFormatValue(t *testing.T) {
	const want = `atlas migrate \w+ --dir: unknown Atlas migration directory format ` +
		`"totally-bogus": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`

	tests := []struct {
		name string
		run  func(c *qt.C, dir string) error
	}{
		{
			name: "lint",
			run: func(c *qt.C, dir string) error {
				_, _, err := runCompat("migrate", "lint",
					"--dir", "file://"+dir+"?format=totally-bogus",
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--latest", "1")
				return err
			},
		},
		{
			name: "status",
			run: func(c *qt.C, dir string) error {
				_, _, err := runCompat("migrate", "status",
					"--dir", "file://"+dir+"?format=totally-bogus",
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := tt.run(c, writeQueryFixtureDir(c))

			c.Assert(err, qt.ErrorMatches, want)
		})
	}
}

// TestCompatMigrateDirQuery_EmptyFormatValueReadsAtlasLayout pins the `?format=`
// value that names no layout at all. A PRESENT but empty value selects the
// native Atlas layout on the community binary — it does not fall through to
// `--dir-format` and it is not an unknown format — so refusing it would be a
// divergence one character wide.
func TestCompatMigrateDirQuery_EmptyFormatValueReadsAtlasLayout(t *testing.T) {
	c := qt.New(t)
	dir := writeQueryFixtureDir(c)

	_, _, err := runCompat("migrate", "status",
		"--dir", "file://"+dir+"?format=",
		"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))

	c.Assert(err, qt.IsNil)
}

// TestCompatMigrateDirQuery_NewAndDiffStillRefuseAQuery pins the two verbs the
// relaxation deliberately skips.
//
// Every verb that accepts a `--dir` query runs the atlas.sum integrity gate
// first, so a converted directory is verified before anything reads it. These
// two do not: they WRITE a migration and a fresh sum, and they do it over a
// directory nothing checked. Measured on the pinned community binary, an
// unhashed directory exits 1 there and 0 here, so accepting the query would
// turn a refusal into a write.
//
// The asymmetry is temporary and belongs to stokaro/ptah#1086, which adds the
// gate. This test is here so the relaxation cannot be extended to these verbs
// by symmetry alone, without the gate arriving with it.
func TestCompatMigrateDirQuery_NewAndDiffStillRefuseAQuery(t *testing.T) {
	tests := []struct {
		name string
		run  func(c *qt.C, dir string) error
	}{
		{
			name: "new",
			run: func(_ *qt.C, dir string) error {
				_, _, err := runCompat("migrate", "new", "demo", "--dir", "file://"+dir+"?nonsense=1")
				return err
			},
		},
		{
			name: "diff",
			run: func(c *qt.C, dir string) error {
				target := filepath.Join(c.TempDir(), "target.sql")
				c.Assert(os.WriteFile(target, []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
				_, _, err := runCompat("migrate", "diff", "dd",
					"--dir", "file://"+dir+"?nonsense=1",
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--to", "file://"+target)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_init.sql"),
				[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

			err := tt.run(c, dir)

			c.Assert(err, qt.ErrorMatches, `.*query parameters are not supported.*`)
		})
	}
}
