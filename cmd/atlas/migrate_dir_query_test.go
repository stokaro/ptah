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

// TestCompatMigrateDirQuery_IgnoresUnknownKeysOnEveryVerb closes stokaro/ptah#1013
// section 2 across all eight verbs that register --dir.
//
// Each row runs the verb twice on identical directories, once with
// `?nonsense=1` and once without, and asserts the two runs agree. The control
// run is what makes the assertion mean something: an exit-0 assertion alone
// would also pass if the verb had started ignoring the whole --dir value, and a
// verb that is broken for an unrelated reason fails both runs instead of
// reporting a false pass.
func TestCompatMigrateDirQuery_IgnoresUnknownKeysOnEveryVerb(t *testing.T) {
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
		{
			name: "new",
			run: func(_ *qt.C, dir, query string) error {
				_, _, err := runCompat("migrate", "new", "demo", "--dir", "file://"+dir+query)
				return err
			},
		},
		{
			name: "diff",
			run: func(c *qt.C, dir, query string) error {
				target := filepath.Join(c.TempDir(), "target.sql")
				c.Assert(os.WriteFile(target, []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
				_, _, err := runCompat("migrate", "diff", "dd",
					"--dir", "file://"+dir+query,
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--to", "file://"+target)
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

// TestCompatMigrateDirQuery_FailurePathForeignFormat pins the part of the query
// that is NOT ignored on the five verbs that read only a native Atlas
// directory.
//
// The community binary honors `?format=` on these verbs; Ptah does not convert
// (lint, status, set) or write (new, diff) a foreign layout there yet, so it
// refuses rather than reading the directory under the wrong layout — the strict
// side of the divergence. stokaro/ptah#1013 section 1 and stokaro/ptah#1002
// track closing it.
//
// The refusal has to survive the relaxation above, which is why it is pinned:
// once unknown keys are ignored, nothing else stops a `?format=goose` from
// being ignored too and the directory being silently read as Atlas.
func TestCompatMigrateDirQuery_FailurePathForeignFormat(t *testing.T) {
	const want = `atlas migrate \w+ --dir: Atlas accepts \?format=goose, ` +
		`but Ptah does not implement that directory format for this command yet`

	tests := []struct {
		name string
		run  func(c *qt.C, dir string) error
	}{
		{
			name: "lint",
			run: func(c *qt.C, dir string) error {
				_, _, err := runCompat("migrate", "lint",
					"--dir", "file://"+dir+"?format=goose",
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--latest", "1")
				return err
			},
		},
		{
			name: "status",
			run: func(c *qt.C, dir string) error {
				_, _, err := runCompat("migrate", "status",
					"--dir", "file://"+dir+"?format=goose",
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))
				return err
			},
		},
		{
			name: "set",
			run: func(c *qt.C, dir string) error {
				_, _, err := runCompat("migrate", "set", "20240101000000",
					"--dir", "file://"+dir+"?format=goose",
					"--url", "sqlite://"+filepath.Join(c.TempDir(), "set.db"))
				return err
			},
		},
		{
			name: "new",
			run: func(_ *qt.C, dir string) error {
				_, _, err := runCompat("migrate", "new", "demo", "--dir", "file://"+dir+"?format=goose")
				return err
			},
		},
		{
			name: "diff",
			run: func(c *qt.C, dir string) error {
				target := filepath.Join(c.TempDir(), "target.sql")
				c.Assert(os.WriteFile(target, []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
				_, _, err := runCompat("migrate", "diff", "dd",
					"--dir", "file://"+dir+"?format=goose",
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--to", "file://"+target)
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

// TestCompatMigrateDirQuery_EmptyFormatValueReadsAtlasLayout pins the one
// `?format=` value the five verbs above still accept. An empty value selects
// the native Atlas layout on the community binary, so refusing it would turn
// the relaxation into a new divergence one character wide.
func TestCompatMigrateDirQuery_EmptyFormatValueReadsAtlasLayout(t *testing.T) {
	c := qt.New(t)
	dir := writeQueryFixtureDir(c)

	_, _, err := runCompat("migrate", "status",
		"--dir", "file://"+dir+"?format=",
		"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))

	c.Assert(err, qt.IsNil)
}
