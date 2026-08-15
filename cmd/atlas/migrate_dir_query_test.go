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

// The desired states `migrate diff` is pointed at. The first adds a table the
// fixture directory does not have, so the verb has something to write; the
// second matches the fixture, because the row that uses it is refused before
// any comparison happens.
const (
	queryDiffTargetSQL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n" +
		"CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n"
	queryUnchangedTargetSQL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"
)

// newQueryScratchDir returns a directory for whatever a verb writes outside the
// migration directory -- a throwaway database, the desired-state file -- with
// target.sql already in it.
//
// A fresh directory per invocation is what keeps a run and its control apart:
// sharing one would let the first run's `migrate apply` leave a database the
// second run finds already migrated.
func newQueryScratchDir(c *qt.C, desired string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "target.sql"), []byte(desired), 0o600), qt.IsNil)
	return dir
}

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
		// args is the invocation against dir, with query appended to the --dir
		// URL and everything the verb writes elsewhere placed under scratch.
		// Each verb needs different companion flags, so the argument list is
		// per-row wiring rather than a branch in the body.
		args func(dir, scratch, query string) []string
	}{
		{
			name: "apply",
			args: func(dir, scratch, query string) []string {
				return []string{"migrate", "apply",
					"--dir", "file://" + dir + query,
					"--url", "sqlite://" + filepath.Join(scratch, "apply.db")}
			},
		},
		{
			name: "hash",
			args: func(dir, _, query string) []string {
				return []string{"migrate", "hash", "--dir", "file://" + dir + query}
			},
		},
		{
			name: "validate",
			args: func(dir, _, query string) []string {
				return []string{"migrate", "validate", "--dir", "file://" + dir + query}
			},
		},
		{
			name: "lint",
			args: func(dir, scratch, query string) []string {
				return []string{"migrate", "lint",
					"--dir", "file://" + dir + query,
					"--dev-url", "sqlite://" + filepath.Join(scratch, "dev.db"),
					"--latest", "1"}
			},
		},
		{
			name: "status",
			args: func(dir, scratch, query string) []string {
				return []string{"migrate", "status",
					"--dir", "file://" + dir + query,
					"--url", "sqlite://" + filepath.Join(scratch, "status.db")}
			},
		},
		{
			name: "set",
			args: func(dir, scratch, query string) []string {
				return []string{"migrate", "set", "20240101000000",
					"--dir", "file://" + dir + query,
					"--url", "sqlite://" + filepath.Join(scratch, "set.db")}
			},
		},
		// new and diff joined the table in stokaro/ptah#1086, once they ran the
		// atlas.sum gate. Until then they refused every query outright, because
		// ignoring an unrecognized key on a verb that WRITES would have relaxed
		// a refusal into a write over a directory nothing had verified. The
		// fixture is hashed, so both rows turn on the query rather than on the
		// gate.
		{
			name: "new",
			args: func(dir, _, query string) []string {
				return []string{"migrate", "new", "demo", "--dir", "file://" + dir + query}
			},
		},
		{
			name: "diff",
			args: func(dir, scratch, query string) []string {
				return []string{"migrate", "diff", "dd",
					"--dir", "file://" + dir + query,
					"--dev-url", "sqlite://" + filepath.Join(scratch, "dev.db"),
					"--to", "file://" + filepath.Join(scratch, "target.sql")}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			queryOut, withQuery := runCompatArgs(
				tt.args(writeQueryFixtureDir(c), newQueryScratchDir(c, queryDiffTargetSQL), "?nonsense=1"))
			controlOut, control := runCompatArgs(
				tt.args(writeQueryFixtureDir(c), newQueryScratchDir(c, queryDiffTargetSQL), ""))

			c.Assert(control, qt.IsNil,
				qt.Commentf("control run without a query must succeed:\n%s", controlOut))
			c.Assert(withQuery, qt.IsNil, qt.Commentf("%s", queryOut))
		})
	}
}

// WHERE THE `migrate diff` FOREIGN-FORMAT REFUSAL WENT. A
// TestCompatMigrateDirQuery_FailurePathForeignFormat used to stand here and pin
// the one verb that still refused a `?format=goose` outright. It was the guard
// on the unknown-key relaxation: once an unrecognized key is dropped, nothing
// else stops a `?format=goose` from being dropped with it and the directory
// being read as Atlas, which on a writing verb would gate the wrong covered set
// and then write.
//
// stokaro/ptah#1013 closed that cell — `migrate diff` writes the five external
// layouts now — so the refusal it pinned no longer exists. The guard it
// provided did not go with it: TestCompatMigrateDirQuery_RejectsUnknownFormatValue
// below still fires if the format KEY's value stops being read, and
// TestCompatMigrateDiff_GolangMigrateQueryWritesThatLayout in
// migrate_diff_foreign_layout_test.go fires if the value stops SELECTING the
// layout, with a control row that stays red when no layout is named.

// TestCompatMigrateDirQuery_RejectsUnknownFormatValue pins the part of the
// query that is NOT ignored: the `format` key's value.
//
// It is the guard on the relaxation above. Once an unrecognized KEY is dropped,
// nothing else stops a misspelled `?format=atals` from being dropped with it and
// the directory being read as native Atlas — which is exactly what the community
// binary does NOT do: measured on v1.3.0, `?format=totally-bogus` exits 1 with
// `unknown dir format "totally-bogus"` on every verb that reads the query.
func TestCompatMigrateDirQuery_RejectsUnknownFormatValue(t *testing.T) {
	const want = `unknown dir format "totally-bogus"`

	tests := []struct {
		name string
		args func(dir, scratch string) []string
	}{
		{
			name: "lint",
			args: func(dir, scratch string) []string {
				return []string{"migrate", "lint",
					"--dir", "file://" + dir + "?format=totally-bogus",
					"--dev-url", "sqlite://" + filepath.Join(scratch, "dev.db"),
					"--latest", "1"}
			},
		},
		{
			name: "status",
			args: func(dir, scratch string) []string {
				return []string{"migrate", "status",
					"--dir", "file://" + dir + "?format=totally-bogus",
					"--url", "sqlite://" + filepath.Join(scratch, "status.db")}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := runCompatArgs(tt.args(writeQueryFixtureDir(c), c.TempDir()))

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

// TestCompatMigrateDirQuery_NewAndDiffRefuseAnUnhashedDirectoryWithAQuery is
// what replaced TestCompatMigrateDirQuery_NewAndDiffStillRefuseAQuery when
// stokaro/ptah#1086 landed.
//
// The old test pinned a blanket refusal of any query on these two verbs. It was
// not there for the query: it was standing in for the missing atlas.sum gate,
// so that the relaxation could not be extended to a verb that WRITES before the
// gate arrived. The gate is now what refuses, so the property to keep is that
// an unhashed directory is still refused WITH the query present — the exact
// shape a symmetric relaxation would have broken, and one the two rows added to
// the ignore-unknown-keys table above cannot show, because those run on a
// hashed fixture.
//
// Measured on the pinned community binary v1.3.0: `migrate new demo --dir
// 'file://d?nonsense=1'` on a one-migration unhashed directory exits 1 with
// `Error: checksum file not found`, the same as without the query.
func TestCompatMigrateDirQuery_NewAndDiffRefuseAnUnhashedDirectoryWithAQuery(t *testing.T) {
	tests := []struct {
		name string
		args func(dir, scratch string) []string
	}{
		{
			name: "new",
			args: func(dir, _ string) []string {
				return []string{"migrate", "new", "demo", "--dir", "file://" + dir + "?nonsense=1"}
			},
		},
		{
			name: "diff",
			args: func(dir, scratch string) []string {
				return []string{"migrate", "diff", "dd",
					"--dir", "file://" + dir + "?nonsense=1",
					"--dev-url", "sqlite://" + filepath.Join(scratch, "dev.db"),
					"--to", "file://" + filepath.Join(scratch, "target.sql")}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_init.sql"),
				[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

			_, err := runCompatArgs(tt.args(dir, newQueryScratchDir(c, queryUnchangedTargetSQL)))

			c.Assert(err, qt.ErrorMatches, `checksum file not found`)
			c.Assert(atlasDirEntryNames(c, dir), qt.DeepEquals, []string{"20240101000000_init.sql"})
		})
	}
}

// atlasDirEntryNames lists the entries a migration directory holds, so a
// refusal can be asserted to have written nothing rather than only to have
// exited non-zero.
func atlasDirEntryNames(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
