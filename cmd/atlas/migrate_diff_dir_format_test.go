package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests pin how `atlas migrate diff` reads the two spellings that select
// a migration directory's layout — the `--dir` URL query and `--dir-format` —
// against the pinned community binary v1.3.0, measured through
// ptah-atlas-conformance/bin/atlas on 2026-08-07 (stokaro/ptah#1013).
//
// `migrate diff` is the one compat verb that both WRITES into the directory and
// registers `--dir-format` as its own cobra flag rather than going through
// resolveAtlasMigrateSource. It carried its own lenient reading of that flag —
// lowercased and trimmed — while every other verb parsed the value verbatim,
// and the two divergences below are what that produced.

// compatDiffFixtureDir writes a hashed native Atlas migration directory holding
// one migration, and returns its path together with a desired-state .sql file
// that adds a second table.
//
// The sum is written with `migrate hash`, so the atlas.sum gate this verb runs
// before anything else passes and each row below turns on the layout selection
// rather than on a checksum refusal.
func compatDiffFixtureDir(c *qt.C) (dir, target string) {
	c.Helper()
	dir = filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "20240101000000_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, _, err := runCompat("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)

	target = filepath.Join(c.TempDir(), "target.sql")
	c.Assert(os.WriteFile(
		target,
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	return dir, target
}

// runCompatDiff runs `migrate diff` against a fresh fixture and reports both the
// error and whether the directory grew a file.
//
// The written half is what makes an exit-code assertion mean something on a
// verb that mutates: a refusal that still wrote is not a refusal, and an
// acceptance that wrote nothing is not the acceptance the community binary
// makes. Every row below asserts both.
func runCompatDiff(c *qt.C, query string, dirFormat []string) (wrote bool, err error) {
	c.Helper()
	dir, target := compatDiffFixtureDir(c)
	before := len(atlasDirEntryNames(c, dir))
	args := []string{
		"migrate", "diff", "dd",
		"--dir", "file://" + dir + query,
		"--dev-url", "sqlite://" + filepath.Join(c.TempDir(), "dev.db"),
		"--to", "file://" + target,
	}
	args = append(args, dirFormat...)
	_, _, err = runCompat(args...)
	return len(atlasDirEntryNames(c, dir)) > before, err
}

// TestCompatMigrateDiff_DirFormatValueIsParsedVerbatim closes the forbidden
// direction on this verb: exit 0 where the community binary exits 1, on the one
// compat verb that mutates the directory.
//
// Only the verbatim value `atlas` selects the native layout on the community
// binary. `migrate diff` used to lowercase and trim its `--dir-format` first, so
// three spellings the community binary refuses were coerced into a match and a
// migration file was written. Measured on the pinned v1.3.0, on the hashed
// fixture above:
//
//	--dir-format ATLAS       CE 1, `unknown dir format "ATLAS"`   ptah 0, WROTE
//	--dir-format Atlas       CE 1                                 ptah 0, WROTE
//	--dir-format ' atlas '   CE 1                                 ptah 0, WROTE
//	--dir-format atlas       CE 0, WROTE                          ptah 0, WROTE
//
// The last row is the control, and it is what keeps this from passing for the
// wrong reason: a `migrate diff` broken for an unrelated reason fails it too,
// rather than reporting three green refusals.
func TestCompatMigrateDiff_DirFormatValueIsParsedVerbatim(t *testing.T) {
	const want = `atlas migrate diff --dir-format: unknown Atlas migration directory format ` +
		`"[^"]*": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`

	tests := []struct {
		name  string
		value string
		// check asserts the outcome for this row. It carries the assertion
		// because the accepted spelling and the refused ones assert opposite
		// things about both the error and the directory.
		check func(c *qt.C, err error, wrote bool)
	}{
		{
			name:  "uppercase",
			value: "ATLAS",
			check: refusedWithoutWriting(want),
		},
		{
			name:  "capitalized",
			value: "Atlas",
			check: refusedWithoutWriting(want),
		},
		{
			name:  "padded",
			value: " atlas ",
			check: refusedWithoutWriting(want),
		},
		{
			// Not an Atlas directory format at all. Ptah's own native
			// `migrations checkpoint` accepts this spelling; the community
			// binary answers `unknown dir format "ptah"`, so the compat surface
			// must too.
			name:  "native ptah layout",
			value: "ptah",
			check: refusedWithoutWriting(want),
		},
		{
			name:  "verbatim atlas",
			value: "atlas",
			check: acceptedAndWrote,
		},
		{
			// An empty value selects the native Atlas layout on both binaries.
			name:  "empty",
			value: "",
			check: acceptedAndWrote,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			wrote, err := runCompatDiff(c, "", []string{"--dir-format", tt.value})

			tt.check(c, err, wrote)
		})
	}
}

// TestCompatMigrateDiff_DirQueryFormatOutranksDirFormatFlag pins the precedence
// the definition of done names: the `?format=` query wins over `--dir-format`.
//
// Measured on the pinned community binary v1.3.0, on the hashed fixture above:
//
//	?format=atlas  --dir-format golang-migrate   CE 0, WROTE   ptah 1, wrote nothing
//	?format=       --dir-format golang-migrate   CE 0, WROTE   ptah 1, wrote nothing
//	?format=atlas  --dir-format goose            CE 0, WROTE   ptah 1, wrote nothing
//	?nonsense=1    --dir-format golang-migrate   CE 1          ptah 1
//	(no query)     --dir-format golang-migrate   CE 1          ptah 1
//
// The last two rows are the controls, and they are the point. Without them a
// green result would also be produced by "any query at all disables
// --dir-format", which is a different and wrong rule: an unrecognized key
// selects no layout, so the flag still decides and its refusal stands.
func TestCompatMigrateDiff_DirQueryFormatOutranksDirFormatFlag(t *testing.T) {
	const refused = `atlas migrate diff currently writes Atlas-format migration directories only`

	tests := []struct {
		name      string
		query     string
		dirFormat string
		check     func(c *qt.C, err error, wrote bool)
	}{
		{
			name:      "query names atlas over a foreign flag",
			query:     "?format=atlas",
			dirFormat: "golang-migrate",
			check:     acceptedAndWrote,
		},
		{
			name:      "empty query value names atlas over a foreign flag",
			query:     "?format=",
			dirFormat: "golang-migrate",
			check:     acceptedAndWrote,
		},
		{
			name:      "query names atlas over a second foreign flag",
			query:     "?format=atlas",
			dirFormat: "goose",
			check:     acceptedAndWrote,
		},
		{
			name:      "ignored key leaves the flag deciding",
			query:     "?nonsense=1",
			dirFormat: "golang-migrate",
			check:     refusedWithoutWriting(refused),
		},
		{
			name:      "no query leaves the flag deciding",
			query:     "",
			dirFormat: "golang-migrate",
			check:     refusedWithoutWriting(refused),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			wrote, err := runCompatDiff(c, tt.query, []string{"--dir-format", tt.dirFormat})

			tt.check(c, err, wrote)
		})
	}
}

// TestCompatMigrateDiff_ForeignQueryFormatStaysRefusedOnEveryLayout pins the one
// cell of stokaro/ptah#1013 that is still open, across the whole format axis
// rather than on the single layout the issue names.
//
// Measured on the pinned community binary v1.3.0 on 2026-08-07, running
// `migrate diff` into an empty directory once per layout, the binary exits 0 and
// writes REVERSE SQL as well as forward SQL in every one of them — a second file
// for golang-migrate (`.down.sql`) and flyway (`U…__….sql`), a directive section
// in the same file for goose (`-- +goose Down`) and dbmate (`-- migrate:down`),
// and one `--rollback:` line per forward statement for liquibase. Ptah's
// `migrate diff` plans forward statements only, so honoring the query would
// write a directory whose rollback half is missing or empty. Refusing is the
// strict side and never exits 0 where the community binary exits 1.
//
// All five rows are here because the layouts do not close together: the paired
// ones are the cheap half, and a change that taught this verb golang-migrate
// alone would leave the other four silently refusing while a single-layout test
// stayed green. The `?format=atlas` control is what shows the refusal turns on
// the layout and not on the query's presence.
func TestCompatMigrateDiff_ForeignQueryFormatStaysRefusedOnEveryLayout(t *testing.T) {
	tests := []struct {
		name   string
		format string
		check  func(c *qt.C, err error, wrote bool)
	}{
		{
			name:   "golang-migrate",
			format: "golang-migrate",
			check:  refusedWithoutWriting(compatDiffForeignFormatRefusal("golang-migrate")),
		},
		{
			name:   "goose",
			format: "goose",
			check:  refusedWithoutWriting(compatDiffForeignFormatRefusal("goose")),
		},
		{
			name:   "flyway",
			format: "flyway",
			check:  refusedWithoutWriting(compatDiffForeignFormatRefusal("flyway")),
		},
		{
			name:   "liquibase",
			format: "liquibase",
			check:  refusedWithoutWriting(compatDiffForeignFormatRefusal("liquibase")),
		},
		{
			name:   "dbmate",
			format: "dbmate",
			check:  refusedWithoutWriting(compatDiffForeignFormatRefusal("dbmate")),
		},
		{
			name:   "atlas is not refused",
			format: "atlas",
			check:  acceptedAndWrote,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			wrote, err := runCompatDiff(c, "?format="+tt.format, nil)

			tt.check(c, err, wrote)
		})
	}
}

func compatDiffForeignFormatRefusal(format string) string {
	return `atlas migrate diff --dir: Atlas accepts \?format=` + format +
		`, but Ptah does not implement that directory format for this command yet`
}

// refusedWithoutWriting builds the assertion for a row the compat surface must
// refuse: the error matches, and the migration directory is untouched.
func refusedWithoutWriting(want string) func(c *qt.C, err error, wrote bool) {
	return func(c *qt.C, err error, wrote bool) {
		c.Assert(err, qt.ErrorMatches, want)
		c.Assert(wrote, qt.IsFalse, qt.Commentf("a refused run must write nothing"))
	}
}

// acceptedAndWrote is the assertion for a row the compat surface must accept.
func acceptedAndWrote(c *qt.C, err error, wrote bool) {
	c.Assert(err, qt.IsNil)
	c.Assert(wrote, qt.IsTrue, qt.Commentf("an accepted run must write the migration"))
}
