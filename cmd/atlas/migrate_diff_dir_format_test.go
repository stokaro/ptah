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
	// The rejected value is quoted verbatim in the community binary's own
	// wording, which is what this surface now prints; the semantic diagnostic
	// stays reachable through the chain. See migrate_dir_format_error.go.
	const want = `unknown dir format "[^"]*"`

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
//
// The REASON those two exit 1 changed with stokaro/ptah#1013, while the exit
// code did not. `--dir-format golang-migrate` used to be refused as a layout
// this verb could not write; it is now honored, and the refusal comes from the
// gate instead — read as golang-migrate this hashed Atlas directory covers no
// file, so its atlas.sum entry reads as removed. Measured on the pinned
// community binary v1.3.0 on 2026-08-08, that binary answers the same two rows
// with `checksum mismatch` and `L2: …_init.sql was removed`, which is the
// message below.
func TestCompatMigrateDiff_DirQueryFormatOutranksDirFormatFlag(t *testing.T) {
	const refused = `checksum mismatch`

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

// TestCompatMigrateDiff_QueryFormatDecidesHowTheDirIsRead pins what `?format=`
// does to a NATIVE Atlas directory on this verb, across the whole format axis.
//
// It used to pin the opposite — every foreign value refused with one message —
// and that refusal is what stokaro/ptah#1013 closed. What replaces it is not
// "all six accepted": the layout decides how the existing directory is READ, so
// on a directory whose files are Atlas-shaped the answer differs per layout, and
// each row below was measured on the pinned community binary v1.3.0 on
// 2026-08-08 against a PostgreSQL dev database, on this fixture's shape.
//
//	?format=          community        ptah-compat
//	golang-migrate    1, mismatch      1, mismatch      covered set is *.up.sql: none, so atlas.sum's entry is "removed"
//	flyway            1, mismatch      1, mismatch      same, for V…__… names
//	goose             0, WROTE         0, WROTE         covered set is *.sql, and the file parses as one Goose migration
//	liquibase         0, WROTE         0, WROTE         same, as one Liquibase changelog
//	dbmate            0, WROTE         1, refused       see below — the one deliberate divergence
//	atlas             0, WROTE         0, WROTE         the control
//
// The dbmate row is the strict direction and is deliberate. The community
// binary reads a file carrying no `-- migrate:up` as a migration with NO up
// SQL: measured, it then plans BOTH tables — the one the existing migration
// already creates and the new one — so the migration in the directory is
// silently dropped from the replay. Ptah's dbmate reader refuses that rather
// than record a migration that can never be re-run, which is older than this
// change and shared with `migrate apply`, `lint` and `new`. Reproducing it here
// would be reproducing a defect on the verb that WRITES the file.
//
// The `?format=atlas` control is what shows the per-layout answers turn on the
// layout and not on the query's presence.
func TestCompatMigrateDiff_QueryFormatDecidesHowTheDirIsRead(t *testing.T) {
	tests := []struct {
		name   string
		format string
		check  func(c *qt.C, err error, wrote bool)
	}{
		{
			name:   "golang-migrate covers no file here",
			format: "golang-migrate",
			check:  refusedWithoutWriting(`checksum mismatch`),
		},
		{
			name:   "flyway covers no file here",
			format: "flyway",
			check:  refusedWithoutWriting(`checksum mismatch`),
		},
		{
			name:   "goose reads the file and writes",
			format: "goose",
			check:  acceptedAndWrote,
		},
		{
			name:   "liquibase reads the file and writes",
			format: "liquibase",
			check:  acceptedAndWrote,
		},
		{
			name:   "dbmate refuses a file its directives do not cover",
			format: "dbmate",
			check: refusedWithoutWriting(
				`read migration directory as dbmate: migration file [^ ]+ carries no "-- migrate:up" directive.*`,
			),
		},
		{
			name:   "atlas",
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
