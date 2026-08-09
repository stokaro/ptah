package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests pin stokaro/ptah#1002 on `ptah-compat migrate status` and
// `migrate set`: a migration directory laid out in a foreign tool's convention
// is read through both spellings that select it, so a directory converted on
// apply has a route to inspect and repair its revision table.
//
// Before this change both verbs refused every foreign layout, under both
// spellings, with `Ptah does not implement that directory format ... yet` —
// which is what every assertion below would print again if the change were
// reverted, except where a row names something else.
//
// Measured against the pinned community binary v1.3.0 on the same fixture: a
// hashed Flyway directory holding V1__first.sql and V2__second.sql reports
// `Migration Status: PENDING` with two pending files on status, and
// `Current version is 1 (1 set)` on `migrate set 1`, both exit 0.

const (
	revisionConvertedFirst  = "V1__first.sql"
	revisionConvertedSecond = "V2__second.sql"
	// revisionConvertedFirstToken is the version the FIRST file spells: what
	// Flyway calls it, what the pinned community binary v1.3.0 calls it, and
	// since stokaro/ptah#1206 what `migrate set` takes here.
	revisionConvertedFirstToken = "1"
)

// writeConvertedFlywayDir writes a two-migration Flyway directory and no
// atlas.sum.
func writeConvertedFlywayDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	writeAtlasApplyProjectMigration(c, dir, revisionConvertedFirst,
		"CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c, dir, revisionConvertedSecond,
		"CREATE TABLE t2 (id INTEGER PRIMARY KEY);\n")
	return dir
}

// hashConvertedFlywayDir writes the atlas.sum the community binary writes for
// the Flyway layout, over the file set that layout covers. It goes through
// `migrate hash` rather than a helper so the sum these tests gate against is
// the one the shipped verb produces (#984, #992).
func hashConvertedFlywayDir(c *qt.C, dir string) {
	c.Helper()
	_, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("hash stderr: %s", stderr))
}

// revisionDBURL returns a URL for a database file that does not exist yet, so
// each row starts from an empty revision table.
func revisionDBURL(c *qt.C) string {
	c.Helper()
	return "sqlite://" + filepath.Join(c.TempDir(), "revisions.db")
}

// TestCompatMigrateStatus_ConvertedDirIsRead is the discriminator for the
// status half of #1002: both spellings that select a foreign layout now report
// on it instead of refusing it.
func TestCompatMigrateStatus_ConvertedDirIsRead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args func(dir string) []string
	}{
		{
			name: "dir_query",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir + "?format=flyway"}
			},
		},
		{
			name: "dir_format_flag",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir, "--dir-format", "flyway"}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeConvertedFlywayDir(c)
			hashConvertedFlywayDir(c, dir)
			args := append([]string{"migrate", "status"}, test.args(dir)...)
			stdout, stderr, err := runCompatExit(append(args, "--url", revisionDBURL(c))...)
			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stdout, qt.Contains, "  -- Pending Files:   2\n")
			c.Assert(stdout, qt.Contains, "  -- Executed Files:  0\n")
		})
	}
}

// TestCompatMigrateStatus_ConvertedDirRefusesDrift pins the ORDER of the two
// changes, not just their sum. Integrity is verified on the source directory,
// over the file set atlas.sum covers for the Flyway layout, BEFORE the layout
// is converted.
//
// Reverting only the gate — converting first and gating the rebuilt filesystem
// — would print `checksum file not found` for both rows, because a converted
// directory carries no atlas.sum by construction. The edited row is what
// separates the two: it must report a mismatch against a sum that exists.
func TestCompatMigrateStatus_ConvertedDirRefusesDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		prepare func(c *qt.C, dir string)
		want    string
	}{
		{
			name:    "unhashed",
			prepare: func(*qt.C, string) {},
			want:    atlasChecksumNotFoundErr,
		},
		{
			name: "edited_after_hashing",
			prepare: func(c *qt.C, dir string) {
				hashConvertedFlywayDir(c, dir)
				writeAtlasApplyProjectMigration(c, dir, revisionConvertedFirst,
					"CREATE TABLE t1 (id INTEGER PRIMARY KEY, extra TEXT);\n")
			},
			want: atlasChecksumMismatchErr,
		},
		{
			name: "covered_file_removed_after_hashing",
			prepare: func(c *qt.C, dir string) {
				hashConvertedFlywayDir(c, dir)
				c.Assert(os.Remove(filepath.Join(dir, revisionConvertedSecond)), qt.IsNil)
			},
			want: atlasChecksumMismatchErr,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeConvertedFlywayDir(c)
			test.prepare(c, dir)
			_, stderr, err := runCompatExit(
				"migrate", "status",
				"--dir", "file://"+dir+"?format=flyway",
				"--url", revisionDBURL(c),
			)
			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Equals, test.want)
		})
	}
}

// TestCompatMigrateSet_ConvertedDirWritesConvertedVersion is the discriminator
// for the set half of #1002, and for what `migrate set` writes: the revision it
// records is the version the SAME directory would be applied under, so a status
// run afterwards reports the first migration executed and the second pending.
//
// Asserting the status that follows is the point. A `migrate set` that merely
// stopped refusing the layout, and recorded a version no converted file
// carries, would still exit 0 here — and would then report two pending
// migrations rather than one.
//
// The operand is the Flyway version TOKEN since stokaro/ptah#1206. It used to be
// the int64 ordering key the token converts to, read back out of the importer;
// that spelling was the only one this build accepted and the only one the
// pinned community binary v1.3.0 refuses, so it is retired here rather than
// carried alongside. The convertedFlywayVersions helper this test used to call
// went with it — nothing else consulted the projection.
func TestCompatMigrateSet_ConvertedDirWritesConvertedVersion(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeConvertedFlywayDir(c)
	hashConvertedFlywayDir(c, dir)
	url := revisionDBURL(c)

	stdout, stderr, err := runCompatExit(
		"migrate", "set", revisionConvertedFirstToken,
		"--dir", "file://"+dir+"?format=flyway",
		"--url", url,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, "Current version is "+revisionConvertedFirstToken+" (1 set)")

	stdout, stderr, err = runCompatExit(
		"migrate", "status",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", url,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, "  -- Executed Files:  1\n")
	c.Assert(stdout, qt.Contains, "  -- Pending Files:   1\n")
}

// TestCompatMigrateRevisionVerbs_DirFormatIsVerbatim pins the case rule these
// two verbs now share with `migrate apply`.
//
// The community binary matches the format value verbatim: measured on v1.3.0,
// `--dir-format ATLAS`, `--dir-format ' atlas '` and `?format=FLYWAY` each exit
// 1 with `unknown dir format`, while the lowercase spellings exit 0. Status and
// set used to lower-and-trim the flag, so the first two exited 0 — looser than
// the binary being mirrored, which is the direction that must never happen.
//
// Reverting the change makes the two `ATLAS` rows and the two ` atlas ` rows
// exit 0. The `?format=FLYWAY` row held before it too — the query spelling was
// already resolved verbatim — and is here so the two spellings stay pinned to
// one rule rather than drifting apart again.
func TestCompatMigrateRevisionVerbs_DirFormatIsVerbatim(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		verb string
		args func(dir string) []string
	}{
		{
			name: "status_uppercase_atlas_flag",
			verb: "status",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir, "--dir-format", "ATLAS"}
			},
		},
		{
			name: "status_padded_atlas_flag",
			verb: "status",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir, "--dir-format", " atlas "}
			},
		},
		{
			name: "status_uppercase_flyway_query",
			verb: "status",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir + "?format=FLYWAY"}
			},
		},
		{
			name: "set_uppercase_atlas_flag",
			verb: "set",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir, "--dir-format", "ATLAS"}
			},
		},
		{
			name: "set_padded_atlas_flag",
			verb: "set",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir, "--dir-format", " atlas "}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeConvertedFlywayDir(c)
			hashConvertedFlywayDir(c, dir)
			args := append([]string{"migrate", test.verb}, test.args(dir)...)
			args = append(args, "--url", revisionDBURL(c))
			_, stderr, err := runCompatExit(append(args, revisionSetVersionArg(test.verb)...)...)
			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Contains, "unknown Atlas migration directory format")
		})
	}
}

// revisionSetVersionArg supplies the positional version `migrate set` requires
// and `migrate status` refuses, so one table can drive both verbs without a
// branch in the test body.
func revisionSetVersionArg(verb string) []string {
	return map[string][]string{"set": {"1"}}[verb]
}
