package generator_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/generator"
)

func TestAtlasCheckpointArtifact_DirectiveIsTheFirstLine(t *testing.T) {
	c := qt.New(t)

	name, contents := generator.AtlasCheckpointArtifact(20250801000003, "snapshot", "CREATE TABLE users (id integer);")

	c.Assert(name, qt.Equals, "20250801000003_snapshot.sql")
	// The reader honors the directive only on line 1. Asserting the whole
	// prefix, not just that the directive appears somewhere, is what separates
	// a correct file from one with a provenance header pushed in front of it.
	c.Assert(strings.HasPrefix(contents, "-- atlas:checkpoint\n\n"), qt.IsTrue, qt.Commentf("contents=%q", contents))
	c.Assert(contents, qt.Equals, "-- atlas:checkpoint\n\nCREATE TABLE users (id integer);\n")
}

func TestAtlasCheckpointArtifact_LeadingBlankLinesDoNotDisplaceTheDirective(t *testing.T) {
	c := qt.New(t)

	// A body that already starts with newlines is the input that separates
	// "trim then prepend" from a naive concatenation, which would leave the
	// directive followed by two blank lines — still valid — or, if the body were
	// placed first, would silently produce a non-checkpoint.
	_, contents := generator.AtlasCheckpointArtifact(1, "x", "\n\nCREATE TABLE t (id integer);\n\n")

	c.Assert(contents, qt.Equals, "-- atlas:checkpoint\n\nCREATE TABLE t (id integer);\n")
	firstLine, _, _ := strings.Cut(contents, "\n")
	c.Assert(firstLine, qt.Equals, "-- atlas:checkpoint")
}

func TestAtlasCheckpointArtifact_NamesAndDescriptions(t *testing.T) {
	tests := []struct {
		name        string
		version     int64
		description string
		want        string
	}{
		{
			// Atlas was measured to write <version>_checkpoint.sql for an
			// unnamed checkpoint, not the bare <version>.sql that `migrate new`
			// falls back to.
			name:        "empty description falls back to checkpoint",
			version:     20250801000003,
			description: "",
			want:        "20250801000003_checkpoint.sql",
		},
		{
			name:        "spaces become hyphens",
			version:     7,
			description: "my snapshot",
			want:        "7_my-snapshot.sql",
		},
		{
			name:        "characters outside the Atlas name set are dropped",
			version:     7,
			description: "we/ird:name!",
			want:        "7_weirdname.sql",
		},
		{
			// A description that sanitizes away entirely must still not produce
			// the bare <version>.sql form.
			name:        "description that sanitizes to nothing falls back too",
			version:     7,
			description: "///",
			want:        "7_checkpoint.sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			name, _ := generator.AtlasCheckpointArtifact(tt.version, tt.description, "SELECT 1;")
			c.Assert(name, qt.Equals, tt.want)
		})
	}
}

func TestWriteAtlasCheckpointFile_WritesFileAndSum(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20250801000001_init.sql"),
		[]byte("CREATE TABLE users (id integer);\n"), 0o600), qt.IsNil)

	path, err := generator.WriteAtlasCheckpointFile(dir, 20250801000003, "snapshot", "CREATE TABLE users (id integer);")
	c.Assert(err, qt.IsNil)
	c.Assert(filepath.Base(path), qt.Equals, "20250801000003_snapshot.sql")

	// atlas.sum must cover BOTH the pre-existing migration and the checkpoint:
	// a sum over the checkpoint alone would verify against itself and still be
	// rejected by any reader of the whole directory.
	sum, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "20250801000001_init.sql")
	c.Assert(string(sum), qt.Contains, "20250801000003_snapshot.sql")

	_, err = os.Stat(filepath.Join(dir, "ptah.sum"))
	c.Assert(os.IsNotExist(err), qt.IsTrue)
}

func TestWriteAtlasCheckpointFile_RollsBackWhenTheSumCannotBeWritten(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20250801000001_init.sql"),
		[]byte("CREATE TABLE users (id integer);\n"), 0o600), qt.IsNil)
	// atlas.sum as a directory: the checkpoint file writes fine, then the
	// atomic sum replace fails. This is the only way to reach the rollback.
	c.Assert(os.Mkdir(filepath.Join(dir, "atlas.sum"), 0o755), qt.IsNil)

	_, err := generator.WriteAtlasCheckpointFile(dir, 20250801000003, "snapshot", "CREATE TABLE users (id integer);")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "checksum")

	// The checkpoint must NOT survive: a checkpoint no integrity file covers
	// makes the whole directory fail verification, and it would be applied on
	// the next run. Asserting the file is gone is the point — the error alone
	// says nothing about what was left behind.
	leftovers, globErr := filepath.Glob(filepath.Join(dir, "*_snapshot.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(leftovers, qt.HasLen, 0)
}

func TestWriteAtlasCheckpointFile_RefusesToOverwrite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	existing := filepath.Join(dir, "20250801000003_snapshot.sql")
	c.Assert(os.WriteFile(existing, []byte("-- original\n"), 0o600), qt.IsNil)

	_, err := generator.WriteAtlasCheckpointFile(dir, 20250801000003, "snapshot", "CREATE TABLE users (id integer);")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "already exists")

	// Assert the protected state, not the message: the original file must still
	// hold its own bytes.
	body, readErr := os.ReadFile(existing)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(body), qt.Equals, "-- original\n")
	// A refused write must not leave a sum behind either.
	_, statErr := os.Stat(filepath.Join(dir, "atlas.sum"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestWriteAtlasCheckpointFile_RejectsNonPositiveVersion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	_, err := generator.WriteAtlasCheckpointFile(dir, 0, "snapshot", "SELECT 1;")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "must be greater than zero")

	entries, readErr := os.ReadDir(dir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

func TestResolveAtlasCheckpointVersion_BumpsPastFutureDatedHistory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	// A directory whose newest migration is dated far in the future: a plain
	// "now()" would sort BEFORE it and produce a checkpoint that does not cover
	// the history. This is the fixture where the two candidate rules diverge.
	c.Assert(os.WriteFile(filepath.Join(dir, "29990101000000_future.sql"),
		[]byte("SELECT 1;\n"), 0o600), qt.IsNil)

	version := generator.ResolveAtlasCheckpointVersion(dir)
	c.Assert(version, qt.Equals, int64(29990101000001))
}

func TestResolveAtlasCheckpointVersion_UsesTimestampOnOrdinaryHistory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20250801000001_init.sql"),
		[]byte("SELECT 1;\n"), 0o600), qt.IsNil)

	// With a past-dated history the version is a current timestamp, not
	// "newest + 1" — that is the ptah counter, and using it here would produce
	// 20250801000002, which this bound excludes.
	version := generator.ResolveAtlasCheckpointVersion(dir)
	c.Assert(version > 20260101000000, qt.IsTrue, qt.Commentf("version=%d", version))
	// 14 digits is the Atlas timestamp width, and specifically not the 10-digit
	// ptah width that format auto-detection refuses to read as Atlas.
	c.Assert(strconv.FormatInt(version, 10), qt.HasLen, 14)
}
