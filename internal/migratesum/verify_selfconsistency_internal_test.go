package migratesum

// White-box testing required: the two refusal shapes this file separates differ by
// whether the recorded sum file hashes to its own directory line, and building
// the fixture that IS self-consistent means computing that line with the
// package's own atlasDirHash. Recomputing it in an external test would mean
// reimplementing the hash next to the code it is checking, so the fixture would
// agree with a wrong implementation just as readily as with a right one.

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
)

// TestVerifyAtlasFiles_ReorderedEntries pins both shapes a reordered atlas.sum
// can take, and the difference between them is the whole point.
//
// Measured on the pinned Atlas community binary v1.3.0 over a two-migration
// directory, `migrate validate`:
//
//   - entry lines swapped, directory line untouched -> exit 1, `checksum
//     mismatch`, and NO `L<line>` detail. The file contradicts itself, so no
//     entry can be blamed.
//   - entry lines swapped, directory line recomputed for the new order ->
//     exit 1 with `L2: <first file> was added`. The file agrees with itself and
//     disagrees with the directory, which is an entry-level answer.
//
// Ptah reported the second shape correctly and returned OK for the first
// (stokaro/ptah#1231 case 4): the diff is name-keyed, so a moved line changes
// nothing it looks at, and the hash recomputed over the DIRECTORY still equals
// the stale recorded line.
func TestVerifyAtlasFiles_ReorderedEntries(t *testing.T) {
	tests := []struct {
		name         string
		recomputeDir bool
		wantMismatch bool
	}{
		{name: "stale directory line", recomputeDir: false, wantMismatch: false},
		{name: "recomputed directory line", recomputeDir: true, wantMismatch: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			names := []string{"1_first.sql", "2_second.sql"}
			files := map[string][]byte{
				names[0]: []byte("CREATE TABLE users (id int);\n"),
				names[1]: []byte("CREATE TABLE pets (id int);\n"),
			}
			valid, err := ComputeAtlasFiles(mapFSOf(files), names)
			c.Assert(err, qt.IsNil)

			reordered := &SumFile{
				DirHash: valid.DirHash,
				Entries: []Entry{valid.Entries[1], valid.Entries[0]},
			}
			dirLines := map[bool]string{
				false: valid.DirHash,
				true:  atlasDirHash(reordered.Entries),
			}
			reordered.DirHash = dirLines[test.recomputeDir]
			files[AtlasFileName] = reordered.Bytes()

			result, err := VerifyAtlasFiles(mapFSOf(files), names)

			c.Assert(err, qt.IsNil)
			c.Assert(result.OK(), qt.IsFalse)
			c.Assert(result.DirHashMismatch, qt.IsTrue)
			c.Assert(result.FirstMismatch() != nil, qt.Equals, test.wantMismatch)
		})
	}
}

// TestVerifyAtlasFiles_UntouchedDirectory is the control for the pair above:
// asking the sum file whether it agrees with itself must not turn a directory
// written by either binary into a refusal.
func TestVerifyAtlasFiles_UntouchedDirectory(t *testing.T) {
	c := qt.New(t)
	names := []string{"1_first.sql", "2_second.sql"}
	files := map[string][]byte{
		names[0]: []byte("CREATE TABLE users (id int);\n"),
		names[1]: []byte("CREATE TABLE pets (id int);\n"),
	}
	valid, err := ComputeAtlasFiles(mapFSOf(files), names)
	c.Assert(err, qt.IsNil)
	files[AtlasFileName] = valid.Bytes()

	result, err := VerifyAtlasFiles(mapFSOf(files), names)

	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func mapFSOf(files map[string][]byte) fstest.MapFS {
	fsys := make(fstest.MapFS, len(files))
	for name, data := range files {
		fsys[name] = &fstest.MapFile{Data: data}
	}
	return fsys
}
