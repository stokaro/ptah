package migratesum_test

import (
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migratesum"
)

func fixture(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	return fsys
}

func TestCompute_HashesOnlyMigrationFilesSortedByName(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000002_add.up.sql":      "ALTER TABLE t ADD COLUMN a INT;\n",
		"0000000002_add.down.sql":    "ALTER TABLE t DROP COLUMN a;\n",
		"0000000001_init.up.sql":     "CREATE TABLE t (id INT);\n",
		"0000000001_init.down.sql":   "DROP TABLE t;\n",
		"README.md":                  "not a migration\n",
		"ptah.sum":                   "stale\n",
		"0000000003_bad_name.txt":    "ignored\n",
		"nested/0000000004_x.up.sql": "CREATE TABLE u (id INT);\n",
	})

	sum, err := migratesum.Compute(fsys)
	c.Assert(err, qt.IsNil)

	var names []string
	for _, e := range sum.Entries {
		names = append(names, e.Name)
		c.Assert(e.Hash, qt.Matches, "h1:.+")
	}
	c.Assert(names, qt.DeepEquals, []string{
		"0000000001_init.down.sql",
		"0000000001_init.up.sql",
		"0000000002_add.down.sql",
		"0000000002_add.up.sql",
		"nested/0000000004_x.up.sql",
	}, qt.Commentf("only recognized migration files, sorted; README/ptah.sum/.txt excluded"))
	c.Assert(sum.DirHash, qt.Matches, "h1:.+")
}

func TestCompute_IsDeterministicAndContentSensitive(t *testing.T) {
	c := qt.New(t)

	base := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE t (id INT);\n",
		"0000000001_init.down.sql": "DROP TABLE t;\n",
	}
	a, err := migratesum.Compute(fixture(base))
	c.Assert(err, qt.IsNil)
	b, err := migratesum.Compute(fixture(base))
	c.Assert(err, qt.IsNil)
	c.Assert(a.DirHash, qt.Equals, b.DirHash)
	c.Assert(a.Bytes(), qt.DeepEquals, b.Bytes())

	changed := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE t (id BIGINT);\n", // one byte differs
		"0000000001_init.down.sql": "DROP TABLE t;\n",
	}
	d, err := migratesum.Compute(fixture(changed))
	c.Assert(err, qt.IsNil)
	c.Assert(d.DirHash, qt.Not(qt.Equals), a.DirHash)
	c.Assert(d.Entries[1].Hash, qt.Not(qt.Equals), a.Entries[1].Hash,
		qt.Commentf("the up file hash reflects its content"))
}

func TestBytesParseRoundTrip(t *testing.T) {
	c := qt.New(t)

	sum, err := migratesum.Compute(fixture(map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE t (id INT);\n",
		"0000000001_init.down.sql": "DROP TABLE t;\n",
	}))
	c.Assert(err, qt.IsNil)

	parsed, err := migratesum.Parse(sum.Bytes())
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.DirHash, qt.Equals, sum.DirHash)
	c.Assert(parsed.Entries, qt.DeepEquals, sum.Entries)
}

func TestParse_Malformed(t *testing.T) {
	c := qt.New(t)

	_, err := migratesum.Parse([]byte(""))
	c.Assert(err, qt.ErrorMatches, "empty or missing directory hash line")

	_, err = migratesum.Parse([]byte("not-a-hash\n0000000001_x.up.sql h1:abc\n"))
	c.Assert(err, qt.ErrorMatches, "malformed directory hash line.*")

	// No space between name and hash.
	_, err = migratesum.Parse([]byte("h1:dir\nnohashhere\n"))
	c.Assert(err, qt.ErrorMatches, "malformed entry line.*")

	// A space is present (so the field split succeeds) but the trailing
	// token is not an h1: hash — this pins the hash-prefix validation.
	_, err = migratesum.Parse([]byte("h1:dir\n0000000001_x.up.sql garbagehash\n"))
	c.Assert(err, qt.ErrorMatches, "malformed entry line.*")
}

func TestBytesParseRoundTrip_NameWithSpaces(t *testing.T) {
	c := qt.New(t)

	// The migrator's name regex allows any character in the description, so
	// "0000000002_add user.up.sql" is a valid, runnable migration. Its name
	// contains a space, which is also the sum-file field separator: the
	// round-trip must recover the full name.
	sum, err := migratesum.Compute(fixture(map[string]string{
		"0000000002_add user.up.sql":   "ALTER TABLE t ADD COLUMN u INT;\n",
		"0000000002_add user.down.sql": "ALTER TABLE t DROP COLUMN u;\n",
	}))
	c.Assert(err, qt.IsNil)

	parsed, err := migratesum.Parse(sum.Bytes())
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Entries, qt.DeepEquals, sum.Entries)
	c.Assert(parsed.Entries[0].Name, qt.Equals, "0000000002_add user.down.sql")
}

func TestParse_ToleratesCRLFLineEndings(t *testing.T) {
	c := qt.New(t)

	files := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE t (id INT);\n",
		"0000000001_init.down.sql": "DROP TABLE t;\n",
	}
	fsys := fixture(files)
	sum, err := migratesum.Compute(fsys)
	c.Assert(err, qt.IsNil)

	// A git checkout with autocrlf, or editing ptah.sum on Windows, yields
	// CRLF line endings. That must not be read as drift: the parsed hashes
	// must match, so a clean directory still verifies clean.
	crlf := strings.ReplaceAll(string(sum.Bytes()), "\n", "\r\n")
	parsed, err := migratesum.Parse([]byte(crlf))
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.DirHash, qt.Equals, sum.DirHash)
	c.Assert(parsed.Entries, qt.DeepEquals, sum.Entries)

	fsys["ptah.sum"] = &fstest.MapFile{Data: []byte(crlf)}
	res, err := migratesum.Verify(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue, qt.Commentf("CRLF ptah.sum on an unchanged dir must not report drift; got %s", res.Describe()))
}

func TestVerify_CleanAddedRemovedChanged(t *testing.T) {
	c := qt.New(t)

	files := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE t (id INT);\n",
		"0000000001_init.down.sql": "DROP TABLE t;\n",
	}
	fsys := fixture(files)
	sum, err := migratesum.Compute(fsys)
	c.Assert(err, qt.IsNil)
	fsys["ptah.sum"] = &fstest.MapFile{Data: sum.Bytes()}

	// Clean.
	res, err := migratesum.Verify(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue)

	// Changed: edit a file's content.
	fsys["0000000001_init.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE t (id BIGINT);\n")}
	res, err = migratesum.Verify(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsFalse)
	c.Assert(res.Changed, qt.DeepEquals, []string{"0000000001_init.up.sql"})
	c.Assert(res.Describe(), qt.Contains, "changed: 0000000001_init.up.sql")

	// Added: a new migration not yet hashed.
	fsys["0000000001_init.up.sql"] = &fstest.MapFile{Data: []byte(files["0000000001_init.up.sql"])}
	fsys["0000000002_more.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE u (id INT);\n")}
	res, err = migratesum.Verify(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(res.Added, qt.DeepEquals, []string{"0000000002_more.up.sql"})

	// Removed: delete a hashed file.
	delete(fsys, "0000000002_more.up.sql")
	delete(fsys, "0000000001_init.down.sql")
	res, err = migratesum.Verify(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(res.Removed, qt.DeepEquals, []string{"0000000001_init.down.sql"})
}

func TestVerify_MissingSumFile(t *testing.T) {
	c := qt.New(t)

	_, err := migratesum.Verify(fixture(map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE t (id INT);\n",
		"0000000001_init.down.sql": "DROP TABLE t;\n",
	}))
	c.Assert(err, qt.ErrorIs, migratesum.ErrSumFileMissing)
}

func TestVerify_HandEditedSumFileDirHashMismatch(t *testing.T) {
	c := qt.New(t)

	files := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE t (id INT);\n",
		"0000000001_init.down.sql": "DROP TABLE t;\n",
	}
	fsys := fixture(files)
	sum, err := migratesum.Compute(fsys)
	c.Assert(err, qt.IsNil)

	// Corrupt only the directory-hash line; the per-file entries still match
	// their files, so this is detectable only via the dir hash.
	tampered := append([]byte("h1:AAAAtampered\n"), sum.Bytes()[len(sum.DirHash)+1:]...)
	fsys["ptah.sum"] = &fstest.MapFile{Data: tampered}

	res, err := migratesum.Verify(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsFalse)
	c.Assert(res.DirHashMismatch, qt.IsTrue)
	c.Assert(res.Describe(), qt.Contains, "directory hash mismatch")
}
