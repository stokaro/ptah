package migratesum_test

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
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

func TestComputeWithFormat_HashesAtlasMigrationFiles(t *testing.T) {
	c := qt.New(t)

	sum, err := migratesum.ComputeWithFormat(fixture(map[string]string{
		"20220318104615_add_users.sql": "CREATE TABLE users (id INT);\n",
		"20220318104614_team_A.sql":    "CREATE TABLE teams (id INT);\n",
		"atlas.sum":                    "ignored\n",
	}), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	var names []string
	for _, entry := range sum.Entries {
		names = append(names, entry.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{
		"20220318104614_team_A.sql",
		"20220318104615_add_users.sql",
	})
}

func TestComputeWithFormat_AtlasGoldenBytes(t *testing.T) {
	c := qt.New(t)

	sum, err := migratesum.ComputeWithFormat(fixture(map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE users (id SERIAL PRIMARY KEY);\n",
		"0000000001_init.down.sql": "DROP TABLE users;\n",
	}), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	const expected = `h1:dV4b2tjr5jLyPdgrKp+m/NTaWTKMVgV80o5ps0Ew/GE=
0000000001_init.down.sql h1:b4afj7upDcaQPh2KA7KdMZl7PEqNfrt1lEui6Gw5lHA=
0000000001_init.up.sql h1:5pdFXxDzI9YASZoApjzSlatn7yMRKqTZ/pSe714fz0w=
`
	c.Assert(string(sum.Bytes()), qt.Equals, expected)
}

func TestComputeWithFormat_AtlasHashIsChained(t *testing.T) {
	c := qt.New(t)

	base := fixture(map[string]string{
		"1_first.sql":  "SELECT 1;\n",
		"2_second.sql": "SELECT 2;\n",
	})
	changedFirst := fixture(map[string]string{
		"1_first.sql":  "SELECT 10;\n",
		"2_second.sql": "SELECT 2;\n",
	})
	baseSum, err := migratesum.ComputeWithFormat(base, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	changedSum, err := migratesum.ComputeWithFormat(changedFirst, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	c.Assert(changedSum.Entries[0].Hash, qt.Not(qt.Equals), baseSum.Entries[0].Hash)
	c.Assert(changedSum.Entries[1].Hash, qt.Not(qt.Equals), baseSum.Entries[1].Hash,
		qt.Commentf("Atlas hashes are chained: editing the first file changes later entry hashes too"))
}

func TestComputeWithFormat_AtlasSumIgnore(t *testing.T) {
	c := qt.New(t)

	withIgnored, err := migratesum.ComputeWithFormat(fixture(map[string]string{
		"1_ignore.sql": "-- atlas:sum ignore\nSELECT ignored;\n",
		"2_keep.sql":   "SELECT kept;\n",
	}), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	withoutIgnored, err := migratesum.ComputeWithFormat(fixture(map[string]string{
		"2_keep.sql": "SELECT kept;\n",
	}), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	c.Assert(withIgnored.Entries, qt.HasLen, 1)
	c.Assert(withIgnored.Entries[0].Name, qt.Equals, "2_keep.sql")
	c.Assert(withIgnored.Entries[0].Hash, qt.Not(qt.Equals), withoutIgnored.Entries[0].Hash,
		qt.Commentf("ignored Atlas files are omitted from atlas.sum but their names still feed the chain"))
}

func TestCompute_AutoUsesAtlasHashWhenAtlasSumPresent(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"1_initial.sql": "CREATE TABLE users (id INT);\n",
		"atlas.sum":     "stale\n",
	})
	autoSum, err := migratesum.Compute(fsys)
	c.Assert(err, qt.IsNil)
	atlasSum, err := migratesum.ComputeWithFormat(fsys, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(autoSum.Bytes(), qt.DeepEquals, atlasSum.Bytes())
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

	// A syntactically valid h1: hash (prefix + base64 of 32 bytes) for the
	// directory line, so the entry-line cases below are not short-circuited
	// by a bad dir line.
	validH := "h1:" + base64.StdEncoding.EncodeToString(make([]byte, 32))

	_, err := migratesum.Parse([]byte(""))
	c.Assert(err, qt.ErrorMatches, "empty or missing directory hash line")

	// Directory line without the h1: prefix.
	_, err = migratesum.Parse([]byte("not-a-hash\n0000000001_x.up.sql " + validH + "\n"))
	c.Assert(err, qt.ErrorMatches, "malformed directory hash line.*")

	// Directory line with the h1: prefix but a non-base64 / wrong-length
	// digest — a corrupt sum must be a usage error, not silent drift.
	_, err = migratesum.Parse([]byte("h1:not-valid-base64!!\n"))
	c.Assert(err, qt.ErrorMatches, "malformed directory hash line.*")

	// No space between name and hash.
	_, err = migratesum.Parse([]byte(validH + "\nnohashhere\n"))
	c.Assert(err, qt.ErrorMatches, "malformed entry line.*")

	// A space is present (so the field split succeeds) but the trailing
	// token is not an h1: hash.
	_, err = migratesum.Parse([]byte(validH + "\n0000000001_x.up.sql garbagehash\n"))
	c.Assert(err, qt.ErrorMatches, "malformed entry line.*")

	// h1: prefix present but the digest is not valid base64/32 bytes.
	_, err = migratesum.Parse([]byte(validH + "\n0000000001_x.up.sql h1:short\n"))
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

func TestVerify_MissingAtlasSumFile(t *testing.T) {
	c := qt.New(t)

	_, err := migratesum.VerifyWithFormat(fixture(map[string]string{
		"1_initial.sql": "CREATE TABLE users (id INT);\n",
	}), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.ErrorIs, migratesum.ErrSumFileMissing)
	c.Assert(err, qt.ErrorMatches, "atlas.sum not found; run `ptah migrations hash --dir-format atlas` to create it")
}

func TestVerify_AutoReadsAtlasSum(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"1_initial.sql": "CREATE TABLE users (id INT);\n",
	})
	sum, err := migratesum.ComputeWithFormat(fsys, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	fsys[migratesum.AtlasFileName] = &fstest.MapFile{Data: sum.Bytes()}

	res, err := migratesum.Verify(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue)
	c.Assert(res.SumFileName, qt.Equals, migratesum.AtlasFileName)
}

func TestVerify_AutoRejectsAmbiguousSumFiles(t *testing.T) {
	c := qt.New(t)

	_, err := migratesum.Verify(fixture(map[string]string{
		"1_initial.sql": "CREATE TABLE users (id INT);\n",
		"atlas.sum":     "h1:fake\n",
		"ptah.sum":      "h1:fake\n",
	}))
	c.Assert(err, qt.ErrorMatches, "both ptah.sum and atlas.sum exist.*")
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

	// Replace only the directory-hash line with a different but still
	// well-formed h1 hash; the per-file entries still match their files, so
	// this is detectable only via the dir hash. (A syntactically broken hash
	// would instead be rejected at parse time — see TestParse_Malformed.)
	wrongDirHash := "h1:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0xFF}, 32))
	tampered := append([]byte(wrongDirHash+"\n"), sum.Bytes()[len(sum.DirHash)+1:]...)
	fsys["ptah.sum"] = &fstest.MapFile{Data: tampered}

	res, err := migratesum.Verify(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsFalse)
	c.Assert(res.DirHashMismatch, qt.IsTrue)
	c.Assert(res.Describe(), qt.Contains, "directory hash mismatch")
}
