package migratesum_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
)

func atlasSourceFS(files map[string]string) fstest.MapFS {
	fsys := make(fstest.MapFS, len(files))
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// sealAtlasSum records the sum of names into the filesystem, the way
// `migrate hash` would.
func sealAtlasSum(c *qt.C, fsys fstest.MapFS, names []string) fstest.MapFS {
	c.Helper()
	sum, err := migratesum.ComputeAtlasFiles(fsys, names)
	c.Assert(err, qt.IsNil)
	fsys[migratesum.AtlasFileName] = &fstest.MapFile{Data: sum.Bytes()}
	return fsys
}

func TestComputeAtlasFilesHonorsTheGivenOrder(t *testing.T) {
	c := qt.New(t)

	fsys := atlasSourceFS(map[string]string{
		"a.sql": "CREATE TABLE a (id int);\n",
		"b.sql": "CREATE TABLE b (id int);\n",
	})

	forward, err := migratesum.ComputeAtlasFiles(fsys, []string{"a.sql", "b.sql"})
	c.Assert(err, qt.IsNil)
	reverse, err := migratesum.ComputeAtlasFiles(fsys, []string{"b.sql", "a.sql"})
	c.Assert(err, qt.IsNil)

	// Atlas chains name and contents into one running hash, so order is part of
	// the sum. Flyway relies on this: it hashes a baseline first and repeatable
	// migrations last, neither of which is name order.
	c.Assert(forward.DirHash, qt.Not(qt.Equals), reverse.DirHash)
	c.Assert(forward.Entries[0].Name, qt.Equals, "a.sql")
	c.Assert(reverse.Entries[0].Name, qt.Equals, "b.sql")
	c.Assert(forward.Entries[0].Hash, qt.Not(qt.Equals), reverse.Entries[1].Hash)
}

func TestComputeAtlasFilesCoversOnlyTheNamedFiles(t *testing.T) {
	c := qt.New(t)

	// The down file of a golang-migrate pair is present but never named, and so
	// is invisible to the sum — matching Atlas CE, which never reads it.
	fsys := atlasSourceFS(map[string]string{
		"1_init.up.sql":   "CREATE TABLE a (id int);\n",
		"1_init.down.sql": "DROP TABLE a;\n",
	})
	names := []string{"1_init.up.sql"}

	before, err := migratesum.ComputeAtlasFiles(fsys, names)
	c.Assert(err, qt.IsNil)

	fsys["1_init.down.sql"] = &fstest.MapFile{Data: []byte("DROP TABLE something_else;\n")}
	after, err := migratesum.ComputeAtlasFiles(fsys, names)
	c.Assert(err, qt.IsNil)

	c.Assert(after.DirHash, qt.Equals, before.DirHash)
	c.Assert(after.Entries, qt.DeepEquals, before.Entries)
}

func TestComputeAtlasFilesSumIgnoreDirective(t *testing.T) {
	c := qt.New(t)

	ignored := "-- atlas:sum ignore\nCREATE TABLE skipped (id int);\n"
	kept := "CREATE TABLE kept (id int);\n"

	withIgnored, err := migratesum.ComputeAtlasFiles(
		atlasSourceFS(map[string]string{"1_ignored.sql": ignored, "2_kept.sql": kept}),
		[]string{"1_ignored.sql", "2_kept.sql"},
	)
	c.Assert(err, qt.IsNil)

	alone, err := migratesum.ComputeAtlasFiles(
		atlasSourceFS(map[string]string{"2_kept.sql": kept}),
		[]string{"2_kept.sql"},
	)
	c.Assert(err, qt.IsNil)

	t.Run("the ignored file gets no entry", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(withIgnored.Entries, qt.HasLen, 1)
		c.Assert(withIgnored.Entries[0].Name, qt.Equals, "2_kept.sql")
	})

	t.Run("but its name still feeds the running hash", func(t *testing.T) {
		// If the name were skipped along with the contents, these would match.
		c := qt.New(t)
		c.Assert(withIgnored.Entries[0].Hash, qt.Not(qt.Equals), alone.Entries[0].Hash)
	})
}

func TestComputeAtlasFilesRejectsBadInput(t *testing.T) {

	t.Run("duplicate name", func(t *testing.T) {
		c := qt.New(t)
		fsys := atlasSourceFS(map[string]string{"a.sql": "CREATE TABLE a (id int);\n"})

		_, err := migratesum.ComputeAtlasFiles(fsys, []string{"a.sql", "a.sql"})

		c.Assert(err, qt.ErrorMatches, `duplicate migration file name "a.sql"`)
	})

	t.Run("missing file", func(t *testing.T) {
		c := qt.New(t)
		fsys := atlasSourceFS(map[string]string{"a.sql": "CREATE TABLE a (id int);\n"})

		_, err := migratesum.ComputeAtlasFiles(fsys, []string{"a.sql", "gone.sql"})

		c.Assert(err, qt.ErrorMatches, "failed to read gone.sql: .*")
	})
}

func TestVerifyAtlasFiles(t *testing.T) {
	c := qt.New(t)

	names := []string{"1_init.sql", "2_more.sql"}
	seed := func() fstest.MapFS {
		return atlasSourceFS(map[string]string{
			"1_init.sql": "CREATE TABLE a (id int);\n",
			"2_more.sql": "CREATE TABLE b (id int);\n",
		})
	}

	c.Run("clean directory verifies", func(c *qt.C) {
		fsys := sealAtlasSum(c, seed(), names)

		result, err := migratesum.VerifyAtlasFiles(fsys, names)

		c.Assert(err, qt.IsNil)
		c.Assert(result.OK(), qt.IsTrue)
		c.Assert(result.SumFileName, qt.Equals, migratesum.AtlasFileName)
	})

	c.Run("edited file is reported as changed", func(c *qt.C) {
		fsys := sealAtlasSum(c, seed(), names)
		fsys["2_more.sql"] = &fstest.MapFile{Data: []byte("DROP TABLE a;\n")}

		result, err := migratesum.VerifyAtlasFiles(fsys, names)

		c.Assert(err, qt.IsNil)
		c.Assert(result.OK(), qt.IsFalse)
		c.Assert(result.Changed, qt.DeepEquals, []string{"2_more.sql"})
		mismatch := result.FirstMismatch()
		c.Assert(mismatch, qt.IsNotNil)
		c.Assert(mismatch.File, qt.Equals, "2_more.sql")
		c.Assert(mismatch.Reason, qt.Equals, migratesum.MismatchReasonEdited)
	})

	c.Run("newly covered file is reported as added", func(c *qt.C) {
		fsys := sealAtlasSum(c, seed(), names)
		fsys["3_new.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE c (id int);\n")}

		result, err := migratesum.VerifyAtlasFiles(fsys, append(names, "3_new.sql"))

		c.Assert(err, qt.IsNil)
		c.Assert(result.OK(), qt.IsFalse)
		c.Assert(result.Added, qt.DeepEquals, []string{"3_new.sql"})
	})

	c.Run("deleted file is reported as removed", func(c *qt.C) {
		fsys := sealAtlasSum(c, seed(), names)
		delete(fsys, "2_more.sql")

		result, err := migratesum.VerifyAtlasFiles(fsys, names[:1])

		c.Assert(err, qt.IsNil)
		c.Assert(result.OK(), qt.IsFalse)
		c.Assert(result.Removed, qt.DeepEquals, []string{"2_more.sql"})
	})

	c.Run("an uncovered sibling file is not drift", func(c *qt.C) {
		// The property the whole converted-format path depends on: verification
		// answers for the named files only, so a golang-migrate down file or a
		// Flyway undo file may change freely.
		fsys := sealAtlasSum(c, seed(), names)
		fsys["notes.sql"] = &fstest.MapFile{Data: []byte("-- scratch\n")}

		result, err := migratesum.VerifyAtlasFiles(fsys, names)

		c.Assert(err, qt.IsNil)
		c.Assert(result.OK(), qt.IsTrue)
	})

	t.Run("missing sum file", func(t *testing.T) {
		c := qt.New(t)
		result, err := migratesum.VerifyAtlasFiles(seed(), names)

		c.Assert(result, qt.IsNil)
		c.Assert(err, qt.ErrorIs, migratesum.ErrSumFileMissing)
	})

	t.Run("malformed sum file", func(t *testing.T) {
		c := qt.New(t)
		fsys := seed()
		fsys[migratesum.AtlasFileName] = &fstest.MapFile{Data: []byte("garbage\n")}

		result, err := migratesum.VerifyAtlasFiles(fsys, names)

		c.Assert(result, qt.IsNil)
		c.Assert(err, qt.ErrorIs, migratesum.ErrSumFileMalformed)
	})
}

func TestVerifyAtlasFilesHashed(t *testing.T) {
	c := qt.New(t)

	names := []string{"1_init.sql"}
	seed := func() fstest.MapFS {
		return atlasSourceFS(map[string]string{"1_init.sql": "CREATE TABLE a (id int);\n"})
	}

	t.Run("unhashed directory reports hashed=false", func(t *testing.T) {
		c := qt.New(t)
		result, hashed, err := migratesum.VerifyAtlasFilesHashed(seed(), names)

		c.Assert(err, qt.IsNil)
		c.Assert(hashed, qt.IsFalse)
		c.Assert(result, qt.IsNil)
	})

	c.Run("hashed clean directory reports hashed=true and OK", func(c *qt.C) {
		result, hashed, err := migratesum.VerifyAtlasFilesHashed(sealAtlasSum(c, seed(), names), names)

		c.Assert(err, qt.IsNil)
		c.Assert(hashed, qt.IsTrue)
		c.Assert(result.OK(), qt.IsTrue)
	})

	c.Run("hashed tampered directory reports hashed=true and drift", func(c *qt.C) {
		fsys := sealAtlasSum(c, seed(), names)
		fsys["1_init.sql"] = &fstest.MapFile{Data: []byte("DROP TABLE a;\n")}

		result, hashed, err := migratesum.VerifyAtlasFilesHashed(fsys, names)

		c.Assert(err, qt.IsNil)
		c.Assert(hashed, qt.IsTrue)
		c.Assert(result.OK(), qt.IsFalse)
	})

	t.Run("a ptah.sum does not count as hashed", func(t *testing.T) {
		c := qt.New(t)
		fsys := seed()
		fsys[migratesum.FileName] = &fstest.MapFile{Data: []byte("h1:whatever\n")}

		_, hashed, err := migratesum.VerifyAtlasFilesHashed(fsys, names)

		c.Assert(err, qt.IsNil)
		c.Assert(hashed, qt.IsFalse)
	})
}
