package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func writeGoFile(c *qt.C, dir, name, content string) {
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func TestParseDirsMergesMultipleRoots(t *testing.T) {
	c := qt.New(t)

	rootA := t.TempDir()
	rootB := t.TempDir()
	writeGoFile(c, rootA, "users.go", usersSource)
	writeGoFile(c, rootB, "orders.go", ordersSource)

	db, err := goschema.ParseDirs(rootA, rootB)
	c.Assert(err, qt.IsNil)

	// Both roots contribute their table, finalized together.
	c.Assert(db.Tables, qt.HasLen, 2)
	c.Assert(db.Fields, qt.HasLen, 4)

	// orders (root B) references users (root A), so the shared dependency sort
	// orders users first across roots.
	c.Assert(tableIndex(db, "users") < tableIndex(db, "orders"), qt.IsTrue)
}

func TestParseDirsSingleRoot(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	writeGoFile(c, root, "users.go", usersSource)

	db, err := goschema.ParseDirs(root)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(tableIndex(db, "users") >= 0, qt.IsTrue)
	c.Assert(db.Fields, qt.HasLen, 2)
}

func TestParseDirsNoRootsReturnsEmpty(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseDirs()
	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.IsNotNil)
	c.Assert(db.Tables, qt.HasLen, 0)
}

func TestParseDirRawFeedsMerge(t *testing.T) {
	c := qt.New(t)

	rootA := t.TempDir()
	rootB := t.TempDir()
	writeGoFile(c, rootA, "users.go", usersSource)
	writeGoFile(c, rootB, "orders.go", ordersSource)

	// ParseDirRaw yields un-finalized sources suitable for Merge to combine and
	// finalize once, matching a single finalized ParseDirs over both roots.
	rawA, err := goschema.ParseDirRaw(rootA)
	c.Assert(err, qt.IsNil)
	rawB, err := goschema.ParseDirRaw(rootB)
	c.Assert(err, qt.IsNil)

	merged, err := goschema.Merge(rawA, rawB)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Tables, qt.HasLen, 2)
	c.Assert(tableIndex(merged, "users") < tableIndex(merged, "orders"), qt.IsTrue)
}
