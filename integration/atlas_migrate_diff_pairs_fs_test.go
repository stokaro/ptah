//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestReadMigrationPairs_PairsADownFileStampedInTheNextSecond pins the pairing
// the byte comparisons with the community binary read through: a step whose
// down file is stamped one second after its up file is still one pair, and
// two steps stamped in one second keep their order.
func TestReadMigrationPairs_PairsADownFileStampedInTheNextSecond(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	for name, body := range map[string]string{
		"20261118140451_s1.up.sql":   "CREATE TABLE a (id int);\n",
		"20261118140452_s1.down.sql": "DROP TABLE a;\n",
		"20261118140452_s2.up.sql":   "CREATE TABLE b (id int);\n",
		"20261118140452_s2.down.sql": "DROP TABLE b;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}

	c.Assert(readMigrationPairs(c, dir), qt.DeepEquals, []migrationPair{
		{Up: "CREATE TABLE a (id int);\n", Down: "DROP TABLE a;\n"},
		{Up: "CREATE TABLE b (id int);\n", Down: "DROP TABLE b;\n"},
	})
}
