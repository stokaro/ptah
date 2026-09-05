//go:build integration

package dbtest_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/dbtest"
)

// TestLoadCasesOfKind_TheConstructsRunAgainstPostgreSQL is the integration
// coverage the epic's own acceptance row asks for, and the shape matters.
//
// The other PostgreSQL test in this package builds its cases in Go, so it
// exercises the engine and the server and NOT the `.test.hcl` reader: with
// `ParseAtlasTestCases` stubbed out it still passes. This one carries every
// construct from HCL TEXT through a run against a live server, so the reader is
// on the path it measures.
//
// Each construct is asserted by something the server had to do: the mapping's
// key names the column the case inserts, the `catch` establishes the table is
// absent before it exists, and the `output` compares a whole result set.
func TestLoadCasesOfKind_TheConstructsRunAgainstPostgreSQL(t *testing.T) {
	c := qt.New(t)
	serverURL := dbtarget.URL(c, dbtarget.PostgreSQL)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "payload.txt"), []byte("from-a-file"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "c.test.hcl"), []byte(`
variable "prefix" {
  default = "acct"
}

test "migrate" "constructs" {
  for_each = { alpha = "one", beta = "two" }
  parallel = true

  log { message = "case ${self.name} for ${each.key}" }

  catch { sql = "SELECT * FROM accounts" }

  exec { sql = "CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT)" }
  exec { sql = "INSERT INTO accounts (id, name) VALUES (1, '${var.prefix}-${each.value}')" }

  exec {
    sql    = "SELECT id, name FROM accounts ORDER BY id"
    output = "1,acct-${each.value}"
  }

  exec {
    sql   = "SELECT name FROM accounts"
    match = "^acct-"
  }

  assert {
    sql           = "SELECT COUNT(*) = 1 FROM accounts"
    error_message = "exactly one account"
  }

  exec { sql = "SELECT '${file("payload.txt")}'" }

  cleanup { sql = "DROP TABLE accounts" }
}

test "migrate" "skipped" {
  skip = true
  exec { sql = "THIS WOULD FAIL LOUDLY" }
}
`), 0o600), qt.IsNil)

	cases, err := dbtest.LoadCasesOfKind(dir, dbtest.AtlasTestKindMigrate)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 3)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		DBURL:       serverURL,
		Parallelism: 2,
		Cases:       cases,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("report: %s", report.Text()))

	// The mapping iterated sorted, so alpha is instance 1 and beta is 2, and
	// each ran against a database of its own -- the `catch` in both proves the
	// table was absent when each began.
	c.Assert(report.Cases[0].Name, qt.Equals, "constructs/1")
	c.Assert(report.Cases[1].Name, qt.Equals, "constructs/2")
	c.Assert(report.Cases[2].Skipped, qt.IsTrue)
}
