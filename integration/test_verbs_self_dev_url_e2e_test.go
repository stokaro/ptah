//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/root"
)

// TestTestVerbs_SelfDevURLResolvesToTheAddressTheCasesRunAgainst covers a
// required construct that had no production caller at all.
//
// `WithAtlasTestDevURL` existed and was tested through the Go API; measured on
// the shipped tree, no command passed it, so `self.dev_url` resolved on NO
// surface and a document naming it was refused everywhere. The option was a
// rule with no caller.
//
// The assertion compares the resolved value against the exact URL rather than
// merely running the step: `SELECT ”` succeeds too, so only an equality check
// distinguishes the real address from an empty one.
func TestTestVerbs_SelfDevURLResolvesToTheAddressTheCasesRunAgainst(t *testing.T) {
	c := qt.New(t)

	target := filepath.Join(t.TempDir(), "target.db")
	url := "sqlite://" + target

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "d.test.hcl"), []byte(`
test "schema" "dev url" {
  exec {
    sql    = "SELECT '${self.dev_url}'"
    output = "`+url+`"
  }
}
`), 0o600), qt.IsNil)

	models := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(models, "m.go"), []byte(`package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--dir", dir, "--root-dir", models, "--db-url", url})

	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("output: %s", out.String()))
	c.Assert(out.String(), qt.Contains, "1 cases, 1 passed, 0 failed, 0 skipped")
}

// TestTestVerbs_SelfDevURLIsRefusedWhenNoAddressIsKnown is the fail-closed half.
//
// Without a database URL every case gets a disposable database of its own, so
// there is no single address to resolve to and none is invented. The refusal
// names the file and line, because the author has to change the document or
// name a database.
func TestTestVerbs_SelfDevURLIsRefusedWhenNoAddressIsKnown(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "d.test.hcl"), []byte(`
test "schema" "dev url" {
  exec { sql = "SELECT '${self.dev_url}'" }
}
`), 0o600), qt.IsNil)

	models := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(models, "m.go"), []byte(`package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--dir", dir, "--root-dir", models})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error()+out.String(), qt.Contains, "d.test.hcl:3")
	c.Assert(err.Error()+out.String(), qt.Contains, "dev_url")
}
