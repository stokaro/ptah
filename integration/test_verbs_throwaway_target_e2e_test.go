//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/root"
)

// TestTestVerbs_TheThrowawayTargetIsExplicitOnly is a destructive default this
// closes rather than a rule it states.
//
// `--db-url` on the test verbs names a database the run MUTATES and treats as
// disposable. It was bound to PTAH_DB_URL -- the same variable `migrations up`,
// `schema apply` and `db drop-all` take their PRODUCTION target from. Measured
// on the shipped binary: with that variable exported and no flag given,
// `ptah schema test` reported success and its CREATE TABLE landed in the named
// database. An operator who exports it for ordinary work ran their suite
// against production by omitting an argument.
//
// The assertion is that the named database is never created, because an error
// alone would not say the run declined to touch it.
func TestTestVerbs_TheThrowawayTargetIsExplicitOnly(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "e.test.hcl"), []byte(`
test "schema" "writes" {
  exec { sql = "CREATE TABLE marker (x INTEGER)" }
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

	target := filepath.Join(t.TempDir(), "pretend-production.db")
	t.Setenv("PTAH_DB_URL", "sqlite://"+target)

	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--dir", dir, "--root-dir", models})

	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("output: %s", out.String()))

	// The run succeeded -- against a database of its own.
	_, err := os.Stat(target)
	c.Assert(os.IsNotExist(err), qt.IsTrue,
		qt.Commentf("the suite wrote to the target PTAH_DB_URL named; output: %s", out.String()))
}

// TestTestVerbs_AnExplicitThrowawayTargetStillWorks is the control.
//
// Making the flag explicit-only must not be achieved by making it stop working:
// without this, deleting the flag entirely would satisfy the test above.
func TestTestVerbs_AnExplicitThrowawayTargetStillWorks(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "e.test.hcl"), []byte(`
test "schema" "writes" {
  exec { sql = "CREATE TABLE marker (x INTEGER)" }
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

	target := filepath.Join(t.TempDir(), "explicit.db")

	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--dir", dir, "--root-dir", models, "--db-url", "sqlite://" + target})

	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("output: %s", out.String()))

	_, err := os.Stat(target)
	c.Assert(err, qt.IsNil, qt.Commentf("the explicit target was not used; output: %s", out.String()))
}
