//go:build integration && !windows

package migrateup_test

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrateup"
	"ptah.run/internal/dbtarget"
)

func TestMigrateUpCommandPgDumpHookWritesArtifact(t *testing.T) {
	c := qt.New(t)
	dbURL := newRollbackTarget(c, dbtarget.PostgreSQL).url
	const tableName = "ptah_preflight_pg_dump_guarded"

	dir := t.TempDir()
	writePgDumpMigration(c, dir, "0000000001_create_dump_guarded.up.sql", fmt.Sprintf("CREATE TABLE %s (id BIGINT);\n", tableName))
	writePgDumpMigration(c, dir, "0000000001_create_dump_guarded.down.sql", fmt.Sprintf("DROP TABLE %s;\n", tableName))

	argsLog := filepath.Join(t.TempDir(), "pg_dump_args.log")
	fakeBin := filepath.Join(t.TempDir(), "pg_dump")
	fakeScript := fmt.Appendf(nil, `#!/bin/sh
out=""
: > %[1]q
while [ "$#" -gt 0 ]; do
  printf '%%s\n' "$1" >> %[1]q
  if [ "$1" = "--file" ]; then
    shift
    out="$1"
    printf '%%s\n' "$1" >> %[1]q
  fi
  shift
done
if [ -z "$out" ]; then
  echo "missing --file" >&2
  exit 64
fi
printf 'fake custom dump\n' > "$out"
`, argsLog)
	c.Assert(os.WriteFile(fakeBin, fakeScript, 0o600), qt.IsNil)
	c.Assert(os.Chmod(fakeBin, 0o700), qt.IsNil)
	t.Setenv("PATH", filepath.Dir(fakeBin)+string(os.PathListSeparator)+os.Getenv("PATH"))

	dumpDir := t.TempDir()
	cmd := migrateup.NewMigrateUpCommand()
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
		"--pg-dump-to", dumpDir,
	})
	c.Assert(cmd.Execute(), qt.IsNil)

	matches, err := filepath.Glob(filepath.Join(dumpDir, "ptah_pre_v0_to_v1_*.dump"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	dumpData, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(dumpData), qt.Equals, "fake custom dump\n")
	argsData, err := os.ReadFile(argsLog)
	c.Assert(err, qt.IsNil)
	c.Assert(string(argsData), qt.Contains, "--format=custom\n")
	c.Assert(string(argsData), qt.Contains, "--file\n"+matches[0]+"\n")
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	password, hasPassword := parsed.User.Password()
	c.Assert(hasPassword, qt.IsTrue)
	c.Assert(string(argsData), qt.Not(qt.Contains), password)
}

func writePgDumpMigration(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}
