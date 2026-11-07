//go:build integration

package notnullname_test

import (
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// namesBothWays is what the NOT NULL constraints of public.t read back as at each step
// of a directory of two migrations: the first file applied, the second, its rollback,
// and the second again.
type namesBothWays struct {
	before, after, restored, again string
}

// searchPathPublic pins database to the public schema, as a dev database URL has to.
func searchPathPublic(c *qt.C, database string) string {
	c.Helper()
	parsed, err := url.Parse(database)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", "public")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// compatFilesBothWays has `migrate diff` of ptah-compat, through run, write a
// golang-migrate directory for before and then after, holds the directory synced with
// after, and runs the files forward, back and forward on a database of their own.
func compatFilesBothWays(c *qt.C, run func(args ...string) (string, error), before, after string) namesBothWays {
	c.Helper()
	dev := searchPathPublic(c, newDatabase(c, dbtarget.PostgreSQL, `SELECT 1`))
	target := newDatabase(c, dbtarget.PostgreSQL, `SELECT 1`)
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	diff := func(name, schema string) string {
		path := filepath.Join(work, name+".sql")
		c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
		output, err := run("migrate", "diff", name,
			"--dir", "file://"+filepath.ToSlash(dir)+"?format=golang-migrate",
			"--to", "file://"+filepath.ToSlash(path),
			"--dev-url", dev,
			"--format", `{{ sql . "  " }}`)
		c.Assert(err, qt.IsNil, qt.Commentf("%s: %s", name, output))
		return output
	}
	diff("before", before)
	diff("after", after)
	c.Assert(diff("again", after), qt.Contains, "The migration directory is synced with the desired state")

	ups, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	slices.Sort(ups)
	c.Assert(ups, qt.HasLen, 2)
	apply := func(file string) string {
		body, readErr := os.ReadFile(file)
		c.Assert(readErr, qt.IsNil)
		execScript(c, target, string(body))
		return notNullNames(c, target)
	}
	return namesBothWays{
		before:   apply(ups[0]),
		after:    apply(ups[1]),
		restored: apply(strings.TrimSuffix(ups[1], ".up.sql") + ".down.sql"),
		again:    apply(ups[1]),
	}
}

// nativeFilesBothWays has `ptah migrations generate` write a file for before and then
// for after over a replay of the directory, holds the directory synced with after, and
// runs `migrations up` and `migrations down` on a database of their own.
func nativeFilesBothWays(c *qt.C, before, after string) namesBothWays {
	c.Helper()
	dev := newDatabase(c, dbtarget.PostgreSQL, `SELECT 1`)
	target := newDatabase(c, dbtarget.PostgreSQL, `SELECT 1`)
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	ptah := func(args ...string) string {
		output, err := runPtah(args...)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
		return output
	}
	generate := func(name, schema string) string {
		file := filepath.Join(work, name+".sql")
		c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
		return ptah("migrations", "generate", "--replay", "--dev-url", dev,
			"--schema-file", file, "--migrations-dir", dir, "--name", name)
	}
	up := func() string {
		ptah("migrations", "up", "--allow-destructive", "--db-url", target, "--migrations-dir", dir)
		return notNullNames(c, target)
	}
	generate("before", before)
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	base, _, _ := strings.Cut(filepath.Base(files[0]), "_")

	names := namesBothWays{before: up()}
	generate("after", after)
	names.after = up()
	c.Assert(generate("again", after), qt.Contains, "no migration files generated")
	ptah("migrations", "down", "--confirm", "--db-url", target, "--migrations-dir", dir, "--target", base)
	names.restored = notNullNames(c, target)
	names.again = up()
	return names
}
