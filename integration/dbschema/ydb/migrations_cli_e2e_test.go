//go:build integration

package ydb_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
)

// cliMigrationsDir is the directory the binary's migrations write to.
const cliMigrationsDir = "ptah_ydb_cli_migrations"

// writeCLIMigrations writes a migration directory and a declarative test for
// it into a fresh temporary directory.
func writeCLIMigrations(c *qt.C) (migrations, tests string) {
	c.Helper()
	root := c.TempDir()
	migrations = filepath.Join(root, "migrations")
	tests = filepath.Join(root, "tests")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	c.Assert(os.Mkdir(tests, 0o750), qt.IsNil)
	files := map[string]string{
		"0000000001_users.up.sql": "CREATE TABLE `" + cliMigrationsDir + "/users` (id Int64 NOT NULL, name Utf8, PRIMARY KEY (id));\n" +
			"$name = 'first'u;\nINSERT INTO `" + cliMigrationsDir + "/users` (id, name) VALUES (1l, $name);\n",
		"0000000001_users.down.sql": "DROP TABLE `" + cliMigrationsDir + "/users`;\n",
		"0000000002_posts.up.sql":   "CREATE TABLE `" + cliMigrationsDir + "/posts` (id Int64 NOT NULL, PRIMARY KEY (id));\n",
		"0000000002_posts.down.sql": "DROP TABLE `" + cliMigrationsDir + "/posts`;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	c.Assert(os.WriteFile(filepath.Join(tests, "users.yaml"), []byte(`cases:
  - name: the first user exists
    steps:
      - migrate_to: latest
      - assert:
          query: "SELECT COUNT(*) FROM `+"`"+cliMigrationsDir+"/users`"+`"
          scalar: "1"
`), 0o600), qt.IsNil)
	return migrations, tests
}

// TestYDBBinary_RunsVersionedMigrations drives the shipped binary's versioned
// verbs against a live YDB database: hash and validate the directory, apply
// it, report it up to date, refuse a statement timeout YDB cannot carry, run
// the declarative tests, and roll everything back.
func TestYDBBinary_RunsVersionedMigrations(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropDirectory(c, conn, cliMigrationsDir, "users", "posts")
			c.Cleanup(func() { dropDirectory(c, conn, cliMigrationsDir, "users", "posts") })
			migrations, tests := writeCLIMigrations(c)
			target := []string{"--db-url", url, "--migrations-dir", migrations, "--migrations-schema", cliMigrationsDir}

			hashed, hashErr := runBinary(ctx, binary, "migrations", "hash", "--dir", migrations)
			validated, validateErr := runBinary(ctx, binary, "migrations", "validate", "--dir", migrations)
			applied, upErr := runBinary(ctx, binary, append([]string{"migrations", "up"}, target...)...)
			status, statusErr := runBinary(ctx, binary, append([]string{"migrations", "status"}, target...)...)
			refused, refusedErr := runBinary(ctx, binary,
				append([]string{"migrations", "up", "--statement-timeout", "5s"}, target...)...)
			tested, testErr := runBinary(ctx, binary, "migrations", "test", "--db-url", url, "--dir", tests,
				"--migrations-dir", migrations, "--migrations-schema", cliMigrationsDir)
			rolledBack, downErr := runBinary(ctx, binary,
				append([]string{"migrations", "down", "--target", "0", "--confirm"}, target...)...)

			c.Assert(hashErr, qt.IsNil, qt.Commentf("hash:\n%s", hashed))
			c.Assert(validateErr, qt.IsNil, qt.Commentf("validate:\n%s", validated))
			c.Assert(upErr, qt.IsNil, qt.Commentf("up:\n%s", applied))
			c.Assert(applied, qt.Contains, "Database is now at version: 2")
			c.Assert(statusErr, qt.IsNil, qt.Commentf("status:\n%s", status))
			c.Assert(status, qt.Contains, "Current Version: 2")
			c.Assert(status, qt.Contains, "Pending Migrations: 0")
			c.Assert(refusedErr, qt.IsNotNil)
			c.Assert(refused, qt.Contains, `--statement-timeout sets a timeout for every migration, and dialect "ydb" `+
				`has no lock or statement timeout Ptah can set and restore around a migration`)
			c.Assert(testErr, qt.IsNil, qt.Commentf("test:\n%s", tested))
			c.Assert(tested, qt.Contains, `PASS  case "the first user exists"`)
			c.Assert(downErr, qt.IsNil, qt.Commentf("down:\n%s", rolledBack))
			c.Assert(tableNames(readScoped(c, conn, []string{cliMigrationsDir})), qt.HasLen, 0)
		})
	}
}

// TestYDBBinary_MigrationsUpWaitsForTheLock runs `migrations up` while another
// session holds the migration lock: under --migration-lock-timeout it exits
// with the timeout, and without one it waits until the holder releases. Two
// runs started together over the same directory then apply it once, which the
// INSERT in the first migration would refuse to do twice.
func TestYDBBinary_MigrationsUpWaitsForTheLock(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropDirectory(c, conn, cliMigrationsDir, "users", "posts")
			c.Cleanup(func() { dropDirectory(c, conn, cliMigrationsDir, "users", "posts") })
			migrations, _ := writeCLIMigrations(c)
			up := []string{"migrations", "up", "--db-url", url, "--migrations-dir", migrations,
				"--migrations-schema", cliMigrationsDir}

			lock, err := dblock.Acquire(ctx, openYDB(c, line), "ptah_migrate", 0)
			c.Assert(err, qt.IsNil)
			timedOut, timeoutErr := runBinary(ctx, binary, append(up, "--migration-lock-timeout", "1s")...)
			c.Assert(timeoutErr, qt.IsNotNil)
			c.Assert(timedOut, qt.Contains, `timed out acquiring migration lock "ptah_migrate" for ydb after 1s`)

			var waitingOutput bytes.Buffer
			waiting := exec.CommandContext(ctx, binary, up...)
			waiting.Stdout, waiting.Stderr = &waitingOutput, &waitingOutput
			c.Assert(waiting.Start(), qt.IsNil)
			exited := make(chan error, 1)
			go func() { exited <- waiting.Wait() }()
			time.Sleep(3 * time.Second)
			c.Assert(exited, qt.HasLen, 0, qt.Commentf("output:\n%s", &waitingOutput))
			c.Assert(tableNames(readScoped(c, conn, []string{cliMigrationsDir})), qt.HasLen, 0)
			c.Assert(lock.Release(context.Background()), qt.IsNil)
			c.Assert(<-exited, qt.IsNil, qt.Commentf("output:\n%s", &waitingOutput))
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+cliMigrationsDir+"/users`"), qt.Equals, int64(1))

			down := []string{"migrations", "down", "--target", "0", "--confirm", "--db-url", url,
				"--migrations-dir", migrations, "--migrations-schema", cliMigrationsDir}
			rolledBack, downErr := runBinary(ctx, binary, down...)
			c.Assert(downErr, qt.IsNil, qt.Commentf("down:\n%s", rolledBack))

			results := make(chan string, 2)
			for range 2 {
				go func() {
					output, err := runBinary(ctx, binary, up...)
					results <- output + "\nerror: " + fmt.Sprint(err)
				}()
			}
			first, second := <-results, <-results
			c.Assert(first, qt.Contains, "\nerror: <nil>")
			c.Assert(second, qt.Contains, "\nerror: <nil>")
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+cliMigrationsDir+"/users`"), qt.Equals, int64(1))
		})
	}
}
