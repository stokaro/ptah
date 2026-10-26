//go:build integration

package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
)

// The commands that replay a golang-migrate directory on a dev database read a
// file the way `migrate apply` runs it (stokaro/ptah#4962). A file that holds
// one CREATE INDEX CONCURRENTLY runs without a transaction there, so lint does
// not report PG103 and validate and diff replay it. A file that opens a
// transaction inside its own one is refused for the same reason by apply and
// by the replay lint and validate run before they judge anything.

// runCompat runs the compat command with args and returns its output with
// whitespace folded, and its error.
func runCompat(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return strings.Join(strings.Fields(out.String()), " "), err
}

// loneConcurrentIndexDirectory is a golang-migrate directory whose latest
// migration holds one concurrent build.
var loneConcurrentIndexDirectory = withGolangMigrateBase(map[string]string{
	"20240829000000_idx.up.sql":   "CREATE INDEX CONCURRENTLY base_id_idx ON base (id);\n",
	"20240829000000_idx.down.sql": "DROP INDEX CONCURRENTLY base_id_idx;\n",
})

func TestGolangMigrateLoneConcurrentIndexDevReplayLive(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	t.Run("lint", func(t *testing.T) {
		c := qt.New(t)
		dir := writeHashedLayoutDirectory(c, "golang-migrate", loneConcurrentIndexDirectory)

		out, err := runCompat(c, "migrate", "lint", "--dir", "file://"+dir+"?format=golang-migrate",
			"--dev-url", newCleanGateDatabase(c, adminURL, nil), "--latest", "1")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
		c.Assert(out, qt.Contains, "-- analyzing version 20240829000000 -- no diagnostics found")
		c.Assert(out, qt.Not(qt.Contains), "PG103")
	})

	t.Run("validate", func(t *testing.T) {
		c := qt.New(t)
		dir := writeHashedLayoutDirectory(c, "golang-migrate", loneConcurrentIndexDirectory)

		out, err := runCompat(c, "migrate", "validate", "--dir", "file://"+dir+"?format=golang-migrate",
			"--dev-url", newCleanGateDatabase(c, adminURL, nil))

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	})

	t.Run("diff", func(t *testing.T) {
		c := qt.New(t)
		dir := writeHashedLayoutDirectory(c, "golang-migrate", loneConcurrentIndexDirectory)
		schema := filepath.Join(c.TempDir(), "schema.sql")
		c.Assert(os.WriteFile(schema, []byte("CREATE TABLE base (id int);\nCREATE INDEX base_id_idx ON base (id);\n"), 0o600), qt.IsNil)

		out, err := runCompat(c, "migrate", "diff", "--dir", "file://"+dir+"?format=golang-migrate",
			"--to", "file://"+schema, "--dev-url", newCleanGateDatabase(c, adminURL, nil))

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
		c.Assert(out, qt.Contains, "The migration directory is synced with the desired state, no changes to be made")
	})
}

// The forms of transaction control golang-migrate applies beyond a block the
// file opens and closes, measured with v4.20.1 through both PostgreSQL drivers
// on PostgreSQL 15.19: a COMMIT outside a block, savepoints inside one, a BEGIN
// inside one, and a lone statement PostgreSQL refuses inside a transaction
// block. Lint, validate and apply all accept each of them.
func TestGolangMigrateUnwrappedFormsDevReplayLive(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	tests := []struct {
		name string
		body string
		// wantTable is a table the apply leaves.
		wantTable string
	}{
		{
			name: "a COMMIT outside a block",
			body: "CREATE TYPE color AS ENUM ('red');\nALTER TYPE color ADD VALUE 'green';\nCOMMIT;\n" +
				"CREATE TABLE t1 (c color DEFAULT 'green');\nINSERT INTO t1 VALUES ('green');\n",
			wantTable: "t1",
		},
		{
			name: "savepoints inside a block",
			body: "BEGIN;\nCREATE TABLE t1 (id int);\nSAVEPOINT s;\nCREATE TABLE t2 (id int);\nROLLBACK TO SAVEPOINT s;\n" +
				"RELEASE SAVEPOINT s;\nCOMMIT;\n",
			wantTable: "t1",
		},
		{
			name:      "a BEGIN inside a block",
			body:      "BEGIN;\nCREATE TABLE t1 (id int);\nBEGIN;\nINSERT INTO t1 VALUES (1);\nCOMMIT;\n",
			wantTable: "t1",
		},
		{
			name:      "a lone VACUUM",
			body:      "VACUUM base;\n",
			wantTable: "base",
		},
	}
	for _, test := range tests {
		files := withGolangMigrateBase(map[string]string{
			"20240827000000_t.up.sql":   test.body,
			"20240827000000_t.down.sql": "SELECT 1;\n",
		})
		for _, command := range [][]string{
			{"migrate", "lint", "--latest", "1"},
			{"migrate", "validate"},
		} {
			t.Run(test.name+"/"+command[1], func(t *testing.T) {
				c := qt.New(t)
				dir := writeHashedLayoutDirectory(c, "golang-migrate", files)
				args := slices.Concat(command, []string{"--dir", "file://" + dir + "?format=golang-migrate",
					"--dev-url", newCleanGateDatabase(c, adminURL, nil)})

				out, err := runCompat(c, args...)

				c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			})
		}
		t.Run(test.name+"/apply", func(t *testing.T) {
			c := qt.New(t)
			db, out, err := applyGolangMigrateDirectory(c, adminURL, files)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			c.Assert(tableExistsIn(c, db, test.wantTable), qt.IsTrue)
		})
	}
}

// The forms Ptah refuses are refused for the same reason by lint, validate
// and apply, before anything runs: a ROLLBACK, which golang-migrate applies,
// a savepoint outside a block, where golang-migrate fails too, and a block
// the file leaves open, which golang-migrate's two drivers settle in opposite
// ways.
func TestGolangMigrateRefusedFormsDevReplayLive(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	tests := []struct {
		name    string
		body    string
		refusal string
	}{
		{
			name:    "a ROLLBACK",
			body:    "BEGIN;\nCREATE TABLE t1 (id int);\nROLLBACK;\nCREATE TABLE t2 (id int);\n",
			refusal: `(?s).*at statement 3: ROLLBACK takes the file's own work back.*`,
		},
		{
			name:    "a savepoint outside a block",
			body:    "CREATE TABLE t1 (id int);\nSAVEPOINT s;\nRELEASE SAVEPOINT s;\n",
			refusal: `(?s).*at statement 2: SAVEPOINT outside a transaction the file opened.*`,
		},
		{
			name:    "a block the file leaves open",
			body:    "CREATE TABLE t1 (id int);\nBEGIN;\nCREATE TABLE t2 (id int);\n",
			refusal: `(?s).*at statement 2: the transaction is never closed by the file.*`,
		},
	}
	for _, test := range tests {
		files := withGolangMigrateBase(map[string]string{
			"20240827000000_t.up.sql":   test.body,
			"20240827000000_t.down.sql": "SELECT 1;\n",
		})
		for _, command := range [][]string{
			{"migrate", "lint", "--latest", "1"},
			{"migrate", "validate"},
		} {
			t.Run(test.name+"/"+command[1], func(t *testing.T) {
				c := qt.New(t)
				dir := writeHashedLayoutDirectory(c, "golang-migrate", files)
				args := slices.Concat(command, []string{"--dir", "file://" + dir + "?format=golang-migrate",
					"--dev-url", newCleanGateDatabase(c, adminURL, nil)})

				out, err := runCompat(c, args...)

				c.Assert(err, qt.ErrorMatches, test.refusal, qt.Commentf("output:\n%s", out))
			})
		}
		t.Run(test.name+"/apply", func(t *testing.T) {
			c := qt.New(t)
			db, out, err := applyGolangMigrateDirectory(c, adminURL, files)

			c.Assert(err, qt.ErrorMatches, test.refusal, qt.Commentf("output:\n%s", out))
			c.Assert(tableExistsIn(c, db, "t1"), qt.IsFalse)
		})
	}
}

// The replay runs a file that opens its own transactions in the transactions
// apply runs it in, golang-migrate's default mode: the concurrent build after
// the file's COMMIT runs in the transaction the end of the file commits, where
// PostgreSQL refuses it, as golang-migrate v4.20.1 leaves it refused (measured
// on PostgreSQL 15.19, stokaro/ptah#4979). Lint, validate and apply stop at the
// same statement.
func TestGolangMigrateOneQueryDevReplayLive(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	files := withGolangMigrateBase(map[string]string{
		"20240830000000_idx.up.sql":   "BEGIN;\nCREATE TABLE t1 (id int);\nCOMMIT;\nCREATE INDEX CONCURRENTLY base_id_idx ON base (id);\n",
		"20240830000000_idx.down.sql": "DROP INDEX base_id_idx;\nDROP TABLE t1;\n",
	})
	const refusal = `(?s).*CREATE INDEX CONCURRENTLY cannot run inside a transaction block.*`
	for _, command := range [][]string{
		{"migrate", "lint", "--latest", "1"},
		{"migrate", "validate"},
	} {
		t.Run(command[1], func(t *testing.T) {
			c := qt.New(t)
			dir := writeHashedLayoutDirectory(c, "golang-migrate", files)
			args := slices.Concat(command, []string{"--dir", "file://" + dir + "?format=golang-migrate",
				"--dev-url", newCleanGateDatabase(c, adminURL, nil)})

			out, err := runCompat(c, args...)

			c.Assert(err, qt.ErrorMatches, refusal, qt.Commentf("output:\n%s", out))
		})
	}

	t.Run("apply", func(t *testing.T) {
		c := qt.New(t)
		db, out, err := applyGolangMigrateDirectory(c, adminURL, files)

		c.Assert(err, qt.ErrorMatches, refusal, qt.Commentf("output:\n%s", out))
		c.Assert(tableExistsIn(c, db, "t1"), qt.IsTrue)
		applied, _ := latestRevision(c, db, "20240830000000")
		c.Assert(applied, qt.Equals, 3)
	})
}

// The replay commits the transaction a file's last statements run in, as the
// end of golang-migrate's single query does, before the next file runs: the
// lone concurrent build after it runs outside any transaction, where
// PostgreSQL accepts it.
func TestGolangMigrateOneQueryDevReplayCommitsTheEndOfTheFileLive(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	dir := writeHashedLayoutDirectory(c, "golang-migrate", withGolangMigrateBase(map[string]string{
		"20240830000000_t.up.sql":     "BEGIN;\nCREATE TABLE t1 (id int);\nCOMMIT;\nCREATE TABLE t2 (id int);\n",
		"20240830000000_t.down.sql":   "DROP TABLE t2;\nDROP TABLE t1;\n",
		"20240831000000_idx.up.sql":   "CREATE INDEX CONCURRENTLY t2_id_idx ON t2 (id);\n",
		"20240831000000_idx.down.sql": "DROP INDEX CONCURRENTLY t2_id_idx;\n",
	}))

	out, err := runCompat(c, "migrate", "validate", "--dir", "file://"+dir+"?format=golang-migrate",
		"--dev-url", newCleanGateDatabase(c, adminURL, nil))

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
}
