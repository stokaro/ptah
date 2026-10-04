//go:build integration

package ydb_test

import (
	"context"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/exeext"
)

// The schema the binary applies, in Go annotations, so the run goes through
// the parser, the renderer, the planner and the writer the way an operator's
// does.
const e2eEntities = `package entities

//ptah:schema:table name="items" schema="ptah_ydb_e2e/nested"
type Item struct {
	//ptah:schema:field name="id" type="BIGINT" primary auto_increment
	ID int64
	//ptah:schema:field name="title" type="VARCHAR(80)" not_null default="untitled"
	Title string
	//ptah:schema:field name="created_at" type="TIMESTAMP"
	CreatedAt string
	//ptah:schema:index name="idx_items_title" fields="title"
	_ int
}
`

// TestYDBBinary_AppliesReadsAndDropsASchema drives the shipped binary against
// a live YDB database: `schema apply` builds the declared table in a nested
// directory, `db read` describes it, `schema compare` finds nothing left to
// change, and `db drop-all` drops it and removes the directories it emptied.
//
// It shares each server with every test in this package and runs among them
// one at a time, as the tests of one package do; drop-all empties the whole
// database, which is why no other package in the contour writes to YDB.
func TestYDBBinary_AppliesReadsAndDropsASchema(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			entities := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(entities, "items.go"), []byte(e2eEntities), 0o600), qt.IsNil)

			applied, applyErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", entities,
				"--auto-approve")
			read, readErr := runBinary(ctx, binary, "db", "read", "--db-url", url, "--schemas", "ptah_ydb_e2e/nested")
			compared, compareErr := runBinary(ctx, binary, "schema", "compare", "--db-url", url, "--root-dir", entities,
				"--schemas", "ptah_ydb_e2e/nested", "--exit-code")
			dropped, dropErr := runBinary(ctx, binary, "db", "drop-all", "--db-url", url, "--auto-approve")

			c.Assert(applyErr, qt.IsNil, qt.Commentf("schema apply:\n%s", applied))
			c.Assert(readErr, qt.IsNil, qt.Commentf("db read:\n%s", read))
			c.Assert(read, qt.Contains, "CREATE TABLE `ptah_ydb_e2e/nested/items` (")
			c.Assert(read, qt.Contains, "`id` BigSerial NOT NULL,")
			c.Assert(read, qt.Contains, "`title` Utf8 NOT NULL DEFAULT 'untitled'u,")
			c.Assert(read, qt.Contains, "INDEX `idx_items_title` GLOBAL SYNC ON (`title`)")
			c.Assert(compareErr, qt.IsNil, qt.Commentf("schema compare:\n%s", compared))
			c.Assert(dropErr, qt.IsNil, qt.Commentf("db drop-all:\n%s", dropped))

			conn := openYDB(c, line)
			live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(live.Tables, qt.HasLen, 0)
			c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), "ptah_ydb_e2e")
		})
	}
}

// buildBinary builds ptah from this checkout.
func buildBinary(c *qt.C, ctx context.Context) string {
	c.Helper()
	_, file, _, ok := runtime.Caller(0)
	c.Assert(ok, qt.IsTrue)
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	binary := filepath.Join(c.TempDir(), "ptah"+exeext.Suffix)
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/ptah")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go build:\n%s", output))
	return binary
}

// runBinary runs the binary and returns what it wrote to both streams.
func runBinary(ctx context.Context, binary string, args ...string) (string, error) {
	output, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
	return string(output), err
}

// directoryNames lists the entries of a directory under the database root,
// asked of the scheme service directly rather than through Ptah's reader.
func directoryNames(c *qt.C, ctx context.Context, line ydbLine, segments ...string) []string {
	c.Helper()
	driver, err := ydbsdk.Open(ctx, dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	defer func() { _ = driver.Close(context.Background()) }()
	directory, err := driver.Scheme().ListDirectory(ctx, path.Join(append([]string{driver.Name()}, segments...)...))
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(directory.Children))
	for _, child := range directory.Children {
		names = append(names, child.Name)
	}
	slices.Sort(names)
	return names
}
