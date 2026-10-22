//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/root"
	"ptah.run/internal/gitsource"
	"ptah.run/internal/gitsource/gitsourcetest"
	"ptah.run/internal/sourcecontent"
)

// gitPullReport is the part of a Git pull's --json document a consumer, such
// as Ptah Operator's fetch, reads.
type gitPullReport struct {
	Source        string `json:"source"`
	Commit        string `json:"commit"`
	ContentDigest string `json:"content_digest"`
	Files         []struct {
		Path string `json:"path"`
		Size int    `json:"size"`
	} `json:"files"`
}

// runPtahStdout runs one native command in-process and returns what it wrote
// to stdout, which is the JSON document alone.
func runPtahStdout(c *qt.C, args ...string) []byte {
	cmd := root.NewRootCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", out.String(), errOut.String()))
	return out.Bytes()
}

// The pull commands read a schema file and a migration directory from a
// branch over HTTPS, write the fetched bytes, and report the commit and the
// content digest; a later pull of that commit with that digest is admitted.
func TestGitPullCommandsE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	repository := gitsourcetest.NewRepository(c)
	files := map[string]string{
		"db/schema.hcl":                      schemaV1,
		"db/migrations/0001_create.up.sql":   "CREATE TABLE t (id int);\r\n",
		"db/migrations/0001_create.down.sql": "DROP TABLE t;\n",
	}
	commit := repository.Commit(c, files, "v1")
	server := gitsourcetest.ServeHTTPS(c, repository.Root, gitsourcetest.HTTPSOptions{Username: gitUser, Password: gitPassword})
	t.Setenv(gitsource.UsernameEnv, gitUser)
	t.Setenv(gitsource.PasswordEnv, gitPassword)
	dir := c.TempDir()

	schemaOut := filepath.Join(dir, "schema.hcl")
	var schema gitPullReport
	c.Assert(json.Unmarshal(runPtahStdout(c, "schema", "pull", server.Reference("app.git"),
		"--branch", "main", "--path", "db/schema.hcl", "--ca-file", server.CAFile, "--out", schemaOut, "--json"), &schema), qt.IsNil)

	c.Assert(schema.Source, qt.Equals, "git")
	c.Assert(schema.Commit, qt.Equals, commit)
	c.Assert(schema.ContentDigest, qt.Equals, schemaDigest(c, schemaV1))
	written, err := os.ReadFile(schemaOut)
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Equals, schemaV1)

	migrationsOut := filepath.Join(dir, "migrations")
	var migrations gitPullReport
	c.Assert(json.Unmarshal(runPtahStdout(c, "migrations", "pull", server.Reference("app.git"),
		"--branch", "main", "--path", "db/migrations", "--ca-file", server.CAFile, "--out", migrationsOut, "--json"), &migrations), qt.IsNil)

	c.Assert(migrations.Commit, qt.Equals, commit)
	c.Assert(migrations.Files, qt.HasLen, 2)
	up, err := os.ReadFile(filepath.Join(migrationsOut, "0001_create.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Equals, files["db/migrations/0001_create.up.sql"], qt.Commentf("line endings are kept as committed"))
	digest, err := sourcecontent.Digest([]sourcecontent.File{
		{Path: "0001_create.up.sql", Data: []byte(files["db/migrations/0001_create.up.sql"])},
		{Path: "0001_create.down.sql", Data: []byte(files["db/migrations/0001_create.down.sql"])},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(migrations.ContentDigest, qt.Equals, digest)

	// What an operator does for every operation after the first: the same
	// commit, held to the digest it was planned from.
	var again gitPullReport
	c.Assert(json.Unmarshal(runPtahStdout(c, "migrations", "pull", server.Reference("app.git"),
		"--branch", "main", "--commit", commit, "--expect-digest", digest, "--path", "db/migrations",
		"--ca-file", server.CAFile, "--out", filepath.Join(dir, "again"), "--json"), &again), qt.IsNil)
	c.Assert(again.ContentDigest, qt.Equals, digest)
}

func TestGitPullCommandsE2E_FailurePath(t *testing.T) {
	t.Run("another digest than expected", func(t *testing.T) {
		c := qt.New(t)
		repository := gitsourcetest.NewRepository(c)
		commit := repository.Commit(c, map[string]string{"db/schema.hcl": schemaV2}, "v2")
		server := gitsourcetest.ServeHTTPS(c, repository.Root, gitsourcetest.HTTPSOptions{Username: gitUser, Password: gitPassword})
		t.Setenv(gitsource.UsernameEnv, gitUser)
		t.Setenv(gitsource.PasswordEnv, gitPassword)
		out := filepath.Join(c.TempDir(), "schema.hcl")

		printed, err := runPtahNativeWithError("schema", "pull", server.Reference("app.git"),
			"--branch", "main", "--commit", commit, "--expect-digest", schemaDigest(c, schemaV1),
			"--path", "db/schema.hcl", "--ca-file", server.CAFile, "--out", out)

		c.Assert(err, qt.ErrorIs, gitsource.ErrDigest)
		c.Assert(printed, qt.Contains, "content digest differs")
		_, err = os.Lstat(out)
		c.Assert(os.IsNotExist(err), qt.IsTrue, qt.Commentf("a refused pull wrote %s", out))
	})
	t.Run("a Git flag on an OCI reference", func(t *testing.T) {
		c := qt.New(t)
		printed, err := runPtahNativeWithError("schema", "pull", "oci://registry.example.com/schema:1",
			"--branch", "main", "--out", filepath.Join(c.TempDir(), "schema.hcl"))
		c.Assert(err, qt.ErrorMatches, `--branch is for a git\+https:// or git\+ssh:// reference`)
		c.Assert(printed, qt.Contains, "--branch is for a git+https:// or git+ssh:// reference")
	})
}
