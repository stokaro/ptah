//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// schemaCommentMigrationDir writes a migration directory holding file, with the
// checksum the reference binary computes, and returns its file:// URL.
func schemaCommentMigrationDir(c *qt.C, reference, file string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_a.sql"), []byte(file), 0o600), qt.IsNil)
	url := "file://" + filepath.ToSlash(dir)
	hashed, err := exec.CommandContext(c.Context(), reference, "migrate", "hash", "--dir", url).CombinedOutput() // #nosec G204 G702 -- operator-provided pinned reference binary
	c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))
	return url
}

// schemaCommentDocument writes a desired state and returns its file:// URL.
func schemaCommentDocument(c *qt.C, body string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return "file://" + filepath.ToSlash(path)
}

// TestMigrateDiffReplaysASchemaCommentE2E replays a directory that comments on a
// schema, on a dev database whose reset returns a schema's state. The pinned
// community binary takes these files, measured on v1.3.0, so a refusal here would
// refuse a history that binary wrote. Both find the directory synced. The dev
// database's public schema reads back as it was claimed, comment included, and the
// schema the file created is gone.
func TestMigrateDiffReplaysASchemaCommentE2E(t *testing.T) {
	tests := []struct {
		name, file, document string
	}{
		{
			name:     "a schema the file created",
			file:     "CREATE SCHEMA \"es_app\";\nCOMMENT ON SCHEMA \"es_app\" IS 'application';\n",
			document: "schema \"public\" {\n}\n\nschema \"es_app\" {\n  comment = \"application\"\n}\n",
		},
		{
			name:     "public",
			file:     "COMMENT ON SCHEMA \"public\" IS 'changed';\n",
			document: "schema \"public\" {\n  comment = \"changed\"\n}\n",
		},
	}
	reference := atlasReferenceBinary(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newEmptyReadScopeDatabase(c, "comment_dev")
			claimed := devPublicSchemaState(c, dev)
			to := schemaCommentDocument(c, test.document)
			dir := schemaCommentMigrationDir(c, reference, test.file)
			args := []string{"migrate", "diff", "next", "--dev-url", dev, "--to", to, "--dir", dir}

			compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, args...)

			c.Assert(compat.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
			c.Assert(compat.Stdout, qt.Contains, "synced")
			c.Assert(devPublicSchemaState(c, dev), qt.Equals, claimed)
			c.Assert(declaredSchemaHolds(c, dev, "es_app"), qt.IsFalse)

			community, err := exec.CommandContext(c.Context(), reference, args...).CombinedOutput() // #nosec G204 G702 -- operator-provided pinned reference binary

			c.Assert(err, qt.IsNil, qt.Commentf("%s", community))
			c.Assert(string(community), qt.Contains, "synced")
		})
	}
}

// TestMigrationsGenerateReplaysASchemaCommentE2E is the native surface, which reaches
// the same replay.
func TestMigrationsGenerateReplaysASchemaCommentE2E(t *testing.T) {
	c := qt.New(t)
	dev := newEmptyReadScopeDatabase(c, "comment_dev")
	claimed := devPublicSchemaState(c, dev)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_a.up.sql"),
		[]byte("CREATE SCHEMA \"es_app\";\nCOMMENT ON SCHEMA \"es_app\" IS 'application';\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_a.down.sql"), []byte("DROP SCHEMA \"es_app\";\n"), 0o600), qt.IsNil)
	document := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(document, []byte("schema \"public\" {\n}\n\nschema \"es_app\" {\n  comment = \"application\"\n}\n"), 0o600), qt.IsNil)

	got := runPtahIn(c, "migrations", "generate", "--replay", "--dev-url", dev, "--schema-file", document,
		"--migrations-dir", dir, "--name", "next")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "no migration files generated")
	c.Assert(devPublicSchemaState(c, dev), qt.Equals, claimed)
	c.Assert(declaredSchemaHolds(c, dev, "es_app"), qt.IsFalse)
}

// TestMigrateDiffRefusesWhatAResetCannotConfineE2E holds the rest of the rule, where
// the community binary takes the file and ptah-compat is stricter, deliberately.
// A URL pinned to one schema is reset without the state, so the comment would stay.
// A privilege depends on the server's roles. A protected namespace is not the run's.
// The dev database reads back as it was claimed in each case.
func TestMigrateDiffRefusesWhatAResetCannotConfineE2E(t *testing.T) {
	const public = "schema \"public\" {\n}\n"
	tests := []struct {
		name, file, pin, wantErr string
	}{
		{
			name: "a URL pinned to one schema", pin: "public",
			file:    "COMMENT ON SCHEMA \"public\" IS 'changed';\n",
			wantErr: "rejects COMMENT ON global metadata",
		},
		{
			name:    "a privilege on a schema",
			file:    "GRANT USAGE ON SCHEMA public TO PUBLIC;\n",
			wantErr: "rejects GRANT database or global privilege",
		},
		{
			name:    "a privilege on a schema the file created",
			file:    "CREATE SCHEMA \"es_app\";\nGRANT USAGE ON SCHEMA \"es_app\" TO PUBLIC;\n",
			wantErr: "rejects GRANT database or global privilege",
		},
		{
			name:    "a protected namespace",
			file:    "COMMENT ON SCHEMA pg_catalog IS 'x';\n",
			wantErr: `rejects protected namespace "pg_catalog" mutation`,
		},
	}
	reference := atlasReferenceBinary(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newEmptyReadScopeDatabase(c, "comment_dev")
			claimed := devPublicSchemaState(c, dev)
			args := []string{"migrate", "diff", "next", "--dev-url", declaredSchemaURL(c, dev, test.pin),
				"--to", schemaCommentDocument(c, public), "--dir", schemaCommentMigrationDir(c, reference, test.file)}

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, args...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.wantErr)
			c.Assert(devPublicSchemaState(c, dev), qt.Equals, claimed)
		})
	}
}
