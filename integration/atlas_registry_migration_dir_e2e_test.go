//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	atlascmd "go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestAtlasRegistryMigrationDirE2E is the live acceptance for resolving an
// `atlas://` migration directory through Ptah's OCI backend
// (stokaro/ptah#1210).
//
// The vendor spelling names a repository and a pointer with no registry host in
// it. Ptah has no hosted account, so the reference means nothing until an
// operator says which OCI namespace it stands for -- and once they have, the
// directory is an ordinary artifact this repository already pushes and pulls.
//
// Both spellings are exercised against ONE pushed artifact, because both are
// what an adopting project actually contains: the reference typed on `--dir`,
// and the one an existing atlas.hcl already carries in `migration.dir`. A test
// covering one of them would leave the other free to keep refusing.
//
// The tag form is included because the VERB has to carry the query through, not
// only the resolver: a verb that handed the reference on without `?tag=` would
// pass every assertion about `atlas://app` while reading the wrong bytes.
// Whether the resolver maps tag and version onto the right OCI references, and
// whether a version stays put when a tag moves, is asserted by digest in
// integration/oci_registry_atlas_reference_e2e_test.go rather than a second
// time here.
func TestAtlasRegistryMigrationDirE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repository := fmt.Sprintf("ptah-atlas-dir-%d", time.Now().UnixNano())
	pushAtlasMigrationArtifact(c, ctx, registry, repository, "latest",
		"1_registry_latest.sql", "CREATE TABLE registry_latest (id INTEGER PRIMARY KEY);")
	pushAtlasMigrationArtifact(c, ctx, registry, repository, "prod",
		"1_registry_prod.sql", "CREATE TABLE registry_prod (id INTEGER PRIMARY KEY);")

	t.Setenv("PTAH_ATLAS_REGISTRY", registry)
	t.Setenv("PTAH_ATLAS_REGISTRY_PLAIN_HTTP", "1")

	tests := []struct {
		name      string
		args      func(project, dbURL string) []string
		wantTable string
	}{
		{
			name: "--dir names the repository",
			args: func(_, dbURL string) []string {
				return []string{"migrate", "apply", "--dir", "atlas://" + repository, "--url", dbURL}
			},
			wantTable: "registry_latest",
		},
		{
			name: "--dir names a tag",
			args: func(_, dbURL string) []string {
				return []string{"migrate", "apply", "--dir", "atlas://" + repository + "?tag=prod", "--url", dbURL}
			},
			wantTable: "registry_prod",
		},
		{
			name: "atlas.hcl migration.dir names the repository",
			args: func(project, _ string) []string {
				return []string{"migrate", "apply", "--config", "file://" + filepath.ToSlash(project), "--env", "local"}
			},
			wantTable: "registry_latest",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			dbURL := "sqlite://" + filepath.ToSlash(filepath.Join(dir, "status.db"))
			project := filepath.Join(dir, "atlas.hcl")
			c.Assert(os.WriteFile(project, []byte(`env "local" {
  url = "`+dbURL+`"
  migration {
    dir = "atlas://`+repository+`"
  }
}
`), 0o600), qt.IsNil)

			out, err := runAtlasCompat(test.args(project, dbURL)...)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			// The applied TABLE is the assertion rather than the count, and
			// that is what makes the tag row a result: an empty directory or
			// the wrong artifact both report a plausible status, and only the
			// bytes decide which table exists afterwards.
			c.Assert(sqliteTableNames(c, dbURL), qt.Contains, test.wantTable)
		})
	}
}

// TestAtlasRegistryMigrationDirValidatesE2E covers the OTHER resolution path.
//
// The integrity verbs -- validate, hash, new, rm -- take their directory
// through the shared migrate source rather than through each verb's own flag
// reading, so a fix applied to one is not applied to the other. Without this
// row, `migrate validate --dir atlas://app` would keep answering that only
// local file:// directories are supported while `migrate apply` read the same
// artifact.
func TestAtlasRegistryMigrationDirValidatesE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repository := fmt.Sprintf("ptah-atlas-dir-validate-%d", time.Now().UnixNano())
	pushAtlasMigrationArtifact(c, ctx, registry, repository, "latest",
		"1_registry_validate.sql", "CREATE TABLE registry_validate (id INTEGER PRIMARY KEY);")

	t.Setenv("PTAH_ATLAS_REGISTRY", registry)
	t.Setenv("PTAH_ATLAS_REGISTRY_PLAIN_HTTP", "1")

	out, err := runAtlasCompat("migrate", "validate", "--dir", "atlas://"+repository)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
}

// TestAtlasRegistryMigrationDirRefusesToWriteE2E is the other half: a registry
// directory is read and never written back to.
//
// Without the refusal the writing verb is handed a path with no local directory
// behind it, writes nowhere, and exits 0 while the operator is told it
// succeeded and the registry is untouched.
func TestAtlasRegistryMigrationDirRefusesToWriteE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repository := fmt.Sprintf("ptah-atlas-dir-write-%d", time.Now().UnixNano())
	pushAtlasMigrationArtifact(c, ctx, registry, repository, "latest",
		"1_registry_write.sql", "CREATE TABLE registry_write (id INTEGER PRIMARY KEY);")

	t.Setenv("PTAH_ATLAS_REGISTRY", registry)
	t.Setenv("PTAH_ATLAS_REGISTRY_PLAIN_HTTP", "1")

	tests := []struct {
		name string
		args []string
	}{
		{name: "migrate new", args: []string{"migrate", "new", "add_column", "--dir", "atlas://" + repository}},
		{name: "migrate hash", args: []string{"migrate", "hash", "--dir", "atlas://" + repository}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := runAtlasCompat(test.args...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("output:\n%s", out))
			// The refusal names the reference the operator typed and where a
			// write does belong. Naming the sentinel path instead would send
			// them to a directory that does not exist.
			c.Assert(err.Error(), qt.Contains, "came from a registry")
			c.Assert(err.Error(), qt.Contains, "ptah migrations push")
		})
	}
}

// TestAtlasRegistryMigrationDirLintsE2E is the control for the refusal above.
//
// `migrate lint` reads the directory and writes nothing, so it must NOT be
// refused. A guard that generalized from "registry" to "refuse" would pass
// every row of the write test while making the read verbs unusable.
func TestAtlasRegistryMigrationDirLintsE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repository := fmt.Sprintf("ptah-atlas-dir-lint-%d", time.Now().UnixNano())
	pushAtlasMigrationArtifact(c, ctx, registry, repository, "latest",
		"1_registry_lint.sql", "CREATE TABLE registry_lint (id INTEGER PRIMARY KEY);")

	t.Setenv("PTAH_ATLAS_REGISTRY", registry)
	t.Setenv("PTAH_ATLAS_REGISTRY_PLAIN_HTTP", "1")

	out, err := runAtlasCompat(
		"migrate", "lint",
		"--dir", "atlas://"+repository,
		"--dev-url", "sqlite://lint?mode=memory",
		"--latest", "1")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
}

// pushAtlasMigrationArtifact publishes one migration directory, hashed the way
// the Atlas-compatible layout expects, under the given tag.
//
// The directory is hashed before it is pushed because every read of it runs the
// atlas.sum gate: an artifact without one is refused by `migrate status`, and a
// test that skipped the hash would be asserting the gate rather than the
// resolver.
func pushAtlasMigrationArtifact(
	c *qt.C,
	ctx context.Context,
	registry, repository, tag, name, sql string,
) {
	c.Helper()
	dir := c.TB.(*testing.T).TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql+"\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	// Pushed through the migration artifact API rather than the raw client, so
	// the layer media type and the directory-format annotation are the ones the
	// pull validates. A raw push produces an artifact this resolver correctly
	// refuses, which would make the test measure the refusal.
	_, err = migrationartifact.Push(
		ctx,
		client,
		fmt.Sprintf("oci://%s/%s:%s", registry, repository, tag),
		os.DirFS(dir),
		migrationartifact.PushOptions{DirFormat: migrator.MigrationDirFormatAtlas},
	)
	c.Assert(err, qt.IsNil)
}

// runAtlasCompat runs one ptah-compat invocation in process and returns
// everything it wrote.
//
// In process rather than through a built binary because what is under test is
// the resolver this command reaches, and a subprocess would add a build step
// and a second copy of the environment for nothing.
func runAtlasCompat(args ...string) (string, error) {
	cmd := atlascmd.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// sqliteTableNames lists the tables a migration created, so a test can say
// WHICH artifact it read rather than how many files it counted.
func sqliteTableNames(c *qt.C, dbURL string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	rows, err := conn.Query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
