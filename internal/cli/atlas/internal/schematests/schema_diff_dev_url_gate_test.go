package schematests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// `schema diff` refuses a side that needs a dev database when --dev-url is
// absent or empty, in the words of the pinned community binary v1.3.0
// (stokaro/ptah#3676). Measured there on 2026-09-26, on PostgreSQL and on
// SQLite, every refusal exits 1 with standard output empty. Two sentences
// exist, and the side refused first decides which one is printed.
const (
	diffDevURLEmpty    = "Error: --dev-url cannot be empty\n"
	diffDevURLEmptySQL = "Error: --dev-url cannot be empty. See: https://atlasgo.io/atlas-schema/sql#dev-database\n"
)

// diffWithoutDevURLEnvVar is the opt-in that allows comparing a schema file
// with a database without a dev database. It is spelled here because the
// constant it mirrors is unexported in package atlas.
const diffWithoutDevURLEnvVar = "PTAH_ATLAS_DIFF_WITHOUT_DEV_URL"

// schemaDiffDevURLFixture is one SQLite database holding a table, and one
// source of every local kind declaring that table and one more, as file://
// URLs. A comparison that reads a local source therefore plans the second
// table, which is what separates a run that compared from one that stopped.
type schemaDiffDevURLFixture struct {
	db           string
	sqlFile      string
	hclFile      string
	sqlDir       string
	hclDir       string
	migrationDir string
	dev          string
}

const (
	diffDevURLDatabaseSQL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"
	diffDevURLSourceSQL   = diffDevURLDatabaseSQL + "CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n"
	diffDevURLSourceHCL   = "schema \"main\" {\n}\n" +
		"table \"widgets\" {\n  schema = schema.main\n" +
		"  column \"id\" {\n    null = false\n    type = integer\n  }\n" +
		"  primary_key {\n    columns = [column.id]\n  }\n}\n" +
		"table \"gadgets\" {\n  schema = schema.main\n" +
		"  column \"id\" {\n    null = false\n    type = integer\n  }\n" +
		"  primary_key {\n    columns = [column.id]\n  }\n}\n"
)

func newSchemaDiffDevURLFixture(c *qt.C) schemaDiffDevURLFixture {
	c.Helper()
	root := c.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(root, name)
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
		return path
	}
	sqlFile := write("schema.sql", diffDevURLSourceSQL)
	hclFile := write("schema.hcl", diffDevURLSourceHCL)
	write("sqldir/schema.sql", diffDevURLSourceSQL)
	write("hcldir/schema.hcl", diffDevURLSourceHCL)
	write("migrations/1_init.sql", diffDevURLSourceSQL)
	migrationDir := filepath.Join(root, "migrations")
	_, err := migratesum.WriteWithFormat(migrationDir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return schemaDiffDevURLFixture{
		db:           "sqlite://" + atlastest.SeedSQLiteDB(c, diffDevURLDatabaseSQL),
		sqlFile:      "file://" + sqlFile,
		hclFile:      "file://" + hclFile,
		sqlDir:       "file://" + filepath.Join(root, "sqldir"),
		hclDir:       "file://" + filepath.Join(root, "hcldir"),
		migrationDir: "file://" + migrationDir,
		dev:          "sqlite://" + filepath.Join(root, "dev.db"),
	}
}

func runSchemaDiffArgs(args ...string) (stdout, stderr string, err error) {
	return atlastest.RunCompat(append([]string{"schema", "diff"}, args...)...)
}

// TestSchemaDiffWithoutDevURL_FailurePath is the regression test for
// stokaro/ptah#3676. Without the refusal, a schema file compared with a
// database plans and exits 0 with no dev database, where the community binary
// refuses. Each row is that binary's answer to the same argv.
func TestSchemaDiffWithoutDevURL_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       func(fx schemaDiffDevURLFixture) []string
		wantStderr string
	}{
		{
			name:       "a SQL file on --from",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.sqlFile, "--to", fx.db} },
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name:       "a SQL file on --to",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.db, "--to", fx.sqlFile} },
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name:       "an HCL file on --from",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.hclFile, "--to", fx.db} },
			wantStderr: diffDevURLEmpty,
		},
		{
			name:       "an HCL file on --to",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.db, "--to", fx.hclFile} },
			wantStderr: diffDevURLEmpty,
		},
		{
			name:       "a directory of SQL files",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.sqlDir, "--to", fx.db} },
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name:       "a directory of HCL files",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.hclDir, "--to", fx.db} },
			wantStderr: diffDevURLEmpty,
		},
		{
			name:       "a migration directory",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.migrationDir, "--to", fx.db} },
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name:       "SQL on --from decides over HCL on --to",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.sqlFile, "--to", fx.hclFile} },
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name:       "HCL on --from decides over SQL on --to",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.hclFile, "--to", fx.sqlFile} },
			wantStderr: diffDevURLEmpty,
		},
		{
			name: "a migration directory on --from decides over HCL on --to",
			args: func(fx schemaDiffDevURLFixture) []string {
				return []string{"--from", fx.migrationDir, "--to", fx.hclFile}
			},
			wantStderr: diffDevURLEmptySQL,
		},
		{
			name: "HCL on --from decides over a migration directory on --to",
			args: func(fx schemaDiffDevURLFixture) []string {
				return []string{"--from", fx.hclFile, "--to", fx.migrationDir}
			},
			wantStderr: diffDevURLEmpty,
		},
		{
			name: "an explicitly empty --dev-url",
			args: func(fx schemaDiffDevURLFixture) []string {
				return []string{"--from", fx.sqlFile, "--to", fx.db, "--dev-url", ""}
			},
			wantStderr: diffDevURLEmptySQL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newSchemaDiffDevURLFixture(c)

			stdout, stderr, err := runSchemaDiffArgs(tt.args(fx)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Equals, tt.wantStderr)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestSchemaDiffWithoutDevURL_HappyPath holds the refusal to its scope: a
// dev database given on the flag or by the atlas.hcl env answers it, and two
// databases need none. The community binary exits 0 on each of these.
func TestSchemaDiffWithoutDevURL_HappyPath(t *testing.T) {
	t.Run("two databases", func(t *testing.T) {
		c := qt.New(t)
		fx := newSchemaDiffDevURLFixture(c)

		stdout, stderr, err := runSchemaDiffArgs("--from", fx.db, "--to", fx.db)

		c.Assert(err, qt.IsNil, qt.Commentf("stderr=%q", stderr))
		c.Assert(stdout, qt.Contains, "Schemas are synced")
	})

	t.Run("a SQL file with --dev-url", func(t *testing.T) {
		c := qt.New(t)
		fx := newSchemaDiffDevURLFixture(c)

		stdout, stderr, err := runSchemaDiffArgs("--from", fx.db, "--to", fx.sqlFile, "--dev-url", fx.dev)

		c.Assert(err, qt.IsNil, qt.Commentf("stderr=%q", stderr))
		c.Assert(stdout, qt.Contains, `CREATE TABLE "gadgets"`)
	})

	t.Run("a SQL file with the atlas.hcl dev database", func(t *testing.T) {
		c := qt.New(t)
		fx := newSchemaDiffDevURLFixture(c)
		config := filepath.Join(c.TempDir(), "atlas.hcl")
		c.Assert(os.WriteFile(config, []byte("env \"local\" {\n  dev = \""+filepath.ToSlash(fx.dev)+"\"\n}\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runSchemaDiffArgs("--config", "file://"+filepath.ToSlash(config), "--env", "local",
			"--from", fx.db, "--to", fx.sqlFile)

		c.Assert(err, qt.IsNil, qt.Commentf("stderr=%q", stderr))
		c.Assert(stdout, qt.Contains, `CREATE TABLE "gadgets"`)
	})
}

// TestSchemaDiffWithoutDevURLEnvVar_HappyPath pins the capability the default
// refusal is paired with: compatibility never removes a capability, so
// comparing a schema file with a database without a dev database, as native
// `ptah schema diff` does, stays reachable on this surface.
func TestSchemaDiffWithoutDevURLEnvVar_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		args       func(fx schemaDiffDevURLFixture) []string
		wantInPlan string
	}{
		{
			name:       "a SQL file on --from",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.sqlFile, "--to", fx.db} },
			wantInPlan: `DROP TABLE IF EXISTS "gadgets"`,
		},
		{
			name:       "an HCL file on --to",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.db, "--to", fx.hclFile} },
			wantInPlan: `CREATE TABLE "main"."gadgets"`,
		},
		{
			name:       "a directory of HCL files",
			args:       func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.hclDir, "--to", fx.db} },
			wantInPlan: `DROP TABLE IF EXISTS "main"."gadgets"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(diffWithoutDevURLEnvVar, "1")
			fx := newSchemaDiffDevURLFixture(c)

			stdout, stderr, err := runSchemaDiffArgs(tt.args(fx)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr=%q", stderr))
			c.Assert(stdout, qt.Contains, tt.wantInPlan)
		})
	}
}

// TestSchemaDiffWithoutDevURLEnvVar_FailurePath: the opt-in lifts the refusal
// and nothing else. Two schema files leave no dialect to compare in, and a
// migration directory has nowhere to be replayed, so each keeps its own
// diagnostic. A malformed value is refused on every run, including one that
// supplies a dev database and one that needs none.
func TestSchemaDiffWithoutDevURLEnvVar_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		args    func(fx schemaDiffDevURLFixture) []string
		wantErr string
	}{
		{
			name:    "two schema files",
			value:   "1",
			args:    func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.hclFile, "--to", fx.sqlFile} },
			wantErr: `--dev-url is required for local schema file diffing`,
		},
		{
			name:  "a migration directory",
			value: "1",
			args: func(fx schemaDiffDevURLFixture) []string {
				return []string{"--from", fx.migrationDir, "--to", fx.db}
			},
			wantErr: `--from "file://.*" is a migration directory; --dev-url is required to replay it on a dev database`,
		},
		{
			name:    "a malformed value with two databases",
			value:   "tru",
			args:    func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.db, "--to", fx.db} },
			wantErr: `invalid boolean value "tru" for PTAH_ATLAS_DIFF_WITHOUT_DEV_URL.*`,
		},
		{
			name:  "a malformed value with --dev-url",
			value: "tru",
			args: func(fx schemaDiffDevURLFixture) []string {
				return []string{"--from", fx.sqlFile, "--to", fx.db, "--dev-url", fx.dev}
			},
			wantErr: `invalid boolean value "tru" for PTAH_ATLAS_DIFF_WITHOUT_DEV_URL.*`,
		},
		{
			name:    "an exported empty value",
			value:   "",
			args:    func(fx schemaDiffDevURLFixture) []string { return []string{"--from", fx.db, "--to", fx.db} },
			wantErr: `invalid boolean value "" for PTAH_ATLAS_DIFF_WITHOUT_DEV_URL.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(diffWithoutDevURLEnvVar, tt.value)
			fx := newSchemaDiffDevURLFixture(c)

			stdout, stderr, err := runSchemaDiffArgs(tt.args(fx)...)

			c.Assert(err, qt.ErrorMatches, tt.wantErr, qt.Commentf("stderr=%q", stderr))
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestSchemaDiffWithoutDevURLEnvVarUnderStrictCE_FailurePath: the opt-in adds
// behavior the community binary does not have, so the CE-only policy refuses it
// by name at the process boundary, where `ptah-compat` resolves its policy.
func TestSchemaDiffWithoutDevURLEnvVarUnderStrictCE_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ATLAS_STRICT_COMPAT", "1")
	t.Setenv(diffWithoutDevURLEnvVar, "1")

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.ErrorMatches, `PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_DIFF_WITHOUT_DEV_URL`)
	c.Assert(policy.IsStrictCE(), qt.IsFalse)
}
