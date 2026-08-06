package atlas_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// testVerbWorkspace is one materialized workspace for the Atlas-shaped test
// verbs: a hashless Atlas migration directory, a Go-annotation model directory,
// a seed directory, and the case-set directory the verb is pointed at.
type testVerbWorkspace struct {
	migrationsDir string
	modelsDir     string
	seedsDir      string
	casesDir      string
}

// writeTestVerbWorkspace materializes a workspace whose only case set has a
// seed step with no inline dir, so the case set can only run when the run
// itself supplies a default seed directory.
//
// migratePreamble is the extra YAML step each verb needs before the seed can
// insert rows: `migrate test` has to migrate first, while `schema test`
// converges the desired schema before any step runs and rejects migrate_to.
func writeTestVerbWorkspace(c *qt.C, migratePreamble string) testVerbWorkspace {
	c.Helper()
	workspace := testVerbWorkspace{
		migrationsDir: c.TempDir(),
		modelsDir:     c.TempDir(),
		seedsDir:      c.TempDir(),
		casesDir:      c.TempDir(),
	}
	c.Assert(os.WriteFile(
		filepath.Join(workspace.migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(workspace.modelsDir, "user.go"), []byte(
		"package models\n\n"+
			"//ptah:schema:table name=\"users\"\n"+
			"type User struct {\n"+
			"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n"+
			"\tID int64\n\n"+
			"\t//ptah:schema:field name=\"name\" type=\"TEXT\" not_null=\"true\"\n"+
			"\tName string\n"+
			"}\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(workspace.seedsDir, "010_users.test.sql"),
		[]byte("INSERT INTO users (id, name) VALUES (1, 'ada');\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(workspace.casesDir, "seed.yaml"), []byte(
		"cases:\n"+
			"  - name: seeded\n"+
			"    steps:\n"+
			migratePreamble+
			"      - name: seed\n"+
			"        seed:\n"+
			"          env: test\n"+
			"      - name: read back\n"+
			"        assert:\n"+
			"          query: SELECT name FROM users\n"+
			"          scalar: ada\n"), 0o600), qt.IsNil)
	return workspace
}

// migrateTestArgs is the Atlas-form argv that points `migrate test` at a
// workspace. No --dev-url is passed: the native runner provisions a throwaway
// SQLite database per run, which keeps every row of a table independent.
func migrateTestArgs(workspace testVerbWorkspace) []string {
	return []string{
		"migrate", "test", workspace.casesDir,
		"--dir", "file://" + workspace.migrationsDir,
	}
}

// schemaTestArgs is the Atlas-form argv that points `schema test` at a
// workspace.
func schemaTestArgs(workspace testVerbWorkspace) []string {
	return []string{
		"schema", "test", workspace.casesDir,
		"-u", "file://" + workspace.modelsDir,
	}
}

const migrateSeedPreamble = "      - name: migrate\n        migrate_to: latest\n"

// TestCompatCommand_TestVerbsForwardSeedDirectory pins --seed-dir on both
// Atlas-shaped test verbs.
//
// The fixture discriminates because the same case set is run twice against the
// same binary: its seed step carries no inline dir, so it is refused as invalid
// without a run-level seed directory and passes with one. A case set whose seed
// step named its own dir would pass either way and would prove nothing.
func TestCompatCommand_TestVerbsForwardSeedDirectory(t *testing.T) {
	tests := []struct {
		name    string
		argv    func(workspace testVerbWorkspace) []string
		preface string
	}{
		{name: "migrate test", argv: migrateTestArgs, preface: migrateSeedPreamble},
		{name: "schema test", argv: schemaTestArgs, preface: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			workspace := writeTestVerbWorkspace(c, tt.preface)
			base := tt.argv(workspace)

			withoutOut, withoutErr := runCompatArgs(base)
			c.Assert(withoutErr, qt.ErrorMatches,
				`invalid test cases: .*seed requires a dir or a run-level seed directory`,
				qt.Commentf("%s", withoutOut))

			withOut, withErr := runCompatArgs(append(slices.Clone(base), "--seed-dir", workspace.seedsDir))
			c.Assert(withErr, qt.IsNil, qt.Commentf("%s", withOut))
			c.Assert(withOut, qt.Contains, `PASS  case "seeded"`)
			c.Assert(withOut, qt.Contains, "seeded 1 file(s)")
			c.Assert(withOut, qt.Contains, "1 cases, 1 passed, 0 failed")
		})
	}
}

// TestCompatCommand_TestVerbsSeedDirectoryURLForms covers the two spellings a
// directory has on this surface. Every other Atlas-shaped directory flag takes
// a file:// URL, so a seed directory written that way has to resolve rather
// than reach the native runner as a path that cannot exist.
func TestCompatCommand_TestVerbsSeedDirectoryURLForms(t *testing.T) {
	tests := []struct {
		name    string
		argv    func(workspace testVerbWorkspace) []string
		preface string
		seedDir func(workspace testVerbWorkspace) string
		check   func(c *qt.C, out string, err error)
	}{
		{
			name:    "migrate test file URL",
			argv:    migrateTestArgs,
			preface: migrateSeedPreamble,
			seedDir: func(workspace testVerbWorkspace) string { return "file://" + workspace.seedsDir },
			check:   assertSeedDirectoryRan,
		},
		{
			name:    "schema test file URL",
			argv:    schemaTestArgs,
			preface: "",
			seedDir: func(workspace testVerbWorkspace) string { return "file://" + workspace.seedsDir },
			check:   assertSeedDirectoryRan,
		},
		{
			name:    "migrate test plain path is the control",
			argv:    migrateTestArgs,
			preface: migrateSeedPreamble,
			seedDir: func(workspace testVerbWorkspace) string { return workspace.seedsDir },
			check:   assertSeedDirectoryRan,
		},
		{
			name:    "schema test database URL is refused",
			argv:    schemaTestArgs,
			preface: "",
			seedDir: func(workspace testVerbWorkspace) string { return "sqlite://" + workspace.seedsDir },
			check:   assertSeedDirectoryRefused,
		},
		{
			name:    "migrate test database URL is refused",
			argv:    migrateTestArgs,
			preface: migrateSeedPreamble,
			seedDir: func(workspace testVerbWorkspace) string { return "sqlite://" + workspace.seedsDir },
			check:   assertSeedDirectoryRefused,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			workspace := writeTestVerbWorkspace(c, tt.preface)
			argv := append(slices.Clone(tt.argv(workspace)), "--seed-dir", tt.seedDir(workspace))

			out, err := runCompatArgs(argv)

			tt.check(c, out, err)
		})
	}
}

func assertSeedDirectoryRan(c *qt.C, out string, err error) {
	c.Helper()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "seeded 1 file(s)")
}

func assertSeedDirectoryRefused(c *qt.C, out string, err error) {
	c.Helper()
	c.Assert(err, qt.ErrorMatches,
		`atlas (migrate|schema) test --seed-dir: a seed directory is a local path or a file:// URL`,
		qt.Commentf("%s", out))
}

// testVerbReport is the machine-readable report shape both native runners
// write. Only the fields this test reads are declared.
type testVerbReport struct {
	Kind   string `json:"kind"`
	Total  int    `json:"total"`
	Passed int    `json:"passed"`
	Failed int    `json:"failed"`
}

// TestCompatCommand_TestVerbsForwardReportFormat pins --report on both
// Atlas-shaped test verbs.
//
// The assertion is that the JSON parses and reports the run, not merely that
// the flag is accepted: a flag that was accepted and dropped would leave the
// default text report on stdout, which is not JSON.
func TestCompatCommand_TestVerbsForwardReportFormat(t *testing.T) {
	tests := []struct {
		name    string
		argv    func(workspace testVerbWorkspace) []string
		preface string
		kind    string
	}{
		{name: "migrate test", argv: migrateTestArgs, preface: migrateSeedPreamble, kind: "MIGRATION"},
		{name: "schema test", argv: schemaTestArgs, preface: "", kind: "SCHEMA"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			workspace := writeTestVerbWorkspace(c, tt.preface)
			base := append(slices.Clone(tt.argv(workspace)), "--seed-dir", workspace.seedsDir)

			out, err := runCompatArgs(append(slices.Clone(base), "--report", "json"))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			var report testVerbReport
			c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
			c.Assert(report.Kind, qt.Equals, tt.kind)
			c.Assert(report.Total, qt.Equals, 1)
			c.Assert(report.Passed, qt.Equals, 1)
			c.Assert(report.Failed, qt.Equals, 0)

			// The value is forwarded verbatim, so the native runner stays the
			// single place that decides which formats exist. A mapper that
			// swallowed unknown values would run the default text report here
			// and exit 0.
			badOut, badErr := runCompatArgs(append(slices.Clone(base), "--report", "nosuch"))
			c.Assert(badErr, qt.ErrorMatches, `unsupported report format "nosuch": want text, json, or html`,
				qt.Commentf("%s", badOut))
		})
	}
}

// dockerDevURLRefusal is the exact diagnostic both test verbs owe a docker://
// dev database URL, in the wording migrate diff, migrate lint and
// migrations validate already use for the same input.
const dockerDevURLRefusal = `atlas (migrate|schema) test --dev-url: docker --dev-url values are accepted by Atlas,` +
	` but Ptah requires a directly connectable dev database URL for test cases`

// TestCompatCommand_TestVerbsRefuseDockerDevURL covers every route a dev
// database URL reaches these verbs by, on both of them.
//
// Fixing only the spelling an issue happened to name would look complete while
// the environment twin and the atlas.hcl env still reached the connector, whose
// answer -- "unsupported database dialect: docker" -- names an internal
// classification rather than the thing the caller has to change.
func TestCompatCommand_TestVerbsRefuseDockerDevURL(t *testing.T) {
	tests := []struct {
		name    string
		preface string
		argv    func(c *qt.C, t *testing.T, workspace testVerbWorkspace) []string
		check   func(c *qt.C, out string, err error)
	}{
		{
			name:    "migrate test --dev-url",
			preface: migrateSeedPreamble,
			argv: func(_ *qt.C, _ *testing.T, workspace testVerbWorkspace) []string {
				return append(migrateTestArgs(workspace), "--dev-url", "docker://postgres/16/dev")
			},
			check: assertDockerDevURLRefused,
		},
		{
			name:    "schema test --dev-url",
			preface: "",
			argv: func(_ *qt.C, _ *testing.T, workspace testVerbWorkspace) []string {
				return append(schemaTestArgs(workspace), "--dev-url", "docker://postgres/16/dev")
			},
			check: assertDockerDevURLRefused,
		},
		{
			name:    "migrate test PTAH_DEV_URL twin",
			preface: migrateSeedPreamble,
			argv: func(_ *qt.C, t *testing.T, workspace testVerbWorkspace) []string {
				t.Setenv("PTAH_DEV_URL", "docker://postgres/16/dev")
				return migrateTestArgs(workspace)
			},
			check: assertDockerDevURLRefused,
		},
		{
			name:    "schema test PTAH_DEV_URL twin",
			preface: "",
			argv: func(_ *qt.C, t *testing.T, workspace testVerbWorkspace) []string {
				t.Setenv("PTAH_DEV_URL", "docker://postgres/16/dev")
				return schemaTestArgs(workspace)
			},
			check: assertDockerDevURLRefused,
		},
		{
			name:    "migrate test atlas.hcl dev",
			preface: migrateSeedPreamble,
			argv: func(c *qt.C, t *testing.T, workspace testVerbWorkspace) []string {
				writeDockerDevProject(c, t, workspace)
				return []string{"migrate", "test", workspace.casesDir, "--env", "local"}
			},
			check: assertDockerDevURLRefused,
		},
		{
			name:    "schema test atlas.hcl dev",
			preface: "",
			argv: func(c *qt.C, t *testing.T, workspace testVerbWorkspace) []string {
				writeDockerDevProject(c, t, workspace)
				return []string{"schema", "test", workspace.casesDir, "--env", "local"}
			},
			check: assertDockerDevURLRefused,
		},
		{
			name:    "a non-docker dialect still answers at the connector",
			preface: "",
			argv: func(_ *qt.C, _ *testing.T, workspace testVerbWorkspace) []string {
				return append(schemaTestArgs(workspace), "--dev-url", "oracle://host/dev")
			},
			check: func(c *qt.C, out string, err error) {
				c.Helper()
				c.Assert(err, qt.ErrorMatches, `connect to test database: unsupported database dialect: oracle`,
					qt.Commentf("%s", out))
			},
		},
		{
			name:    "a directly connectable dev URL is the passing control",
			preface: "",
			argv: func(c *qt.C, _ *testing.T, workspace testVerbWorkspace) []string {
				return append(schemaTestArgs(workspace), "--dev-url", freshDevURL(c))
			},
			check: func(c *qt.C, out string, err error) {
				c.Helper()
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			workspace := writeTestVerbWorkspace(c, tt.preface)
			// Every row supplies the seed directory so the case set is valid.
			// Without it the run is refused before any dev database is
			// consulted, and a row that never consults the dev URL cannot tell
			// a docker refusal from its absence: the rows would stay green with
			// the refusal removed.
			argv := append(tt.argv(c, t, workspace), "--seed-dir", workspace.seedsDir)

			out, err := runCompatArgs(argv)

			tt.check(c, out, err)
		})
	}
}

func assertDockerDevURLRefused(c *qt.C, out string, err error) {
	c.Helper()
	c.Assert(err, qt.ErrorMatches, dockerDevURLRefusal, qt.Commentf("%s", out))
}

// writeDockerDevProject writes an atlas.hcl whose env supplies the docker dev
// database URL, and moves the process into its directory so the default
// --config location finds it.
func writeDockerDevProject(c *qt.C, t *testing.T, workspace testVerbWorkspace) {
	c.Helper()
	dir := c.TempDir()
	t.Chdir(dir)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(
		"env \"local\" {\n"+
			"  src = \"file://"+filepath.ToSlash(workspace.modelsDir)+"\"\n"+
			"  migration {\n"+
			"    dir = \"file://"+filepath.ToSlash(workspace.migrationsDir)+"\"\n"+
			"  }\n"+
			"  dev = \"docker://postgres/16/dev\"\n"+
			"}\n"), 0o600), qt.IsNil)
}
