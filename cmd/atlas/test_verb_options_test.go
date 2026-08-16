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

// TestCompatCommand_TestVerbsResolveSeedDirectoryURLForms covers the two
// spellings a directory has on this surface. Every other Atlas-shaped directory
// flag takes a file:// URL, so a seed directory written that way has to resolve
// rather than reach the native runner as a path that cannot exist.
func TestCompatCommand_TestVerbsResolveSeedDirectoryURLForms(t *testing.T) {
	tests := []struct {
		name    string
		argv    func(workspace testVerbWorkspace) []string
		preface string
		seedDir func(workspace testVerbWorkspace) string
	}{
		{
			name:    "migrate test file URL",
			argv:    migrateTestArgs,
			preface: migrateSeedPreamble,
			seedDir: func(workspace testVerbWorkspace) string { return "file://" + workspace.seedsDir },
		},
		{
			name:    "schema test file URL",
			argv:    schemaTestArgs,
			preface: "",
			seedDir: func(workspace testVerbWorkspace) string { return "file://" + workspace.seedsDir },
		},
		{
			name:    "migrate test plain path is the control",
			argv:    migrateTestArgs,
			preface: migrateSeedPreamble,
			seedDir: func(workspace testVerbWorkspace) string { return workspace.seedsDir },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			workspace := writeTestVerbWorkspace(c, tt.preface)
			argv := append(slices.Clone(tt.argv(workspace)), "--seed-dir", tt.seedDir(workspace))

			out, err := runCompatArgs(argv)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "seeded 1 file(s)")
		})
	}
}

// TestCompatCommand_TestVerbsRefuseADatabaseSeedDirectory is the other half of
// the surface above: a directory flag that accepted a database URL would hand
// the native runner a path that can never hold seed files, so the two verbs
// name what a seed directory is instead.
func TestCompatCommand_TestVerbsRefuseADatabaseSeedDirectory(t *testing.T) {
	tests := []struct {
		name    string
		argv    func(workspace testVerbWorkspace) []string
		preface string
	}{
		{name: "schema test", argv: schemaTestArgs, preface: ""},
		{name: "migrate test", argv: migrateTestArgs, preface: migrateSeedPreamble},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			workspace := writeTestVerbWorkspace(c, tt.preface)
			argv := append(slices.Clone(tt.argv(workspace)), "--seed-dir", "sqlite://"+workspace.seedsDir)

			out, err := runCompatArgs(argv)

			c.Assert(err, qt.ErrorMatches,
				`atlas (migrate|schema) test --seed-dir: a seed directory is a local path or a file:// URL`,
				qt.Commentf("%s", out))
		})
	}
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

// dockerProvisionerVerdict is what a docker:// dev URL now answers on the test
// verbs: the provisioner's own words, not a refusal of the scheme.
//
// The rows below use an engine no engine table names, so the verdict is
// decidable from the URL text and the assertion needs no container runtime. A
// runnable engine would either provision a real database or report whichever
// runtime the host happens to be missing, and neither is a fact about this
// wiring.
const dockerProvisionerVerdict = `unsupported docker image "notanengine"`

// TestCompatCommand_TestVerbsAnswerADevURLFlag covers the --dev-url spelling on
// both verbs, and the two boundaries that make "is this a docker URL" a
// decision rather than a substring match.
//
// Both verbs used to refuse the scheme outright, before anything could
// provision one; stokaro/ptah#844 wired their native runners to the
// provisioner, so a docker:// value now travels to it and is answered in its
// words. Fixing only the spelling an issue happened to name would look complete
// while the environment twin and the atlas.hcl env still refused: those two
// routes have tests of their own below.
func TestCompatCommand_TestVerbsAnswerADevURLFlag(t *testing.T) {
	tests := []struct {
		name    string
		preface string
		argv    func(workspace testVerbWorkspace) []string
		devURL  string
		wantErr string
	}{
		{
			name:    "migrate test --dev-url",
			preface: migrateSeedPreamble,
			argv:    migrateTestArgs,
			devURL:  "docker://notanengine/16/dev",
			wantErr: dockerProvisionerVerdict,
		},
		{
			name:    "schema test --dev-url",
			preface: "",
			argv:    schemaTestArgs,
			devURL:  "docker://notanengine/16/dev",
			wantErr: dockerProvisionerVerdict,
		},
		{
			// url.Parse lowercases a scheme, so this is the SAME dev URL, and
			// the pinned community binary v1.3.0 reads it as one: measured on
			// `schema inspect --dev-url DOCKER://postgres/16/dev` it answers
			// `failed to connect to the docker API`, byte for byte what the
			// lowercase spelling answers, where `notascheme://` answers
			// `unknown driver`. A case-sensitive prefix match here let the
			// value past this refusal and into the connector, which answered
			// `connect to test database: unsupported database dialect: docker`
			// -- the internal classification this refusal exists to replace.
			name:    "migrate test --dev-url with an uppercase scheme",
			preface: migrateSeedPreamble,
			argv:    migrateTestArgs,
			devURL:  "DOCKER://notanengine/16/dev",
			wantErr: dockerProvisionerVerdict,
		},
		{
			// Both verbs share the mapper, so both had the gap; the second row
			// is here because this repository has repeatedly fixed the verb an
			// issue named and left its twin.
			name:    "schema test --dev-url with a mixed-case scheme",
			preface: "",
			argv:    schemaTestArgs,
			devURL:  "DoCkEr://notanengine/16/dev",
			wantErr: dockerProvisionerVerdict,
		},
		{
			// The other direction of the same prefix match, and the reason it
			// is not simply lowercased here. A LEADING space is not the same
			// URL with whitespace on it: url.Parse reads the whole value as a
			// relative path whose first segment is `docker:`, so there is no
			// docker URL to refuse, and the trimmed prefix match invented one.
			// The value now reaches the connector, which names the parse
			// failure -- the verdict `migrate diff` already gives it.
			name:    "a leading space is not a docker dev URL",
			preface: migrateSeedPreamble,
			argv:    migrateTestArgs,
			devURL:  " docker://postgres/16/dev",
			wantErr: `connect to test database: invalid database URL: parse " docker://postgres/16/dev":` +
				` first path segment in URL cannot contain colon`,
		},
		{
			name:    "a non-docker dialect still answers at the connector",
			preface: "",
			argv:    schemaTestArgs,
			devURL:  "oracle://host/dev",
			wantErr: `connect to test database: unsupported database dialect: oracle`,
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
			argv := append(tt.argv(workspace),
				"--dev-url", tt.devURL,
				"--seed-dir", workspace.seedsDir)

			out, err := runCompatArgs(argv)

			c.Assert(err, qt.ErrorMatches, tt.wantErr, qt.Commentf("%s", out))
		})
	}
}

// TestCompatCommand_TestVerbsReachTheProvisionerFromTheEnvironment pins the
// environment twin of --dev-url. It is a separate route into the same mapper,
// and wiring that reached only the flag would leave this one behind.
func TestCompatCommand_TestVerbsReachTheProvisionerFromTheEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		preface string
		argv    func(workspace testVerbWorkspace) []string
	}{
		{name: "migrate test", preface: migrateSeedPreamble, argv: migrateTestArgs},
		{name: "schema test", preface: "", argv: schemaTestArgs},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("PTAH_DEV_URL", "docker://notanengine/16/dev")
			workspace := writeTestVerbWorkspace(c, tt.preface)
			argv := append(tt.argv(workspace), "--seed-dir", workspace.seedsDir)

			out, err := runCompatArgs(argv)

			c.Assert(err, qt.ErrorMatches, dockerProvisionerVerdict, qt.Commentf("%s", out))
		})
	}
}

// TestCompatCommand_TestVerbsReachTheProvisionerFromTheProjectFile pins the
// third route: an atlas.hcl env whose `dev` attribute carries the value. The
// verb never sees a flag at all here, so wiring placed on the flag path alone
// would leave this one behind -- and it is the route an Atlas project uses.
func TestCompatCommand_TestVerbsReachTheProvisionerFromTheProjectFile(t *testing.T) {
	tests := []struct {
		name    string
		preface string
		verb    string
	}{
		{name: "migrate test", preface: migrateSeedPreamble, verb: "migrate"},
		{name: "schema test", preface: "", verb: "schema"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			workspace := writeTestVerbWorkspace(c, tt.preface)
			writeDockerDevProject(c, t, workspace)

			out, err := runCompatArgs([]string{
				tt.verb, "test", workspace.casesDir,
				"--env", "local",
				"--seed-dir", workspace.seedsDir,
			})

			c.Assert(err, qt.ErrorMatches, dockerProvisionerVerdict, qt.Commentf("%s", out))
		})
	}
}

// TestCompatCommand_TestVerbsAcceptAConnectableDevURL is the passing control
// for the three refusals above: the same workspace and the same flag run to
// completion once the dev database URL is one the runner can connect to, so
// each refusal is attributable to its value rather than to the fixture.
func TestCompatCommand_TestVerbsAcceptAConnectableDevURL(t *testing.T) {
	c := qt.New(t)
	workspace := writeTestVerbWorkspace(c, "")

	out, err := runCompatArgs(append(schemaTestArgs(workspace),
		"--dev-url", freshDevURL(c),
		"--seed-dir", workspace.seedsDir))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// writeDockerDevProject writes an atlas.hcl whose env supplies the docker dev
// database URL, and moves the process into its directory so the default
// --config location finds it.
func writeDockerDevProject(c *qt.C, t *testing.T, workspace testVerbWorkspace) {
	c.Helper()
	dir := c.TempDir()
	t.Chdir(dir)
	c.Assert(os.CopyFS(filepath.Join(dir, "migrations"), os.DirFS(workspace.migrationsDir)), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(
		"env \"local\" {\n"+
			"  src = \"file://"+filepath.ToSlash(workspace.modelsDir)+"\"\n"+
			"  migration {\n"+
			"    dir = \"file://migrations\"\n"+
			"  }\n"+
			"  dev = \"docker://notanengine/16/dev\"\n"+
			"}\n"), 0o600), qt.IsNil)
}
