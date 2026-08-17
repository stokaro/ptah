package lint_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	cmdlint "go.5x5.cz/ptah/cmd/lint"
	migrationlint "go.5x5.cz/ptah/migration/lint"
)

func execute(args ...string) (stdout, stderr string, err error) {
	cmd := cmdlint.NewLintCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func writeLintTestFile(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func runGit(c *qt.C, dir string, args ...string) {
	c.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("git %s: %s", strings.Join(args, " "), strings.TrimSpace(string(output))))
}

type sarifForTest struct {
	Version string `json:"version"`
	Schema  string `json:"$schema"`
	Runs    []struct {
		Tool struct {
			Driver struct {
				Name           string             `json:"name"`
				InformationURI string             `json:"informationUri"`
				Rules          []sarifRuleForTest `json:"rules"`
			} `json:"driver"`
		} `json:"tool"`
		OriginalURIBaseIDs map[string]sarifArtifactLocationForTest `json:"originalUriBaseIds"`
		Results            []sarifResultForTest                    `json:"results"`
	} `json:"runs"`
}

type sarifRuleForTest struct {
	ID                   string                 `json:"id"`
	Name                 string                 `json:"name"`
	ShortDescription     struct{ Text string }  `json:"shortDescription"`
	DefaultConfiguration struct{ Level string } `json:"defaultConfiguration"`
}

type sarifResultForTest struct {
	RuleID              string                 `json:"ruleId"`
	RuleIndex           int                    `json:"ruleIndex"`
	Level               string                 `json:"level"`
	Message             struct{ Text string }  `json:"message"`
	Locations           []sarifLocationForTest `json:"locations"`
	PartialFingerprints map[string]string      `json:"partialFingerprints"`
}

type sarifLocationForTest struct {
	PhysicalLocation struct {
		ArtifactLocation sarifArtifactLocationForTest `json:"artifactLocation"`
		Region           struct {
			StartLine int `json:"startLine"`
		} `json:"region"`
	} `json:"physicalLocation"`
}

type sarifArtifactLocationForTest struct {
	URI       string `json:"uri"`
	URIBaseID string `json:"uriBaseId"`
}

func TestNewLintCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := cmdlint.NewLintCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "lint")
	c.Assert(cmd.Short, qt.Contains, "Lint")
}

func TestRunLint_DefaultTextKeepsNativePtahDiagnostics(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeLintTestFile(c, dir, "0000000001_drop.up.sql", "DROP TABLE users;\n")
	writeLintTestFile(c, dir, "0000000001_drop.down.sql", "CREATE TABLE users (id INT);\n")

	stdout, stderr, err := execute(
		"--dir", dir,
		"--dir-format", "ptah",
		"--fail-on", "none",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, "DROP TABLE permanently deletes table users and every row in it")
	c.Assert(stdout, qt.Contains, "verified backup first and consider a rename-and-retire window instead")
	c.Assert(stdout, qt.Not(qt.Contains), `Dropping table "users"`)
	c.Assert(stdout, qt.Not(qt.Contains), "https://atlasgo.io/lint/analyzers")
}

// sarifResultMessagesOf collects each SARIF result's message text, in order.
func sarifResultMessagesOf(results []sarifResultForTest) []string {
	messages := make([]string, 0, len(results))
	for _, result := range results {
		messages = append(messages, result.Message.Text)
	}
	return messages
}

// distinctSARIFFingerprints collects the set of primary-location fingerprints
// across results.
func distinctSARIFFingerprints(results []sarifResultForTest) map[string]struct{} {
	fingerprints := make(map[string]struct{}, len(results))
	for _, result := range results {
		fingerprints[result.PartialFingerprints["primaryLocationLineHash"]] = struct{}{}
	}
	return fingerprints
}

// TestRunLint_MultiTargetDropFingerprintsEveryTable pins the SARIF consequence
// of reporting one finding per dropped table. All three results share a rule, a
// file and a line, so their fingerprints can only differ through the message.
// Were the three to carry one shared message, GitHub code scanning would fold
// them into a single alert and two of the three destroyed tables would never
// reach the security tab.
func TestRunLint_MultiTargetDropFingerprintsEveryTable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeLintTestFile(c, dir, "0000000001_init.up.sql",
		"CREATE TABLE mid (id INT);\nCREATE TABLE zeta (id INT);\nCREATE TABLE alpha (id INT);\n")
	writeLintTestFile(c, dir, "0000000001_init.down.sql", "DROP TABLE alpha;\n")
	writeLintTestFile(c, dir, "0000000002_drop.up.sql", "DROP TABLE zeta, alpha, mid;\n")
	writeLintTestFile(c, dir, "0000000002_drop.down.sql", "CREATE TABLE mid (id INT);\n")

	stdout, _, err := execute("--dir", dir, "--dir-format", "ptah", "--format", "sarif", "--fail-on", "none")

	c.Assert(err, qt.IsNil)
	assertSARIFSchemaValid(c, stdout)
	var report sarifForTest
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	results := report.Runs[0].Results
	c.Assert(sarifResultMessagesOf(results), qt.DeepEquals, []string{
		"table dropped: DROP TABLE permanently deletes table alpha and every row in it; " +
			"take a verified backup first and consider a rename-and-retire window instead",
		"table dropped: DROP TABLE permanently deletes table mid and every row in it; " +
			"take a verified backup first and consider a rename-and-retire window instead",
		"table dropped: DROP TABLE permanently deletes table zeta and every row in it; " +
			"take a verified backup first and consider a rename-and-retire window instead",
	})
	c.Assert(distinctSARIFFingerprints(results), qt.HasLen, 3)
}

func TestRunLint_CuratedFixtureProducesExpectedRuleHits(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", "testdata/bad", "--format", "json")

	// The fixture contains DS errors, so the default --fail-on=error exits 1.
	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)

	var report struct {
		Failed   bool                    `json:"failed"`
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stderr), &report), qt.IsNil)
	c.Assert(report.Failed, qt.IsTrue)

	rules := map[string]int{}
	for _, f := range report.Findings {
		rules[f.Rule]++
	}
	for _, want := range []string{"DS101", "DS102", "DS103", "BC101", "MF101", "MF102", "MF103", "PG101", "PG102", "MY101"} {
		c.Assert(rules[want] >= 1, qt.IsTrue,
			qt.Commentf("expected at least one %s hit; got rule tally %v", want, rules))
	}
}

func TestRunLint_GitHubActionsFormatAnnotatesFileAndLine(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", "testdata/bad", "--format", "github-actions")

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr, qt.Contains, "::error file=testdata/bad/0000000002_bad_stuff.up.sql,line=2::DS101:")
	c.Assert(stderr, qt.Contains, "::warning file=testdata/bad/0000000002_bad_stuff.up.sql,line=10::PG101:")
	c.Assert(stderr, qt.Contains, "::warning file=testdata/bad/misnamed.sql::MF103:")
}

func TestRunLint_SARIFFormat(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := execute("--dir", "testdata/bad", "--format", "sarif", "--fail-on", "none")

	c.Assert(err, qt.IsNil)
	assertSARIFSchemaValid(c, stdout)
	assertGitHubCodeScanningSARIF(c, stdout)
	var report sarifForTest
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Version, qt.Equals, "2.1.0")
	c.Assert(report.Schema, qt.Equals, "https://json.schemastore.org/sarif-2.1.0.json")
	c.Assert(report.Runs, qt.HasLen, 1)
	c.Assert(report.Runs[0].Tool.Driver.Name, qt.Equals, "ptah migrations lint")
	c.Assert(report.Runs[0].Tool.Driver.Rules[0].ID, qt.Not(qt.Equals), "")
	resultsByRule := make(map[string]sarifResultForTest, len(report.Runs[0].Results))
	for _, result := range report.Runs[0].Results {
		resultsByRule[result.RuleID] = result
	}
	dropTableResult := resultsByRule["DS101"]
	c.Assert(dropTableResult.RuleID, qt.Equals, "DS101")
	c.Assert(dropTableResult.RuleIndex, qt.Equals, ruleIndexByID(report.Runs[0].Tool.Driver.Rules, "DS101"))
	c.Assert(dropTableResult.Level, qt.Equals, "error")
	c.Assert(dropTableResult.Locations[0].PhysicalLocation.ArtifactLocation.URI, qt.Contains, "testdata/bad/")
	c.Assert(dropTableResult.Locations[0].PhysicalLocation.ArtifactLocation.URIBaseID, qt.Equals, "%SRCROOT%")
	c.Assert(dropTableResult.Locations[0].PhysicalLocation.Region.StartLine, qt.Equals, 2)
}

func TestRunLint_SARIFGoldenOutput(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := execute("--dir", "testdata/sarif/migrations", "--format", "sarif", "--fail-on", "none")

	c.Assert(err, qt.IsNil)
	assertSARIFSchemaValid(c, stdout)
	assertGitHubCodeScanningSARIF(c, stdout)
	expected, err := os.ReadFile("testdata/sarif/expected.sarif.json")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, string(expected))
}

func TestRunLint_SARIFCleanOutputValidatesForUpload(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := execute("--dir", "testdata/clean", "--format", "sarif", "--fail-on", "none")

	c.Assert(err, qt.IsNil)
	assertSARIFSchemaValid(c, stdout)
	var report sarifForTest
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	assertSARIFCommonGitHubFields(c, report)
	c.Assert(report.Runs[0].Tool.Driver.Rules, qt.HasLen, 0)
	c.Assert(report.Runs[0].Results, qt.HasLen, 0)
}

func TestRunLint_ConfigFileDisablesRulesAndSetsDialect(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", "testdata/with-config", "--format", "json")

	c.Assert(err, qt.IsNotNil, qt.Commentf("DS errors remain, so the run still fails"))
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)

	var report struct {
		Dialect  string                  `json:"dialect"`
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stderr), &report), qt.IsNil)
	c.Assert(report.Dialect, qt.Equals, "postgres")

	for _, f := range report.Findings {
		c.Assert(f.Rule, qt.Not(qt.Contains), "MF",
			qt.Commentf("the MF family is disabled by .ptah-lint.yaml; got %v", f))
		c.Assert(f.Rule, qt.Not(qt.Equals), "BC101")
		c.Assert(f.Rule, qt.Not(qt.Equals), "MY101",
			qt.Commentf("dialect: postgres from the config must gate MY rules; got %v", f))
	}
}

func TestRunLint_LatestRestrictsToLatestMigrationVersions(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeLintTestFile(c, dir, "0000000001_old.up.sql", "DROP TABLE old_data;\n")
	writeLintTestFile(c, dir, "0000000001_old.down.sql", "CREATE TABLE old_data (id INT);\n")
	writeLintTestFile(c, dir, "0000000002_new.up.sql", "ALTER TABLE users DROP COLUMN legacy;\n")
	writeLintTestFile(c, dir, "0000000002_new.down.sql", "ALTER TABLE users ADD COLUMN legacy TEXT;\n")

	stdout, _, err := execute("--dir", dir, "--format", "json", "--fail-on", "none", "--latest", "1")

	c.Assert(err, qt.IsNil)
	var report struct {
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Findings[0].File, qt.Contains, "0000000002_new.up.sql")
}

func TestRunLint_ProjectConfigLatestRestrictsToLatestMigrationVersions(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	writeLintTestFile(c, migrationsDir, "0000000001_old.up.sql", "DROP TABLE old_data;\n")
	writeLintTestFile(c, migrationsDir, "0000000001_old.down.sql", "CREATE TABLE old_data (id INT);\n")
	writeLintTestFile(c, migrationsDir, "0000000002_new.up.sql", "ALTER TABLE users DROP COLUMN legacy;\n")
	writeLintTestFile(c, migrationsDir, "0000000002_new.down.sql", "ALTER TABLE users ADD COLUMN legacy TEXT;\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "ci" {
  migration {
    dir = "file://migrations"
  }
  lint {
    latest = 1
  }
}
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	stdout, _, err := execute("--env", "ci", "--format", "json", "--fail-on", "none")

	c.Assert(err, qt.IsNil)
	var report struct {
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Findings[0].File, qt.Contains, "0000000002_new.up.sql")
}

func TestRunLint_ExplicitGitBaseSuppressesProjectLatest(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	writeLintTestFile(c, migrationsDir, "1.sql", "CREATE TABLE users (id INT);\n")
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(`env "ci" {
  migration {
    dir = "file://migrations"
  }
  lint {
    latest = 1
  }
}
`), 0o600), qt.IsNil)
	t.Chdir(root)

	_, stderr, err := execute("--env", "ci", "--git-base=-unsafe")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, `--git-base "-unsafe" is not a safe Git ref`)
	c.Assert(stderr, qt.Not(qt.Contains), "--latest and --git-base are mutually exclusive")
}

func TestRunLint_ExplicitLatestSuppressesProjectGitSelector(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	writeLintTestFile(c, migrationsDir, "1.sql", "CREATE TABLE users (id INT);\n")
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(`env "ci" {
  migration {
    dir = "file://migrations"
  }
  lint {
    git {
      base = "-unsafe"
      dir  = "/not/a/repository"
    }
  }
}
`), 0o600), qt.IsNil)
	t.Chdir(root)

	_, _, err := execute("--env", "ci", "--latest", "1", "--format", "json", "--fail-on", "none")

	c.Assert(err, qt.IsNil)
}

func TestRunLint_GitBaseRestrictsToChangedMigrationVersions(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	runGit(c, dir, "init", "-b", "master")
	runGit(c, dir, "config", "user.email", "ptah@example.test")
	runGit(c, dir, "config", "user.name", "Ptah Test")
	runGit(c, dir, "config", "commit.gpgsign", "false")
	writeLintTestFile(c, migrationsDir, "0000000001_old.up.sql", "DROP TABLE old_data;\n")
	writeLintTestFile(c, migrationsDir, "0000000001_old.down.sql", "CREATE TABLE old_data (id INT);\n")
	runGit(c, dir, "add", ".")
	runGit(c, dir, "commit", "-m", "base")
	runGit(c, dir, "checkout", "-b", "feature")
	writeLintTestFile(c, migrationsDir, "0000000002_new.up.sql", "ALTER TABLE users DROP COLUMN legacy;\n")
	writeLintTestFile(c, migrationsDir, "0000000002_new.down.sql", "ALTER TABLE users ADD COLUMN legacy TEXT;\n")
	runGit(c, dir, "add", ".")
	runGit(c, dir, "commit", "-m", "feature")
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	stdout, _, err := execute("--dir", "migrations", "--format", "json", "--fail-on", "none", "--git-base", "master")

	c.Assert(err, qt.IsNil)
	var report struct {
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Findings[0].File, qt.Contains, "0000000002_new.up.sql")
}

func TestRunLint_ProjectConfigGitBaseRestrictsToChangedMigrationVersions(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	runGit(c, dir, "init", "-b", "master")
	runGit(c, dir, "config", "user.email", "ptah@example.test")
	runGit(c, dir, "config", "user.name", "Ptah Test")
	runGit(c, dir, "config", "commit.gpgsign", "false")
	writeLintTestFile(c, migrationsDir, "0000000001_old.up.sql", "DROP TABLE old_data;\n")
	writeLintTestFile(c, migrationsDir, "0000000001_old.down.sql", "CREATE TABLE old_data (id INT);\n")
	runGit(c, dir, "add", ".")
	runGit(c, dir, "commit", "-m", "base")
	runGit(c, dir, "checkout", "-b", "feature")
	writeLintTestFile(c, migrationsDir, "0000000002_new.up.sql", "ALTER TABLE users DROP COLUMN legacy;\n")
	writeLintTestFile(c, migrationsDir, "0000000002_new.down.sql", "ALTER TABLE users ADD COLUMN legacy TEXT;\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "ci" {
  migration {
    dir = "file://migrations"
  }
  lint {
    git {
      base = "master"
      dir  = "."
    }
  }
}
`), 0o600), qt.IsNil)
	runGit(c, dir, "add", ".")
	runGit(c, dir, "commit", "-m", "feature")
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	stdout, _, err := execute("--env", "ci", "--format", "json", "--fail-on", "none")

	c.Assert(err, qt.IsNil)
	var report struct {
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Findings[0].File, qt.Contains, "0000000002_new.up.sql")
}

func TestRunLint_GitBaseRejectsUnversionedSQLFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	runGit(c, dir, "init", "-b", "master")
	runGit(c, dir, "config", "user.email", "ptah@example.test")
	runGit(c, dir, "config", "user.name", "Ptah Test")
	runGit(c, dir, "config", "commit.gpgsign", "false")
	writeLintTestFile(c, migrationsDir, "0000000001_base.up.sql", "CREATE TABLE users (id INT);\n")
	writeLintTestFile(c, migrationsDir, "0000000001_base.down.sql", "DROP TABLE users;\n")
	runGit(c, dir, "add", ".")
	runGit(c, dir, "commit", "-m", "base")
	runGit(c, dir, "checkout", "-b", "feature")
	writeLintTestFile(c, migrationsDir, "misnamed.sql", "DROP TABLE users;\n")
	runGit(c, dir, "add", ".")
	runGit(c, dir, "commit", "-m", "feature")
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	_, stderr, err := execute("--dir", "migrations", "--git-base", "master")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "--git-base requires versioned migration files")
	c.Assert(stderr, qt.Contains, "migrations/misnamed.sql")
}

func TestRunLint_GitBaseHonorsExplicitAtlasDirFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	runGit(c, dir, "init", "-b", "master")
	runGit(c, dir, "config", "user.email", "ptah@example.test")
	runGit(c, dir, "config", "user.name", "Ptah Test")
	runGit(c, dir, "config", "commit.gpgsign", "false")
	writeLintTestFile(c, migrationsDir, "1_base.sql", "CREATE TABLE users (id INT);\n")
	runGit(c, dir, "add", ".")
	runGit(c, dir, "commit", "-m", "base")
	runGit(c, dir, "checkout", "-b", "feature")
	writeLintTestFile(c, migrationsDir, "2_drop_users.sql", "DROP TABLE users;\n")
	runGit(c, dir, "add", ".")
	runGit(c, dir, "commit", "-m", "feature")
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	_, stderr, err := execute("--dir", "migrations", "--dir-format", "atlas", "--git-base", "master")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "DS101")
	c.Assert(stderr, qt.Contains, "migrations/2_drop_users.sql")
	c.Assert(stderr, qt.Not(qt.Contains), "migrations/1_base.sql")
}

func TestRunLint_GitBaseRejectsOptionShapedRef(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeLintTestFile(c, dir, "0000000001_base.up.sql", "CREATE TABLE users (id INT);\n")

	_, stderr, err := execute("--dir", dir, "--git-base=-c")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, `--git-base "-c" is not a safe Git ref`)
}

func TestRunLint_ProjectConfigGitBaseRejectsOptionShapedRef(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	writeLintTestFile(c, migrationsDir, "0000000001_base.up.sql", "CREATE TABLE users (id INT);\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "ci" {
  migration {
    dir = "file://migrations"
  }
  lint {
    git {
      base = "-c"
      dir  = "."
    }
  }
}
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	_, stderr, err := execute("--env", "ci")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, `--git-base "-c" is not a safe Git ref`)
}

func TestRunLint_LatestRejectsZero(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", "testdata/clean", "--latest", "0")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "--latest must be greater than zero")
}

func TestRunLint_LatestRejectsUnversionedSQLFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeLintTestFile(c, dir, "0000000002_new.up.sql", "CREATE TABLE users (id INT);\n")
	writeLintTestFile(c, dir, "0000000002_new.down.sql", "DROP TABLE users;\n")
	writeLintTestFile(c, dir, "misnamed.sql", "DROP TABLE hidden;\n")

	_, stderr, err := execute("--dir", dir, "--latest", "1")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "--latest requires versioned migration files")
	c.Assert(stderr, qt.Contains, "misnamed.sql")
}

func TestRunLint_LatestHonorsExplicitAtlasDirFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeLintTestFile(c, dir, "1_init.sql", "CREATE TABLE users (id INT);\n")
	writeLintTestFile(c, dir, "2_drop_users.sql", "DROP TABLE users;\n")

	_, stderr, err := execute("--dir", dir, "--dir-format", "atlas", "--latest", "1")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "DS101")
	c.Assert(stderr, qt.Contains, "2_drop_users.sql")
	c.Assert(stderr, qt.Not(qt.Contains), "1_init.sql")
}

func TestRunLint_ProjectConfigDisablesRulesAndSetsDialect(t *testing.T) {
	c := qt.New(t)
	badDir, err := filepath.Abs("testdata/bad")
	c.Assert(err, qt.IsNil)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte(`lint:
  dialect: postgres
  disabled-rules:
    - MF
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	_, stderr, err := execute("--dir", badDir, "--format", "json")

	c.Assert(err, qt.IsNotNil, qt.Commentf("DS errors remain, so the run still fails"))
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)

	var report struct {
		Dialect  string                  `json:"dialect"`
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stderr), &report), qt.IsNil)
	c.Assert(report.Dialect, qt.Equals, "postgres")
	for _, f := range report.Findings {
		c.Assert(f.Rule, qt.Not(qt.Contains), "MF",
			qt.Commentf("the MF family is disabled by ptah.yaml; got %v", f))
		c.Assert(f.Rule, qt.Not(qt.Equals), "MY101",
			qt.Commentf("dialect: postgres from ptah.yaml must gate MY rules; got %v", f))
	}
}

func TestRunLint_AtlasProjectConfigDestructivePolicyDowngradesSeverity(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	writeLintTestFile(c, migrationsDir, "0000000001_drop_column.up.sql", "ALTER TABLE users DROP COLUMN legacy;\n")
	writeLintTestFile(c, migrationsDir, "0000000001_drop_column.down.sql", "ALTER TABLE users ADD COLUMN legacy TEXT;\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "ci" {
  migration {
    dir = "file://migrations"
  }
  lint {
    destructive {
      error = false
    }
  }
}
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	stdout, _, err := execute("--env", "ci", "--format", "json")

	c.Assert(err, qt.IsNil)
	var report struct {
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Findings[0].Severity, qt.Equals, migrationlint.SeverityWarning)
}

func TestRunLint_AtlasProjectConfigConcurrentIndexPolicyRaisesSeverity(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	writeLintTestFile(c, migrationsDir, "0000000001_index.up.sql", "CREATE INDEX user_email_idx ON users (email);\n")
	writeLintTestFile(c, migrationsDir, "0000000001_index.down.sql", "DROP INDEX user_email_idx;\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "ci" {
  lint {
    concurrent_index {
      error = true
    }
  }
}
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	stdout, _, err := execute("--env", "ci", "--dir", migrationsDir, "--dialect", "postgres", "--format", "json", "--fail-on", "none")

	c.Assert(err, qt.IsNil)
	var report struct {
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	// The down file's blocking DROP INDEX is reported too (stokaro/ptah#997),
	// and the concurrent_index policy family does not cover PG106, so its
	// severity stays at the rule default while PG101 is raised.
	severityByRule := make(map[string]migrationlint.Severity, len(report.Findings))
	for _, finding := range report.Findings {
		severityByRule[finding.Rule] = finding.Severity
	}
	c.Assert(severityByRule, qt.DeepEquals, map[string]migrationlint.Severity{
		"PG101": migrationlint.SeverityError,
		"PG106": migrationlint.SeverityWarning,
	})
}

func TestRunLint_AtlasProjectConfigPolicyAffectsSARIFLevels(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	writeLintTestFile(c, migrationsDir, "0000000001_policy_levels.up.sql", `ALTER TABLE users DROP COLUMN legacy;
CREATE INDEX user_email_idx ON users (email);
`)
	writeLintTestFile(c, migrationsDir, "0000000001_policy_levels.down.sql", `DROP INDEX user_email_idx;
ALTER TABLE users ADD COLUMN legacy TEXT;
`)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "ci" {
  lint {
    destructive {
      error = false
    }
    concurrent_index {
      error = true
    }
  }
}
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	stdout, _, err := execute("--env", "ci", "--dir", migrationsDir, "--dialect", "postgres", "--format", "sarif", "--fail-on", "none")

	c.Assert(err, qt.IsNil)
	var report sarifForTest
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	run := report.Runs[0]
	rulesByID := make(map[string]sarifRuleForTest, len(run.Tool.Driver.Rules))
	for _, rule := range run.Tool.Driver.Rules {
		rulesByID[rule.ID] = rule
	}
	c.Assert(rulesByID["DS102"].DefaultConfiguration.Level, qt.Equals, "warning")
	c.Assert(rulesByID["PG101"].DefaultConfiguration.Level, qt.Equals, "error")
	// Keyed by rule rather than by position: the down file's blocking DROP
	// INDEX is now a result too (stokaro/ptah#997), and it sorts ahead of the
	// up file's results.
	levelByRule := make(map[string]string, len(run.Results))
	for _, result := range run.Results {
		levelByRule[result.RuleID] = result.Level
	}
	c.Assert(levelByRule, qt.DeepEquals, map[string]string{
		"DS102": "warning",
		"PG101": "error",
		"PG106": "warning",
	})
}

func TestRunLint_ConfigRuleSeverityAndExclude(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	write := func(name, content string) {
		path := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
	write(migrationlint.ConfigFileName, `rules:
  DS102:
    severity: warning
    exclude:
      - legacy/**
`)
	write("legacy/0000000001_legacy.up.sql", "ALTER TABLE users DROP COLUMN old_legacy;\n")
	write("legacy/0000000001_legacy.down.sql", "ALTER TABLE users ADD COLUMN old_legacy TEXT;\n")
	write("main/0000000002_main.up.sql", "ALTER TABLE users DROP COLUMN old_main;\n")
	write("main/0000000002_main.down.sql", "ALTER TABLE users ADD COLUMN old_main TEXT;\n")

	stdout, _, err := execute("--dir", dir, "--format", "json")

	c.Assert(err, qt.IsNil)
	var report struct {
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Findings[0].Severity, qt.Equals, migrationlint.SeverityWarning)
	c.Assert(report.Findings[0].File, qt.Contains, "main/0000000002_main.up.sql")
}

// TestRunLint_SeverityDecidesTheExitCode pins the whole severity vocabulary
// against the one thing it governs.
//
// stokaro/ptah#1633 added `info` because a rule was either loud enough to fail
// or absent from the report, with nothing in between -- which is what a team
// needs to introduce a rule to a repository that still violates it. The three
// rows are the vocabulary, and the exit code is the only axis that separates
// them: all three REPORT, and only `error` gates.
//
// The exit code is asserted rather than inferred from the finding count,
// because "reported and not gated" is exactly the pair a severity level is for.
func TestRunLint_SeverityDecidesTheExitCode(t *testing.T) {
	tests := []struct {
		name     string
		severity migrationlint.Severity
		wantExit int
	}{
		{name: "info reports and does not gate", severity: migrationlint.SeverityInfo, wantExit: 0},
		{name: "warning reports and does not gate", severity: migrationlint.SeverityWarning, wantExit: 0},
		{name: "error reports and gates", severity: migrationlint.SeverityError, wantExit: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			c.Assert(os.WriteFile(
				filepath.Join(dir, migrationlint.ConfigFileName),
				[]byte("rules:\n  DS101:\n    severity: "+string(test.severity)+"\n"),
				0o600,
			), qt.IsNil)
			c.Assert(os.WriteFile(
				filepath.Join(dir, "0000000001_drop.up.sql"),
				[]byte("DROP TABLE users;\n"),
				0o600,
			), qt.IsNil)

			stdout, stderr, err := execute("--dir", dir, "--format", "json")

			c.Assert(exitcode.Code(err, 0), qt.Equals, test.wantExit)
			var report struct {
				Findings []migrationlint.Finding `json:"findings"`
			}
			// A gating run writes its report to stderr and a passing one to
			// stdout, which is the same choice `ptah migrations lint` makes for
			// a caller piping the report somewhere. The row still reads the
			// report either way: "reported and not gated" needs both halves.
			c.Assert(json.Unmarshal([]byte(reportStream(stdout, stderr)), &report), qt.IsNil)
			// The fixture also trips MF101 (a migration with no down file), so
			// the DS101 finding is selected rather than assumed to be the only
			// one. Adding a down file would silence MF101 and take DS101's
			// second statement with it.
			dropped := findingForRule(c, report.Findings, "DS101")
			c.Assert(dropped.Severity, qt.Equals, test.severity)
		})
	}
}

// sarifLevelForRule returns the level of the one SARIF result for a rule,
// failing when the report carries none. Selecting inside a loop in the test
// body would let a report with no such result assert nothing at all.
func sarifLevelForRule(c *qt.C, report sarifForTest, code string) string {
	c.Helper()
	levels := make([]string, 0, 1)
	for _, run := range report.Runs {
		for _, result := range run.Results {
			if result.RuleID == code {
				levels = append(levels, result.Level)
			}
		}
	}
	c.Assert(levels, qt.HasLen, 1, qt.Commentf("results: %+v", report.Runs))
	return levels[0]
}

// reportStream picks whichever stream carried the JSON report.
func reportStream(stdout, stderr string) string {
	if strings.TrimSpace(stdout) != "" {
		return stdout
	}
	return stderr
}

// findingForRule returns the one finding for a rule code, failing when the
// report carries none or more than one.
func findingForRule(c *qt.C, findings []migrationlint.Finding, code string) migrationlint.Finding {
	c.Helper()
	matched := make([]migrationlint.Finding, 0, 1)
	for _, finding := range findings {
		if finding.Rule == code {
			matched = append(matched, finding)
		}
	}
	c.Assert(matched, qt.HasLen, 1, qt.Commentf("report: %+v", findings))
	return matched[0]
}

// TestRunLint_InfoSeverityIsASARIFNote pins the other half of what the level
// means. SARIF has a rank below "warning" and a finding nothing should act on
// is what it is for; collapsing info into "warning" would put an advisory
// result at the same level as one asking for review, which is the distinction
// the level was added to make (stokaro/ptah#1633).
func TestRunLint_InfoSeverityIsASARIFNote(t *testing.T) {
	tests := []struct {
		name     string
		severity migrationlint.Severity
		want     string
	}{
		{name: "info is a note", severity: migrationlint.SeverityInfo, want: "note"},
		{name: "warning stays a warning", severity: migrationlint.SeverityWarning, want: "warning"},
		{name: "error stays an error", severity: migrationlint.SeverityError, want: "error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			c.Assert(os.WriteFile(
				filepath.Join(dir, migrationlint.ConfigFileName),
				[]byte("rules:\n  DS101:\n    severity: "+string(test.severity)+"\n"),
				0o600,
			), qt.IsNil)
			c.Assert(os.WriteFile(
				filepath.Join(dir, "0000000001_drop.up.sql"),
				[]byte("DROP TABLE users;\n"),
				0o600,
			), qt.IsNil)

			stdout, _, err := execute("--dir", dir, "--format", "sarif", "--fail-on", "none")

			c.Assert(err, qt.IsNil)
			var report sarifForTest
			c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
			c.Assert(report.Runs, qt.HasLen, 1)
			c.Assert(sarifLevelForRule(c, report, "DS101"), qt.Equals, test.want)
		})
	}
}

// TestRunLint_UnknownSeverityIsStillRefused is the control for the table above.
// Adding a level must not turn the vocabulary into "anything goes": a value
// nothing understands is a configuration error, and refusing it at exit 2 is
// what tells the operator their file does not say what they think it says.
func TestRunLint_UnknownSeverityIsStillRefused(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, migrationlint.ConfigFileName),
		[]byte("rules:\n  DS101:\n    severity: informational\n"),
		0o600,
	), qt.IsNil)

	_, stderr, err := execute("--dir", dir)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, `unsupported severity "informational"`)
}

func TestRunLint_MalformedExclusionGlobFailsBeforeAnalysis(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, migrationlint.ConfigFileName),
		[]byte("rules:\n  DS101:\n    exclude:\n      - '[legacy/**'\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "0000000001_create_users.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "0000000001_create_users.down.sql"),
		[]byte("DROP TABLE users;\n"),
		0o600,
	), qt.IsNil)

	stdout, stderr, err := execute("--dir", dir)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `rule DS101 has invalid exclude pattern "[legacy/**": syntax error in pattern`)
}

func TestRunLint_FailOnThresholds(t *testing.T) {
	c := qt.New(t)

	// none: findings are reported but the exit code stays zero.
	stdout, _, err := execute("--dir", "testdata/bad", "--fail-on", "none")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "DS101")

	// any: even warning-only runs fail. Disable the DS error rules and keep
	// warnings; the exit code must still be 1.
	_, _, err = execute("--dir", "testdata/bad", "--fail-on", "any", "--disable", "DS")
	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)

	// error (default): warnings alone do not fail.
	stdout, _, err = execute("--dir", "testdata/bad", "--disable", "DS")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "PG101")
}

func TestRunLint_InvalidFlagValuesExitCode2(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", "testdata/bad", "--format", "yaml")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "invalid --format")

	_, stderr, err = execute("--dir", "testdata/bad", "--fail-on", "sometimes")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "invalid --fail-on")

	_, stderr, err = execute("--dir", "testdata/bad", "--dialect", "oracle")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "invalid --dialect")
	c.Assert(stderr, qt.Contains, "clickhouse")
	c.Assert(stderr, qt.Contains, "spanner")

	stdout, _, err := execute("--dir", "testdata/clean", "--dialect", "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "No lint findings.")

	_, stderr, err = execute("--dir", "testdata/does-not-exist")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "migrations directory testdata/does-not-exist")

	_, stderr, err = execute("--dir", "testdata/bad", "--config", "testdata/nope.yaml")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "lint config")

	_, stderr, err = execute("--dir", "testdata/bad", "--config", "testdata/invalid-dialect.yaml")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, `unsupported lint dialect "oracle"`)

	_, stderr, err = execute("--dir", "testdata/bad", "--no-such-flag")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "unknown flag")

	// Positional arguments would silently lint the default --dir instead of
	// what the user pointed at — a silent false negative in CI.
	_, stderr, err = execute("testdata/bad")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "unexpected positional arguments")
}

func TestRunLint_LocalSourceValidationPrecedence(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	missingDir := filepath.Join(root, "missing")
	validDir := filepath.Join(root, "valid")
	c.Assert(os.Mkdir(validDir, 0o700), qt.IsNil)
	invalidMetadataDir := filepath.Join(root, "invalid-metadata")
	c.Assert(os.Mkdir(invalidMetadataDir, 0o700), qt.IsNil)
	writeLintTestFile(c, invalidMetadataDir, "ATLAS.SUM", "metadata\n")

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "dialect before directory",
			args: []string{"--dir", missingDir, "--dialect", "oracle"},
			want: "invalid --dialect",
		},
		{
			name: "positional arguments before directory",
			args: []string{"--dir", missingDir, "unexpected"},
			want: "unexpected positional arguments",
		},
		{
			name: "directory before directory format",
			args: []string{"--dir", missingDir, "--dir-format", "custom"},
			want: "migrations directory",
		},
		{
			name: "directory format before dev url",
			args: []string{
				"--dir", validDir,
				"--dir-format", "custom",
				"--dev-url", "spanner://localhost/dev",
			},
			want: `unknown migration directory format "custom"`,
		},
		{
			name: "dev url before directory capture",
			args: []string{
				"--dir", invalidMetadataDir,
				"--dev-url", "spanner://localhost/dev",
			},
			want: `unsupported --dev-url dialect "spanner://localhost/dev"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, stderr, err := execute(test.args...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, test.want)
		})
	}
}

func TestRunLint_ExplicitEmptyDialectOverridesConfig(t *testing.T) {
	c := qt.New(t)

	// The config sets dialect: postgres; an explicit --dialect "" must win
	// and re-enable the MY family.
	_, stderr, err := execute("--dir", "testdata/with-config", "--format", "json", "--dialect", "")

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)

	var report struct {
		Dialect  string                  `json:"dialect"`
		Findings []migrationlint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stderr), &report), qt.IsNil)
	c.Assert(report.Dialect, qt.Equals, "")

	rules := map[string]int{}
	for _, f := range report.Findings {
		rules[f.Rule]++
	}
	c.Assert(rules["MY101"] >= 1, qt.IsTrue,
		qt.Commentf("explicit empty --dialect runs every rule; got tally %v", rules))
}

func TestRunLint_JSONReportsEmptyFindingsAsArray(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := execute("--dir", "testdata/clean", "--format", "json")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, `"findings": []`,
		qt.Commentf("an empty findings list must serialize as [], not null; got: %s", stdout))
}

func assertSARIFSchemaValid(c *qt.C, data string) {
	c.Helper()

	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft4)
	compiler.AssertFormat()
	schema, err := compiler.Compile("testdata/sarif/sarif-schema-2.1.0.json")
	c.Assert(err, qt.IsNil)
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(data))
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Validate(doc), qt.IsNil)
}

func assertGitHubCodeScanningSARIF(c *qt.C, data string) {
	c.Helper()

	var report sarifForTest
	c.Assert(json.Unmarshal([]byte(data), &report), qt.IsNil)
	assertSARIFCommonGitHubFields(c, report)
	run := report.Runs[0]
	c.Assert(run.Tool.Driver.Rules, qt.Not(qt.HasLen), 0)

	rulesByID := make(map[string]sarifRuleForTest, len(run.Tool.Driver.Rules))
	for _, rule := range run.Tool.Driver.Rules {
		c.Assert(rule.ID, qt.Not(qt.Equals), "")
		c.Assert(rule.Name, qt.Not(qt.Equals), "")
		c.Assert(rule.ShortDescription.Text, qt.Not(qt.Equals), "")
		c.Assert(validSARIFLevel(rule.DefaultConfiguration.Level), qt.IsTrue,
			qt.Commentf("unexpected defaultConfiguration.level for rule %s", rule.ID))
		rulesByID[rule.ID] = rule
	}

	for _, result := range run.Results {
		c.Assert(result.RuleID, qt.Not(qt.Equals), "")
		c.Assert(rulesByID[result.RuleID].ID, qt.Equals, result.RuleID)
		c.Assert(result.RuleIndex >= 0 && result.RuleIndex < len(run.Tool.Driver.Rules), qt.IsTrue)
		c.Assert(run.Tool.Driver.Rules[result.RuleIndex].ID, qt.Equals, result.RuleID)
		c.Assert(validSARIFLevel(result.Level), qt.IsTrue,
			qt.Commentf("unexpected level for result %s", result.RuleID))
		c.Assert(result.Message.Text, qt.Not(qt.Equals), "")
		c.Assert(result.Locations, qt.Not(qt.HasLen), 0)
		location := result.Locations[0].PhysicalLocation
		c.Assert(location.ArtifactLocation.URI, qt.Not(qt.Equals), "")
		c.Assert(strings.HasPrefix(location.ArtifactLocation.URI, "/"), qt.IsFalse)
		c.Assert(strings.HasPrefix(location.ArtifactLocation.URI, "file:"), qt.IsFalse)
		c.Assert(location.ArtifactLocation.URIBaseID, qt.Equals, "%SRCROOT%")
		c.Assert(location.Region.StartLine, qt.Not(qt.Equals), 0)
		c.Assert(result.PartialFingerprints["primaryLocationLineHash"], qt.Not(qt.Equals), "")
	}
}

func assertSARIFCommonGitHubFields(c *qt.C, report sarifForTest) {
	c.Helper()

	c.Assert(report.Version, qt.Equals, "2.1.0")
	c.Assert(report.Schema, qt.Equals, "https://json.schemastore.org/sarif-2.1.0.json")
	c.Assert(report.Runs, qt.HasLen, 1)
	run := report.Runs[0]
	c.Assert(run.Tool.Driver.Name, qt.Not(qt.Equals), "")
	c.Assert(run.Tool.Driver.InformationURI, qt.Not(qt.Equals), "")
	c.Assert(run.OriginalURIBaseIDs["%SRCROOT%"].URI, qt.Equals, "file:///")
}

func ruleIndexByID(rules []sarifRuleForTest, id string) int {
	for i, rule := range rules {
		if rule.ID == id {
			return i
		}
	}
	return -1
}

func validSARIFLevel(level string) bool {
	switch level {
	case "none", "note", "warning", "error":
		return true
	default:
		return false
	}
}
