package lint

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/migration/lint"
)

func execute(args ...string) (stdout, stderr string, err error) {
	cmd := NewLintCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func TestNewLintCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := NewLintCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "lint")
	c.Assert(cmd.Short, qt.Contains, "Lint")
}

func TestRunLint_CuratedFixtureProducesExpectedRuleHits(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", "testdata/bad", "--format", "json")

	// The fixture contains DS errors, so the default --fail-on=error exits 1.
	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)

	var report struct {
		Failed   bool           `json:"failed"`
		Findings []lint.Finding `json:"findings"`
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
	var report struct {
		Version string `json:"version"`
		Runs    []struct {
			Tool struct {
				Driver struct {
					Name  string `json:"name"`
					Rules []struct {
						ID string `json:"id"`
					} `json:"rules"`
				} `json:"driver"`
			} `json:"tool"`
			Results []struct {
				RuleID    string `json:"ruleId"`
				Level     string `json:"level"`
				Locations []struct {
					PhysicalLocation struct {
						ArtifactLocation struct {
							URI string `json:"uri"`
						} `json:"artifactLocation"`
						Region struct {
							StartLine int `json:"startLine"`
						} `json:"region"`
					} `json:"physicalLocation"`
				} `json:"locations"`
			} `json:"results"`
		} `json:"runs"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Version, qt.Equals, "2.1.0")
	c.Assert(report.Runs, qt.HasLen, 1)
	c.Assert(report.Runs[0].Tool.Driver.Name, qt.Equals, "ptah lint")
	c.Assert(report.Runs[0].Tool.Driver.Rules[0].ID, qt.Not(qt.Equals), "")
	var dropTableResult struct {
		RuleID    string `json:"ruleId"`
		Level     string `json:"level"`
		Locations []struct {
			PhysicalLocation struct {
				ArtifactLocation struct {
					URI string `json:"uri"`
				} `json:"artifactLocation"`
				Region struct {
					StartLine int `json:"startLine"`
				} `json:"region"`
			} `json:"physicalLocation"`
		} `json:"locations"`
	}
	for _, result := range report.Runs[0].Results {
		if result.RuleID == "DS101" {
			dropTableResult = result
			break
		}
	}
	c.Assert(dropTableResult.RuleID, qt.Equals, "DS101")
	c.Assert(dropTableResult.Level, qt.Equals, "error")
	c.Assert(dropTableResult.Locations[0].PhysicalLocation.ArtifactLocation.URI, qt.Contains, "testdata/bad/")
	c.Assert(dropTableResult.Locations[0].PhysicalLocation.Region.StartLine, qt.Equals, 2)
}

func TestRunLint_ConfigFileDisablesRulesAndSetsDialect(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", "testdata/with-config", "--format", "json")

	c.Assert(err, qt.IsNotNil, qt.Commentf("DS errors remain, so the run still fails"))
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)

	var report struct {
		Dialect  string         `json:"dialect"`
		Findings []lint.Finding `json:"findings"`
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

func TestRunLint_ConfigRuleSeverityAndExclude(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	write := func(name, content string) {
		path := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
	write(lint.ConfigFileName, `rules:
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
		Findings []lint.Finding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Findings[0].Severity, qt.Equals, lint.SeverityWarning)
	c.Assert(report.Findings[0].File, qt.Contains, "main/0000000002_main.up.sql")
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
	c.Assert(stderr, qt.Contains, `invalid dialect "oracle" in lint config`)

	_, stderr, err = execute("--dir", "testdata/bad", "--no-such-flag")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "unknown flag")

	// Positional arguments would silently lint the default --dir instead of
	// what the user pointed at — a silent false negative in CI.
	_, stderr, err = execute("testdata/bad")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "unexpected positional arguments")
}

func TestRunLint_ExplicitEmptyDialectOverridesConfig(t *testing.T) {
	c := qt.New(t)

	// The config sets dialect: postgres; an explicit --dialect "" must win
	// and re-enable the MY family.
	_, stderr, err := execute("--dir", "testdata/with-config", "--format", "json", "--dialect", "")

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)

	var report struct {
		Dialect  string         `json:"dialect"`
		Findings []lint.Finding `json:"findings"`
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

func TestWriteGitHubActions_EscapesWorkflowCommandCharacters(t *testing.T) {
	c := qt.New(t)

	var buf bytes.Buffer
	writeGitHubActions(&buf, lintReport{
		Findings: []lint.Finding{{
			Rule:     "DS101",
			Severity: lint.SeverityError,
			File:     "dir/evil,file::name.sql",
			Line:     3,
			Message:  "50% data loss\r\nsecond line",
		}},
	})

	out := buf.String()
	c.Assert(out, qt.Contains, "::error file=dir/evil%2Cfile%3A%3Aname.sql,line=3::")
	c.Assert(out, qt.Contains, "DS101: 50%25 data loss%0D%0Asecond line")
	c.Assert(out, qt.Not(qt.Contains), "evil,file::name")

	buf.Reset()
	writeGitHubActions(&buf, lintReport{Error: "bad\nnews: 100%"})
	c.Assert(buf.String(), qt.Equals, "::error::bad%0Anews: 100%25\n")
}

func TestShouldFail(t *testing.T) {
	c := qt.New(t)

	warning := []lint.Finding{{Severity: lint.SeverityWarning}}
	fatal := []lint.Finding{{Severity: lint.SeverityError}}

	c.Assert(shouldFail(nil, failOnError), qt.IsFalse)
	c.Assert(shouldFail(warning, failOnError), qt.IsFalse)
	c.Assert(shouldFail(fatal, failOnError), qt.IsTrue)
	c.Assert(shouldFail(warning, failOnAny), qt.IsTrue)
	c.Assert(shouldFail(fatal, failOnNone), qt.IsFalse)
}
