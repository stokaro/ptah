package migraterollout_test

import (
	"bytes"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migraterollout"
	"ptah.run/internal/errorkind"
	"ptah.run/internal/migrationrollout"
)

const definition = `
targets:
  - {name: canary, url: "sqlite://canary.db"}
  - {name: tenant-a, url_env: ROLLOUT_TEST_TENANT_A_URL}
stages:
  - {name: canary, canary: true, targets: [canary]}
  - {name: wave, depends_on: [canary], targets: [tenant-a], concurrency: 2, on_failure: continue}
`

// workspace is a directory holding a definition and a one-migration release.
type workspace struct {
	dir        string
	definition string
	migrations string
	state      string
}

func newWorkspace(c *qt.C) workspace {
	c.Helper()
	dir := c.TempDir()
	ws := workspace{
		dir:        dir,
		definition: filepath.Join(dir, "rollout.yaml"),
		migrations: filepath.Join(dir, "migrations"),
		state:      filepath.Join(dir, "state.json"),
	}
	c.Assert(os.WriteFile(ws.definition, []byte(definition), 0o600), qt.IsNil)
	c.Assert(os.MkdirAll(ws.migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(ws.migrations, "0000000001_init.up.sql"), []byte("CREATE TABLE a (id INTEGER);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(ws.migrations, "0000000001_init.down.sql"), []byte("DROP TABLE a;\n"), 0o600), qt.IsNil)
	tenant := url.URL{Scheme: "postgres", User: url.UserPassword("app", "Xk9pass"), Host: "db.internal", Path: "/tenant_a"}
	c.Setenv("ROLLOUT_TEST_TENANT_A_URL", tenant.String())
	return ws
}

// execute runs `rollout` with args and returns both streams and the error.
func execute(args ...string) (stdout, stderr string, err error) {
	cmd := migraterollout.NewRolloutCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), diagnostics.String(), err
}

func TestPlan_HappyPath(t *testing.T) {
	c := qt.New(t)
	ws := newWorkspace(c)

	stdout, _, err := execute("plan", "--definition", ws.definition)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Matches, `(?s)Rollout plan sha256:[0-9a-f]{64}\n`+
		`\nStage 1: canary \(canary\)\n`+
		`  concurrency: 1, on failure: stop, requires: all \(1 of 1\)\n`+
		`  - canary  sqlite://canary\.db\n`+
		`\nStage 2: wave\n`+
		`  after: canary\n`+
		`  concurrency: 2, on failure: continue, requires: all \(1 of 1\)\n`+
		`  - tenant-a  postgres://app:\*\*\*@db\.internal/tenant_a  \(url from \$ROLLOUT_TEST_TENANT_A_URL\)\n`)
	c.Assert(stdout, qt.Not(qt.Contains), "Xk9pass")
}

func TestPlan_JSONDocument(t *testing.T) {
	c := qt.New(t)
	ws := newWorkspace(c)

	stdout, _, err := execute("plan", "--definition", ws.definition, "--json")

	c.Assert(err, qt.IsNil)
	var document migrationrollout.PlanDocument
	c.Assert(json.Unmarshal([]byte(stdout), &document), qt.IsNil)
	c.Assert(document.ContractVersion, qt.Equals, migrationrollout.PlanContractVersion)
	c.Assert(document.Stages, qt.HasLen, 2)
	c.Assert(document.Stages[1].Targets[0].Database, qt.Equals, "postgres://app:***@db.internal/tenant_a")
	c.Assert(stdout, qt.Not(qt.Contains), "Xk9pass")
}

func TestPlan_FailurePath(t *testing.T) {
	c := qt.New(t)
	ws := newWorkspace(c)
	c.Assert(os.WriteFile(ws.definition, []byte(`
targets: [{name: a, url: "sqlite://a.db"}]
stages:
  - {name: x, targets: [a], depends_on: [y]}
  - {name: y, targets: [ghost], depends_on: [x]}
`), 0o600), qt.IsNil)

	stdout, _, err := execute("plan", "--definition", ws.definition, "--json")

	c.Assert(err, qt.ErrorIs, migrationrollout.ErrInvalidDefinition)
	c.Assert(err, qt.ErrorMatches, `(?s).*rollout\.yaml: invalid rollout definition:\n`+
		`- stage "y": target "ghost" is not in the inventory\n`+
		`- stages form a dependency cycle: x -> y -> x`)
	var failure errorkind.Failure
	c.Assert(json.Unmarshal([]byte(stdout), &failure), qt.IsNil)
	c.Assert(failure.Command, qt.Equals, "migrations rollout plan")
	c.Assert(failure.Error.Message, qt.Contains, "stages form a dependency cycle: x -> y -> x")
}

// Every refusal below happens before a target runs, so none of them leaves a
// state file behind for a run that never started.
func TestRun_RefusesBeforeAnyTargetRuns(t *testing.T) {
	cases := []struct {
		name    string
		args    func(ws workspace) []string
		wantErr string
	}{
		{
			name:    "missing flags",
			args:    func(workspace) []string { return []string{"run"} },
			wantErr: `--definition, --migrations-dir, --state-file required`,
		},
		{
			name: "missing release directory",
			args: func(ws workspace) []string {
				return []string{"run", "--definition", ws.definition, "--migrations-dir", filepath.Join(ws.dir, "absent"), "--state-file", ws.state}
			},
			wantErr: `(?s)open migrations directory: .*`,
		},
		{
			name: "invalid definition",
			args: func(ws workspace) []string {
				return []string{"run", "--definition", filepath.Join(ws.dir, "absent.yaml"), "--migrations-dir", ws.migrations, "--state-file", ws.state}
			},
			wantErr: `(?s)read rollout definition: .*`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			ws := newWorkspace(c)

			_, _, err := execute(tc.args(ws)...)

			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			_, statErr := os.Stat(ws.state)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
			_, lockErr := os.Stat(ws.state + ".lock")
			c.Assert(lockErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// A variable that would waive a guard, or change what a run does, for every
// target at once refuses the rollout, whatever its value.
func TestRun_RefusesAGroupBypassInTheEnvironment(t *testing.T) {
	variables := []string{
		"PTAH_ALLOW_DIRTY",
		"PTAH_SKIP_CHECKS",
		"PTAH_ALLOW_DESTRUCTIVE",
		"PTAH_EXPECT_SEQUENCE",
		"PTAH_DRY_RUN",
		"PTAH_LIMIT",
		"PTAH_TO_VERSION",
	}
	for _, variable := range variables {
		t.Run(variable, func(t *testing.T) {
			c := qt.New(t)
			ws := newWorkspace(c)
			c.Setenv(variable, "false")

			_, _, err := execute("run", "--definition", ws.definition, "--migrations-dir", ws.migrations, "--state-file", ws.state)

			c.Assert(err, qt.ErrorMatches, variable+` set: a rollout passes its environment to every target, .*`)
			_, statErr := os.Stat(ws.state)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// A new run never takes over the record of another, and a resume needs one.
func TestRun_StateFileRules(t *testing.T) {
	c := qt.New(t)
	ws := newWorkspace(c)
	c.Assert(os.WriteFile(ws.state, []byte("{}"), 0o600), qt.IsNil)

	_, _, runErr := execute("run", "--definition", ws.definition, "--migrations-dir", ws.migrations, "--state-file", ws.state)
	_, _, resumeErr := execute("resume", "--definition", ws.definition, "--migrations-dir", ws.migrations,
		"--state-file", filepath.Join(ws.dir, "absent.json"))

	c.Assert(runErr, qt.ErrorMatches, `the state file .*state\.json already records a rollout; `+
		`continue it with "ptah migrations rollout resume", or name a new --state-file to start another`)
	c.Assert(resumeErr, qt.ErrorMatches, `read rollout state: .*absent\.json: no such file or directory`)
	content, err := os.ReadFile(ws.state)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, "{}")
}

// A second invocation on one state refuses while the first holds its lock.
func TestRun_RefusesALockedState(t *testing.T) {
	c := qt.New(t)
	ws := newWorkspace(c)
	unlock, err := migrationrollout.LockState(ws.state)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = unlock() })

	_, _, runErr := execute("run", "--definition", ws.definition, "--migrations-dir", ws.migrations, "--state-file", ws.state)

	c.Assert(runErr, qt.ErrorMatches, `another rollout holds .*state\.json\.lock .*`)
}

// A resumed run refuses a release whose content is not the one recorded,
// before it reads or runs any target.
func TestResume_RefusesAChangedRelease(t *testing.T) {
	c := qt.New(t)
	ws := newWorkspace(c)
	decoded, err := migrationrollout.Decode([]byte(definition))
	c.Assert(err, qt.IsNil)
	plan, err := decoded.Resolve(migrationrollout.ResolveOptions{BaseDir: ws.dir})
	c.Assert(err, qt.IsNil)
	recorded := migrationrollout.Release{
		Kind:   migrationrollout.ReleaseDirectory,
		Source: ws.migrations,
		Digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	}
	c.Assert(migrationrollout.CreateState(ws.state, migrationrollout.NewState(plan, recorded, ws.definition, time.Now())), qt.IsNil)

	stdout, _, err := execute("resume", "--definition", ws.definition, "--migrations-dir", ws.migrations, "--state-file", ws.state, "--json")

	c.Assert(err, qt.ErrorIs, migrationrollout.ErrReleaseChanged)
	var failure errorkind.Failure
	c.Assert(json.Unmarshal([]byte(stdout), &failure), qt.IsNil)
	c.Assert(failure.Command, qt.Equals, "migrations rollout resume")
	state, err := migrationrollout.ReadState(ws.state)
	c.Assert(err, qt.IsNil)
	c.Assert(state.Invocations, qt.Equals, 0)
}

// --retry-failed is a decision about this resume, so no variable makes it.
func TestResume_RetryFailedReadsNoVariable(t *testing.T) {
	c := qt.New(t)
	cmd := migraterollout.NewRolloutCommand()
	resume, _, err := cmd.Find([]string{"resume"})
	c.Assert(err, qt.IsNil)

	flag := resume.Flags().Lookup("retry-failed")

	c.Assert(flag, qt.Not(qt.IsNil))
	c.Assert(flag.Annotations["ptah.env.disabled"], qt.DeepEquals, []string{"true"})
}
