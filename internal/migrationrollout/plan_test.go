package migrationrollout_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationrollout"
)

const fleetDefinition = `
targets:
  - name: canary
    url: sqlite://canary.db
  - name: tenant-c
    url: sqlite://c.db
  - name: tenant-a
    url_env: TENANT_A_URL
  - name: tenant-b
    url: sqlite://b.db
stages:
  - name: wave-2
    depends_on: [wave-1]
    targets: [tenant-c]
  - name: wave-1
    depends_on: [canary]
    targets: [tenant-b, tenant-a]
    concurrency: 2
    on_failure: continue
    require: 50%
  - name: canary
    canary: true
    targets: [canary]
`

// redactedTenantA is the display form of tenant-a's URL.
const redactedTenantA = "postgres://app:***@db.internal/tenant_a" // #nosec G101 -- the display form, whose password is redacted

// The plan runs a stage after the stages it depends on whatever order the
// file declares them in, keeps each stage's members in the order the stage
// lists them, and fills in the defaults.
func TestResolve_HappyPath(t *testing.T) {
	c := qt.New(t)

	plan := mustPlan(c, fleetDefinition, map[string]string{"TENANT_A_URL": credentialURL("hunter22", "db.internal", "tenant_a")})

	c.Assert(plan.Document().Stages, qt.DeepEquals, []migrationrollout.StageRecord{
		{
			Name:        "canary",
			Canary:      true,
			Concurrency: 1,
			OnFailure:   migrationrollout.FailureStop,
			Require:     "all",
			Required:    1,
			Targets:     []migrationrollout.TargetRecord{{Name: "canary", Database: "sqlite://canary.db"}},
		},
		{
			Name:        "wave-1",
			DependsOn:   []string{"canary"},
			Concurrency: 2,
			OnFailure:   migrationrollout.FailureContinue,
			Require:     "50%",
			Required:    1,
			Targets: []migrationrollout.TargetRecord{
				{Name: "tenant-b", Database: "sqlite://b.db"},
				{Name: "tenant-a", Database: redactedTenantA, URLEnv: "TENANT_A_URL"},
			},
		},
		{
			Name:        "wave-2",
			DependsOn:   []string{"wave-1"},
			Concurrency: 1,
			OnFailure:   migrationrollout.FailureStop,
			Require:     "all",
			Required:    1,
			Targets:     []migrationrollout.TargetRecord{{Name: "tenant-c", Database: "sqlite://c.db"}},
		},
	})
	c.Assert(plan.Stages[1].Targets[1].URL, qt.Equals, credentialURL("hunter22", "db.internal", "tenant_a"))
	c.Assert(plan.Digest, qt.Matches, `sha256:[0-9a-f]{64}`)
}

// Stages ready at the same time run in the order the file declares them.
func TestResolve_IndependentStagesKeepDeclarationOrder(t *testing.T) {
	c := qt.New(t)

	plan := mustPlan(c, `
targets:
  - {name: a, url: sqlite://a.db}
  - {name: b, url: sqlite://b.db}
  - {name: c, url: sqlite://c.db}
stages:
  - {name: second, targets: [b]}
  - {name: third, targets: [c], depends_on: [first]}
  - {name: first, targets: [a]}
`, nil)

	names := make([]string, 0, len(plan.Stages))
	for _, stage := range plan.Stages {
		names = append(names, stage.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"second", "first", "third"})
}

// The digest is what binds a resumed run to the plan it started with. A
// credential may rotate between the two; anything else that changes what runs
// where changes the digest.
func TestResolve_DigestIgnoresCredentialsAndNothingElse(t *testing.T) {
	c := qt.New(t)
	base := mustPlan(c, fleetDefinition, map[string]string{"TENANT_A_URL": credentialURL("hunter22", "db.internal", "tenant_a")})

	cases := []struct {
		name       string
		definition string
		url        string
		wantSame   bool
	}{
		{name: "same definition", definition: fleetDefinition, url: credentialURL("hunter22", "db.internal", "tenant_a"), wantSame: true},
		{name: "rotated password", definition: fleetDefinition, url: credentialURL("rotated99", "db.internal", "tenant_a"), wantSame: true},
		{name: "another host", definition: fleetDefinition, url: credentialURL("hunter22", "db2.internal", "tenant_a"), wantSame: false},
		{
			name:       "another concurrency",
			definition: replaceOnce(c, fleetDefinition, "concurrency: 2", "concurrency: 3"),
			url:        credentialURL("hunter22", "db.internal", "tenant_a"),
			wantSame:   false,
		},
		{
			name:       "members in another order",
			definition: replaceOnce(c, fleetDefinition, "targets: [tenant-b, tenant-a]", "targets: [tenant-a, tenant-b]"),
			url:        credentialURL("hunter22", "db.internal", "tenant_a"),
			wantSame:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan := mustPlan(c, tc.definition, map[string]string{"TENANT_A_URL": tc.url})
			c.Assert(plan.Digest == base.Digest, qt.Equals, tc.wantSame)
		})
	}
}

// Paths in a definition are read against the definition's directory, and the
// plan records them as written so the digest does not depend on where the
// checkout lives.
func TestResolve_PathsAreReadAgainstTheDefinitionDirectory(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(dir, "approvals"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "approvals", "a.json"), []byte(`{"migrations":[]}`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "a.yaml"), []byte("{}\n"), 0o600), qt.IsNil)
	decoded, err := migrationrollout.Decode([]byte(`
targets:
  - {name: a, url: sqlite://a.db, config: a.yaml, expect_sequence: approvals/a.json}
stages:
  - {name: only, targets: [a]}
`))
	c.Assert(err, qt.IsNil)

	plan, err := decoded.Resolve(migrationrollout.ResolveOptions{BaseDir: dir})

	c.Assert(err, qt.IsNil)
	target := plan.Stages[0].Targets[0]
	c.Assert(target.Config, qt.Equals, "a.yaml")
	c.Assert(target.ConfigPath, qt.Equals, filepath.Join(dir, "a.yaml"))
	c.Assert(target.ExpectSequence, qt.Equals, "approvals/a.json")
	c.Assert(target.ExpectSequencePath, qt.Equals, filepath.Join(dir, "approvals", "a.json"))
}

// Two targets in one database are two targets when something selects a
// different history: the connection options or the config.
func TestResolve_OneDatabaseHoldsSeveralTargets(t *testing.T) {
	c := qt.New(t)

	plan := mustPlan(c, `
targets:
  - {name: tenant-a, url: "postgres://app@db.internal:5432/fleet?search_path=tenant_a"}
  - {name: tenant-b, url: "postgres://app@db.internal:5432/fleet?search_path=tenant_b"}
stages:
  - {name: only, targets: [tenant-a, tenant-b]}
`, nil)

	c.Assert(plan.Stages[0].Targets, qt.HasLen, 2)
}

func TestResolve_FailurePath(t *testing.T) {
	cases := []struct {
		name       string
		definition string
		wantErr    string
	}{
		{
			name:       "empty definition",
			definition: "targets: []\nstages: []\n",
			wantErr:    `(?s).*targets: the inventory names no target.*stages: the definition names no stage.*`,
		},
		{
			name: "duplicate target name",
			definition: `
targets: [{name: a, url: sqlite://a.db}, {name: a, url: sqlite://b.db}]
stages: [{name: s, targets: [a]}]`,
			wantErr: `(?s).*target "a" is declared more than once.*`,
		},
		{
			name: "one database under two names",
			definition: `
targets:
  - {name: a, url: "postgres://alice@db.internal/app"}
  - {name: b, url: "postgres://bob@db.internal:5432/app"}
stages: [{name: s, targets: [a, b]}]`,
			wantErr: `(?s).*targets "a" and "b" address one database with the same options and config.*`,
		},
		{
			name: "both url and url_env",
			definition: `
targets: [{name: a, url: sqlite://a.db, url_env: A_URL}]
stages: [{name: s, targets: [a]}]`,
			wantErr: `(?s).*target "a": set url or url_env, not both.*`,
		},
		{
			name: "neither url nor url_env",
			definition: `
targets: [{name: a}]
stages: [{name: s, targets: [a]}]`,
			wantErr: `(?s).*target "a": set url or url_env$`,
		},
		{
			name: "url_env not set",
			definition: `
targets: [{name: a, url_env: ROLLOUT_TEST_UNSET}]
stages: [{name: s, targets: [a]}]`,
			wantErr: `(?s).*target "a": the variable ROLLOUT_TEST_UNSET that url_env names is not set.*`,
		},
		{
			name: "config that does not exist",
			definition: `
targets: [{name: a, url: sqlite://a.db, config: missing.yaml}]
stages: [{name: s, targets: [a]}]`,
			wantErr: `(?s).*target "a": config "missing.yaml" cannot be read.*`,
		},
		{
			name: "name that is not a word",
			definition: `
targets: [{name: "tenant a", url: sqlite://a.db}]
stages: [{name: s, targets: [b]}]`,
			wantErr: `(?s).*targets\[0\]: name "tenant a" is not a word.*`,
		},
		{
			name: "stage names an unknown target",
			definition: `
targets: [{name: a, url: sqlite://a.db}]
stages: [{name: s, targets: [a, ghost]}]`,
			wantErr: `(?s).*stage "s": target "ghost" is not in the inventory.*`,
		},
		{
			name: "target in two stages",
			definition: `
targets: [{name: a, url: sqlite://a.db}]
stages: [{name: s, targets: [a]}, {name: t, targets: [a], depends_on: [s]}]`,
			wantErr: `(?s).*stage "t": target "a" is already a member of stage "s".*`,
		},
		{
			name: "target in no stage",
			definition: `
targets: [{name: a, url: sqlite://a.db}, {name: b, url: sqlite://b.db}]
stages: [{name: s, targets: [a]}]`,
			wantErr: `(?s).*target "b" belongs to no stage.*`,
		},
		{
			name: "stage with no targets",
			definition: `
targets: [{name: a, url: sqlite://a.db}]
stages: [{name: s, targets: [a]}, {name: empty}]`,
			wantErr: `(?s).*stage "empty": names no target.*`,
		},
		{
			name: "duplicate stage name",
			definition: `
targets: [{name: a, url: sqlite://a.db}]
stages: [{name: s, targets: [a]}, {name: s, targets: [a]}]`,
			wantErr: `(?s).*stage "s" is declared more than once.*`,
		},
		{
			name: "unknown dependency",
			definition: `
targets: [{name: a, url: sqlite://a.db}]
stages: [{name: s, targets: [a], depends_on: [ghost]}]`,
			wantErr: `(?s).*stage "s" depends on "ghost", which is not a stage.*`,
		},
		{
			name: "depends on itself",
			definition: `
targets: [{name: a, url: sqlite://a.db}]
stages: [{name: s, targets: [a], depends_on: [s]}]`,
			wantErr: `(?s).*stage "s": depends on itself.*stages form a dependency cycle: s -> s.*`,
		},
		{
			name: "dependency cycle",
			definition: `
targets: [{name: a, url: sqlite://a.db}, {name: b, url: sqlite://b.db}, {name: c, url: sqlite://c.db}]
stages:
  - {name: first, targets: [a]}
  - {name: x, targets: [b], depends_on: [first, y]}
  - {name: y, targets: [c], depends_on: [x]}`,
			wantErr: `(?s).*stages form a dependency cycle: x -> y -> x.*`,
		},
		{
			name: "concurrency below one",
			definition: `
targets: [{name: a, url: sqlite://a.db}]
stages: [{name: s, targets: [a], concurrency: -1}]`,
			wantErr: `(?s).*stage "s": concurrency -1 is below one.*`,
		},
		{
			name: "unknown failure policy",
			definition: `
targets: [{name: a, url: sqlite://a.db}]
stages: [{name: s, targets: [a], on_failure: retry}]`,
			wantErr: `(?s).*stage "s": on_failure "retry" is not stop or continue.*`,
		},
		{
			name: "requirement above the member count",
			definition: `
targets: [{name: a, url: sqlite://a.db}]
stages: [{name: s, targets: [a], require: 2}]`,
			wantErr: `(?s).*stage "s": require 2 is more than its 1 targets.*`,
		},
		{
			name: "two canaries",
			definition: `
targets: [{name: a, url: sqlite://a.db}, {name: b, url: sqlite://b.db}]
stages: [{name: s, targets: [a], canary: true}, {name: t, targets: [b], canary: true, depends_on: [s]}]`,
			wantErr: `(?s).*stages s, t are each marked canary; a rollout has at most one canary.*`,
		},
		{
			name: "canary with a dependency",
			definition: `
targets: [{name: a, url: sqlite://a.db}, {name: b, url: sqlite://b.db}]
stages: [{name: s, targets: [a]}, {name: canary, targets: [b], canary: true, depends_on: [s]}]`,
			wantErr: `(?s).*canary stage "canary" depends on another stage.*stage "s" does not depend on the canary stage "canary".*`,
		},
		{
			name: "stage that does not wait for the canary",
			definition: `
targets: [{name: a, url: sqlite://a.db}, {name: b, url: sqlite://b.db}, {name: c, url: sqlite://c.db}]
stages:
  - {name: canary, targets: [a], canary: true}
  - {name: wave, targets: [b], depends_on: [canary]}
  - {name: side, targets: [c]}`,
			wantErr: `(?s).*stage "side" does not depend on the canary stage "canary", so it could start before the canary succeeds.*`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := migrationrollout.Decode([]byte(tc.definition))
			c.Assert(err, qt.IsNil)

			plan, err := decoded.Resolve(migrationrollout.ResolveOptions{
				BaseDir:   c.TempDir(),
				LookupEnv: func(string) (string, bool) { return "", false },
			})

			c.Assert(err, qt.ErrorIs, migrationrollout.ErrInvalidDefinition)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(plan.Stages, qt.IsNil)
		})
	}
}

// A transitive dependency on the canary is enough: the canary still has to
// succeed before the stage can start.
func TestResolve_CanaryReachedThroughAnotherStage(t *testing.T) {
	c := qt.New(t)

	plan := mustPlan(c, `
targets: [{name: a, url: sqlite://a.db}, {name: b, url: sqlite://b.db}, {name: c, url: sqlite://c.db}]
stages:
  - {name: canary, targets: [a], canary: true}
  - {name: wave-1, targets: [b], depends_on: [canary]}
  - {name: wave-2, targets: [c], depends_on: [wave-1]}
`, nil)

	c.Assert(plan.Stages, qt.HasLen, 3)
}

func replaceOnce(c *qt.C, text, old, replacement string) string {
	c.Helper()
	c.Assert(strings.Count(text, old), qt.Equals, 1, qt.Commentf("%q must occur once in the definition", old))
	return strings.Replace(text, old, replacement, 1)
}
