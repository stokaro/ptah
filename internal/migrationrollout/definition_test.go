package migrationrollout_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationrollout"
)

func TestDecode_HappyPath(t *testing.T) {
	c := qt.New(t)

	definition, err := migrationrollout.Decode([]byte(`
targets:
  - name: canary
    url: sqlite://canary.db
  - name: tenant-a
    url_env: TENANT_A_URL
    config: tenant-a.yaml
    expect_sequence: approvals/tenant-a.json
    allow_destructive: true
stages:
  - name: canary
    canary: true
    targets: [canary]
  - name: wave
    depends_on: [canary]
    targets: [tenant-a]
    concurrency: 3
    on_failure: continue
    require: 50%
`))

	c.Assert(err, qt.IsNil)
	c.Assert(definition, qt.CmpEquals(requirementComparer), migrationrollout.Definition{
		Targets: []migrationrollout.TargetDefinition{
			{Name: "canary", URL: "sqlite://canary.db"},
			{
				Name:             "tenant-a",
				URLEnv:           "TENANT_A_URL",
				Config:           "tenant-a.yaml",
				ExpectSequence:   "approvals/tenant-a.json",
				AllowDestructive: true,
			},
		},
		Stages: []migrationrollout.StageDefinition{
			{Name: "canary", Canary: true, Targets: []string{"canary"}},
			{
				Name:        "wave",
				DependsOn:   []string{"canary"},
				Targets:     []string{"tenant-a"},
				Concurrency: 3,
				OnFailure:   "continue",
				Require:     migrationrollout.RequirePercent(50),
			},
		},
	})
}

// The spellings a requirement takes, and the one it reads as when absent.
func TestDecode_RequirementSpellings(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  string
	}{
		{name: "absent means all", value: "", want: "all"},
		{name: "all", value: "require: all", want: "all"},
		{name: "count", value: "require: 2", want: "2"},
		{name: "percentage", value: "require: 34%", want: "34%"},
		{name: "every member by percentage", value: "require: 100%", want: "100%"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			definition, err := migrationrollout.Decode([]byte("stages:\n  - name: s\n    " + tc.value + "\n"))
			c.Assert(err, qt.IsNil)
			c.Assert(definition.Stages[0].Require.String(), qt.Equals, tc.want)
		})
	}
}

func TestDecode_FailurePath(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "unknown top-level key",
			yaml:    "targets: []\nwaves: []\n",
			wantErr: `(?s)invalid rollout definition: .*field waves not found.*`,
		},
		{
			name:    "unknown target key",
			yaml:    "targets:\n  - name: a\n    url: sqlite://a.db\n    password: x\n",
			wantErr: `(?s)invalid rollout definition: .*field password not found.*`,
		},
		{
			name:    "repeated key",
			yaml:    "targets:\n  - name: a\n    name: b\n",
			wantErr: `(?s)invalid rollout definition: .*already defined.*`,
		},
		{
			name:    "wrong type",
			yaml:    "stages:\n  - name: s\n    concurrency: many\n",
			wantErr: `(?s)invalid rollout definition: .*cannot unmarshal.*`,
		},
		{
			name:    "a second document",
			yaml:    "targets: []\n---\nstages: []\n",
			wantErr: `invalid rollout definition: the file holds more than one document`,
		},
		{
			name:    "no document",
			yaml:    "# nothing here\n",
			wantErr: `invalid rollout definition: the file holds no document`,
		},
		{
			name:    "zero count",
			yaml:    "stages:\n  - name: s\n    require: 0\n",
			wantErr: `(?s).*require "0" is not all, a positive count of targets, or a percentage.*`,
		},
		{
			name:    "percentage above a hundred",
			yaml:    "stages:\n  - name: s\n    require: 101%\n",
			wantErr: `(?s).*require "101%" is not a percentage from 1% to 100%.*`,
		},
		{
			name:    "a word that is not all",
			yaml:    "stages:\n  - name: s\n    require: most\n",
			wantErr: `(?s).*require "most" is not all.*`,
		},
		{
			name:    "a list where a scalar belongs",
			yaml:    "stages:\n  - name: s\n    require: [1]\n",
			wantErr: `(?s).*require is all, a count of targets, or a percentage such as 50%.*`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			definition, err := migrationrollout.Decode([]byte(tc.yaml))
			c.Assert(err, qt.ErrorIs, migrationrollout.ErrInvalidDefinition)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(definition, qt.CmpEquals(requirementComparer), migrationrollout.Definition{})
		})
	}
}
