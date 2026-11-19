package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// driftEnv wraps body in an env named "local", whose drift block, when body
// declares one, starts on line 4.
func driftEnv(body string) []byte {
	return []byte("env \"local\" {\n  url = \"sqlite://m.db\"\n  dev = \"sqlite://d.db\"\n" + body + "}\n")
}

// ignoredNames lists the names cfg reports as ignored, in order.
func ignoredNames(cfg projectconfig.Config) []string {
	names := make([]string, 0, len(cfg.IgnoredConstructs))
	for _, construct := range cfg.IgnoredConstructs {
		names = append(names, construct.Name)
	}
	return names
}

func TestParseAtlasPreApplyDrift_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		body    string
		want    *projectconfig.PreApplyDrift
		ignored []string
	}{
		{
			name: "an empty drift block refuses on drift",
			body: "  check \"migrate_apply\" {\n    drift {}\n  }\n",
			want: &projectconfig.PreApplyDrift{OnError: projectconfig.DriftOnErrorFail, Filename: "atlas.hcl", Line: 5},
		},
		{
			name: "FAIL written as the bare identifier",
			body: "  check \"migrate_apply\" {\n    drift {\n      on_error = FAIL\n    }\n  }\n",
			want: &projectconfig.PreApplyDrift{OnError: projectconfig.DriftOnErrorFail, Filename: "atlas.hcl", Line: 5},
		},
		{
			name: "CONTINUE written as the bare identifier",
			body: "  check \"migrate_apply\" {\n    drift {\n      on_error = CONTINUE\n    }\n  }\n",
			want: &projectconfig.PreApplyDrift{OnError: projectconfig.DriftOnErrorContinue, Filename: "atlas.hcl", Line: 5},
		},
		{
			name: "an expression choosing between the identifiers by env",
			body: "  check \"migrate_apply\" {\n    drift {\n      on_error = atlas.env == \"prod\" ? FAIL : CONTINUE\n    }\n  }\n",
			want: &projectconfig.PreApplyDrift{OnError: projectconfig.DriftOnErrorContinue, Filename: "atlas.hcl", Line: 5},
		},
		{
			name: "an exclude list replaces the env's",
			body: "  exclude = [\"audit\"]\n  check \"migrate_apply\" {\n    drift {\n      exclude = [\"scratch\", \"*.*.comment\"]\n    }\n  }\n",
			want: &projectconfig.PreApplyDrift{
				OnError: projectconfig.DriftOnErrorFail, Exclude: []string{"scratch", "*.*.comment"}, ExcludeSet: true,
				Filename: "atlas.hcl", Line: 6,
			},
		},
		{
			name: "an empty exclude list still replaces the env's",
			body: "  exclude = [\"audit\"]\n  check \"migrate_apply\" {\n    drift {\n      exclude = []\n    }\n  }\n",
			want: &projectconfig.PreApplyDrift{
				OnError: projectconfig.DriftOnErrorFail, Exclude: make([]string, 0), ExcludeSet: true, Filename: "atlas.hcl", Line: 6,
			},
		},
		{
			name:    "a pre-execution rule beside the drift block is ignored",
			body:    "  check \"migrate_apply\" {\n    drift {}\n    deny {\n      message = \"no\"\n    }\n  }\n",
			want:    &projectconfig.PreApplyDrift{OnError: projectconfig.DriftOnErrorFail, Filename: "atlas.hcl", Line: 5},
			ignored: []string{"deny"},
		},
		{
			name:    "an unknown drift setting is ignored",
			body:    "  check \"migrate_apply\" {\n    drift {\n      frobnicate = true\n    }\n  }\n",
			want:    &projectconfig.PreApplyDrift{OnError: projectconfig.DriftOnErrorFail, Filename: "atlas.hcl", Line: 5},
			ignored: []string{"frobnicate"},
		},
		{
			name:    "a check of another command beside it is ignored",
			body:    "  check \"migrate_apply\" {\n    drift {}\n  }\n  check \"schema_apply\" {\n    drift {}\n  }\n",
			want:    &projectconfig.PreApplyDrift{OnError: projectconfig.DriftOnErrorFail, Filename: "atlas.hcl", Line: 5},
			ignored: []string{"check"},
		},
		{
			name:    "a migrate_apply check without a drift block runs nothing",
			body:    "  check \"migrate_apply\" {\n    deny {\n      message = \"no\"\n    }\n  }\n",
			ignored: []string{"deny", "check"},
		},
		{
			name:    "a check without a label runs nothing",
			body:    "  check {\n    drift {}\n  }\n",
			ignored: []string{"check"},
		},
		{
			name:    "a check of another command runs nothing",
			body:    "  check \"schema_apply\" {\n    drift {}\n  }\n",
			ignored: []string{"check"},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := projectconfig.ParseAtlas(driftEnv(row.body), "atlas.hcl", "local")
			c.Assert(err, qt.IsNil)
			c.Assert(cfg.PreApplyDrift, qt.DeepEquals, row.want)
			c.Assert(ignoredNames(cfg), qt.DeepEquals, append(make([]string, 0), row.ignored...))
		})
	}
}

func TestParseAtlasPreApplyDrift_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "an identifier other than FAIL and CONTINUE",
			body:    "  check \"migrate_apply\" {\n    drift {\n      on_error = ABORT\n    }\n  }\n",
			wantErr: `cannot evaluate atlas\.hcl "on_error" at atlas\.hcl:6: .*There is no variable named "ABORT".*`,
		},
		{
			name:    "a string other than FAIL and CONTINUE",
			body:    "  check \"migrate_apply\" {\n    drift {\n      on_error = \"ABORT\"\n    }\n  }\n",
			wantErr: `atlas\.hcl "on_error" at atlas\.hcl:6 must be FAIL or CONTINUE`,
		},
		{
			name:    "a number",
			body:    "  check \"migrate_apply\" {\n    drift {\n      on_error = 1\n    }\n  }\n",
			wantErr: `atlas\.hcl "on_error" at atlas\.hcl:6 must be FAIL or CONTINUE`,
		},
		{
			name:    "an exclude that is not a list",
			body:    "  check \"migrate_apply\" {\n    drift {\n      exclude = \"scratch\"\n    }\n  }\n",
			wantErr: `.*"exclude" at atlas\.hcl:6.*`,
		},
		{
			name:    "two drift blocks in one check",
			body:    "  check \"migrate_apply\" {\n    drift {}\n    drift {}\n  }\n",
			wantErr: `unsupported atlas\.hcl construct "drift" at atlas\.hcl:6`,
		},
		{
			name:    "two migrate_apply checks",
			body:    "  check \"migrate_apply\" {\n    drift {}\n  }\n  check \"migrate_apply\" {\n    drift {}\n  }\n",
			wantErr: `duplicate check "migrate_apply" at atlas\.hcl:7`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := projectconfig.ParseAtlas(driftEnv(row.body), "atlas.hcl", "local")
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(cfg.PreApplyDrift, qt.IsNil)
		})
	}
}

// TestPreApplyDriftOrigin pins the phrase a diagnostic names the block by.
func TestPreApplyDriftOrigin(t *testing.T) {
	c := qt.New(t)
	drift := projectconfig.PreApplyDrift{Filename: "deploy/atlas.hcl", Line: 12}
	c.Assert(drift.Origin(), qt.Equals, `atlas.hcl check "migrate_apply" drift at deploy/atlas.hcl:12`)
}
