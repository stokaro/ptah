package migrationlintgate_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationlintgate"
	"ptah.run/migration/lint"
)

// gateFS is a MySQL migration directory whose one pending version carries a
// lock-heavy MODIFY (MY101, warning by default) and nothing destructive, so
// what the gate refuses on is decided by the policy alone.
func gateFS(policy string) fstest.MapFS {
	return fstest.MapFS{
		lint.ConfigFileName:         {Data: []byte(policy)},
		"0000000001_widen.up.sql":   {Data: []byte("ALTER TABLE users MODIFY COLUMN email VARCHAR(320) NOT NULL;\n")},
		"0000000001_widen.down.sql": {Data: []byte("ALTER TABLE users MODIFY COLUMN email VARCHAR(255) NOT NULL;\n")},
	}
}

func gateRules(findings []lint.Finding) []string {
	rules := make([]string, 0, len(findings))
	for _, finding := range findings {
		rules = append(rules, finding.Rule)
	}
	return rules
}

// TestAnalyze_GateFamiliesWidenWhatBlocks pins the policy mechanism: a family
// named under `gate` runs even though the gate disables it by default, and
// its error-severity findings refuse the apply the way DS findings do.
func TestAnalyze_GateFamiliesWidenWhatBlocks(t *testing.T) {
	c := qt.New(t)

	findings, err := migrationlintgate.Analyze(gateFS("dialect: mysql\ngate:\n  families: [MY]\nrules:\n  MY101:\n    severity: error\n"), []int64{1}, "mysql", "")

	c.Assert(err, qt.IsNil)
	c.Assert(gateRules(findings), qt.DeepEquals, []string{"MY101"})
}

// TestAnalyze_GateFamiliesStillNeedAnErrorSeverity: widening the gate names
// a family; the severity the finding must reach stays the same, so a warning
// in a gated family is reported by lint and does not refuse the apply.
func TestAnalyze_GateFamiliesStillNeedAnErrorSeverity(t *testing.T) {
	c := qt.New(t)

	findings, err := migrationlintgate.Analyze(gateFS("dialect: mysql\ngate:\n  families: [MY]\n"), []int64{1}, "mysql", "")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

// TestAnalyze_WithoutAGateSectionOnlyDataSafetyBlocks is the control: the
// same error-severity MY101 is dropped by the default gate, which is the
// behavior every existing directory relies on.
func TestAnalyze_WithoutAGateSectionOnlyDataSafetyBlocks(t *testing.T) {
	c := qt.New(t)

	findings, err := migrationlintgate.Analyze(gateFS("dialect: mysql\nrules:\n  MY101:\n    severity: error\n"), []int64{1}, "mysql", "")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

func TestLoadPolicy_GateFamiliesAreListed(t *testing.T) {
	c := qt.New(t)

	policy, err := migrationlintgate.LoadPolicy(gateFS("gate:\n  families: [PG, MY, DS]\n"), "mysql")

	c.Assert(err, qt.IsNil)
	c.Assert(policy.BlockingFamilies(), qt.DeepEquals, []string{"DS", "PG", "MY"})
}

func TestLoadPolicy_GateRefusesAFamilyNoRuleBelongsTo(t *testing.T) {
	tests := []struct {
		name    string
		policy  string
		wantErr string
	}{
		{
			name:    "an unknown family",
			policy:  "gate:\n  families: [ZZ]\n",
			wantErr: `.*gate: family "ZZ" matches no registered rule`,
		},
		{
			name:    "a lowercase spelling",
			policy:  "gate:\n  families: [my]\n",
			wantErr: `.*gate: family "my" matches no registered rule`,
		},
		{
			name:    "an empty list",
			policy:  "gate:\n  families: []\n",
			wantErr: `.*gate: families lists nothing, so the section would widen nothing`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := migrationlintgate.LoadPolicy(gateFS(test.policy), "mysql")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
