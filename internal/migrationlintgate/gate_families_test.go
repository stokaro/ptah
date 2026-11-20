package migrationlintgate_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/migration/lint"
)

// gateFS is a MySQL migration directory whose one pending version carries a
// lock-heavy MODIFY (locking/blocking-alter-table, warning by default) and nothing destructive, so
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

// TestAnalyze_GateRulesWidenWhatBlocks pins the policy mechanism: a rule
// named under `gate` runs even though the gate disables it by default, and
// its error-severity findings refuse the apply the way data-safety findings do.
func TestAnalyze_GateRulesWidenWhatBlocks(t *testing.T) {
	c := qt.New(t)

	findings, err := migrationlintgate.Analyze(c.Context(), builtintest.Runtime(), gateFS("dialect: mysql\ngate:\n  rules: [locking/blocking-alter-table]\nrules:\n  locking/blocking-alter-table:\n    severity: error\n"), migrationlintgate.Pending{Versions: []int64{1}}, "mysql", "")

	c.Assert(err, qt.IsNil)
	c.Assert(gateRules(findings), qt.DeepEquals, []string{"locking/blocking-alter-table"})
}

// TestAnalyze_GateRulesStillNeedAnErrorSeverity: widening the gate names
// a rule; the severity the finding must reach stays the same, so a warning
// from a gated rule is reported by lint and does not refuse the apply.
func TestAnalyze_GateRulesStillNeedAnErrorSeverity(t *testing.T) {
	c := qt.New(t)

	findings, err := migrationlintgate.Analyze(c.Context(), builtintest.Runtime(), gateFS("dialect: mysql\ngate:\n  rules: [locking/blocking-alter-table]\n"), migrationlintgate.Pending{Versions: []int64{1}}, "mysql", "")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

// TestAnalyze_WithoutAGateSectionOnlyDataSafetyBlocks is the control: the
// same error-severity locking/blocking-alter-table is dropped by the default gate, which is the
// behavior every existing directory relies on.
func TestAnalyze_WithoutAGateSectionOnlyDataSafetyBlocks(t *testing.T) {
	c := qt.New(t)

	findings, err := migrationlintgate.Analyze(c.Context(), builtintest.Runtime(), gateFS("dialect: mysql\nrules:\n  locking/blocking-alter-table:\n    severity: error\n"), migrationlintgate.Pending{Versions: []int64{1}}, "mysql", "")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

func TestLoadPolicy_GateRulesAreListed(t *testing.T) {
	c := qt.New(t)

	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), gateFS("gate:\n  rules: [locking, rewrite/set-not-null]\n"), "mysql")

	c.Assert(err, qt.IsNil)
	c.Assert(policy.BlockingRules(), qt.DeepEquals, append(migrationlintgate.ReportedRules(), "locking", "rewrite/set-not-null"))
}

func TestLoadPolicy_GateRefusesARuleNobodyHas(t *testing.T) {
	tests := []struct {
		name    string
		policy  string
		wantErr string
	}{
		{
			name:    "an unknown name",
			policy:  "gate:\n  rules: [design/zz]\n",
			wantErr: `.*gate: rule "design/zz" is not the name of a migration lint rule; .*`,
		},
		{
			name:    "a code",
			policy:  "gate:\n  rules: [MY101]\n",
			wantErr: `.*gate: rule lint rules are named category/name; MY101 is not a rule name`,
		},
		{
			name:    "a family prefix",
			policy:  "gate:\n  rules: [MY]\n",
			wantErr: `.*gate: rule lint rules are named category/name; MY is not a rule name`,
		},
		{
			name:    "an empty list",
			policy:  "gate:\n  rules: []\n",
			wantErr: `.*gate: rules lists nothing, so the section would widen nothing`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), gateFS(test.policy), "mysql")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
