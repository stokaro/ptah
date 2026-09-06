package schemasecurity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemasecurity"
)

// grantedSchema is one role granted SELECT on two tables.
func grantedSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "reporting", Privileges: []string{"SELECT"}, OnTable: "orders"},
			{Role: "reporting", Privileges: []string{"SELECT"}, OnTable: "customers"},
		},
	}
}

// codes returns the finding codes a report carries.
func codes(report schemasecurity.Report) []string {
	found := make([]string, 0, len(report.Findings))
	for _, finding := range report.Findings {
		found = append(found, finding.Code)
	}
	return found
}

// skippedCodes returns the codes a report reported as skipped.
func skippedCodes(report schemasecurity.Report) []string {
	skipped := make([]string, 0, len(report.SkippedRules))
	for _, rule := range report.SkippedRules {
		skipped = append(skipped, rule.Code)
	}
	return skipped
}

// Without a usage signal, ROL01 says it did not run -- stokaro/ptah#1961.
//
// It cannot be answered from a schema: a privilege is not use, and no catalog
// Ptah reads records which roles read which objects. A rule that silently
// reported nothing here would be indistinguishable from one that found nothing,
// which is the difference a clean report exists to state.
func TestROL01_IsSkippedWithoutAUsageSignal(t *testing.T) {
	c := qt.New(t)

	report := schemasecurity.Analyze(grantedSchema(), schemasecurity.Options{})

	c.Assert(codes(report), qt.Not(qt.Contains), "ROL01")
	c.Assert(skippedCodes(report), qt.Contains, "ROL01")
	c.Assert(skipReason(report, "ROL01"), qt.Contains, "usage")
}

// skipReason returns the reason a report gave for skipping a code, or the empty
// string when it did not skip it.
func skipReason(report schemasecurity.Report, code string) string {
	reasons := make(map[string]string, len(report.SkippedRules))
	for _, rule := range report.SkippedRules {
		reasons[rule.Code] = rule.Reason
	}
	return reasons[code]
}

// An empty non-nil signal is a real answer, and it is the opposite of nil.
//
// A window in which nothing was used makes every grant in it unused. Collapsing
// the two states would make the rule either always silent or always noisy.
func TestROL01_AnEmptySignalIsCollectedRatherThanAbsent(t *testing.T) {
	c := qt.New(t)

	report := schemasecurity.Analyze(grantedSchema(), schemasecurity.Options{
		RoleObjectUsage: make([]schemasecurity.RoleObjectUsage, 0),
	})

	c.Assert(skippedCodes(report), qt.Not(qt.Contains), "ROL01")
	c.Assert(report.Findings, qt.HasLen, 2)
	c.Assert(codes(report), qt.DeepEquals, []string{"ROL01", "ROL01"})
}

// An observed object is not reported; an unobserved one is.
func TestROL01_NamesOnlyTheGrantsNothingWasObservedUsing(t *testing.T) {
	tests := []struct {
		name    string
		usage   []schemasecurity.RoleObjectUsage
		wantOn  []string
		wantLen int
	}{
		{
			name:    "nothing observed",
			usage:   make([]schemasecurity.RoleObjectUsage, 0),
			wantOn:  []string{"customers", "orders"},
			wantLen: 2,
		},
		{
			name:    "one observed",
			usage:   []schemasecurity.RoleObjectUsage{{Role: "reporting", Kind: "table", Name: "orders"}},
			wantOn:  []string{"customers"},
			wantLen: 1,
		},
		{
			name: "both observed",
			usage: []schemasecurity.RoleObjectUsage{
				{Role: "reporting", Kind: "table", Name: "orders"},
				{Role: "reporting", Kind: "table", Name: "customers"},
			},
			wantOn:  make([]string, 0),
			wantLen: 0,
		},
		{
			name:    "another role's use is not this role's",
			usage:   []schemasecurity.RoleObjectUsage{{Role: "analytics", Kind: "table", Name: "orders"}},
			wantOn:  []string{"customers", "orders"},
			wantLen: 2,
		},
		{
			name:    "a different object kind does not match",
			usage:   []schemasecurity.RoleObjectUsage{{Role: "reporting", Kind: "schema", Name: "orders"}},
			wantOn:  []string{"customers", "orders"},
			wantLen: 2,
		},
		{
			name:    "case does not separate a role from its own use",
			usage:   []schemasecurity.RoleObjectUsage{{Role: "REPORTING", Kind: "TABLE", Name: "Orders"}},
			wantOn:  []string{"customers"},
			wantLen: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report := schemasecurity.Analyze(grantedSchema(), schemasecurity.Options{
				RoleObjectUsage: test.usage,
			})

			subjects := make([]string, 0, len(report.Findings))
			for _, finding := range report.Findings {
				c.Assert(finding.Code, qt.Equals, "ROL01")
				subjects = append(subjects, finding.Subject.Name)
			}
			c.Assert(subjects, qt.HasLen, test.wantLen)
			c.Assert(subjects, qt.DeepEquals, test.wantOn)
		})
	}
}

// PUBLIC is left to PRV03, which has the better remedy for it.
//
// Usage cannot be attributed to PUBLIC — every role holds it, so an observation
// naming a real role says nothing about the PUBLIC grant — and reporting the
// same grant twice would give one grant two remedies.
func TestROL01_LeavesPublicGrantsToPRV03(t *testing.T) {
	c := qt.New(t)

	report := schemasecurity.Analyze(&schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "orders"},
		},
	}, schemasecurity.Options{RoleObjectUsage: make([]schemasecurity.RoleObjectUsage, 0)})

	c.Assert(codes(report), qt.Not(qt.Contains), "ROL01")
	c.Assert(codes(report), qt.Contains, "PRV03")
}

// The finding carries the privileges and the role, so a consumer can act on it
// without parsing the message.
func TestROL01_CarriesThePrivilegesAndTheRole(t *testing.T) {
	c := qt.New(t)

	report := schemasecurity.Analyze(&schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "reporting", Privileges: []string{"select", "UPDATE"}, OnTable: "orders"},
		},
	}, schemasecurity.Options{RoleObjectUsage: make([]schemasecurity.RoleObjectUsage, 0)})

	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].Detail.Roles, qt.DeepEquals, []string{"reporting"})
	c.Assert(report.Findings[0].Detail.Privileges, qt.DeepEquals, []string{"SELECT", "UPDATE"})
	c.Assert(report.Findings[0].Suggestion, qt.Not(qt.Equals), "")
}
