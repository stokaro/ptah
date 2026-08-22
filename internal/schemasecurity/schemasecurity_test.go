package schemasecurity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/schemasecurity"
	"go.5x5.cz/ptah/migration/risk"
)

// TestAnalyze_EachRuleHasACaseWhereItDoesNotFire pairs every rule with a schema
// that trips it and one that deliberately does not.
//
// An analyzer with no negative case is indistinguishable from one that always
// fires, and the always-firing version passes every test written only from the
// positive side (stokaro/ptah#1035).
func TestAnalyze_EachRuleHasACaseWhereItDoesNotFire(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *goschema.Database
		// wantCodes are the finding codes, in the order Analyze sorts them.
		wantCodes []string
		// wantSkipped are the rules that did not run at all.
		wantSkipped []string
	}{
		{
			name:    "a grant to PUBLIC is reported",
			dialect: "postgres",
			database: &goschema.Database{
				Grants: []goschema.Grant{
					{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
				RLSEnabledTables: []goschema.RLSEnabledTable{{Table: "users"}},
			},
			wantCodes:   []string{"PRV03"},
			wantSkipped: make([]string, 0),
		},
		{
			name:    "the same grant to a named role is not",
			dialect: "postgres",
			database: &goschema.Database{
				Grants: []goschema.Grant{
					{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
				RLSEnabledTables: []goschema.RLSEnabledTable{{Table: "users"}},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: make([]string, 0),
		},
		{
			name:    "a SECURITY DEFINER routine is reported",
			dialect: "postgres",
			database: &goschema.Database{
				Functions: []goschema.Function{
					{Name: "set_tenant", Security: "DEFINER", Language: "plpgsql"},
				},
			},
			wantCodes:   []string{"PRV02"},
			wantSkipped: make([]string, 0),
		},
		{
			name:    "a SECURITY INVOKER routine is not",
			dialect: "postgres",
			database: &goschema.Database{
				Functions: []goschema.Function{
					{Name: "set_tenant", Security: "INVOKER", Language: "plpgsql"},
				},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: make([]string, 0),
		},
		{
			name:    "a granted table with no row-level security is reported",
			dialect: "postgres",
			database: &goschema.Database{
				Grants: []goschema.Grant{
					{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
			},
			wantCodes:   []string{"PRV01"},
			wantSkipped: make([]string, 0),
		},
		{
			name:    "the same table with row-level security enabled is not",
			dialect: "postgres",
			database: &goschema.Database{
				Grants: []goschema.Grant{
					{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
				RLSEnabledTables: []goschema.RLSEnabledTable{{Table: "users"}},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: make([]string, 0),
		},
		{
			// The rule cannot be answered where the target has no such
			// concept, and reporting every granted table there would be noise
			// dressed as a finding.
			name:    "the row-level rule does not run where the target has no row-level security",
			dialect: "mysql",
			database: &goschema.Database{
				Grants: []goschema.Grant{
					{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"PRV01"},
		},
		{
			// PostgreSQL creates this grant in every database; a rule that
			// fires on every database is one a reader learns to skip.
			name:    "the shipped USAGE on schema public to PUBLIC is not a finding",
			dialect: "postgres",
			database: &goschema.Database{
				Grants: []goschema.Grant{
					{Role: "PUBLIC", Privileges: []string{"USAGE"}, OnSchema: "public"},
				},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: make([]string, 0),
		},
		{
			// CREATE was revoked from PUBLIC by default in PostgreSQL 15, so a
			// database that grants it is stating something.
			name:    "CREATE on the same schema to PUBLIC is",
			dialect: "postgres",
			database: &goschema.Database{
				Grants: []goschema.Grant{
					{Role: "PUBLIC", Privileges: []string{"USAGE", "CREATE"}, OnSchema: "public"},
				},
			},
			wantCodes:   []string{"PRV03"},
			wantSkipped: make([]string, 0),
		},
		{
			name:        "an empty schema reports nothing",
			dialect:     "postgres",
			database:    &goschema.Database{},
			wantCodes:   make([]string, 0),
			wantSkipped: make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report := schemasecurity.Analyze(test.database, schemasecurity.Options{Capabilities: capability.ForDialect(test.dialect)})

			codes := make([]string, 0, len(report.Findings))
			for _, finding := range report.Findings {
				codes = append(codes, finding.Code)
				// A finding nobody can act on is a complaint, so this is
				// asserted for every rule rather than for the one under test.
				c.Assert(finding.Suggestion, qt.Not(qt.Equals), "")
				c.Assert(finding.Subject.Name, qt.Not(qt.Equals), "")
			}
			c.Assert(codes, qt.DeepEquals, test.wantCodes)

			skipped := make([]string, 0, len(report.SkippedRules))
			for _, rule := range report.SkippedRules {
				skipped = append(skipped, rule.Code)
				c.Assert(rule.Reason, qt.Not(qt.Equals), "")
			}
			c.Assert(skipped, qt.DeepEquals, test.wantSkipped)
		})
	}
}

// TestAnalyze_FindingsCarryTheValuesTheirSuggestionNames pins the structured
// half.
//
// The suggestion for these rules names the very values here -- which
// privileges, which roles -- so a message alone would leave a consumer parsing
// prose to act on a finding.
func TestAnalyze_FindingsCarryTheValuesTheirSuggestionNames(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Grants: []goschema.Grant{
			{Role: "public", Privileges: []string{"select", " insert "}, OnTable: "users"},
			{Role: "reporting", Privileges: []string{"SELECT"}, OnTable: "users"},
		},
		Functions: []goschema.Function{{Name: "set_tenant", Security: "definer", Language: "plpgsql"}},
	}

	report := schemasecurity.Analyze(database, schemasecurity.Options{Capabilities: capability.ForDialect("postgres")})

	byCode := make(map[string]schemasecurity.Finding, len(report.Findings))
	for _, finding := range report.Findings {
		byCode[finding.Code] = finding
	}

	// Privileges are normalized, so two spellings of one grant produce one
	// finding rather than two that look different.
	c.Assert(byCode["PRV03"].Detail.Privileges, qt.DeepEquals, []string{"INSERT", "SELECT"})
	c.Assert(byCode["PRV03"].Severity, qt.Equals, risk.Warning)
	// The row-level finding names every role that reaches the table, which is
	// what its suggestion asks the reader to decide about.
	c.Assert(byCode["PRV01"].Detail.Roles, qt.DeepEquals, []string{"public", "reporting"})
	c.Assert(byCode["PRV01"].Severity, qt.Equals, risk.Info)
	// The routine finding carries the language, because what a definer body can
	// be redirected by depends on it.
	c.Assert(byCode["PRV02"].Detail.Language, qt.Equals, "plpgsql")
	c.Assert(byCode["PRV02"].Severity, qt.Equals, risk.Info)

	c.Assert(report.Summary, qt.DeepEquals, schemasecurity.Summary{Info: 2, Warning: 1})
}

// TestAnalyze_OrdersFindingsSoTwoRunsAgreeIsTheDiffableProperty pins the sort.
//
// Two runs over an unchanged schema have to produce identical output, or a diff
// of two reports says something changed when a map iterated differently.
func TestAnalyze_OrdersFindingsSoTwoRunsAgreeIsTheDiffableProperty(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Grants: []goschema.Grant{
			{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "orders"},
			{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "accounts"},
			{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "invoices"},
		},
	}

	report := schemasecurity.Analyze(database, schemasecurity.Options{Capabilities: capability.ForDialect("postgres")})

	subjects := make([]string, 0, len(report.Findings))
	for _, finding := range report.Findings {
		subjects = append(subjects, finding.Code+" "+finding.Subject.Name)
	}
	c.Assert(subjects, qt.DeepEquals, []string{
		"PRV01 accounts", "PRV01 invoices", "PRV01 orders", "PRV03 accounts", "PRV03 orders",
	})
}
