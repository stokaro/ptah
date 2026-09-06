package schemasecurity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemasecurity"
	"ptah.run/migration/risk"
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
		database *schemamodel.Database
		// memberships is what the caller read; nil means it read none, which
		// is a different answer from an empty slice.
		memberships []schemasecurity.RoleMembership
		// owners is what the caller read about object ownership.
		owners []schemasecurity.ObjectOwner
		// wantCodes are the finding codes, in the order Analyze sorts them.
		wantCodes []string
		// wantSkipped are the rules that did not run at all.
		wantSkipped []string
	}{
		{
			name:    "a grant to PUBLIC is reported",
			dialect: "postgres",
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{
					{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
				RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "users"}},
			},
			wantCodes:   []string{"PRV03"},
			wantSkipped: []string{"OWN01", "ROL01", "ROL03", "ROL04"},
		},
		{
			name:    "the same grant to a named role is not",
			dialect: "postgres",
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{
					{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
				RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "users"}},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"OWN01", "ROL01", "ROL03", "ROL04"},
		},
		{
			name:    "a SECURITY DEFINER routine is reported",
			dialect: "postgres",
			database: &schemamodel.Database{
				Functions: []schemamodel.Function{
					{Name: "set_tenant", Security: "DEFINER", Language: "plpgsql"},
				},
			},
			wantCodes:   []string{"PRV02"},
			wantSkipped: []string{"OWN01", "ROL01", "ROL03", "ROL04"},
		},
		{
			name:    "a SECURITY INVOKER routine is not",
			dialect: "postgres",
			database: &schemamodel.Database{
				Functions: []schemamodel.Function{
					{Name: "set_tenant", Security: "INVOKER", Language: "plpgsql"},
				},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"OWN01", "ROL01", "ROL03", "ROL04"},
		},
		{
			name:    "a granted table with no row-level security is reported",
			dialect: "postgres",
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{
					{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
			},
			wantCodes:   []string{"PRV01"},
			wantSkipped: []string{"OWN01", "ROL01", "ROL03", "ROL04"},
		},
		{
			name:    "the same table with row-level security enabled is not",
			dialect: "postgres",
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{
					{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
				RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "users"}},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"OWN01", "ROL01", "ROL03", "ROL04"},
		},
		{
			// The rule cannot be answered where the target has no such
			// concept, and reporting every granted table there would be noise
			// dressed as a finding.
			name:    "the row-level rule does not run where the target has no row-level security",
			dialect: "mysql",
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{
					{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"},
				},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"PRV01", "OWN01", "ROL01", "ROL03", "ROL04"},
		},
		{
			// PostgreSQL creates this grant in every database; a rule that
			// fires on every database is one a reader learns to skip.
			name:    "the shipped USAGE on schema public to PUBLIC is not a finding",
			dialect: "postgres",
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{
					{Role: "PUBLIC", Privileges: []string{"USAGE"}, OnSchema: "public"},
				},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"OWN01", "ROL01", "ROL03", "ROL04"},
		},
		{
			// CREATE was revoked from PUBLIC by default in PostgreSQL 15, so a
			// database that grants it is stating something.
			name:    "CREATE on the same schema to PUBLIC is",
			dialect: "postgres",
			database: &schemamodel.Database{
				Grants: []schemamodel.Grant{
					{Role: "PUBLIC", Privileges: []string{"USAGE", "CREATE"}, OnSchema: "public"},
				},
			},
			wantCodes:   []string{"PRV03"},
			wantSkipped: []string{"OWN01", "ROL01", "ROL03", "ROL04"},
		},
		{
			name:    "a role nobody holds and that cannot log in is reported",
			dialect: "postgres",
			database: &schemamodel.Database{
				Roles: []schemamodel.Role{{Name: "reporting", Login: false}},
			},
			memberships: make([]schemasecurity.RoleMembership, 0),
			wantCodes:   []string{"ROL04"},
			wantSkipped: []string{"OWN01", "ROL01"},
		},
		{
			name:    "the same role with a member is not",
			dialect: "postgres",
			database: &schemamodel.Database{
				Roles: []schemamodel.Role{{Name: "reporting", Login: false}, {Name: "alice", Login: true}},
			},
			memberships: []schemasecurity.RoleMembership{{Role: "reporting", Member: "alice"}},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"OWN01", "ROL01"},
		},
		{
			// A login role is its own principal. Reporting every application
			// account would bury the rule that matters.
			name:    "a login role with no members is not reported",
			dialect: "postgres",
			database: &schemamodel.Database{
				Roles: []schemamodel.Role{{Name: "app_user", Login: true}},
			},
			memberships: make([]schemasecurity.RoleMembership, 0),
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"OWN01", "ROL01"},
		},
		{
			name:    "two roles held by one member that grant the same privileges are reported",
			dialect: "postgres",
			database: &schemamodel.Database{
				Roles: []schemamodel.Role{
					{Name: "reader", Login: false},
					{Name: "analyst", Login: false},
					{Name: "alice", Login: true},
				},
				Grants: []schemamodel.Grant{
					{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "users"},
					{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"},
					{Role: "analyst", Privileges: []string{"SELECT"}, OnTable: "users"},
					{Role: "analyst", Privileges: []string{"SELECT"}, OnTable: "orders"},
				},
				RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "users"}, {Table: "orders"}},
			},
			memberships: []schemasecurity.RoleMembership{
				{Role: "reader", Member: "alice"},
				{Role: "analyst", Member: "alice"},
			},
			wantCodes:   []string{"ROL03"},
			wantSkipped: []string{"OWN01", "ROL01"},
		},
		{
			// One shared privilege out of two is a coincidence, not a
			// duplicate role: below the threshold, and nothing is reported.
			name:    "two roles that mostly differ are not",
			dialect: "postgres",
			database: &schemamodel.Database{
				Roles: []schemamodel.Role{
					{Name: "reader", Login: false},
					{Name: "writer", Login: false},
					{Name: "alice", Login: true},
				},
				Grants: []schemamodel.Grant{
					{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "users"},
					{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"},
					{Role: "writer", Privileges: []string{"SELECT"}, OnTable: "users"},
					{Role: "writer", Privileges: []string{"INSERT", "UPDATE", "DELETE"}, OnTable: "audit"},
				},
				RLSEnabledTables: []schemamodel.RLSEnabledTable{
					{Table: "users"}, {Table: "orders"}, {Table: "audit"},
				},
			},
			memberships: []schemasecurity.RoleMembership{
				{Role: "reader", Member: "alice"},
				{Role: "writer", Member: "alice"},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"OWN01", "ROL01"},
		},
		{
			// Measured on MariaDB 11.8: CREATE ROLE inserts an admin edge from
			// the creator, so counting it would make this rule unable to fire
			// on any MariaDB server.
			name:    "a role held only with admin option is still reported",
			dialect: "postgres",
			database: &schemamodel.Database{
				Roles: []schemamodel.Role{{Name: "reporting", Login: false}, {Name: "root", Login: true}},
			},
			memberships: []schemasecurity.RoleMembership{
				{Role: "reporting", Member: "root", AdminOption: true},
			},
			wantCodes:   []string{"ROL04"},
			wantSkipped: []string{"OWN01", "ROL01"},
		},
		{
			name:    "two roles held only with admin option are not an overlap",
			dialect: "postgres",
			database: &schemamodel.Database{
				Roles: []schemamodel.Role{
					{Name: "reader", Login: false},
					{Name: "analyst", Login: false},
					{Name: "root", Login: true},
					{Name: "alice", Login: true},
				},
				Grants: []schemamodel.Grant{
					{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "users"},
					{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"},
					{Role: "analyst", Privileges: []string{"SELECT"}, OnTable: "users"},
					{Role: "analyst", Privileges: []string{"SELECT"}, OnTable: "orders"},
				},
				RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "users"}, {Table: "orders"}},
			},
			memberships: []schemasecurity.RoleMembership{
				{Role: "reader", Member: "root", AdminOption: true},
				{Role: "analyst", Member: "root", AdminOption: true},
				{Role: "reader", Member: "alice"},
				{Role: "analyst", Member: "alice"},
			},
			// alice only: root holds both roles with admin option, which is the
			// right to grant them rather than evidence anybody uses them.
			wantCodes:   []string{"ROL03"},
			wantSkipped: []string{"OWN01", "ROL01"},
		},
		{
			name:     "objects owned by a login role are reported once for that role",
			dialect:  "postgres",
			database: &schemamodel.Database{},
			owners: []schemasecurity.ObjectOwner{
				{Kind: "table", Name: "users", Owner: "app_user", OwnerCanLogin: true},
				{Kind: "table", Name: "orders", Owner: "app_user", OwnerCanLogin: true},
				{Kind: "schema", Name: "public", Owner: "app_user", OwnerCanLogin: true},
			},
			// One finding, not three: the rule is about the owner, and a row
			// per object would bury every other finding on a real schema.
			wantCodes:   []string{"OWN01"},
			wantSkipped: []string{"ROL01", "ROL03", "ROL04"},
		},
		{
			name:     "the same objects owned by a role that cannot log in are not",
			dialect:  "postgres",
			database: &schemamodel.Database{},
			owners: []schemasecurity.ObjectOwner{
				{Kind: "table", Name: "users", Owner: "app_owner"},
				{Kind: "schema", Name: "public", Owner: "app_owner"},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"ROL01", "ROL03", "ROL04"},
		},
		{
			// An owner Ptah does not describe as a role cannot be classified,
			// and guessing that an unknown owner can log in would report every
			// object of every database read by a restricted account.
			name:     "an owner this description does not define is not reported",
			dialect:  "postgres",
			database: &schemamodel.Database{},
			owners: []schemasecurity.ObjectOwner{
				{Kind: "table", Name: "users", Owner: "postgres"},
			},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"ROL01", "ROL03", "ROL04"},
		},
		{
			name:        "an empty schema reports nothing",
			dialect:     "postgres",
			database:    &schemamodel.Database{},
			wantCodes:   make([]string, 0),
			wantSkipped: []string{"OWN01", "ROL01", "ROL03", "ROL04"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report := schemasecurity.Analyze(test.database, schemasecurity.Options{
				Capabilities:    capability.ForDialect(test.dialect),
				RoleMemberships: test.memberships,
				ObjectOwners:    test.owners,
			})

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
	database := &schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "public", Privileges: []string{"select", " insert "}, OnTable: "users"},
			{Role: "reporting", Privileges: []string{"SELECT"}, OnTable: "users"},
		},
		Functions: []schemamodel.Function{{Name: "set_tenant", Security: "definer", Language: "plpgsql"}},
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
	database := &schemamodel.Database{
		Grants: []schemamodel.Grant{
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
