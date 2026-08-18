package planlint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/planlint"
	migrationlint "go.5x5.cz/ptah/migration/lint"
)

// The SQL a plan carries is analyzed by the migration lint rules, and the
// rules that describe a migration DIRECTORY -- a missing rollback half, a file
// name that does not follow the convention -- must stay silent on it. A plan
// file has neither, so reporting either would be reporting the plan for not
// being something it never claimed to be.

func TestAnalyze_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		dialect   string
		wantRules []string
	}{
		{
			name:      "additive column reports nothing",
			sql:       "ALTER TABLE users ADD COLUMN nick text;\n",
			dialect:   "sqlite",
			wantRules: nil,
		},
		{
			name:      "dropping a table is destructive",
			sql:       "DROP TABLE users;\n",
			dialect:   "sqlite",
			wantRules: []string{"DS101"},
		},
		{
			name:      "dropping a column is destructive",
			sql:       "ALTER TABLE users DROP COLUMN nick;\n",
			dialect:   "postgres",
			wantRules: []string{"DS102"},
		},
		{
			name:      "a non-nullable add is data dependent",
			sql:       "ALTER TABLE users ADD COLUMN nick text NOT NULL;\n",
			dialect:   "postgres",
			wantRules: []string{"DD101"},
		},
		{
			name:      "a dialect rule fires only on its dialect",
			sql:       "CREATE INDEX idx_users_nick ON users (nick);\n",
			dialect:   "postgres",
			wantRules: []string{"PG101"},
		},
		{
			name:      "the same statement is quiet on another dialect",
			sql:       "CREATE INDEX idx_users_nick ON users (nick);\n",
			dialect:   "sqlite",
			wantRules: nil,
		},
		{
			name:      "an alias resolves to its canonical dialect",
			sql:       "CREATE INDEX idx_users_nick ON users (nick);\n",
			dialect:   "postgresql",
			wantRules: []string{"PG101"},
		},
		{
			name:      "an atlas nolint directive silences the finding",
			sql:       "-- atlas:nolint destructive\nDROP TABLE users;\n",
			dialect:   "sqlite",
			wantRules: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := planlint.Analyze(test.sql, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(findingRules(analysis), qt.DeepEquals, test.wantRules)
		})
	}
}

// TestAnalyzeIgnoresMigrationDirectoryForm is the negative half of the claim
// the package doc makes. Without it, a plan carrying one statement would report
// a missing rollback half and a non-conventional file name on every run, and
// the table above would still be green because it only names the rules it
// expects to see.
func TestAnalyzeIgnoresMigrationDirectoryForm(t *testing.T) {
	tests := []struct {
		name string
		rule string
	}{
		{name: "no missing down migration", rule: "MF101"},
		{name: "no file name complaint", rule: "MF103"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := planlint.Analyze("ALTER TABLE users ADD COLUMN nick text;\n", "sqlite")

			c.Assert(err, qt.IsNil)
			c.Assert(findingRules(analysis), qt.Not(qt.Contains), test.rule)
		})
	}
}

func TestAnalyze_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		wantErr string
	}{
		{
			name:    "unknown dialect",
			dialect: "frobnicate",
			wantErr: `unsupported lint dialect "frobnicate"; expected one of .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := planlint.Analyze("DROP TABLE users;", test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(analysis.Findings(), qt.HasLen, 0)
		})
	}
}

func TestHasErrorSeverity_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{name: "a destructive drop is error severity", sql: "DROP TABLE users;\n", want: true},
		{name: "a data-dependent add is not", sql: "ALTER TABLE users ADD COLUMN nick text NOT NULL;\n", want: false},
		{name: "nothing found is not", sql: "ALTER TABLE users ADD COLUMN nick text;\n", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := planlint.Analyze(test.sql, "postgres")
			c.Assert(err, qt.IsNil)

			got := planlint.HasErrorSeverity(analysis)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// findingRules lists the rule codes an analysis reported, in report order.
func findingRules(analysis migrationlint.Analysis) []string {
	var codes []string
	for _, finding := range analysis.Findings() {
		codes = append(codes, finding.Rule)
	}
	return codes
}
