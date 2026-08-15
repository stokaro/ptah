package atlas

// White-box testing required: the dev-database policy for plan rehearsal is an
// unexported decision function whose most security-relevant branches belong to
// dialects with no server available in CI (PostgreSQL, MySQL, and friends).
// Driving it through the exported command would require live databases for
// exactly the branches that most need coverage, so the decision is asserted
// directly; the SQLite branches are additionally covered end to end.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// desiredFixture is a non-nil desired schema; the decision only cares whether
// one is present.
func desiredFixture() *goschema.Database { return &goschema.Database{} }

// desiredFor maps a table-driven presence flag onto the desired-schema
// argument.
func desiredFor(present bool) *goschema.Database {
	m := map[bool]*goschema.Database{true: desiredFixture()}
	return m[present]
}

func TestResolveAtlasSchemaApplyPlanRehearsal(t *testing.T) {
	// Every row states its policy explicitly. The policy is the axis under test
	// here, so letting rows inherit the zero value would make this table
	// silently change meaning the next time that value moves — which is exactly
	// what happened when rehearseAlways became the zero value.
	tests := []struct {
		name          string
		policy        planRehearsalPolicy
		format        atlasschema.PlanFormat
		dialect       string
		devURL        string
		hasDesired    bool
		wantSkip      bool
		wantEphemeral bool
		wantDevURL    string
	}{
		{
			name:       "no_desired_state_skips",
			policy:     rehearseWhenUnverified,
			format:     atlasschema.PlanFormatHCL,
			dialect:    "postgres",
			hasDesired: false,
			wantSkip:   true,
		},
		{
			name:          "atlas_plan_on_sqlite_uses_ephemeral_dev",
			policy:        rehearseWhenUnverified,
			format:        atlasschema.PlanFormatHCL,
			dialect:       "sqlite",
			hasDesired:    true,
			wantEphemeral: true,
		},
		{
			name:       "atlas_plan_on_postgres_with_dev_url",
			policy:     rehearseWhenUnverified,
			format:     atlasschema.PlanFormatHCL,
			dialect:    "postgres",
			devURL:     "postgres://localhost/dev",
			hasDesired: true,
			wantDevURL: "postgres://localhost/dev",
		},
		{
			name:       "json_plan_without_dev_url_skips",
			policy:     rehearseWhenUnverified,
			format:     atlasschema.PlanFormatJSON,
			dialect:    "postgres",
			hasDesired: true,
			wantSkip:   true,
		},
		{
			name:       "json_plan_with_dev_url_rehearses",
			policy:     rehearseWhenUnverified,
			format:     atlasschema.PlanFormatJSON,
			dialect:    "postgres",
			devURL:     "postgres://localhost/dev",
			hasDesired: true,
			wantDevURL: "postgres://localhost/dev",
		},
		{
			name:       "json_plan_on_sqlite_without_dev_url_skips",
			policy:     rehearseWhenUnverified,
			format:     atlasschema.PlanFormatJSON,
			dialect:    "sqlite",
			hasDesired: true,
			wantSkip:   true,
		},
		// rehearseAlways is what `schema plan validate` passes. The two rows
		// below are the ONLY inputs where the policies disagree; every other
		// row above resolves identically under both, which is why the policy
		// is asserted on exactly these.
		{
			name:          "always_json_plan_on_sqlite_without_dev_url_rehearses",
			policy:        rehearseAlways,
			format:        atlasschema.PlanFormatJSON,
			dialect:       "sqlite",
			hasDesired:    true,
			wantEphemeral: true,
		},
		{
			name:       "always_json_plan_with_dev_url_rehearses",
			policy:     rehearseAlways,
			format:     atlasschema.PlanFormatJSON,
			dialect:    "postgres",
			devURL:     "postgres://localhost/dev",
			hasDesired: true,
			wantDevURL: "postgres://localhost/dev",
		},
		{
			name:       "always_without_desired_state_still_skips",
			policy:     rehearseAlways,
			format:     atlasschema.PlanFormatJSON,
			dialect:    "sqlite",
			hasDesired: false,
			wantSkip:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			decision, err := resolveAtlasSchemaApplyPlanRehearsal(
				tt.policy, tt.format, tt.dialect, tt.devURL, desiredFor(tt.hasDesired))

			c.Assert(err, qt.IsNil)
			c.Assert(decision.skip, qt.Equals, tt.wantSkip)
			c.Assert(decision.ephemeral, qt.Equals, tt.wantEphemeral)
			c.Assert(decision.devURL, qt.Equals, tt.wantDevURL)
		})
	}
}

// TestResolveAtlasSchemaApplyPlanRehearsalRequiresDevDatabase covers the
// dialects that cannot get a throwaway dev database for free: an Atlas-format
// plan is unverifiable without one, so it is refused rather than applied
// unverified.
// TestResolveAtlasSchemaApplyPlanRehearsalAlwaysRequiresDevDatabaseForJSON
// pins the rehearseAlways refusal, which has its own wording: the apply-path
// message explains that ATLAS hashes cannot be recomputed, which is false for
// a native JSON plan and would send a `schema plan validate` operator looking
// for a problem that is not there.
func TestResolveAtlasSchemaApplyPlanRehearsalAlwaysRequiresDevDatabaseForJSON(t *testing.T) {
	c := qt.New(t)

	_, err := resolveAtlasSchemaApplyPlanRehearsal(
		rehearseAlways, atlasschema.PlanFormatJSON, "postgres", "", desiredFixture())

	c.Assert(err, qt.ErrorMatches,
		`verifying a plan file requires a dev database:.*pass --dev-url with a postgres dev database URL`)
}

func TestResolveAtlasSchemaApplyPlanRehearsalRequiresDevDatabase(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		devURL  string
		wantErr string
	}{
		{
			name:    "postgres",
			dialect: "postgres",
			wantErr: `verifying an Atlas plan file requires a dev database:.*pass --dev-url with a postgres dev database URL`,
		},
		{
			name:    "mysql",
			dialect: "mysql",
			wantErr: `verifying an Atlas plan file requires a dev database:.*pass --dev-url with a mysql dev database URL`,
		},
		{
			name:    "mariadb",
			dialect: "mariadb",
			wantErr: `verifying an Atlas plan file requires a dev database:.*mariadb dev database URL`,
		},
		{
			name:    "postgresql_spelling",
			dialect: "postgresql",
			wantErr: `verifying an Atlas plan file requires a dev database:.*postgresql dev database URL`,
		},
		{
			name:    "blank_dev_url_is_treated_as_absent",
			dialect: "postgres",
			devURL:  "   ",
			wantErr: `verifying an Atlas plan file requires a dev database:.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := resolveAtlasSchemaApplyPlanRehearsal(
				rehearseWhenUnverified, atlasschema.PlanFormatHCL, tt.dialect, tt.devURL, desiredFixture())

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
		})
	}
}

// TestResolveAtlasSchemaApplyPlanRehearsalIgnoresFingerprintShape pins the
// security property behind MAJOR 1: the rehearsal decision depends only on the
// plan format, never on how the plan's fingerprints look. The `sha256:<hex>`
// derivation is public, so a forged "Ptah-written" plan must not be able to
// switch the verification off.
func TestResolveAtlasSchemaApplyPlanRehearsalIgnoresFingerprintShape(t *testing.T) {
	for _, dialect := range []string{"postgres", "mysql", "sqlserver", "clickhouse"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			_, err := resolveAtlasSchemaApplyPlanRehearsal(
				rehearseWhenUnverified, atlasschema.PlanFormatHCL, dialect, "", desiredFixture())

			c.Assert(err, qt.ErrorMatches, `verifying an Atlas plan file requires a dev database:.*`)
		})
	}
}
