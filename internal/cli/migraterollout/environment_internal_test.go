package migraterollout

// White-box testing required: each target runs as a child process of the
// ptah binary, and under `go test` that binary is the test binary, which
// TestMain stops at once when it is started as a target run. No test here can
// observe a child's environment, so the environment a run is given is asserted
// on the functions that build it.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationrollout"
)

// tenantURL is a fixture URL whose password is the tenant's name, so a test
// can tell one target's credentials from another's.
func tenantURL(tenant string) string {
	return "postgres://" + tenant + ":secret-" + tenant + "@db/" + tenant
}

func environmentPlan() migrationrollout.Plan {
	return migrationrollout.Plan{Stages: []migrationrollout.Stage{
		{Name: "canary", Targets: []migrationrollout.Target{
			{TargetRecord: migrationrollout.TargetRecord{Name: "a", URLEnv: "TENANT_A_URL"}, URL: tenantURL("a")},
		}},
		{Name: "wave", Targets: []migrationrollout.Target{
			{TargetRecord: migrationrollout.TargetRecord{Name: "b", URLEnv: "TENANT_B_URL"}, URL: tenantURL("b")},
			{TargetRecord: migrationrollout.TargetRecord{Name: "c"}, URL: "sqlite://c.db"},
		}},
	}}
}

// A target run holds its own credentials and no other target's.
func TestTargetEnvironment_HoldsOnlyItsOwnURL(t *testing.T) {
	environ := []string{
		"PATH=/usr/bin",
		"PTAH_DB_URL=postgres://inherited:secret@db/x",
		"TENANT_A_URL=postgres://a:secret-a@db/a",
		"TENANT_B_URL=postgres://b:secret-b@db/b",
		"TENANT_A_URL_SUFFIX=kept",
	}
	plan := environmentPlan()
	base := childEnvironment(environ, plan)
	cases := []struct {
		name   string
		target migrationrollout.Target
		want   []string
	}{
		{
			name:   "a target read through url_env",
			target: plan.Stages[0].Targets[0],
			want: []string{
				"PATH=/usr/bin",
				"TENANT_A_URL_SUFFIX=kept",
				"PTAH_DB_URL=postgres://a:secret-a@db/a",
				"TENANT_A_URL=postgres://a:secret-a@db/a",
			},
		},
		{
			name:   "a target with a literal url",
			target: plan.Stages[1].Targets[1],
			want:   []string{"PATH=/usr/bin", "TENANT_A_URL_SUFFIX=kept", "PTAH_DB_URL=sqlite://c.db"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(targetEnvironment(base, tc.target), qt.DeepEquals, tc.want)
		})
	}
}
