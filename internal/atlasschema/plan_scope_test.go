package atlasschema_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
)

// TestStalePlanErrorNamesTheScopes holds the refusal of a stale plan to what
// differs. When the plan's URL covered something other than the target URL
// does, the fingerprints differ because the reads do, and the refusal names
// both scopes instead of reporting a changed database. A change under the same
// scope, a plan that records no scope, and moved rows keep their own words.
func TestStalePlanErrorNamesTheScopes(t *testing.T) {
	tests := []struct {
		name  string
		stale atlasschema.StalePlanError
		want  string
	}{
		{
			name: "a plan for a whole server applied to one database",
			stale: atlasschema.StalePlanError{
				PlanFingerprint: "sha256:plan", DatabaseFingerprint: "sha256:db",
				PlanScope: "the whole server", DatabaseScope: `database "app"`,
			},
			want: "pre-planned migration is stale: the plan was computed for the whole server, and the target URL " +
				`reaches database "app" (plan sha256:plan, database sha256:db); apply the plan through a URL that ` +
				"reaches the whole server, or re-run `schema plan` through this one and review the fresh plan",
		},
		{
			name: "a plan for every schema applied to one schema",
			stale: atlasschema.StalePlanError{
				PlanFingerprint: "sha256:plan", DatabaseFingerprint: "sha256:db",
				PlanScope: "every schema of the database", DatabaseScope: `schema "public"`,
			},
			want: "pre-planned migration is stale: the plan was computed for every schema of the database, and the " +
				`target URL reaches schema "public" (plan sha256:plan, database sha256:db); apply the plan through a URL ` +
				"that reaches every schema of the database, or re-run `schema plan` through this one and review the fresh plan",
		},
		{
			name: "a change under the same scope",
			stale: atlasschema.StalePlanError{
				PlanFingerprint: "sha256:plan", DatabaseFingerprint: "sha256:db",
				PlanScope: `schema "main"`, DatabaseScope: `schema "main"`,
			},
			want: "pre-planned migration is stale: the target database schema does not match the plan's source " +
				"fingerprint (plan sha256:plan, database sha256:db); the database changed since the plan was computed, " +
				"so re-run `schema plan` against the current database and review the fresh plan",
		},
		{
			name: "a plan that records no scope",
			stale: atlasschema.StalePlanError{
				PlanFingerprint: "sha256:plan", DatabaseFingerprint: "sha256:db",
				DatabaseScope: `schema "main"`,
			},
			want: "pre-planned migration is stale: the target database schema does not match the plan's source " +
				"fingerprint (plan sha256:plan, database sha256:db); the database changed since the plan was computed, " +
				"so re-run `schema plan` against the current database and review the fresh plan",
		},
		{
			name: "a connection whose role has memberships the fingerprint can cover",
			stale: atlasschema.StalePlanError{
				PlanFingerprint: "sha256:plan", DatabaseFingerprint: "sha256:db",
				ConnectedRole: "app",
			},
			want: "pre-planned migration is stale: the target database schema does not match the plan's source " +
				"fingerprint (plan sha256:plan, database sha256:db); the database changed since the plan was computed, " +
				`or the plan was computed as a role other than "app" and the fingerprint covers the memberships of ` +
				"the role that runs it, so re-run `schema plan` as \"app\" against the current database and review " +
				"the fresh plan",
		},
		{
			name: "moved rows",
			stale: atlasschema.StalePlanError{
				PlanFingerprint: "sha256:plan", DatabaseFingerprint: "sha256:db", Rows: true,
				PlanScope: "the whole server", DatabaseScope: `database "app"`,
			},
			want: "pre-planned migration is stale: the declared rows it reads no longer hold what the plan was " +
				"computed against (plan sha256:plan, database sha256:db); a row changed since the plan was computed, " +
				"so re-run `schema plan` against the current database and review the fresh plan",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.stale.Error(), qt.Equals, test.want)
		})
	}
}

// TestVerifyPlanTargetNamesTheScopeAPlanRecorded drives the refusal through
// a saved plan. The plan records the scope its URL covered, here SQLite's one
// schema; a plan whose recorded scope is another one and whose fingerprint no
// longer matches names both, and the same plan under its own scope reports
// the changed database.
func TestVerifyPlanTargetNamesTheScopeAPlanRecorded(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	conn := connectPlanSQLite(c, filepath.Join(dir, "scope.db"))
	desired := writePlanDesiredSchema(c, dir, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		ToURLs: []string{desired}, Runtime: must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll,
		`CREATE TABLE drifted (id INTEGER PRIMARY KEY);`), qt.IsNil)
	elsewhere := plan
	elsewhere.Scope = &atlasschema.PlanScope{Kind: atlasschema.PlanScopeServer}

	sameScope := atlasschema.VerifyPlanTarget(t.Context(), conn, plan, must.Must(builtin.New()))
	otherScope := atlasschema.VerifyPlanTarget(t.Context(), conn, elsewhere, must.Must(builtin.New()))

	c.Assert(plan.Scope, qt.DeepEquals, &atlasschema.PlanScope{Kind: atlasschema.PlanScopeSchema, Name: "main"})
	c.Assert(sameScope, qt.ErrorMatches, `pre-planned migration is stale: the target database schema does not match .*`)
	c.Assert(otherScope, qt.ErrorMatches,
		`pre-planned migration is stale: the plan was computed for the whole server, and the target URL reaches schema "main" .*`)
}
