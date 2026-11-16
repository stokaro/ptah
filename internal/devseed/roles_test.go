package devseed_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/devseed"
)

// roleOptions reads statements as PostgreSQL does, in public.
var roleOptions = devseed.Options{Dialect: "postgres", DefaultSchema: "public"}

// roleCandidates are the roles a target holds that the dev server lacks.
var roleCandidates = []catalog.Role{{Name: "app_admin"}, {Name: "Reader"}, {Name: "auditor"}}

// roleNames returns the names of roles, in order.
func roleNames(roles []catalog.Role) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}

// TestRolesNamed_ReadsTheCodeTheDevDatabaseRuns names a role wherever a
// statement, a DO block or an SQL routine body of the state writes it as a
// bare name, in the order the candidates list them.
func TestRolesNamed_ReadsTheCodeTheDevDatabaseRuns(t *testing.T) {
	tests := []struct {
		name       string
		state      *catalog.Database
		statements []string
		want       []string
	}{
		{
			name:       "a policy for a role",
			statements: []string{`CREATE POLICY p ON public.tasks FOR SELECT TO app_admin USING (true)`},
			want:       []string{"app_admin"},
		},
		{
			name:       "a quoted name and a grant, in the candidates' order",
			statements: []string{`GRANT SELECT ON public.tasks TO auditor`, `CREATE POLICY p ON public.tasks TO "app_admin" USING (true)`},
			want:       []string{"app_admin", "auditor"},
		},
		{
			name:       "a role stored in mixed case, written unquoted",
			statements: []string{`ALTER TABLE public.tasks OWNER TO reader`},
			want:       []string{"Reader"},
		},
		{
			name:       "a DO block, which runs at once",
			statements: []string{`DO $$ BEGIN EXECUTE 'SELECT 1'; GRANT USAGE ON SCHEMA public TO auditor; END $$`},
			want:       []string{"auditor"},
		},
		{
			name: "an SQL body of the state's routine",
			state: &catalog.Database{Functions: []catalog.Function{{
				Name: "f", Language: "sql", Body: "SELECT 1 FROM public.tasks WHERE app_admin IS NOT NULL",
			}}},
			want: []string{"app_admin"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := devseed.RolesNamed(tt.state, tt.statements, roleCandidates, roleOptions)
			c.Assert(roleNames(got), qt.DeepEquals, tt.want)
		})
	}
}

// TestRolesNamed_LeavesOutWhatTheServerResolvesLater names no role that
// appears only in a string, in a comment, or in the statements of a PL/pgSQL
// body a CREATE FUNCTION quotes, which the server resolves when the code runs.
func TestRolesNamed_LeavesOutWhatTheServerResolvesLater(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{name: "a string", statements: []string{`CREATE POLICY p ON public.tasks USING (pg_has_role('app_admin', 'USAGE'))`}},
		{name: "a comment", statements: []string{"-- for app_admin\nCREATE TABLE public.tasks (id int)"}},
		{name: "a PL/pgSQL body's statements", statements: []string{
			`CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN GRANT SELECT ON public.tasks TO app_admin; END $$`,
		}},
		{name: "no statement", statements: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := devseed.RolesNamed(&catalog.Database{}, tt.statements, roleCandidates, roleOptions)
			c.Assert(got, qt.HasLen, 0)
		})
	}
}
