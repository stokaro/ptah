package schema

// White-box testing required: the subject is dropClusterScopedTestState, the
// package-local accommodation that lets a database desired-state source past
// the runner's roles-and-grants guard. Reaching it through the exported command
// tree needs a live PostgreSQL database, because SQLite has no roles or grants
// to introspect and so can never produce the input this reports on. Driving the
// helper directly is what lets the reported bytes be pinned in a unit test that
// needs no server.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

// TestDropClusterScopedTestState pins the reported omission, byte for byte.
//
// The runner refuses any desired schema carrying roles or grants, and it is
// right to: applying them mutates cluster-scoped security state that outlives
// the throwaway database. But every introspected PostgreSQL database carries
// its own `GRANT USAGE ON SCHEMA public TO PUBLIC` and the connecting role, so
// leaving the guard in the way of a database source refuses it over security
// state the author never wrote. Dropping it silently is the opposite defect --
// an author who did write grants would get a green test that never applied
// them. Only the reported drop is safe, and only the bytes tell a reported drop
// from a silent one: they share an exit code.
func TestDropClusterScopedTestState(t *testing.T) {
	tests := []struct {
		name   string
		schema func() *goschema.Database
		want   string
	}{
		{
			name: "one role and one grant",
			schema: func() *goschema.Database {
				return &goschema.Database{
					Roles: []goschema.Role{{Name: "app_reader"}},
					Grants: []goschema.Grant{
						{Role: "PUBLIC", Privileges: []string{"USAGE"}, OnSchema: "public"},
					},
				}
			},
			want: "note: dropped 1 role and 1 grant introspected from the desired-state database;" +
				" schema tests do not apply cluster-scoped security state\n",
		},
		{
			name: "grants only, pluralized",
			schema: func() *goschema.Database {
				return &goschema.Database{
					Grants: []goschema.Grant{
						{Role: "PUBLIC", Privileges: []string{"USAGE"}, OnSchema: "public"},
						{Role: "app_reader", Privileges: []string{"SELECT"}, OnTable: "public.orders"},
					},
				}
			},
			want: "note: dropped 0 roles and 2 grants introspected from the desired-state database;" +
				" schema tests do not apply cluster-scoped security state\n",
		},
		{
			name:   "nothing to drop stays silent",
			schema: func() *goschema.Database { return &goschema.Database{} },
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			desired := tt.schema()
			var out bytes.Buffer

			c.Assert(dropClusterScopedTestState(&out, desired), qt.IsNil)

			c.Assert(out.String(), qt.Equals, tt.want)
			c.Assert(desired.Roles, qt.HasLen, 0)
			c.Assert(desired.Grants, qt.HasLen, 0)
		})
	}
}
