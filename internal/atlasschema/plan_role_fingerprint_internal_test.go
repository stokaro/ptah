package atlasschema

// White-box testing required: this test exercises the saved-plan fingerprint after role graph selection.
// A helper-only test cannot show whether the selected rows change the plan's
// digest or whether a plan that names the role still binds to it.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
)

func TestPlanSourceFingerprintIgnoresUnrelatedCreatedRoles(t *testing.T) {
	no := false
	for _, test := range []struct {
		name, dialect string
		membership    []catalog.RoleMembership
	}{
		{name: "MySQL", dialect: "mysql"},
		{name: "MariaDB membership between unrelated roles", dialect: "mariadb", membership: []catalog.RoleMembership{{Role: "created", Member: "other"}}},
		{name: "PostgreSQL creator", dialect: "postgres", membership: []catalog.RoleMembership{{
			Role: "created", Member: "runner", AdminOption: true, InheritOption: &no, SetOption: &no,
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			before := &catalog.Database{ConnectedRole: "runner", Roles: []catalog.Role{{Name: "runner"}}}
			after := &catalog.Database{ConnectedRole: "runner", Roles: []catalog.Role{{Name: "runner"}, {Name: "created"}},
				RoleMemberships: test.membership}
			info := catalog.ServerInfo{Dialect: test.dialect}

			prior := must.Must(planSourceFingerprint(t.Context(), before, info, nil, runtime))
			current := must.Must(planSourceFingerprint(t.Context(), after, info, nil, runtime))
			c.Assert(current, qt.Equals, prior)

			statements := []PlanStatement{{SQL: "GRANT SELECT ON t TO created"}}
			prior = must.Must(planSourceFingerprint(t.Context(), before, info, statements, runtime))
			current = must.Must(planSourceFingerprint(t.Context(), after, info, statements, runtime))
			c.Assert(current, qt.Not(qt.Equals), prior)
		})
	}
}
