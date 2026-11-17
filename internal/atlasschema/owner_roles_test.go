package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
)

// TestMissingOwnerRoleError_Error pins the refusal of a plan that gives a view
// to a role the server does not hold: it names the statement and the role,
// says why a role must exist on the server, and adds what the place the plan
// runs knows when it knows more.
func TestMissingOwnerRoleError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *atlasschema.MissingOwnerRoleError
		want string
	}{
		{
			name: "no hint",
			err:  &atlasschema.MissingOwnerRoleError{Statement: `ALTER VIEW "v" OWNER TO "reporting";` + "\n", Role: "reporting"},
			want: `ALTER VIEW "v" OWNER TO "reporting" gives the object to role "reporting", which the server does not hold and no ` +
				`statement before it creates; a role belongs to the server, not to a database, so create the role on the server ` +
				`or declare it in the desired schema`,
		},
		{
			name: "a hint",
			err:  &atlasschema.MissingOwnerRoleError{Statement: `ALTER VIEW "v" OWNER TO "reporting"`, Role: "reporting", Hint: "use a docker:// dev URL"},
			want: `ALTER VIEW "v" OWNER TO "reporting" gives the object to role "reporting", which the server does not hold and no ` +
				`statement before it creates; a role belongs to the server, not to a database, so create the role on the server ` +
				`or declare it in the desired schema; use a docker:// dev URL`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.err.Error(), qt.Equals, test.want)
		})
	}
}
