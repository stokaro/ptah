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

// TestOwnerTransferError_Error pins the refusal of a plan that gives a view to
// a role the connection may not give an object to. It names the statement, the
// role, the connected role and what the server requires, says so when the role
// owns the view now and the plan gives it back, and names both ways out: the
// grant that lets the connection give it, and an owner the declaration could
// name instead. The grant is the one the server's check calls for.
func TestOwnerTransferError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *atlasschema.OwnerTransferError
		want string
	}{
		{
			name: "an owner given back, on a server that checks SET",
			err: &atlasschema.OwnerTransferError{Statement: `ALTER VIEW "public"."v" OWNER TO "rv_a";` + "\n", Kind: "VIEW", Role: "rv_a",
				Connected: "app", Privilege: "SET", Holds: true},
			want: `ALTER VIEW "public"."v" OWNER TO "rv_a" gives the view to role "rv_a", which connected role "app" may not give ` +
				`an object to: the server requires the SET privilege on the new owner, and "app" lacks it; "rv_a" owns the view now, ` +
				`and the plan gives it back after it creates the view again; run GRANT "rv_a" TO "app" WITH SET TRUE, ` +
				`or declare the owner the view should have, one "app" may give it to`,
		},
		{
			name: "a declared owner, on a server that checks membership",
			err: &atlasschema.OwnerTransferError{Statement: `ALTER MATERIALIZED VIEW "Doc Totals" OWNER TO "Audit Team"`,
				Kind: "MATERIALIZED VIEW", Role: "Audit Team", Connected: "app", Privilege: "MEMBER", Hint: "the dev database runs as app"},
			want: `ALTER MATERIALIZED VIEW "Doc Totals" OWNER TO "Audit Team" gives the materialized view to role "Audit Team", ` +
				`which connected role "app" may not give an object to: the server requires membership in the new owner, ` +
				`and "app" is not a member; run GRANT "Audit Team" TO "app", or declare the owner the materialized view should have, ` +
				`one "app" may give it to; the dev database runs as app`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.err.Error(), qt.Equals, test.want)
		})
	}
}

// TestOwnerSchemaError_Error pins the refusal of a plan that gives a view to a
// role that holds no CREATE privilege on the view's schema: it names the
// statement, the role and the schema, says so when the role owns the view now,
// and names the grant that lets the role own it and the declaration that
// avoids it.
func TestOwnerSchemaError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *atlasschema.OwnerSchemaError
		want string
	}{
		{
			name: "an owner given back",
			err: &atlasschema.OwnerSchemaError{Statement: `ALTER VIEW "public"."v" OWNER TO "rv_a";`, Kind: "VIEW", Role: "rv_a",
				Schema: "public", Holds: true},
			want: `ALTER VIEW "public"."v" OWNER TO "rv_a" gives the view to role "rv_a", which holds no CREATE privilege on schema ` +
				`"public": the server requires it of the new owner; "rv_a" owns the view now, and the plan gives it back after it ` +
				`creates the view again; run GRANT CREATE ON SCHEMA "public" TO "rv_a", or declare the owner the view should have, ` +
				`one that holds it`,
		},
		{
			name: "a declared owner",
			err: &atlasschema.OwnerSchemaError{Statement: `ALTER MATERIALIZED VIEW "Sales"."Doc Totals" OWNER TO "Audit Team"`,
				Kind: "MATERIALIZED VIEW", Role: "Audit Team", Schema: "Sales"},
			want: `ALTER MATERIALIZED VIEW "Sales"."Doc Totals" OWNER TO "Audit Team" gives the materialized view to role ` +
				`"Audit Team", which holds no CREATE privilege on schema "Sales": the server requires it of the new owner; ` +
				`run GRANT CREATE ON SCHEMA "Sales" TO "Audit Team", or declare the owner the materialized view should have, ` +
				`one that holds it`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.err.Error(), qt.Equals, test.want)
		})
	}
}
