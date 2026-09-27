package schemaclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/schemaclean"
)

// serverCleanPlan is the cleanup of a whole server holding r1 and r4, r4
// keeping a foreign key into r1.
func serverCleanPlan() schemaclean.Plan {
	return schemaclean.PlanFromObjects([]schemaclean.Object{
		{Type: schemaclean.ObjectTypeSchema, Schema: "r1", Name: "r1"},
		{Type: schemaclean.ObjectTypeSchema, Schema: "r4", Name: "r4"},
		{Type: schemaclean.ObjectTypeForeignKey, Schema: "r4", Table: "z", Name: "zfk"},
	}, "mysql")
}

// TestServerCleanPolicy_FailurePath refuses the cleanup of a whole server
// without the opt-in, and lists the databases it would drop as a dry run
// prints them (stokaro/ptah#3789). The pinned community binary v1.3.0 drops
// them at exit 0.
func TestServerCleanPolicy_FailurePath(t *testing.T) {
	c := qt.New(t)
	policy, err := schemaclean.ResolveServerCleanPolicy()
	c.Assert(err, qt.IsNil)

	err = policy.Refuse(catalog.ServerInfo{Dialect: "mysql", WholeServer: true}, serverCleanPlan())

	c.Assert(err, qt.ErrorIs, schemaclean.ErrServerCleanRefused)
	c.Assert(err, qt.ErrorMatches, "refusing to clean a whole MySQL or MariaDB server without PTAH_ALLOW_SERVER_CLEAN=1: "+
		"the URL names no database, and the cleanup would drop every user database on the server:\n"+
		"- DROP DATABASE `r1`\n- DROP DATABASE `r4`\n"+
		"Set PTAH_ALLOW_SERVER_CLEAN=1 to clean the server, or name a database in the URL to clean that database alone")
}

// TestResolveServerCleanPolicy_FailurePath refuses a value that is not a
// boolean, rather than reading it as the default.
func TestResolveServerCleanPolicy_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv(schemaclean.AllowServerCleanEnvVar, "maybe")

	policy, err := schemaclean.ResolveServerCleanPolicy()

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for PTAH_ALLOW_SERVER_CLEAN`)
	c.Assert(policy, qt.Equals, schemaclean.ServerCleanPolicy{})
}

// TestServerCleanPolicy_HappyPath allows what the refusal does not concern: a
// connection to one database and a server with nothing to drop.
func TestServerCleanPolicy_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		info catalog.ServerInfo
		plan schemaclean.Plan
	}{
		{name: "one database", info: catalog.ServerInfo{Dialect: "mysql", Schema: "r1"}, plan: serverCleanPlan()},
		{name: "a server with no user database", info: catalog.ServerInfo{Dialect: "mariadb", WholeServer: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy, err := schemaclean.ResolveServerCleanPolicy()
			c.Assert(err, qt.IsNil)

			c.Assert(policy.Refuse(test.info, test.plan), qt.IsNil)
		})
	}
}

// TestServerCleanPolicy_AllowsAServerOnRequest is the control for the
// refusal: the opt-in lets the same cleanup through.
func TestServerCleanPolicy_AllowsAServerOnRequest(t *testing.T) {
	c := qt.New(t)
	t.Setenv(schemaclean.AllowServerCleanEnvVar, "1")

	policy, err := schemaclean.ResolveServerCleanPolicy()

	c.Assert(err, qt.IsNil)
	c.Assert(policy.Refuse(catalog.ServerInfo{Dialect: "mysql", WholeServer: true}, serverCleanPlan()), qt.IsNil)
}
