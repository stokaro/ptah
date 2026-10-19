package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/doblockext"
	"ptah.run/internal/doblockext/postgres"
)

// prefixed records a CREATE ROLE under a name the reader would not derive
// itself, so a result shows that the request's recording was used.
func prefixed(node *ast.CreateRoleNode) schemamodel.Role {
	return schemamodel.Role{Name: "app_" + node.Name, Login: node.Login}
}

// TestServices serves PL/pgSQL, the language a descriptor names for
// PostgreSQL's blocks.
func TestServices(t *testing.T) {
	c := qt.New(t)
	services := postgres.Services()
	c.Assert(services.Languages, qt.DeepEquals, []string{descriptor.DoBlocksPLpgSQL})
	c.Assert(services.Label, qt.Equals, "PL/pgSQL")
	c.Assert(services.Roles, qt.IsNotNil)
}

// TestRoles_HappyPath reads a block against the roles the request declares:
// a condition sees them, a CREATE ROLE is recorded by the request's function,
// and a later condition sees what the block created under the recorded name.
func TestRoles_HappyPath(t *testing.T) {
	c := qt.New(t)
	roles, err := postgres.Services().Roles.Roles(doblockext.Request{
		Dialect: "postgres",
		Body: ` BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_reader') THEN CREATE ROLE reader; END IF;
 CREATE ROLE writer LOGIN;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_writer') THEN CREATE ROLE auditor; END IF;
 END `,
		Declared: []string{"app_reader"},
		Role:     prefixed,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(roles, qt.DeepEquals, []schemamodel.Role{{Name: "app_writer", Login: true}, {Name: "app_auditor"}})
}

// TestRoles_FailurePath refuses a block that repeats a declared role, with a
// text that carries no sentinel: the schema reader adds its own.
func TestRoles_FailurePath(t *testing.T) {
	c := qt.New(t)
	roles, err := postgres.Services().Roles.Roles(doblockext.Request{
		Dialect:  "postgres",
		Body:     ` BEGIN CREATE ROLE writer; END `,
		Declared: []string{"app_writer"},
		Role:     prefixed,
	})
	c.Assert(err, qt.ErrorMatches, `desired-state DO block at body byte 25: CREATE ROLE would repeat a role already declared `+
		`in this desired schema; use supported role-bootstrap declarations or an explicit migration`)
	c.Assert(roles, qt.IsNil)
}

// TestRoles_RefusesNUL refuses a body that holds a NUL byte before reading
// it.
func TestRoles_RefusesNUL(t *testing.T) {
	c := qt.New(t)
	roles, err := postgres.Services().Roles.Roles(doblockext.Request{
		Dialect: "postgres",
		Body:    " BEGIN NULL; END \x00",
		Role:    prefixed,
	})
	c.Assert(err, qt.ErrorMatches, `desired-state DO block at body byte 18: NUL is not allowed in a procedural body; `+
		`use supported role-bootstrap declarations or an explicit migration`)
	c.Assert(roles, qt.IsNil)
}

// TestRoles_RefusesNestedComments refuses a nested block comment for what it
// is, though the text after the inner closing marker would be refused too.
func TestRoles_RefusesNestedComments(t *testing.T) {
	c := qt.New(t)
	roles, err := postgres.Services().Roles.Roles(doblockext.Request{
		Dialect: "postgres",
		Body:    " BEGIN /* outer /* nested */ comment */ NULL; END ",
		Role:    prefixed,
	})
	c.Assert(err, qt.ErrorMatches, `desired-state DO block at body byte 1: unterminated or nested block comment; `+
		`use supported role-bootstrap declarations or an explicit migration`)
	c.Assert(roles, qt.IsNil)
}
