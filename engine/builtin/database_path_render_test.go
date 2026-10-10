package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// databaseGrantRequest renders a YDB user granted CONNECT on the database
// itself, as a read of that database describes it.
func databaseGrantRequest(databasePath string) renderer.SchemaRequest {
	return renderer.SchemaRequest{
		Target: platform.YDB, Capabilities: capability.YDB262(), Identifiers: identifier.ForDialect(platform.YDB),
		Schema: &schemamodel.Database{
			Roles:  []schemamodel.Role{{Name: "app", Login: true, Inherit: true}},
			Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"CONNECT"}, OnDatabase: true}},
		},
		DatabasePath: databasePath,
	}
}

// TestRenderSchema_DatabaseGrant_HappyPath names the database a grant is on by
// the path of the database the request says the schema was read from. The path
// is where the read was made, so it travels on the request rather than in the
// schema.
func TestRenderSchema_DatabaseGrant_HappyPath(t *testing.T) {
	c := qt.New(t)

	rendered, err := renderer.RenderSchema(c.Context(), must.Must(builtin.New()), databaseGrantRequest("/local"))

	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
	c.Assert(strings.Join(rendered.Statements, ""), qt.Contains, "GRANT 'ydb.database.connect' ON `/local` TO `app`;")
}

// TestRenderSchema_DatabaseGrant_FailurePath refuses the same grant when the
// request names no database: a declaration is not about one database, and YDB
// takes the database only by its absolute path.
func TestRenderSchema_DatabaseGrant_FailurePath(t *testing.T) {
	c := qt.New(t)

	rendered, err := renderer.RenderSchema(c.Context(), must.Must(builtin.New()), databaseGrantRequest(""))

	c.Assert(err, qt.ErrorAs, new(*renderer.SchemaRefusalError))
	c.Assert(err, qt.ErrorMatches, `(?s)GRANT on the database to app: YDB takes this object only by its absolute path.*`)
	c.Assert(rendered.Statements, qt.HasLen, 0)
}
