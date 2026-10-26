//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// commonObjectsClient starts the common-objects program with a host that
// lists hostFeatures.
func commonObjectsClient(c *qt.C, hostFeatures ...string) *transport.Client {
	c.Helper()
	binary := buildFixture(c, "common-objects")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature}, HostFeatures: hostFeatures})
}

func fixtureObject(kind, name string, path ...string) *protocol.Reference {
	return &protocol.Reference{Engine: "fixture", Kind: kind, Name: name, Path: path}
}

// commonChange is a change to a common object, its subject named as the
// transport requires.
func commonChange(object *protocol.Reference, operation *protocol.CommonObjectChange) *protocol.Change {
	return &protocol.Change{Object: object, Operation: &protocol.Change_CommonObject{CommonObject: operation}}
}

// renderAndExecute renders changes in one batch and runs the units the
// provider wrote, in one execution that has to apply all of them.
func renderAndExecute(c *qt.C, client *transport.Client, session string, changes ...*protocol.Change) {
	c.Helper()
	for i, change := range changes {
		change.Id = strconv.Itoa(i)
	}
	rendered, err := client.Render(c.Context(), &protocol.RenderRequest{Engine: "fixture", Changes: changes, Capabilities: map[string]bool{"transactional_ddl": false}})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.GetUnits(), qt.HasLen, len(changes))
	executed, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session, RequestId: "common", Units: rendered.GetUnits()})
	c.Assert(err, qt.IsNil)
	c.Assert(executed.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	c.Assert(int(executed.GetAppliedCount()), qt.Equals, len(changes))
}

// TestCommonObjects_HappyPath spells a declaration as the program reads it
// back, renders a view, a group, a role, a membership, a grant and comments,
// runs the units, and reads each object back. It then renders their removal
// and reads none back.
func TestCommonObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	client := commonObjectsClient(c, provider.CommonObjectsFeature)
	c.Assert(client.Description().GetEngines()[0].GetCommonObjects().GetKinds(), qt.DeepEquals, []protocol.CommonObjectSupport_Kind{
		protocol.CommonObjectSupport_VIEW, protocol.CommonObjectSupport_ROLE, protocol.CommonObjectSupport_GRANT, protocol.CommonObjectSupport_COMMENT,
	})
	session := open(c, client, "")
	created, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session.GetId(), RequestId: "create", Units: []*protocol.Unit{
		{ChangeId: "create", Body: &protocol.Unit_Sql{Sql: `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY);`}},
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(created.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	items := fixtureObject("table", "items")
	grant := &protocol.SQLGrant{Role: "reader", Privileges: []string{"SELECT"}, Target: items}

	engine := client.Description().GetEngines()[0]
	identifiers := &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(engine.GetIdentifiers()), DefaultSchema: engine.GetDefaultSchema()}
	prepared, err := client.PrepareCommonObjects(c.Context(), &protocol.PrepareCommonObjectsRequest{Engine: "fixture", Identifiers: identifiers, Declared: &protocol.DeclaredCommonObjects{
		Views:  []*protocol.SQLView{{Name: "recent", Body: "SELECT   id\n FROM items"}},
		Roles:  []*protocol.SQLRole{{Name: "reader", Login: true, Inherit: true}},
		Grants: []*protocol.SQLGrant{{Role: "reader", Privileges: []string{"select"}, Target: items}},
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(prepared, qt.CmpEquals(protocmp.Transform()), &protocol.PreparedCommonObjects{Complete: true, Prepared: &protocol.DeclaredCommonObjects{
		Views:  []*protocol.SQLView{{Name: "recent", Body: "SELECT id FROM items"}},
		Roles:  []*protocol.SQLRole{{Name: "reader", Login: true}},
		Grants: []*protocol.SQLGrant{grant},
	}})

	renderAndExecute(c, client, session.GetId(),
		commonChange(fixtureObject("view", "recent"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateView{CreateView: &protocol.ViewCreation{
			View: &protocol.SQLView{Name: "recent", Body: "SELECT   id FROM items", Comment: "the recent items"}}}}),
		commonChange(fixtureObject("group", "readers"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateRole{CreateRole: &protocol.RoleCreation{
			Role: &protocol.SQLRole{Name: "readers", Group: true}}}}),
		commonChange(fixtureObject("role", "reader"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateRole{CreateRole: &protocol.RoleCreation{
			Role: &protocol.SQLRole{Name: "reader", Login: true, Comment: "reads items"}}}}),
		commonChange(fixtureObject("group", "readers"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_AddMember{AddMember: &protocol.RoleMembership{
			Group: "readers", Member: "reader"}}}),
		commonChange(items, &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_Grant{Grant: grant}}),
		commonChange(items, &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{Object: items, Text: "the items"}}}),
		commonChange(fixtureObject("column", "id", "items"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{
			Object: fixtureObject("column", "id", "items"), Text: "the key"}}}),
	)
	read, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.GetId()})
	c.Assert(err, qt.IsNil)
	observed := read.GetCommonObjects()
	c.Assert(observed.GetViews(), qt.CmpEquals(protocmp.Transform()), []*protocol.ObservedView{{Identity: fixtureObject("view", "recent"), Body: "SELECT id FROM items"}})
	c.Assert(observed.GetRoles(), qt.CmpEquals(protocmp.Transform()), []*protocol.ObservedRole{
		{Name: "readers", Group: true, Password: protocol.ObservedRole_PASSWORD_NONE},
		{Name: "reader", Login: true, MemberOf: []string{"readers"}, Password: protocol.ObservedRole_PASSWORD_NONE},
	})
	c.Assert(observed.GetGrants(), qt.CmpEquals(protocmp.Transform()), []*protocol.ObservedGrant{{Role: "reader", Privilege: "SELECT", Target: items}})
	c.Assert(observed.GetComments(), qt.CmpEquals(protocmp.Transform()), []*protocol.ObjectComment{
		{Object: fixtureObject("view", "recent"), Text: "the recent items"},
		{Object: fixtureObject("role", "reader"), Text: "reads items"},
		{Object: items, Text: "the items"},
		{Object: fixtureObject("column", "id", "items"), Text: "the key"},
	})
	c.Assert(observed.GetCoverage(), qt.HasLen, 4)

	renderAndExecute(c, client, session.GetId(),
		commonChange(items, &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_Revoke{Revoke: grant}}),
		commonChange(fixtureObject("group", "readers"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_RemoveMember{RemoveMember: &protocol.RoleMembership{
			Group: "readers", Member: "reader"}}}),
		commonChange(fixtureObject("role", "reader"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_DropRole{DropRole: fixtureObject("role", "reader")}}),
		commonChange(fixtureObject("group", "readers"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_DropRole{DropRole: fixtureObject("group", "readers")}}),
		commonChange(fixtureObject("view", "recent"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_DropView{DropView: fixtureObject("view", "recent")}}),
		commonChange(items, &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{Object: items}}}),
		commonChange(fixtureObject("column", "id", "items"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{
			Object: fixtureObject("column", "id", "items")}}}),
	)
	read, err = client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.GetId()})
	c.Assert(err, qt.IsNil)
	observed = read.GetCommonObjects()
	c.Assert(observed.GetViews(), qt.HasLen, 0)
	c.Assert(observed.GetRoles(), qt.HasLen, 0)
	c.Assert(observed.GetGrants(), qt.HasLen, 0)
	c.Assert(observed.GetComments(), qt.HasLen, 0)
	c.Assert(observed.GetCoverage(), qt.HasLen, 4)
}

// TestCommonObjects_FailurePath refuses the wire to a host that did not list
// it, and a role the program cannot hold. The program's engine has common
// table identity and so no schema.validate of its own: Render is where it
// judges a declaration.
func TestCommonObjects_FailurePath(t *testing.T) {
	t.Run("a host without the wire", func(t *testing.T) {
		c := qt.New(t)
		client := commonObjectsClient(c)
		session := open(c, client, "")

		read, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.GetId()})
		c.Assert(err, qt.IsNil)
		c.Assert(read.GetCommonObjects(), qt.IsNil)
		c.Assert(client.Description().GetEngines()[0].GetCommonObjects(), qt.IsNil)
		engine := client.Description().GetEngines()[0]
		identifiers := &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(engine.GetIdentifiers()), DefaultSchema: engine.GetDefaultSchema()}
		prepared, err := client.PrepareCommonObjects(c.Context(), &protocol.PrepareCommonObjectsRequest{Engine: "fixture", Identifiers: identifiers, Declared: &protocol.DeclaredCommonObjects{
			Views: []*protocol.SQLView{{Name: "recent", Body: "SELECT 1"}},
		}})
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(prepared, qt.IsNil)
	})
	t.Run("a role the program cannot hold", func(t *testing.T) {
		c := qt.New(t)
		client := commonObjectsClient(c, provider.CommonObjectsFeature)
		change := commonChange(fixtureObject("role", "admin"), &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateRole{CreateRole: &protocol.RoleCreation{
			Role: &protocol.SQLRole{Name: "admin", Superuser: true}}}})
		change.Id = "0"

		rendered, err := client.Render(c.Context(), &protocol.RenderRequest{Engine: "fixture", Changes: []*protocol.Change{change}, Capabilities: map[string]bool{"transactional_ddl": false}})

		c.Assert(err, qt.ErrorMatches, `provider render failed \(Unknown\)`)
		c.Assert(rendered, qt.IsNil)
	})
}
