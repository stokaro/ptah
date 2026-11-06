package providerhost

// White-box testing required: which common statements a provider render
// writes after its scheduled steps is decided on the host's private common
// sequence before the provider renders any of it, and the order a provider's
// SQL comes back in cannot show which side placed a statement. The provider
// live tests cover what a read writes.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// permissionRender is the target and the creation context of a render of a
// table, a grant on it, a grant and a revocation on the queue main.jobs and a
// grant on the queue main.other, through a provider whose queues take grants
// as TOPIC.
func permissionRender(c *qt.C) (*target, authoredCreation) {
	c.Helper()
	selections, err := newScopeSelections(scopeDescription())
	c.Assert(err, qt.IsNil)
	selected := &target{host: &Host{selections: selections, description: scopeDescription()}, engine: scopeDescription().Engines[0]}
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "records", Schema: "main", StructName: "Record"}},
		Fields: []schemamodel.Field{{StructName: "Record", Name: "id", Type: "INTEGER", Primary: true}},
		Grants: []schemamodel.Grant{
			{Role: "app", Privileges: []string{"SELECT"}, OnObject: "main.jobs", ObjectKind: "TOPIC"},
			{Role: "app", Privileges: []string{"SELECT"}, OnObject: "main.other", ObjectKind: "TOPIC"},
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: "main.records"},
		},
		RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnObject: "main.jobs", ObjectKind: "TOPIC"}},
	}
	creation, err := selected.authoredCreationContext(renderer.SchemaRequest{Target: "example", Schema: schema,
		Identifiers: identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact}})
	c.Assert(err, qt.IsNil)
	return selected, creation
}

// declaredQueue is the queue main.jobs as a declaration planner creates it.
func declaredQueue() schemaext.Object {
	return schemaext.Object{Ref: objectidentity.ID{Kind: "example.org/queue", Schema: scopePart("main"), Name: scopePart("jobs")}}
}

// grantTargets names each permission statement of nodes by its verb and
// object, in order, and every other statement "other".
func grantTargets(nodes []ast.Node) []string {
	var named []string
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.GrantPrivilegeNode:
			named = append(named, "GRANT "+typed.ObjectType+" "+typed.ObjectName)
		case *ast.RevokePrivilegeNode:
			named = append(named, "REVOKE "+typed.ObjectType+" "+typed.ObjectName)
		default:
			named = append(named, "other")
		}
	}
	return named
}

// commonObjects is the common object each step of metadata carries, nil for
// a step of a table.
func commonObjects(metadata []featureplan.CommonStep) []ast.Node {
	objects := make([]ast.Node, 0, len(metadata))
	for _, step := range metadata {
		objects = append(objects, step.CommonObject)
	}
	return objects
}

// A render writes each grant and revocation on an object a declaration
// planner creates after every scheduled statement, as the YDB engine built
// into Ptah at 74de0b65b wrote the grants on its topics and nodes after the
// grants on its tables. A permission on an object nothing declares keeps its
// place among the others, with its metadata.
func TestRenderWritesPermissionsOnDeclaredObjectsLast_HappyPath(t *testing.T) {
	c := qt.New(t)
	selected, creation := permissionRender(c)
	metadata, err := commonPlanningMetadata(creation.plan, creation.nodes, selected.host.selections)
	c.Assert(err, qt.IsNil)
	start := creation.creationStart

	kept, trailing := creation.takeDeclaredObjectPermissions(metadata, []schemaext.Object{declaredQueue()})

	c.Assert(grantTargets(creation.nodes), qt.DeepEquals, []string{"other", "GRANT TOPIC main.other", "GRANT TABLE main.records"})
	c.Assert(grantTargets(trailing), qt.DeepEquals, []string{"GRANT TOPIC main.jobs", "REVOKE TOPIC main.jobs"})
	c.Assert(creation.creationStart, qt.Equals, start)
	c.Assert(grantTargets(commonObjects(kept)), qt.DeepEquals, grantTargets(creation.nodes))
}

// A render whose provider declares no object a permission names keeps every
// statement where it was, so an engine without such objects renders as
// before.
func TestRenderKeepsPermissionsOnUndeclaredObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	selected, creation := permissionRender(c)
	metadata, err := commonPlanningMetadata(creation.plan, creation.nodes, selected.host.selections)
	c.Assert(err, qt.IsNil)
	before := grantTargets(creation.nodes)

	kept, trailing := creation.takeDeclaredObjectPermissions(metadata, nil)

	c.Assert(before, qt.DeepEquals, []string{"other", "GRANT TOPIC main.jobs", "GRANT TOPIC main.other", "GRANT TABLE main.records", "REVOKE TOPIC main.jobs"})
	c.Assert(grantTargets(creation.nodes), qt.DeepEquals, before)
	c.Assert(trailing, qt.HasLen, 0)
	c.Assert(grantTargets(commonObjects(kept)), qt.DeepEquals, before)
}
