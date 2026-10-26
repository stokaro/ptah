package providerhost

// White-box testing required: the adapter maps the host's AST nodes, its
// schema model and its catalog onto the common-object wire in private
// functions. The fixture provider drives one shape of each through the CLI;
// every node kind, refusal and read field is only observable here.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// commonObjectTarget is a target whose engine carries kinds on a negotiated
// common-object wire, with common table identity.
func commonObjectTarget(kinds ...protocol.CommonObjectSupport_Kind) *target {
	return &target{
		engine: &protocol.Engine{Id: "fixture", DefaultSchema: "main", Features: []string{provider.CommonObjectsFeature, provider.CommonTableIdentityFeature},
			CommonObjects: &protocol.CommonObjectSupport{Kinds: kinds}},
		host: &Host{description: &protocol.Description{Features: []string{provider.CommonObjectsFeature}}},
	}
}

var everyCommonKind = []protocol.CommonObjectSupport_Kind{
	protocol.CommonObjectSupport_VIEW, protocol.CommonObjectSupport_MATERIALIZED_VIEW, protocol.CommonObjectSupport_SEQUENCE,
	protocol.CommonObjectSupport_ROLE, protocol.CommonObjectSupport_GRANT, protocol.CommonObjectSupport_COMMENT,
}

func ref(kind, schema, name string, path ...string) *protocol.Reference {
	return &protocol.Reference{Engine: "fixture", Kind: kind, Schema: schema, Name: name, Path: path}
}

func TestCommonObjectChange_HappyPath(t *testing.T) {
	alterSequence := ast.NewAlterSequence("counter").SetSchema("app").SetStart(5).SetCycle(true)
	alterRole := ast.NewAlterRole("reader")
	alterRole.AddOperation(ast.NewSetLoginOperation(false))
	alterRole.AddOperation(ast.NewSetPasswordOperation("secret"))
	tests := []struct {
		name string
		node ast.Node
		want *protocol.Change
	}{
		{name: "a replaced view", node: ast.NewCreateView("app.recent").SetBody("SELECT 1").SetComment("recent").SetReplace(),
			want: &protocol.Change{Object: ref("view", "app", "recent"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_CreateView{CreateView: &protocol.ViewCreation{
					View: &protocol.SQLView{Schema: "app", Name: "recent", Body: "SELECT 1", Comment: "recent"}, Replace: true}}}}}},
		{name: "a materialized view", node: ast.NewCreateMaterializedView("totals").SetBody("SELECT 2"),
			want: &protocol.Change{Object: ref("materialized_view", "", "totals"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_CreateView{CreateView: &protocol.ViewCreation{
					View: &protocol.SQLView{Name: "totals", Body: "SELECT 2", Materialized: true}}}}}}},
		{name: "a dropped materialized view", node: ast.NewDropMaterializedView("totals"),
			want: &protocol.Change{Object: ref("materialized_view", "", "totals"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_DropView{DropView: ref("materialized_view", "", "totals")}}}}},
		{name: "a sequence", node: &ast.CreateSequenceNode{Schema: "app", Name: "counter", Start: new(int64(1)), Increment: new(int64(2)), Comment: "counts"},
			want: &protocol.Change{Object: ref("sequence", "app", "counter"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_CreateSequence{CreateSequence: &protocol.SQLSequence{Schema: "app", Name: "counter",
					Start: new(int64(1)), Increment: new(int64(2)), Comment: "counts"}}}}}},
		{name: "a sequence alteration names what it sets", node: alterSequence,
			want: &protocol.Change{Object: ref("sequence", "app", "counter"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_AlterSequence{AlterSequence: &protocol.SequenceAlteration{Sequence: ref("sequence", "app", "counter"),
					Desired: &protocol.SQLSequence{Schema: "app", Name: "counter", Start: new(int64(5)), Cycle: true}, Changed: []string{"start", "cycle"}}}}}}},
		{name: "a role with a password", node: ast.NewCreateRole("reader").SetLogin(true).SetPassword("secret"),
			want: &protocol.Change{Object: ref("role", "", "reader"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_CreateRole{CreateRole: &protocol.RoleCreation{
					Role: &protocol.SQLRole{Name: "reader", Login: true, Inherit: true, PasswordDeclared: true}, Password: "secret"}}}}}},
		{name: "a role alteration carries the password it sets", node: alterRole,
			want: &protocol.Change{Object: ref("role", "", "reader"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_AlterRole{AlterRole: &protocol.RoleAlteration{
					Desired: &protocol.SQLRole{Name: "reader", PasswordDeclared: true}, Changed: []string{"login", "password"}, Password: "secret"}}}}}},
		{name: "a dropped group", node: ast.NewDropRole("readers").SetGroup(true),
			want: &protocol.Change{Object: ref("group", "", "readers"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_DropRole{DropRole: ref("group", "", "readers")}}}}},
		{name: "a removed membership", node: ast.NewRevokeRoleMembership("readers", "reader"),
			want: &protocol.Change{Object: ref("group", "", "readers"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_RemoveMember{RemoveMember: &protocol.RoleMembership{Group: "readers", Member: "reader"}}}}}},
		{name: "a column grant with its option", node: ast.NewGrantPrivilege("reader", "TABLE", "app.items", []string{"SELECT"}).SetColumns([]string{"id"}).SetWithOption(true),
			want: &protocol.Change{Object: ref("table", "app", "items"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_Grant{Grant: &protocol.SQLGrant{Role: "reader", Privileges: []string{"SELECT"}, Target: ref("table", "app", "items"),
					Columns: []string{"id"}, WithGrantOption: true}}}}}},
		{name: "a revoked grant option", node: ast.NewRevokePrivilege("reader", "SCHEMA", "app", []string{"USAGE"}).SetGrantOptionFor(true),
			want: &protocol.Change{Object: ref("schema", "", "app"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_Revoke{Revoke: &protocol.SQLGrant{Role: "reader", Privileges: []string{"USAGE"}, Target: ref("schema", "", "app"),
					WithGrantOption: true}}}}}},
		{name: "a removed schema comment", node: ast.NewObjectComment(ast.CommentedSchema, "app", ""),
			want: &protocol.Change{Object: ref("schema", "", "app"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{Object: ref("schema", "", "app")}}}}}},
		{name: "a column comment", node: commentStep("app.items", "id", "the key"),
			want: &protocol.Change{Object: ref("column", "app", "id", "items"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{Object: ref("column", "app", "id", "items"), Text: "the key"}}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			change, common, err := commonObjectTarget(everyCommonKind...).commonObjectChange(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(common, qt.IsTrue)
			c.Assert(change, qt.CmpEquals(protocmp.Transform()), test.want)
		})
	}
}

// renameRole is a role operation the common-object wire has no name for.
type renameRole struct{}

func (renameRole) GetOperationType() string { return "RENAME" }

func TestCommonObjectChange_FailurePath(t *testing.T) {
	unknownOperation := ast.NewAlterRole("reader")
	unknownOperation.AddOperation(renameRole{})
	tests := []struct {
		name    string
		target  *target
		node    ast.Node
		wantErr string
	}{
		{name: "an engine without the wire", target: &target{engine: &protocol.Engine{Id: "fixture"}, host: &Host{description: &protocol.Description{}}},
			node: ast.NewCreateView("recent").SetBody("SELECT 1"), wantErr: `.*\*ast.CreateViewNode on the fixture engine, whose provider does not carry common objects`},
		{name: "a comment on a function", target: commonObjectTarget(everyCommonKind...),
			node: ast.NewObjectComment(ast.CommentedFunction, "f", "x"), wantErr: `.*a comment on function f`},
		{name: "a role operation the wire has no name for", target: commonObjectTarget(everyCommonKind...),
			node: unknownOperation, wantErr: `.*role operation providerhost.renameRole`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			change, common, err := test.target.commonObjectChange(test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(common, qt.IsTrue)
			c.Assert(change, qt.IsNil)
		})
	}
}

// TestCommonObjectChange_LeavesOtherNodes reports a table change as not a
// common object, so the table adapter keeps it.
func TestCommonObjectChange_LeavesOtherNodes(t *testing.T) {
	c := qt.New(t)

	change, common, err := commonObjectTarget(everyCommonKind...).commonObjectChange(&ast.DropTableNode{Name: "items"})

	c.Assert(err, qt.IsNil)
	c.Assert(common, qt.IsFalse)
	c.Assert(change, qt.IsNil)
}

func TestDeclaredCommonObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{
		Views:             []schemamodel.View{{Name: "app.recent", Body: "SELECT 1", Comment: "recent"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "totals", Body: "SELECT 2"}},
		Roles:             []schemamodel.Role{{Name: "reader", Login: true, Password: "secret", MemberOf: []string{"readers"}}},
		Grants:            []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "app.items", Columns: []string{"id"}}},
		RevokedGrants:     []schemamodel.Grant{{Role: "reader", Privileges: []string{"USAGE"}, OnSchema: "app"}},
	}

	declared, err := commonObjectTarget(everyCommonKind...).declaredCommonObjects(source)

	c.Assert(err, qt.IsNil)
	c.Assert(declared, qt.CmpEquals(protocmp.Transform()), &protocol.DeclaredCommonObjects{
		Views: []*protocol.SQLView{{Schema: "app", Name: "recent", Body: "SELECT 1", Comment: "recent"}, {Name: "totals", Body: "SELECT 2", Materialized: true}},
		Roles: []*protocol.SQLRole{{Name: "reader", Login: true, MemberOf: []string{"readers"}, PasswordDeclared: true}},
		Grants: []*protocol.SQLGrant{
			{Role: "reader", Privileges: []string{"SELECT"}, Target: ref("table", "app", "items"), Columns: []string{"id"}},
			{Role: "reader", Privileges: []string{"USAGE"}, Target: ref("schema", "", "app"), Revoked: true},
		},
	})
}

// TestDeclaredCommonObjects_FailurePath refuses, by name, a kind the engine
// does not carry, and a comment on an object of a kind it does carry when it
// carries no comments.
func TestDeclaredCommonObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		kinds   []protocol.CommonObjectSupport_Kind
		source  *schemamodel.Database
		wantErr string
	}{
		{name: "a view", kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_ROLE},
			source: &schemamodel.Database{Views: []schemamodel.View{{Name: "recent", Body: "SELECT 1"}}}, wantErr: `.*views on the fixture engine, whose provider does not carry them`},
		{name: "a sequence", kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_VIEW},
			source: &schemamodel.Database{Sequences: []schemamodel.Sequence{{Name: "counter"}}}, wantErr: `.*sequences on the fixture engine, whose provider does not carry them`},
		{name: "a revoked grant", kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_ROLE},
			source:  &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"USAGE"}, OnSchema: "app"}}},
			wantErr: `.*grants on the fixture engine, whose provider does not carry them`},
		{name: "a role comment", kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_ROLE},
			source: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader", Comment: "reads"}}}, wantErr: `.*comments on the fixture engine, whose provider does not carry them`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared, err := commonObjectTarget(test.kinds...).declaredCommonObjects(test.source)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(declared, qt.IsNil)
		})
	}
}

// TestCommonSchemaPlacesCommonObjectsAroundTables creates the roles before
// the tables, sets a table's comments after its creation, and creates the
// views, in dependency order, the memberships, the grants and the revocations
// after the tables.
func TestCommonSchemaPlacesCommonObjectsAroundTables(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Comment: "the items"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INTEGER", Primary: true, Comment: "the key"}},
		Views: []schemamodel.View{
			{Name: "summary", Body: "SELECT * FROM recent", DependsOn: []string{"recent"}},
			{Name: "recent", Body: "SELECT id FROM items"},
		},
		Roles:         []schemamodel.Role{{Name: "reader", MemberOf: []string{"readers"}}, {Name: "readers", Group: true}},
		Grants:        []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "items"}},
		RevokedGrants: []schemamodel.Grant{{Role: "readers", Privileges: []string{"DELETE"}, OnTable: "items"}},
	}

	nodes, err := commonObjectTarget(everyCommonKind...).commonSchema(schema)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.CmpEquals(), []ast.Node{
		ast.NewCreateRole("reader").SetInherit(false),
		ast.NewCreateRole("readers").SetInherit(false).SetGroup(true),
		&ast.CreateTableNode{Name: "items", Columns: []*ast.ColumnNode{{Name: "id", Type: "INTEGER", Primary: true}}},
		commentStep("items", "", "the items"),
		commentStep("items", "id", "the key"),
		ast.NewCreateView("recent").SetBody("SELECT id FROM items"),
		ast.NewCreateView("summary").SetBody("SELECT * FROM recent"),
		ast.NewGrantRoleMembership("readers", "reader"),
		ast.NewGrantPrivilege("reader", "TABLE", "items", []string{"SELECT"}),
		ast.NewRevokePrivilege("readers", "TABLE", "items", []string{"DELETE"}),
	})
	c.Assert(schema.Tables[0].Comment, qt.Equals, "the items")
	c.Assert(schema.Fields[0].Comment, qt.Equals, "the key")
}

// TestCommonSchemaKeepsCommentsWithoutTheWire leaves a table's comment on its
// declaration for an engine that carries no comments, so the common table
// refuses it rather than drop it.
func TestCommonSchemaKeepsCommentsWithoutTheWire(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Comment: "the items"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INTEGER", Primary: true}},
	}

	nodes, err := commonObjectTarget(protocol.CommonObjectSupport_VIEW).commonSchema(schema)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*Table.Comment`)
	c.Assert(nodes, qt.IsNil)
}

func TestCommonObjectRead_HappyPath(t *testing.T) {
	c := qt.New(t)
	result := &catalog.Database{Tables: []catalog.Table{{Name: "items", Columns: []catalog.Column{{Name: "id"}}}}}
	observed := &protocol.ObservedCommonObjects{
		Coverage: []*protocol.CommonObjectCoverage{
			{Kind: protocol.CommonObjectSupport_VIEW, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}},
			{Kind: protocol.CommonObjectSupport_SEQUENCE, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "not-read"}},
		},
		Views: []*protocol.ObservedView{
			{Identity: ref("view", "app", "recent"), Body: "SELECT 1", CheckOption: "LOCAL"},
			{Identity: ref("materialized_view", "", "totals"), Body: "SELECT 2"},
		},
		Roles: []*protocol.ObservedRole{{Name: "reader", Login: true, MemberOf: []string{"readers"}, Password: protocol.ObservedRole_PASSWORD_SET}},
		Grants: []*protocol.ObservedGrant{{Role: "reader", Privilege: "SELECT", Target: ref("table", "", "items"), Columns: []string{"id", "name"},
			WithGrantOption: true}},
		RolesOutOfScope: []string{"admin"},
		Comments: []*protocol.ObjectComment{
			{Object: ref("table", "", "items"), Text: "the items"},
			{Object: ref("column", "", "id", "items"), Text: "the key"},
			{Object: ref("view", "app", "recent"), Text: "recent"},
			{Object: ref("materialized_view", "", "totals"), Text: "totals"},
			{Object: ref("role", "", "reader"), Text: "reads"},
		},
	}

	err := reader{}.commonObjects(result, observed)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables[0].Comment, qt.Equals, "the items")
	c.Assert(result.Tables[0].Columns[0].Comment, qt.Equals, "the key")
	c.Assert(result.Views, qt.DeepEquals, []catalog.View{{Name: "recent", Schema: "app", Body: "SELECT 1", CheckOption: "LOCAL", Comment: "recent"}})
	c.Assert(result.MatViews, qt.DeepEquals, []catalog.MaterializedView{{Name: "totals", Body: "SELECT 2", Comment: "totals"}})
	c.Assert(result.Roles, qt.DeepEquals, []catalog.Role{{Name: "reader", Login: true, PasswordState: catalog.RolePasswordPresent, Comment: "reads"}})
	c.Assert(result.RoleMemberships, qt.DeepEquals, []catalog.RoleMembership{{Role: "readers", Member: "reader"}})
	c.Assert(result.RolesOutOfScope, qt.DeepEquals, []catalog.Role{{Name: "admin"}})
	c.Assert(result.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "items", Column: "id", WithOption: true},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "items", Column: "name", WithOption: true},
	})
	c.Assert(result.NotDescribed, qt.DeepEquals, coverage.Set{}.With(coverage.Refused(coverage.Sequence)))
}

func TestCommonObjectRead_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		observed *protocol.ObservedCommonObjects
		wantErr  string
	}{
		{name: "an uninspected kind no record expresses", observed: &protocol.ObservedCommonObjects{Coverage: []*protocol.CommonObjectCoverage{
			{Kind: protocol.CommonObjectSupport_COMMENT, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "not-read"}},
		}}, wantErr: `.*a read that leaves comment objects uninspected`},
		{name: "a comment on an object the read does not report", observed: &protocol.ObservedCommonObjects{Comments: []*protocol.ObjectComment{
			{Object: ref("view", "", "missing"), Text: "gone"},
		}}, wantErr: `.*a comment on view "missing", which the read does not report`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := &catalog.Database{}

			err := reader{}.commonObjects(result, test.observed)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestSchemaValidationCarriesCommonObjects sends an owned validation the
// declared common objects of an engine that carries them, and no field to an
// engine that does not.
func TestSchemaValidationCarriesCommonObjects(t *testing.T) {
	tests := []struct {
		name   string
		target *target
		roles  []schemamodel.Role
		want   *protocol.DeclaredCommonObjects
	}{
		{name: "an engine with the wire", target: commonObjectTarget(everyCommonKind...), roles: []schemamodel.Role{{Name: "reader", Login: true}},
			want: &protocol.DeclaredCommonObjects{Roles: []*protocol.SQLRole{{Name: "reader", Login: true}}}},
		{name: "an engine without it", target: &target{host: &Host{}, engine: &protocol.Engine{Id: "fixture"}}, roles: nil, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			capture := validationCapture()
			capture.target = test.target
			source := validationSchema()
			source.Roles = test.roles

			captured, err := capture.validation(source)

			c.Assert(err, qt.IsNil)
			c.Assert(captured.GetCommonObjects(), qt.CmpEquals(protocmp.Transform()), test.want)
		})
	}
}
