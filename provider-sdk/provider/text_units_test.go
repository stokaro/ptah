package provider_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func commonChange(id string, object *protocol.Reference, operation *protocol.CommonObjectChange) *protocol.Change {
	return &protocol.Change{Id: id, Object: object, Operation: &protocol.Change_CommonObject{CommonObject: operation}}
}

// commonChanges holds one change of every common operation.
func commonChanges() []*protocol.Change {
	view := &protocol.SQLView{Schema: "app", Name: "active", Body: "select 1", Comment: "Active"}
	sequence := &protocol.SQLSequence{Schema: "app", Name: "orders_seq", Increment: new(int64(1))}
	role := &protocol.SQLRole{Name: "app", Login: true, PasswordDeclared: true}
	grant := &protocol.SQLGrant{Role: "app", Privileges: []string{"SELECT"}, Target: commonRef("table", "app", "users")}
	membership := &protocol.RoleMembership{Group: "readers", Member: "app"}
	column := commonRef("column", "app", "id", "users")
	return []*protocol.Change{
		commonChange("create-view", commonRef("view", "app", "active"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_CreateView{CreateView: &protocol.ViewCreation{View: view}}}),
		commonChange("drop-view", commonRef("materialized_view", "app", "totals"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_DropView{DropView: commonRef("materialized_view", "app", "totals")}}),
		commonChange("create-sequence", commonRef("sequence", "app", "orders_seq"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_CreateSequence{CreateSequence: proto.CloneOf(sequence)}}),
		commonChange("alter-sequence", commonRef("sequence", "app", "orders_seq"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_AlterSequence{AlterSequence: &protocol.SequenceAlteration{
				Sequence: commonRef("sequence", "app", "orders_seq"), Desired: proto.CloneOf(sequence), Changed: []string{"start"}, Restart: true}}}),
		commonChange("drop-sequence", commonRef("sequence", "app", "orders_seq"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_DropSequence{DropSequence: commonRef("sequence", "app", "orders_seq")}}),
		commonChange("create-role", commonRef("role", "", "app"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_CreateRole{CreateRole: &protocol.RoleCreation{Role: proto.CloneOf(role), Password: "secret"}}}),
		commonChange("alter-role", commonRef("role", "", "app"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_AlterRole{AlterRole: &protocol.RoleAlteration{Desired: proto.CloneOf(role),
				Changed: []string{"login", "password"}, Password: "next"}}}),
		commonChange("drop-role", commonRef("group", "", "readers"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_DropRole{DropRole: commonRef("group", "", "readers")}}),
		commonChange("grant", commonRef("table", "app", "users"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_Grant{Grant: proto.CloneOf(grant)}}),
		commonChange("revoke", commonRef("table", "app", "users"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_Revoke{Revoke: proto.CloneOf(grant)}}),
		commonChange("add-member", commonRef("group", "", "readers"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_AddMember{AddMember: proto.CloneOf(membership)}}),
		commonChange("remove-member", commonRef("group", "", "readers"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_RemoveMember{RemoveMember: proto.CloneOf(membership)}}),
		commonChange("comment", proto.CloneOf(column), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{Object: column}}}),
	}
}

func commonRender() (*protocol.Description, *protocol.RenderRequest, *protocol.RenderResult) {
	request := &protocol.RenderRequest{Engine: "fixture", Changes: commonChanges()}
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.Changes {
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.Id, Body: &protocol.Unit_Sql{Sql: "STATEMENT " + change.Id + ";"}})
	}
	return commonDescription(), request, result
}

// textDescription lists render.text-units on the provider and its engine.
func textDescription() *protocol.Description {
	d := commonDescription()
	d.Features = append(d.Features, provider.TextUnitsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.TextUnitsFeature)
	return d
}

func TestRenderCommonObjectChanges_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := commonRender()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateRenderedBatch(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	// A role alteration that keeps the password sends none, and a
	// materialized view is dropped by its own kind.
	alteration := request.Changes[6].GetCommonObject().GetAlterRole()
	alteration.Changed, alteration.Password = []string{"login"}, ""
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
	// A group is created and named as a group.
	group := commonChange("create-group", commonRef("group", "", "readers"), &protocol.CommonObjectChange{
		Operation: &protocol.CommonObjectChange_CreateRole{CreateRole: &protocol.RoleCreation{Role: &protocol.SQLRole{Name: "readers", Group: true}}}})
	request.Changes = append(request.Changes, group)
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
}

func TestRenderCommonObjectChanges_FailurePath(t *testing.T) {
	operation := func(r *protocol.RenderRequest, i int) *protocol.CommonObjectChange {
		return r.Changes[i].GetCommonObject()
	}
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.RenderRequest)
	}{
		{"another engine", "value engine is not owned by this provider", func(_ *protocol.Description, r *protocol.RenderRequest) { r.Engine = "other" }},
		{"an engine without common objects", "the engine carries no common object", func(d *protocol.Description, _ *protocol.RenderRequest) { d.Engines[0].CommonObjects = nil }},
		{"a kind the engine does not carry", "the engine does not carry grant objects", func(d *protocol.Description, _ *protocol.RenderRequest) {
			d.Engines[0].CommonObjects.Kinds = []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_VIEW, protocol.CommonObjectSupport_MATERIALIZED_VIEW,
				protocol.CommonObjectSupport_SEQUENCE, protocol.CommonObjectSupport_ROLE, protocol.CommonObjectSupport_COMMENT}
		}},
		{"an object that is not the subject", "a change's object differs from its subject", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].Object.Name = "other"
		}},
		{"an object of another kind", "a change's object differs from its subject", func(_ *protocol.Description, r *protocol.RenderRequest) { r.Changes[8].Object.Kind = "view" }},
		{"no operation", "a change without an operation", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].Operation = &protocol.Change_CommonObject{}
		}},
		{"an empty operation", "a change without an operation", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].Operation = &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{}}
		}},
		{"a view without a body", "a view needs a name and a body", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 0).GetCreateView().View.Body = ""
		}},
		{"a view creation without a view", "a view needs a name and a body", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 0).GetCreateView().View = nil
		}},
		{"a dropped view that is a table", "a view removal needs the view", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 1).GetDropView().Kind = "table"
			r.Changes[1].Object.Kind = "table"
		}},
		{"a sequence alteration that changes nothing", "a sequence alteration needs the sequence and what changes", func(_ *protocol.Description, r *protocol.RenderRequest) {
			alteration := operation(r, 3).GetAlterSequence()
			alteration.Changed, alteration.Restart = nil, false
		}},
		{"an unknown sequence attribute", "an unknown or repeated changed attribute", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 3).GetAlterSequence().Changed = []string{"owner"}
		}},
		{"a repeated sequence attribute", "an unknown or repeated changed attribute", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 3).GetAlterSequence().Changed = []string{"start", "start"}
		}},
		{"a sequence alteration without its desired state", "a sequence needs a name and a nonzero increment", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 3).GetAlterSequence().Desired = nil
		}},
		{"a dropped sequence of another kind", "a sequence removal needs the sequence", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 4).GetDropSequence().Kind = "view"
			r.Changes[4].Object.Kind = "view"
		}},
		{"a dropped sequence of another engine", "a sequence removal needs the sequence", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 4).GetDropSequence().Engine = "other"
			r.Changes[4].Object.Engine = "other"
		}},
		{"an altered sequence of another kind", "a sequence alteration needs the sequence and what changes", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 3).GetAlterSequence().Sequence.Kind = "view"
			r.Changes[3].Object.Kind = "view"
		}},
		{"an altered sequence of another engine", "a sequence alteration needs the sequence and what changes", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 3).GetAlterSequence().Sequence.Engine = "other"
			r.Changes[3].Object.Engine = "other"
		}},
		{"a dropped view of another engine", "a view removal needs the view", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 1).GetDropView().Engine = "other"
			r.Changes[1].Object.Engine = "other"
		}},
		{"a dropped role of another engine", "a role removal needs the role", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 7).GetDropRole().Engine = "other"
			r.Changes[7].Object.Engine = "other"
		}},
		{"a comment on an object of another engine", "a comment needs its object", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 12).GetSetComment().Object.Engine = "other"
			r.Changes[12].Object.Engine = "other"
		}},
		{"a password the role does not declare", "a role carries a password exactly when it declares one", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 5).GetCreateRole().Role.PasswordDeclared = false
		}},
		{"a declared password that does not travel", "a role carries a password exactly when it declares one", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 5).GetCreateRole().Password = ""
		}},
		{"a password that does not change", "a password travels only when it changes", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 6).GetAlterRole().Changed = []string{"login"}
		}},
		{"a role alteration that changes nothing", "a role alteration needs what changes", func(_ *protocol.Description, r *protocol.RenderRequest) {
			alteration := operation(r, 6).GetAlterRole()
			alteration.Changed, alteration.Password = nil, ""
		}},
		{"an unknown role attribute", "an unknown or repeated changed attribute", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 6).GetAlterRole().Changed = []string{"owner", "password"}
		}},
		{"a dropped role of another kind", "a role removal needs the role", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 7).GetDropRole().Kind = "user"
			r.Changes[7].Object.Kind = "user"
		}},
		{"a grant marked revoked", "a grant change says by its operation whether it revokes", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 8).GetGrant().Revoked = true
		}},
		{"a grant without privileges", "a grant needs a role and privileges", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 8).GetGrant().Privileges = nil
		}},
		{"a revocation without a target", "a grant needs a target", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 9).GetRevoke().Target = nil
		}},
		{"a group that is its own member", "a membership needs a group and another member", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 10).GetAddMember().Member = "readers"
		}},
		{"a membership without a group", "a membership needs a group and another member", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 11).GetRemoveMember().Group = ""
		}},
		{"a membership without a member", "a membership needs a group and another member", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 10).GetAddMember().Member = " "
		}},
		{"a view without a name", "a view needs a name and a body", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 0).GetCreateView().View.Name = ""
		}},
		{"a sequence without a name", "a sequence needs a name and a nonzero increment", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 2).GetCreateSequence().Name = ""
		}},
		{"a role without a name", "a role needs a name", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 5).GetCreateRole().Role.Name = ""
		}},
		{"a column comment without its table", "a column or index comment needs its table", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 12).GetSetComment().Object.Path = nil
			r.Changes[12].Object.Path = nil
		}},
		{"a comment on an engine without comments", "the engine does not carry comment objects", func(d *protocol.Description, r *protocol.RenderRequest) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_COMMENT)
			operation(r, 0).GetCreateView().View.Comment = ""
		}},
		{"a view on an engine without views", "the engine does not carry view objects", func(d *protocol.Description, _ *protocol.RenderRequest) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_VIEW)
		}},
		{"a sequence on an engine without sequences", "the engine does not carry sequence objects", func(d *protocol.Description, r *protocol.RenderRequest) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_SEQUENCE)
			r.Changes = r.Changes[2:3]
		}},
		{"a role on an engine without roles", "the engine does not carry role objects", func(d *protocol.Description, r *protocol.RenderRequest) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_ROLE)
			r.Changes = r.Changes[5:6]
		}},
		{"a membership on an engine without roles", "the engine does not carry role objects", func(d *protocol.Description, r *protocol.RenderRequest) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_ROLE)
			r.Changes = r.Changes[10:11]
		}},
		{"an altered role that is a member of itself", "a role is a member of an empty, repeated or its own group", func(_ *protocol.Description, r *protocol.RenderRequest) {
			operation(r, 6).GetAlterRole().Desired.MemberOf = []string{"app"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := commonRender()
			test.mutate(d, request)
			err := provider.ValidateRenderRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

func TestTextUnits_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, request, result := commonRender()
	request.Form = protocol.RenderRequest_TEXT
	c.Assert(provider.ValidateRenderedBatch(textDescription(), request, result), qt.IsNil)
	refused := &protocol.RenderResult{Refusal: &protocol.RenderRefusal{ChangeId: "comment", Reason: protocol.RenderRefusal_NO_TEXT_FORM,
		Message: "a column comment is kept as an attribute"}}
	c.Assert(provider.ValidateRenderedBatch(textDescription(), request, refused), qt.IsNil)
	c.Assert(provider.ValidateDescription(textDescription(), nil), qt.IsNil)
	c.Assert(provider.NegotiatedDescription(textDescription(), nil).Engines[0].Features, qt.DeepEquals, []string{provider.ValidateSchemaFeature})
}

func TestTextUnitsRequest_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name        string
		description *protocol.Description
		form        protocol.RenderRequest_Form
	}{
		{"the provider does not list the feature", commonDescription(), protocol.RenderRequest_TEXT},
		{"only the engine lists the feature", func() *protocol.Description {
			d := textDescription()
			d.Features = d.Features[:len(d.Features)-1]
			return d
		}(), protocol.RenderRequest_TEXT},
		{"the engine does not list the feature", func() *protocol.Description {
			d := textDescription()
			d.Engines[0].Features = d.Engines[0].Features[:2]
			return d
		}(), protocol.RenderRequest_TEXT},
		{"an unknown form", textDescription(), 9},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, request, _ := commonRender()
			request.Form = test.form
			c.Assert(provider.ValidateRenderRequest(test.description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestTextUnitsResult_FailurePath(t *testing.T) {
	refusal := func() *protocol.RenderResult {
		return &protocol.RenderResult{Refusal: &protocol.RenderRefusal{ChangeId: "comment", Reason: protocol.RenderRefusal_NO_TEXT_FORM, Message: "no text form"}}
	}
	native := &protocol.Unit_Native{Native: &protocol.Envelope{ProviderId: "example.org/fixture", Kind: "comment", SchemaVersion: 1, Encoding: "json"}}
	for _, test := range []struct {
		name   string
		form   protocol.RenderRequest_Form
		mutate func(*protocol.RenderResult) *protocol.RenderResult
	}{
		{"a native unit in the TEXT form", protocol.RenderRequest_TEXT, func(r *protocol.RenderResult) *protocol.RenderResult {
			r.Units[12].Body = native
			return r
		}},
		{"a refusal in the ANY form", protocol.RenderRequest_ANY, func(*protocol.RenderResult) *protocol.RenderResult { return refusal() }},
		{"a refusal beside units", protocol.RenderRequest_TEXT, func(r *protocol.RenderResult) *protocol.RenderResult {
			r.Complete, r.Refusal = false, refusal().Refusal
			return r
		}},
		{"a complete refusal", protocol.RenderRequest_TEXT, func(*protocol.RenderResult) *protocol.RenderResult {
			refused := refusal()
			refused.Complete = true
			return refused
		}},
		{"a refusal of a change the request lacks", protocol.RenderRequest_TEXT, func(*protocol.RenderResult) *protocol.RenderResult {
			refused := refusal()
			refused.Refusal.ChangeId = "other"
			return refused
		}},
		{"a refusal with a row-write reason", protocol.RenderRequest_TEXT, func(*protocol.RenderResult) *protocol.RenderResult {
			refused := refusal()
			refused.Refusal.Reason = protocol.RenderRefusal_UNSUPPORTED_MODE
			return refused
		}},
		{"a refusal without a message", protocol.RenderRequest_TEXT, func(*protocol.RenderResult) *protocol.RenderResult {
			refused := refusal()
			refused.Refusal.Message = ""
			return refused
		}},
		{"a refusal of two lines", protocol.RenderRequest_TEXT, func(*protocol.RenderResult) *protocol.RenderResult {
			refused := refusal()
			refused.Refusal.Message = "no\ntext"
			return refused
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, request, result := commonRender()
			request.Form = test.form
			c.Assert(provider.ValidateRenderedBatch(textDescription(), request, test.mutate(result)), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// textRows lists render.text-units on the row-writing provider and engine.
func textRows() *protocol.Description {
	d := dataDescription()
	d.Features = append(d.Features, provider.TextUnitsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.TextUnitsFeature)
	return d
}

func textRowWrites() (*protocol.RenderRowWritesRequest, *protocol.RenderResult) {
	request, result := rowWritesExchange()
	request.Form = protocol.RenderRequest_TEXT
	result.Units[3].Body = &protocol.Unit_Sql{Sql: "DELETE FROM rates WHERE code = 'CZ'"}
	return request, result
}

func TestTextRowWrites_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.ValidateDescription(textRows(), nil), qt.IsNil)
	request, result := textRowWrites()
	c.Assert(provider.ValidateRenderedRowWrites(textRows(), request, result), qt.IsNil)
	refused := &protocol.RenderResult{Refusal: &protocol.RenderRefusal{ChangeId: "delete", Reason: protocol.RenderRefusal_NO_TEXT_FORM, Message: "deletes are native"}}
	c.Assert(provider.ValidateRenderedRowWrites(textRows(), request, refused), qt.IsNil)
}

func TestTextRowWrites_FailurePath(t *testing.T) {
	c := qt.New(t)
	request, result := textRowWrites()
	c.Assert(provider.ValidateRenderRowWritesRequest(dataDescription(), request), qt.ErrorIs, provider.ErrProtocol)
	_, native := rowWritesExchange()
	c.Assert(provider.ValidateRenderedRowWrites(textRows(), request, native), qt.ErrorMatches, `.*the TEXT form carries a native unit`)
	refused := &protocol.RenderResult{Refusal: &protocol.RenderRefusal{ChangeId: "delete", Reason: protocol.RenderRefusal_NO_TEXT_FORM, Message: "deletes are native"}}
	request.Form = protocol.RenderRequest_ANY
	c.Assert(provider.ValidateRenderedRowWrites(textRows(), request, refused), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateRenderedRowWrites(textRows(), request, result), qt.IsNil)
}

// Text units extend rendering: a provider that renders neither changes nor
// row writes cannot offer them.
func TestTextUnitsFeatureNeedsRendering(t *testing.T) {
	c := qt.New(t)
	d := description()
	d.Features = append(d.Features, provider.TextUnitsFeature)
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*text units require rendering`)
}
