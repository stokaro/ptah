package provider_test

import (
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func allCommonKinds() []protocol.CommonObjectSupport_Kind {
	return []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_VIEW, protocol.CommonObjectSupport_MATERIALIZED_VIEW,
		protocol.CommonObjectSupport_SEQUENCE, protocol.CommonObjectSupport_ROLE, protocol.CommonObjectSupport_GRANT, protocol.CommonObjectSupport_COMMENT}
}

// kindsWithout is every common kind but one.
func kindsWithout(kind protocol.CommonObjectSupport_Kind) []protocol.CommonObjectSupport_Kind {
	return slices.DeleteFunc(allCommonKinds(), func(other protocol.CommonObjectSupport_Kind) bool { return other == kind })
}

// commonDescription is a provider whose one engine carries every kind of
// common object and validates and renders them.
func commonDescription() *protocol.Description {
	d := description()
	d.Features = append(d.Features, provider.NormalizeValuesFeature, provider.RenderFeature, provider.ValidateSchemaFeature, provider.CommonObjectsFeature)
	d.Engines[0].Features = []string{provider.ValidateSchemaFeature, provider.CommonObjectsFeature}
	d.Engines[0].CommonObjects = &protocol.CommonObjectSupport{Kinds: allCommonKinds()}
	return d
}

// commonIdentifiers fold table names, so "Active" and "active" are one view.
func commonIdentifiers() *protocol.ComparisonIdentifiers {
	return &protocol.ComparisonIdentifiers{DefaultSchema: "app", Rules: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
		ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}
}

func commonRef(kind, schema, name string, path ...string) *protocol.Reference {
	return &protocol.Reference{Engine: "fixture", Kind: kind, Schema: schema, Path: path, Name: name}
}

func declaredCommon() *protocol.DeclaredCommonObjects {
	return &protocol.DeclaredCommonObjects{
		Views: []*protocol.SQLView{{Schema: "app", Name: "active", Body: "select id from users", Comment: "Active users"},
			{Name: "totals", Body: "select count(*) from users", Materialized: true}},
		Sequences: []*protocol.SQLSequence{{Schema: "app", Name: "orders_seq", Start: new(int64(1)), Increment: new(int64(1))}},
		Roles: []*protocol.SQLRole{{Name: "readers", Group: true},
			{Name: "app", Login: true, MemberOf: []string{"readers"}, PasswordDeclared: true}},
		Grants: []*protocol.SQLGrant{{Role: "readers", Privileges: []string{"SELECT"}, Target: commonRef("table", "app", "users"), Columns: []string{"id"}},
			{Role: "app", Privileges: []string{"CONNECT"}, Target: commonRef("database", "", ""), Revoked: true}},
	}
}

func complete() *protocol.ModelKnowledge {
	return &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}
}

func observedCommon() *protocol.ObservedCommonObjects {
	observed := &protocol.ObservedCommonObjects{
		Views: []*protocol.ObservedView{{Identity: commonRef("view", "app", "active"), Body: "select id from users"},
			{Identity: commonRef("materialized_view", "app", "totals"), Body: "select count(*) from users"}},
		Sequences: []*protocol.ObservedSequence{{Identity: commonRef("sequence", "app", "orders_seq"), Start: new(int64(1)), Increment: new(int64(1)),
			OwnedBy: commonRef("column", "app", "id", "orders")}},
		Roles: []*protocol.ObservedRole{{Name: "readers", Group: true, Password: protocol.ObservedRole_PASSWORD_NONE},
			{Name: "app", Login: true, MemberOf: []string{"readers"}, Password: protocol.ObservedRole_PASSWORD_SET}},
		Grants: []*protocol.ObservedGrant{{Role: "readers", Privilege: "SELECT", Target: commonRef("table", "app", "users"), Columns: []string{"id"}},
			{Role: "app", Privilege: "CONNECT", Target: commonRef("database", "", "")}},
		Comments: []*protocol.ObjectComment{{Object: commonRef("view", "app", "active"), Text: "Active users"},
			{Object: commonRef("column", "app", "id", "users"), Text: "The key"}},
		RolesOutOfScope: []string{"root"},
	}
	for _, kind := range allCommonKinds() {
		observed.Coverage = append(observed.Coverage, &protocol.CommonObjectCoverage{Kind: kind, Knowledge: complete()})
	}
	return observed
}

func commonInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	return commonDescription(), &protocol.InspectRequest{SessionId: "session"}, &protocol.Schema{Complete: true, Engine: "fixture",
		Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED}, CommonObjects: observedCommon()}
}

func TestCommonObjectSupport_HappyPath(t *testing.T) {
	c := qt.New(t)
	d := commonDescription()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	listed := provider.NegotiatedDescription(d, []string{provider.CommonObjectsFeature})
	c.Assert(listed.Engines[0].CommonObjects.GetKinds(), qt.DeepEquals, allCommonKinds())
	c.Assert(provider.ValidateDescription(listed, nil), qt.IsNil)
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.Engines[0].CommonObjects, qt.IsNil)
	c.Assert(unlisted.Engines[0].Features, qt.DeepEquals, []string{provider.ValidateSchemaFeature})
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
}

func TestCommonObjectSupport_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"an engine lists the feature without the provider", func(d *protocol.Description) { d.Features = d.Features[:len(d.Features)-1] }},
		{"an engine lists the feature without kinds", func(d *protocol.Description) { d.Engines[0].CommonObjects = nil }},
		{"an engine declares kinds without the feature", func(d *protocol.Description) { d.Engines[0].Features = d.Engines[0].Features[:1] }},
		{"no kind", func(d *protocol.Description) { d.Engines[0].CommonObjects.Kinds = nil }},
		{"an unspecified kind", func(d *protocol.Description) {
			d.Engines[0].CommonObjects.Kinds = []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_KIND_UNSPECIFIED}
		}},
		{"an unknown kind", func(d *protocol.Description) {
			d.Engines[0].CommonObjects.Kinds = []protocol.CommonObjectSupport_Kind{99}
		}},
		{"a repeated kind", func(d *protocol.Description) {
			d.Engines[0].CommonObjects.Kinds = append(d.Engines[0].CommonObjects.Kinds, protocol.CommonObjectSupport_VIEW)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := commonDescription()
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func prepareRequest() *protocol.PrepareCommonObjectsRequest {
	return &protocol.PrepareCommonObjectsRequest{Engine: "fixture", Identifiers: commonIdentifiers(), Capabilities: map[string]bool{"view_comments": true},
		Declared: declaredCommon()}
}

// preparedCommon spells the declaration as the engine stores it: a view body
// folded to its tokens and privileges by their permission names.
func preparedCommon() *protocol.PreparedCommonObjects {
	prepared := declaredCommon()
	prepared.Views[0].Body = "SELECT id FROM users"
	prepared.Grants[0].Privileges = []string{"engine.select_row", "engine.describe"}
	prepared.Sequences[0].AsType = "bigint"
	return &protocol.PreparedCommonObjects{Complete: true, Prepared: prepared}
}

func TestPrepareCommonObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	request := prepareRequest()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidatePreparedCommonObjects(commonDescription(), request, preparedCommon()), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	empty := &protocol.PrepareCommonObjectsRequest{Engine: "fixture", Identifiers: commonIdentifiers()}
	c.Assert(provider.ValidatePreparedCommonObjects(commonDescription(), empty, &protocol.PreparedCommonObjects{Complete: true}), qt.IsNil)
}

func TestPrepareCommonObjectsRequest_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.PrepareCommonObjectsRequest)
	}{
		{"another engine", "value engine is not owned by this provider", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) { r.Engine = "other" }},
		{"no identifier rules", "identifier rules are required", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) { r.Identifiers = nil }},
		{"a view of a kind the engine does not carry", "the engine does not carry materialized_view objects", func(d *protocol.Description, _ *protocol.PrepareCommonObjectsRequest) {
			d.Engines[0].CommonObjects.Kinds = []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_VIEW, protocol.CommonObjectSupport_SEQUENCE,
				protocol.CommonObjectSupport_ROLE, protocol.CommonObjectSupport_GRANT, protocol.CommonObjectSupport_COMMENT}
		}},
		{"a comment on an engine without comments", "the engine does not carry comment objects", func(d *protocol.Description, _ *protocol.PrepareCommonObjectsRequest) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_COMMENT)
		}},
		{"a sequence on an engine without sequences", "the engine does not carry sequence objects", func(d *protocol.Description, _ *protocol.PrepareCommonObjectsRequest) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_SEQUENCE)
		}},
		{"a role on an engine without roles", "the engine does not carry role objects", func(d *protocol.Description, _ *protocol.PrepareCommonObjectsRequest) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_ROLE)
		}},
		{"a view without a body", "a view needs a unique name and a body", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Views[0].Body = " "
		}},
		{"a view without a name", "a view needs a unique name and a body", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Views[0].Name = ""
		}},
		{"a view repeated under the identifier rules", "a view needs a unique name and a body", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Views[1] = &protocol.SQLView{Name: "ACTIVE", Body: "select 1"}
		}},
		{"a sequence without a name", "a sequence needs a name and a nonzero increment", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Sequences[0].Name = ""
		}},
		{"a zero increment", "a sequence needs a name and a nonzero increment", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Sequences[0].Increment = new(int64(0))
		}},
		{"a repeated sequence", "a repeated sequence", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Sequences = append(r.Declared.Sequences, &protocol.SQLSequence{Name: "ORDERS_SEQ"})
		}},
		{"a role without a name", "a role needs a name", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) { r.Declared.Roles[0].Name = "" }},
		{"a repeated role", "a repeated role", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Roles[1].Name, r.Declared.Roles[1].MemberOf = "readers", nil
		}},
		{"an empty group", "a role is a member of an empty, repeated or its own group", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Roles[1].MemberOf = []string{" "}
		}},
		{"a member of itself", "a role is a member of an empty, repeated or its own group", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Roles[1].MemberOf = []string{"app"}
		}},
		{"a repeated group", "a role is a member of an empty, repeated or its own group", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Roles[1].MemberOf = []string{"readers", "readers"}
		}},
		{"a grant without a role", "a grant needs a role and privileges", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Grants[0].Role = ""
		}},
		{"a grant without privileges", "a grant needs a role and privileges", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Grants[0].Privileges = nil
		}},
		{"an empty privilege", "a grant needs a role and privileges", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Grants[0].Privileges = []string{"SELECT", " "}
		}},
		{"a grant without a target", "a grant needs a target", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Grants[0].Target = nil
		}},
		{"a table target without a name", "a grant needs a target", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Grants[0].Target.Name = ""
		}},
		{"a repeated column", "an empty or repeated column", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Grants[0].Columns = []string{"id", "id"}
		}},
		{"an empty column", "an empty or repeated column", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Grants[0].Columns = []string{" "}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := commonDescription(), prepareRequest()
			test.mutate(d, request)
			err := provider.ValidatePrepareCommonObjectsRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

func TestPreparedCommonObjects_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.PreparedCommonObjects)
	}{
		{"an incomplete reply", func(r *protocol.PreparedCommonObjects) { r.Complete = false }},
		{"a dropped view", func(r *protocol.PreparedCommonObjects) { r.Prepared.Views = r.Prepared.Views[:1] }},
		{"a dropped sequence", func(r *protocol.PreparedCommonObjects) { r.Prepared.Sequences = nil }},
		{"a dropped role", func(r *protocol.PreparedCommonObjects) { r.Prepared.Roles = r.Prepared.Roles[:1] }},
		{"a dropped grant", func(r *protocol.PreparedCommonObjects) { r.Prepared.Grants = r.Prepared.Grants[:1] }},
		{"a renamed view", func(r *protocol.PreparedCommonObjects) { r.Prepared.Views[0].Name = "inactive" }},
		{"a view in another schema", func(r *protocol.PreparedCommonObjects) { r.Prepared.Views[0].Schema = "other" }},
		{"a view made materialized", func(r *protocol.PreparedCommonObjects) { r.Prepared.Views[0].Materialized = true }},
		{"a renamed sequence", func(r *protocol.PreparedCommonObjects) { r.Prepared.Sequences[0].Name = "other_seq" }},
		{"a sequence in another schema", func(r *protocol.PreparedCommonObjects) { r.Prepared.Sequences[0].Schema = "other" }},
		{"a renamed role", func(r *protocol.PreparedCommonObjects) { r.Prepared.Roles[0].Name = "writers" }},
		{"a role made a group", func(r *protocol.PreparedCommonObjects) { r.Prepared.Roles[1].Group = true }},
		{"a grant to another role", func(r *protocol.PreparedCommonObjects) { r.Prepared.Grants[0].Role = "app" }},
		{"a grant on another target", func(r *protocol.PreparedCommonObjects) { r.Prepared.Grants[0].Target.Name = "orders" }},
		{"a revoke made a grant", func(r *protocol.PreparedCommonObjects) { r.Prepared.Grants[1].Revoked = false }},
		{"a grant left without privileges", func(r *protocol.PreparedCommonObjects) { r.Prepared.Grants[0].Privileges = nil }},
		{"a view left without a body", func(r *protocol.PreparedCommonObjects) { r.Prepared.Views[0].Body = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := preparedCommon()
			test.mutate(result)
			c.Assert(provider.ValidatePreparedCommonObjects(commonDescription(), prepareRequest(), result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A reply is checked against a valid request: a request the reply alone
// cannot fault is still refused.
func TestPreparedCommonObjectsCheckTheRequest(t *testing.T) {
	c := qt.New(t)
	request := prepareRequest()
	request.Identifiers = nil
	c.Assert(provider.ValidatePreparedCommonObjects(commonDescription(), request, preparedCommon()), qt.ErrorIs, provider.ErrProtocol)
}

func TestObservedCommonObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, snapshot := commonInspection()
	// A sequence that no column owns.
	snapshot.CommonObjects.Sequences = append(snapshot.CommonObjects.Sequences, &protocol.ObservedSequence{Identity: commonRef("sequence", "app", "free_seq")})
	_, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.IsNil)
	uninspected := &protocol.ObservedCommonObjects{RolesOutOfScope: []string{"root"}}
	for _, kind := range allCommonKinds() {
		uninspected.Coverage = append(uninspected.Coverage, &protocol.CommonObjectCoverage{Kind: kind,
			Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "The read did not look"}})
	}
	uninspected.Coverage[3].Knowledge = complete()
	snapshot.CommonObjects = uninspected
	_, err = provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.IsNil)
	plain := provider.NegotiatedDescription(d, nil)
	snapshot.CommonObjects = nil
	_, err = provider.InspectionValues(plain, request, snapshot)
	c.Assert(err, qt.IsNil)
}

func TestObservedCommonObjects_FailurePath(t *testing.T) {
	uninspected := func(o *protocol.ObservedCommonObjects, kind int) {
		o.Coverage[kind].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "Not read"}
	}
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.ObservedCommonObjects)
	}{
		{"a view the read did not cover", "a view of a kind the read did not cover", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { uninspected(o, 0) }},
		{"a materialized view the read did not cover", "a view of a kind the read did not cover", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			uninspected(o, 1)
		}},
		{"a sequence the read did not cover", "a sequence the read did not cover", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { uninspected(o, 2) }},
		{"a role the read did not cover", "a role the read did not cover", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { uninspected(o, 3) }},
		{"a grant the read did not cover", "a grant the read did not cover", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { uninspected(o, 4) }},
		{"a comment the read did not cover", "a comment the read did not cover", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { uninspected(o, 5) }},
		{"a view of another kind", "a view needs an identity and a body", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.Views[0].Identity.Kind = "table" }},
		{"a view of another engine", "a view needs an identity and a body", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Views[0].Identity.Engine = "other"
		}},
		{"a view without a body", "a view needs an identity and a body", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.Views[0].Body = "" }},
		{"a view repeated under the identifier rules", "a repeated view", func(d *protocol.Description, o *protocol.ObservedCommonObjects) {
			d.Engines[0].Identifiers.TableNames = protocol.IdentifierRules_ASCII_CASE_INSENSITIVE
			o.Views[1].Identity = commonRef("materialized_view", "APP", "Active")
		}},
		{"a sequence of another kind", "a sequence needs an identity and a nonzero increment", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Sequences[0].Identity.Kind = "view"
		}},
		{"a zero increment", "a sequence needs an identity and a nonzero increment", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Sequences[0].Increment = new(int64(0))
		}},
		{"an owning column without its table", "an owning column needs its table", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Sequences[0].OwnedBy.Path = nil
		}},
		{"an owner that is no column", "an owning column needs its table", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Sequences[0].OwnedBy.Kind = "table"
		}},
		{"a repeated sequence", "a repeated sequence", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Sequences = append(o.Sequences, proto.CloneOf(o.Sequences[0]))
		}},
		{"a role without a name", "a role needs a unique name", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.Roles[0].Name = "" }},
		{"a repeated role", "a role needs a unique name", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Roles[1].Name, o.Roles[1].MemberOf = "readers", nil
		}},
		{"an empty group", "a role is a member of an empty, repeated or its own group", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Roles[1].MemberOf = []string{""}
		}},
		{"a sequence of another engine", "a sequence needs an identity and a nonzero increment", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Sequences[0].Identity.Engine = "other"
		}},
		{"an owning column of another engine", "an owning column needs its table", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Sequences[0].OwnedBy.Engine = "other"
		}},
		{"an empty grant column", "an empty or repeated column", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.Grants[0].Columns = []string{""} }},
		{"a member of itself", "a role is a member of an empty, repeated or its own group", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Roles[1].MemberOf = []string{"app"}
		}},
		{"an unknown password state", "an unknown password state", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.Roles[0].Password = 7 }},
		{"a described role out of scope", "a role out of scope is empty, repeated, described or not covered", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.RolesOutOfScope = []string{"readers"}
		}},
		{"an empty role out of scope", "a role out of scope is empty, repeated, described or not covered", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.RolesOutOfScope = []string{""}
		}},
		{"a repeated role out of scope", "a role out of scope is empty, repeated, described or not covered", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.RolesOutOfScope = []string{"root", "root"}
		}},
		{"roles out of scope the read did not cover", "a role out of scope is empty, repeated, described or not covered", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			uninspected(o, 3)
			o.Roles = nil
		}},
		{"a grant without a privilege", "a grant needs a role, a privilege and a target", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Grants[0].Privilege = ""
		}},
		{"a grant without a role", "a grant needs a role, a privilege and a target", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.Grants[0].Role = " " }},
		{"a grant target without a name", "a grant needs a role, a privilege and a target", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Grants[0].Target.Name = ""
		}},
		{"a repeated grant", "a repeated grant", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			again := proto.CloneOf(o.Grants[0])
			again.GrantedBy, again.WithGrantOption = "admin", true
			o.Grants = append(o.Grants, again)
		}},
		{"a grant column repeated", "an empty or repeated column", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Grants[0].Columns = []string{"id", "id"}
		}},
		{"a grant whose role is not UTF-8", "a grant that does not encode: string field contains invalid UTF-8", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Grants[0].Role = "\xff"
		}},
		{"a comment without its object", "a comment needs its object", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.Comments[0].Object = nil }},
		{"a column comment without its table", "a column or index comment needs its table", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Comments[1].Object.Path = nil
		}},
		{"an index comment without its table", "a column or index comment needs its table", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Comments[1].Object.Kind, o.Comments[1].Object.Path = "index", nil
		}},
		{"an empty comment", "an observed comment needs its text", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.Comments[0].Text = "" }},
		{"a repeated comment", "a repeated comment", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Comments = append(o.Comments, proto.CloneOf(o.Comments[0]))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := commonInspection()
			test.mutate(d, snapshot.CommonObjects)
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

func TestObservedCommonObjectsOfAnotherEngine(t *testing.T) {
	c := qt.New(t)
	d, request, snapshot := commonInspection()
	snapshot.Engine = "other"
	_, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.ErrorMatches, `.*value engine is not owned by this provider`)
}

func TestObservedCommonObjectsAreRequiredWhereTheEngineCarriesThem(t *testing.T) {
	c := qt.New(t)
	d, request, snapshot := commonInspection()
	snapshot.CommonObjects = nil
	_, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.ErrorMatches, `.*needs their coverage`)
}

// A host that does not list the feature gets no declaration, so a request
// that uses the wire and a read that reports common objects are refused. So
// is a preparation for an engine that carries none.
func TestCommonObjectsNeedTheNegotiatedWire(t *testing.T) {
	c := qt.New(t)
	unlisted := provider.NegotiatedDescription(commonDescription(), nil)
	c.Assert(provider.ValidatePrepareCommonObjectsRequest(unlisted, prepareRequest()), qt.ErrorMatches, `.*the wire was not negotiated`)
	plain := commonDescription()
	plain.Engines[0].Features, plain.Engines[0].CommonObjects = []string{provider.ValidateSchemaFeature}, nil
	empty := &protocol.PrepareCommonObjectsRequest{Engine: "fixture", Identifiers: commonIdentifiers()}
	c.Assert(provider.ValidateDescription(plain, nil), qt.IsNil)
	c.Assert(provider.ValidatePrepareCommonObjectsRequest(plain, empty), qt.ErrorMatches, `.*the engine carries no common object`)
	c.Assert(provider.ValidatePrepareCommonObjectsRequest(plain, nil), qt.ErrorIs, provider.ErrProtocol)
	_, request, snapshot := commonInspection()
	_, err := provider.InspectionValues(unlisted, request, snapshot)
	c.Assert(err, qt.ErrorMatches, `.*the engine carries no common object`)
}

// withCommonObjects lets the first engine of d carry every kind.
func withCommonObjects(d *protocol.Description) *protocol.Description {
	d.Features = append(d.Features, provider.CommonObjectsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.CommonObjectsFeature)
	d.Engines[0].CommonObjects = &protocol.CommonObjectSupport{Kinds: allCommonKinds()}
	return d
}

func TestValidateSchemaCarriesCommonObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request := validationExchange()
	d = withCommonObjects(d)
	request.CommonObjects = declaredCommon()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSchemaRequest(d, request), qt.IsNil)
}

func TestValidateSchemaCarriesCommonObjects_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, request := validationExchange()
	request.CommonObjects = declaredCommon()
	c.Assert(provider.ValidateSchemaRequest(d, request), qt.ErrorMatches, `.*the engine carries no common object`)
	d = withCommonObjects(d)
	request.CommonObjects.Views[0].Body = ""
	c.Assert(provider.ValidateSchemaRequest(d, request), qt.ErrorMatches, `.*a view needs a unique name and a body`)
}

// commonPlanning carries the declared and observed common objects and a
// common step that grants a privilege.
func commonPlanning() (*protocol.Description, *protocol.PlanFeaturesRequest) {
	d, request, _ := planningExchange()
	d = withCommonObjects(d)
	request.DesiredCommon, request.CurrentCommon = declaredCommon(), observedCommon()
	request.CommonSteps[0].CommonObject = commonChanges()[8].GetCommonObject()
	return d, request
}

func TestPlanningCarriesCommonObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request := commonPlanning()
	// A common step that changes no common object travels beside one that does.
	request.CommonSteps = append(request.CommonSteps, &protocol.CommonPlanningStep{Id: &protocol.StepIdentity{Owner: "host", Name: "other"},
		Metadata: &protocol.PlanningMetadata{Impact: protocol.ChangeSemantics_UNKNOWN}})
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
}

func TestPlanningCarriesCommonObjects_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.PlanFeaturesRequest)
	}{
		{"an engine without common objects", "the engine carries no common object", func(d *protocol.Description, _ *protocol.PlanFeaturesRequest) {
			d.Engines[0].CommonObjects = nil
		}},
		{"a step operand on an engine without common objects", "the engine carries no common object", func(d *protocol.Description, r *protocol.PlanFeaturesRequest) {
			d.Engines[0].CommonObjects = nil
			r.DesiredCommon, r.CurrentCommon = nil, nil
		}},
		{"a desired view without a body", "a view needs a unique name and a body", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.DesiredCommon.Views[0].Body = ""
		}},
		{"an observed kind without coverage", "a declared kind has no coverage", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.CurrentCommon.Coverage = r.CurrentCommon.Coverage[1:]
		}},
		{"a step operand without privileges", "a grant needs a role and privileges", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.CommonSteps[0].CommonObject.GetGrant().Privileges = nil
		}},
		{"a step operand without an operation", "a change without an operation", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.CommonSteps[0].CommonObject = &protocol.CommonObjectChange{}
		}},
		{"an empty declaration on an engine without common objects", "the engine carries no common object", func(d *protocol.Description, r *protocol.PlanFeaturesRequest) {
			d.Engines[0].CommonObjects = nil
			r.DesiredCommon, r.CurrentCommon, r.CommonSteps[0].CommonObject = &protocol.DeclaredCommonObjects{}, nil, nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := commonPlanning()
			test.mutate(d, request)
			err := provider.ValidatePlanningRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

// A declared password and grant columns are contract 25.3 additions to a
// SQL schema reply, sent only to a host that negotiated the wire.
func TestSQLSchemaCommonAdditions(t *testing.T) {
	c := qt.New(t)
	d, request, result := sqlSchemaExchange()
	result.Roles[0].PasswordDeclared = true
	c.Assert(provider.ValidateSQLSchema(d, request, result), qt.ErrorMatches, `.*a declared password needs schema\.common-objects`)
	result.Roles[0].PasswordDeclared = false
	result.Grants[0].Columns = []string{"id"}
	c.Assert(provider.ValidateSQLSchema(d, request, result), qt.ErrorMatches, `.*grant columns need schema\.common-objects`)
	d = withCommonObjects(d)
	result.Roles[0].PasswordDeclared = true
	c.Assert(provider.ValidateSQLSchema(d, request, result), qt.IsNil)
	result.Grants[0].Columns = []string{"id", "id"}
	c.Assert(provider.ValidateSQLSchema(d, request, result), qt.ErrorMatches, `.*an empty or repeated column`)
}

// coverageOnly is a read that covers every kind completely and found nothing,
// so a coverage refusal is the only one it can draw.
func coverageOnly() *protocol.ObservedCommonObjects {
	observed := &protocol.ObservedCommonObjects{}
	for _, kind := range allCommonKinds() {
		observed.Coverage = append(observed.Coverage, &protocol.CommonObjectCoverage{Kind: kind, Knowledge: complete()})
	}
	return observed
}

func TestCommonObjectCoverage_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.ObservedCommonObjects)
	}{
		{"a declared kind without coverage", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.Coverage = o.Coverage[1:] }},
		{"a repeated coverage entry", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Coverage = append(o.Coverage, proto.CloneOf(o.Coverage[0]))
		}},
		{"coverage of an undeclared kind", func(d *protocol.Description, o *protocol.ObservedCommonObjects) {
			// The view entry stands where the materialized view's should, so
			// the count of entries still matches the declared kinds.
			d.Engines[0].CommonObjects.Kinds = allCommonKinds()[1:]
			o.Coverage = append(o.Coverage[:1], o.Coverage[2:]...)
		}},
		{"a kind known absent", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Coverage[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_ABSENT}
		}},
		{"an uninspected kind without a reason", func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
			o.Coverage[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := commonInspection()
			snapshot.CommonObjects = coverageOnly()
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.IsNil)
			test.mutate(d, snapshot.CommonObjects)
			_, err = provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
