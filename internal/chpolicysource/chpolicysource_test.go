package chpolicysource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chpolicysource"
)

// TestParseRoles_HappyPath reads each form of a TO clause, and a name quoted
// to spell a keyword or hold a comma as a name.
func TestParseRoles_HappyPath(t *testing.T) {
	tests := []struct {
		text string
		want chschema.RoleSelection
	}{
		{text: "", want: chschema.RoleSelection{}},
		{text: "none", want: chschema.RoleSelection{}},
		{text: "ALL", want: chschema.RoleSelection{All: true}},
		{text: "all  except alice, `b,ob`", want: chschema.RoleSelection{All: true, Except: []string{"alice", "b,ob"}}},
		{text: "alice, bob", want: chschema.RoleSelection{Names: []string{"alice", "bob"}}},
		{text: "`ALL`", want: chschema.RoleSelection{Names: []string{"ALL"}}},
		{text: `"a""b", ` + "`c``d`", want: chschema.RoleSelection{Names: []string{`a"b`, "c`d"}}},
		// A backslash escapes the quote after it, so the comma between them is
		// inside the name, as the server reads it.
		{text: "`a\\`,b`, c", want: chschema.RoleSelection{Names: []string{"a`,b", "c"}}},
		{text: `"a\",b", c`, want: chschema.RoleSelection{Names: []string{`a",b`, "c"}}},
	}
	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			got, err := chpolicysource.ParseRoles(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestParseRoles_FailurePath refuses a clause a declaration could not mean:
// CURRENT_USER, which ClickHouse records as the user it resolves to, a bare
// keyword used as a name, an empty entry, and ALL EXCEPT naming nobody.
func TestParseRoles_FailurePath(t *testing.T) {
	tests := []struct {
		text    string
		wantErr string
	}{
		{text: "alice, CURRENT_USER", wantErr: `.*CURRENT_USER is a keyword; quote it to name a role`},
		{text: "alice, all", wantErr: `.*all is a keyword; quote it to name a role`},
		{text: "alice,,bob", wantErr: `.*an empty entry`},
		{text: "ALL EXCEPT", wantErr: `.*an empty entry`},
		{text: "bob smith", wantErr: `.*"bob smith" must be quoted to be a name`},
		{text: "`open", wantErr: ".*`open is not one quoted name"},
	}
	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			got, err := chpolicysource.ParseRoles(test.text)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(got, qt.DeepEquals, chschema.RoleSelection{})
		})
	}
}

// TestFormatRoles writes each selection as the clause ParseRoles reads back
// to the same selection, quoting a name that spells a keyword or is not a
// bare identifier.
func TestFormatRoles(t *testing.T) {
	tests := []struct {
		roles chschema.RoleSelection
		want  string
	}{
		{roles: chschema.RoleSelection{}, want: ""},
		{roles: chschema.RoleSelection{All: true}, want: "ALL"},
		{roles: chschema.RoleSelection{All: true, Except: []string{"admin", "b ob"}}, want: "ALL EXCEPT admin, `b ob`"},
		{roles: chschema.RoleSelection{Names: []string{"none", "c`d"}}, want: "`none`, `c``d`"},
		{roles: chschema.RoleSelection{Names: []string{`a\,b`, "c"}}, want: "`a\\\\,b`, c"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			c := qt.New(t)
			text := chpolicysource.FormatRoles(test.roles)
			c.Assert(text, qt.Equals, test.want)
			c.Assert(must.Must(chpolicysource.ParseRoles(text)).Equal(test.roles), qt.IsTrue)
		})
	}
}

// TestAttributesPolicy_HappyPath reads the attributes: the filter, the role
// selection and the struct.
func TestAttributesPolicy_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		input chpolicysource.Attributes
		want  chschema.DesiredRowPolicy
	}{
		{name: "nothing", want: chschema.DesiredRowPolicy{}},
		{name: "every attribute", input: chpolicysource.Attributes{To: "ALL EXCEPT admin", Using: "tenant = 1", StructName: "Order"},
			want: chschema.DesiredRowPolicy{Filter: new("tenant = 1"),
				Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}, StructName: "Order"}},
		{name: "a role", input: chpolicysource.Attributes{To: "alice"},
			want: chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Names: []string{"alice"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.input.Policy()
			c.Assert(err, qt.IsNil)
			c.Assert(&got, qt.CmpEquals(), &test.want)
		})
	}
}

// TestAttributesPolicy_FailurePath refuses a role selection ClickHouse does
// not parse.
func TestAttributesPolicy_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		input   chpolicysource.Attributes
		wantErr string
	}{
		{name: "a role selection", input: chpolicysource.Attributes{To: "CURRENT_USER"}, wantErr: `.*CURRENT_USER is a keyword.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.input.Policy()
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(&got, qt.CmpEquals(), &chschema.DesiredRowPolicy{})
		})
	}
}

// TestCollector_RefusesTwoDeclarationsOfOnePolicy names both declarations of
// one policy, and keeps the scope of the one it took.
func TestCollector_RefusesTwoDeclarationsOfOnePolicy(t *testing.T) {
	c := qt.New(t)
	var collector chpolicysource.Collector
	ref := chpolicysource.Ref("", "orders", "tenant")

	c.Assert(collector.AddPolicy("first", ref, chschema.DesiredRowPolicy{}, []string{"clickhouse"}), qt.IsNil)
	err := collector.AddPolicy("second", ref, chschema.DesiredRowPolicy{}, nil)

	c.Assert(err, qt.ErrorMatches, `.*first and second both declare row policy "tenant" on table "orders"; keep one declaration`)
	objects := must.Must(collector.Objects().All())
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Targets, qt.DeepEquals, []string{"clickhouse"})
}

// TestRequireDescribed_FailurePath refuses an export of row policies the read
// did not inspect, as a whole or one by one, and an export of a read that
// could not describe the model as a whole: a format that declares row
// policies claims every one, so the export would declare none, and applying
// it would drop the policies the server holds.
func TestRequireDescribed_FailurePath(t *testing.T) {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	unread := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the account may not read system.row_policies"}
	unmodeled := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the server reports a form Ptah does not model"}
	tests := []struct {
		name     string
		coverage schemaext.Coverage
		wantErr  string
	}{
		{name: "the model", coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, unread, nil)),
			wantErr: `.*ClickHouse row policies cannot be exported: the read did not describe them \(the account may not read system.row_policies\)`},
		{name: "the model unrepresentable", coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, unmodeled, nil)),
			wantErr: `.*ClickHouse row policies cannot be exported: the read did not describe them \(the server reports a form Ptah does not model\)`},
		{name: "one policy", coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, complete, []schemaext.SubjectCoverage{
			{Kind: chschema.RowPolicyKind, Subject: chpolicysource.Ref("", "orders", "tenant"), Knowledge: unread}})),
			wantErr: `.*ClickHouse row policy .* cannot be exported: the read did not describe it.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := chpolicysource.RequireDescribed(test.coverage)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}

// TestRequireDescribed_HappyPath passes a complete description, a policy the
// read knows is absent, and a policy the read met and no source can declare,
// such as one written ON db.*: the export leaves that one out, and
// [chpolicysource.Undescribed] names it.
func TestRequireDescribed_HappyPath(t *testing.T) {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	tests := []struct {
		name     string
		subjects []schemaext.SubjectCoverage
	}{
		{name: "a complete description"},
		{name: "an absent policy", subjects: []schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind,
			Subject: chpolicysource.Ref("", "orders", "tenant"), Knowledge: schemaext.Knowledge{State: schemaext.Absent}}}},
		{name: "a database-wide policy", subjects: []schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind,
			Subject: chpolicysource.Ref("d1", "*", "tenant"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "written ON d1.*"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := chpolicysource.RequireDescribed(must.Must(chschema.RowPolicyCoverage(schemaext.Observed, complete, test.subjects)))
			c.Assert(err, qt.IsNil)
		})
	}
}

// TestUndescribed lists the policies the read met and could not describe, in
// coverage order, and leaves out the ones it knows are absent or did not
// inspect.
func TestUndescribed(t *testing.T) {
	c := qt.New(t)
	wide := schemaext.SubjectCoverage{Kind: chschema.RowPolicyKind, Subject: chpolicysource.Ref("d1", "*", "tenant"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "written ON d1.*"}}
	onView := schemaext.SubjectCoverage{Kind: chschema.RowPolicyKind, Subject: chpolicysource.Ref("d1", "v", "p"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "on a view"}}
	absent := schemaext.SubjectCoverage{Kind: chschema.RowPolicyKind, Subject: chpolicysource.Ref("d1", "orders", "gone"),
		Knowledge: schemaext.Knowledge{State: schemaext.Absent}}
	coverage := must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{wide, absent, onView}))

	got := chpolicysource.Undescribed(coverage)

	c.Assert(got, qt.HasLen, 2)
	c.Assert(got[0].Subject.Key(), qt.Equals, wide.Subject.Key())
	c.Assert(got[1].Subject.Key(), qt.Equals, onView.Subject.Key())
	c.Assert(chpolicysource.Undescribed(schemaext.Coverage{}), qt.HasLen, 0)
}
