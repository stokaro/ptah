package pgpolicy_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgpolicy"
)

func registry(c *qt.C) schemaext.Registry {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: pgpolicy.Owner, Codecs: pgpolicy.Codecs()})
	c.Assert(err, qt.IsNil)
	return runtime.Codecs()
}

var (
	tenant = "tenant_id = current_setting('app.tenant')::int"
	owner  = "owner_id = current_user"
)

// TestCodecs_RoundTripEveryModel pins that each representation survives its
// codec unchanged. The values a policy's meaning hangs on are each present:
// a keyword beside a role named in its spelling but upper case, which
// PostgreSQL does not reserve, a name with a comma and one that is a space, an
// omitted clause beside a supplied one, restrictive composition, and the two
// table switches apart from each other.
func TestCodecs_RoundTripEveryModel(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "a declared policy with every value", representation: schemaext.Desired, value: &pgpolicy.DesiredPolicy{
			Command: pgpolicy.CommandUpdate,
			Roles: []pgpolicy.RoleSelector{
				{Keyword: pgpolicy.CurrentUser}, {Name: "PUBLIC"}, {Name: "app,reader"}, {Name: " "},
			},
			Using: &tenant, WithCheck: &owner, Composition: pgpolicy.Restrictive, Comment: "tenants", StructName: "Tenant",
		}},
		{name: "a declared policy taking every default", representation: schemaext.Desired, value: &pgpolicy.DesiredPolicy{}},
		{name: "a declared policy with a server's spelling", representation: schemaext.Desired, value: &pgpolicy.DesiredPolicy{
			Command: pgpolicy.CommandUpdate, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}, {Name: "reader"}},
			Using: &tenant, WithCheck: &owner,
			Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "reader"}, {Name: "app"}}, Using: &owner, WithCheck: &tenant},
		}},
		{name: "a declared policy whose spelling is PUBLIC", representation: schemaext.Desired, value: &pgpolicy.DesiredPolicy{
			Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}},
		}},
		{name: "a declared INSERT policy", representation: schemaext.Desired,
			value: &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandInsert, WithCheck: &owner}},
		{name: "an observed policy", representation: schemaext.Observed, value: &pgpolicy.ObservedPolicy{
			Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Name: "reader"}, {Name: "Mixed Case"}},
			Using: &tenant, Composition: pgpolicy.Permissive, Comment: "c",
		}},
		{name: "an observed policy for every role", representation: schemaext.Observed, value: &pgpolicy.ObservedPolicy{
			Command: pgpolicy.CommandDelete, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}, Using: &tenant, Composition: pgpolicy.Permissive,
		}},
		{name: "an observed SELECT policy", representation: schemaext.Observed, value: &pgpolicy.ObservedPolicy{
			Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: &tenant, Composition: pgpolicy.Restrictive,
		}},
		{name: "a declared table enabled only", representation: schemaext.Desired,
			value: &pgpolicy.DesiredTableState{Enabled: true, Comment: "c", StructName: "Tenant"}},
		{name: "a declared table forced only", representation: schemaext.Desired, value: &pgpolicy.DesiredTableState{Forced: true}},
		{name: "an observed table both enabled and forced", representation: schemaext.Observed,
			value: &pgpolicy.ObservedTableState{Enabled: true, Forced: true}},
		{name: "an observed table with neither", representation: schemaext.Observed, value: &pgpolicy.ObservedTableState{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codecs := registry(c)

			data, err := codecs.Marshal(t.Context(), test.representation, []schemaext.Payload{test.value})
			c.Assert(err, qt.IsNil)
			decoded, err := codecs.Unmarshal(t.Context(), data)

			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.HasLen, 1)
			c.Assert(test.value.Equal(decoded[0].(schemaext.Value)), qt.IsTrue, qt.Commentf("%s", data))
		})
	}
}

// TestCodecs_EncodeRolesAsASet pins the canonical encoding: a role list has no
// meaningful order, so two policies that list the same roles differently
// encode to the same bytes and are equal, keywords first.
func TestCodecs_EncodeRolesAsASet(t *testing.T) {
	c := qt.New(t)
	codecs := registry(c)
	first := &pgpolicy.DesiredPolicy{
		Roles: []pgpolicy.RoleSelector{{Name: "writer"}, {Keyword: pgpolicy.SessionUser}, {Name: "reader"}, {Keyword: pgpolicy.CurrentUser}}}
	second := &pgpolicy.DesiredPolicy{
		Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}, {Name: "reader"}, {Keyword: pgpolicy.SessionUser}, {Name: "writer"}}}

	encoded := must.Must(codecs.Encode(t.Context(), schemaext.Desired, []schemaext.Payload{first, second}))

	c.Assert(string(encoded[0].Payload), qt.Equals, string(encoded[1].Payload))
	c.Assert(string(encoded[0].Payload), qt.Equals,
		`{"roles":[{"keyword":"CURRENT_USER"},{"keyword":"SESSION_USER"},{"name":"reader"},{"name":"writer"}]}`)
	c.Assert(first.Equal(second), qt.IsTrue)
	c.Assert(first.Roles[0], qt.Equals, pgpolicy.RoleSelector{Name: "writer"}, qt.Commentf("encoding leaves the value alone"))
}

// TestCodecs_EncodeTheServersRolesAsASet pins the same for the role list a
// server's spelling holds.
func TestCodecs_EncodeTheServersRolesAsASet(t *testing.T) {
	c := qt.New(t)
	codecs := registry(c)
	first := &pgpolicy.DesiredPolicy{Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "writer"}, {Name: "app"}}}}
	second := &pgpolicy.DesiredPolicy{Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "app"}, {Name: "writer"}}}}

	encoded := must.Must(codecs.Encode(t.Context(), schemaext.Desired, []schemaext.Payload{first, second}))

	c.Assert(string(encoded[0].Payload), qt.Equals, string(encoded[1].Payload))
	c.Assert(string(encoded[0].Payload), qt.Equals, `{"normalized":{"roles":[{"name":"app"},{"name":"writer"}]}}`)
	c.Assert(first.Normalized.Roles[0], qt.Equals, pgpolicy.RoleSelector{Name: "writer"}, qt.Commentf("encoding leaves the value alone"))
}

// TestCodecs_RefuseWhatTheModelCannotHold pins the wire rules and the
// invariants the validators keep: unknown, null and case-variant keys, a role
// that is both or neither a keyword and a name, an empty clause, and the
// clauses PostgreSQL refuses for a command.
func TestCodecs_RefuseWhatTheModelCannotHold(t *testing.T) {
	desired, observed := pgpolicy.PolicyCodecs()[0], pgpolicy.PolicyCodecs()[1]
	desiredTable, observedTable := pgpolicy.TableStateCodecs()[0], pgpolicy.TableStateCodecs()[1]
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "null", codec: desired, input: `null`},
		{name: "an unknown key", codec: desired, input: `{"permissive":true}`},
		{name: "a null key", codec: desired, input: `{"comment":null}`},
		{name: "a key in another case", codec: desired, input: `{"Command":"ALL"}`},
		{name: "a command in lower case", codec: desired, input: `{"command":"select"}`},
		{name: "an unknown command", codec: desired, input: `{"command":"MERGE"}`},
		{name: "an unknown composition", codec: desired, input: `{"composition":"shared"}`},
		{name: "an empty role list", codec: desired, input: `{"roles":[]}`},
		{name: "a role that is both a keyword and a name", codec: desired, input: `{"roles":[{"keyword":"PUBLIC","name":"public"}]}`},
		{name: "a role that is neither", codec: desired, input: `{"roles":[{}]}`},
		{name: "a role with an unknown key", codec: desired, input: `{"roles":[{"role":"reader"}]}`},
		{name: "an unknown role keyword", codec: desired, input: `{"roles":[{"keyword":"ALL"}]}`},
		{name: "a role listed twice", codec: desired, input: `{"roles":[{"name":"reader"},{"name":"reader"}]}`},
		{name: "the reserved role name public", codec: desired, input: `{"roles":[{"name":"public"}]}`},
		{name: "the reserved role name none", codec: desired, input: `{"roles":[{"name":"none"}]}`},
		{name: "PUBLIC beside another role", codec: desired, input: `{"roles":[{"keyword":"PUBLIC"},{"name":"reader"}]}`},
		{name: "an empty role name", codec: desired, input: `{"roles":[{"name":""}]}`},
		{name: "an empty keyword beside a name", codec: desired, input: `{"roles":[{"keyword":"","name":"reader"}]}`},
		{name: "a keyword beside an empty name", codec: desired, input: `{"roles":[{"keyword":"PUBLIC","name":""}]}`},
		{name: "an empty command", codec: desired, input: `{"command":""}`},
		{name: "an empty composition", codec: desired, input: `{"composition":""}`},
		{name: "an empty comment written out", codec: desired, input: `{"comment":""}`},
		{name: "an empty struct name written out", codec: desired, input: `{"struct_name":""}`},
		{name: "an observation's empty comment written out", codec: observed,
			input: `{"command":"ALL","roles":[{"keyword":"PUBLIC"}],"composition":"permissive","comment":""}`},
		{name: "a declared table's empty comment written out", codec: desiredTable, input: `{"enabled":true,"forced":false,"comment":""}`},
		{name: "a declared table's empty struct name written out", codec: desiredTable, input: `{"enabled":true,"forced":false,"struct_name":""}`},
		{name: "an empty USING expression", codec: desired, input: `{"using":" "}`},
		{name: "an INSERT policy with USING", codec: desired, input: `{"command":"INSERT","using":"true"}`},
		{name: "a SELECT policy with WITH CHECK", codec: desired, input: `{"command":"SELECT","with_check":"true"}`},
		{name: "a DELETE policy with WITH CHECK", codec: desired, input: `{"command":"DELETE","with_check":"true"}`},
		{name: "an observation without a command", codec: observed, input: `{"roles":[{"keyword":"PUBLIC"}],"composition":"permissive"}`},
		{name: "an observation without a composition", codec: observed, input: `{"command":"ALL","roles":[{"keyword":"PUBLIC"}]}`},
		{name: "an observation without roles", codec: observed, input: `{"command":"ALL","roles":[],"composition":"permissive"}`},
		{name: "an observation with a keyword the catalog resolves", codec: observed,
			input: `{"command":"ALL","roles":[{"keyword":"CURRENT_USER"}],"composition":"permissive"}`},
		{name: "an observation with a declaration's key", codec: observed,
			input: `{"command":"ALL","roles":[{"keyword":"PUBLIC"}],"composition":"permissive","struct_name":"T"}`},
		{name: "an observation of a role selector with both keys", codec: observed,
			input: `{"command":"ALL","roles":[{"keyword":"PUBLIC","name":"reader"}],"composition":"permissive"}`},
		{name: "an observation of the reserved role name", codec: observed,
			input: `{"command":"ALL","roles":[{"name":"public"}],"composition":"permissive"}`},
		{name: "an observation of PUBLIC beside another role", codec: observed,
			input: `{"command":"ALL","roles":[{"name":"reader"},{"keyword":"PUBLIC"}],"composition":"permissive"}`},
		{name: "a null spelling", codec: desired, input: `{"normalized":null}`},
		{name: "a spelling without roles", codec: desired, input: `{"normalized":{}}`},
		{name: "a spelling with an unknown key", codec: desired, input: `{"normalized":{"roles":[{"name":"r"}],"command":"ALL"}}`},
		{name: "a spelling with a key in another case", codec: desired, input: `{"using":"x","normalized":{"roles":[{"name":"r"}],"Using":"x"}}`},
		{name: "a spelling with an empty role list", codec: desired, input: `{"normalized":{"roles":[]}}`},
		{name: "a spelling with a keyword the catalog resolves", codec: desired, input: `{"normalized":{"roles":[{"keyword":"CURRENT_USER"}]}}`},
		{name: "a spelling with a role selector of both kinds", codec: desired, input: `{"normalized":{"roles":[{"keyword":"PUBLIC","name":"r"}]}}`},
		{name: "a spelling missing a declared clause", codec: desired, input: `{"using":"x","normalized":{"roles":[{"name":"r"}]}}`},
		{name: "a spelling with a clause nobody declared", codec: desired, input: `{"normalized":{"roles":[{"name":"r"}],"with_check":"x"}}`},
		{name: "a spelling with an empty clause", codec: desired, input: `{"using":"x","normalized":{"roles":[{"name":"r"}],"using":""}}`},
		{name: "an observation with a spelling", codec: observed,
			input: `{"command":"ALL","roles":[{"keyword":"PUBLIC"}],"composition":"permissive","normalized":{"roles":[{"name":"r"}]}}`},
		{name: "a declared table without its forced flag", codec: desiredTable, input: `{"enabled":true}`},
		{name: "an observed table with a declaration's key", codec: observedTable, input: `{"enabled":true,"forced":false,"comment":"c"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, err := test.codec.Decode(json.RawMessage(test.input))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestCodecs_EncodingRefusesAnInvalidValue pins that a value no decoder would
// accept cannot be written either.
func TestCodecs_EncodingRefusesAnInvalidValue(t *testing.T) {
	c := qt.New(t)
	codecs := registry(c)

	encoded, err := codecs.Encode(t.Context(), schemaext.Desired, []schemaext.Payload{
		&pgpolicy.DesiredPolicy{Command: pgpolicy.CommandInsert, Using: &tenant},
	})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*an INSERT policy takes no USING expression`)
	c.Assert(encoded, qt.IsNil)
}

// TestValues_CloneIsIndependent pins that a clone shares no slice or pointer
// with its source.
func TestValues_CloneIsIndependent(t *testing.T) {
	c := qt.New(t)
	using := "true"
	spelled := "(true)"
	original := &pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: &using,
		Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: &spelled}}
	observedUsing := "true"
	observed := &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Composition: pgpolicy.Permissive,
		Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: &observedUsing}

	clone := original.Clone().(*pgpolicy.DesiredPolicy)
	clone.Roles[0].Name = "writer"
	*clone.Using = "false"
	clone.Normalized.Roles[0].Name = "writer"
	*clone.Normalized.Using = "(false)"
	observedClone := observed.Clone().(*pgpolicy.ObservedPolicy)
	observedClone.Roles[0].Name = "writer"
	*observedClone.Using = "false"

	c.Assert(original.Roles[0].Name, qt.Equals, "reader")
	c.Assert(*original.Using, qt.Equals, "true")
	c.Assert(original.Normalized.Roles[0].Name, qt.Equals, "reader")
	c.Assert(*original.Normalized.Using, qt.Equals, "(true)")
	c.Assert(observed.Roles[0].Name, qt.Equals, "reader")
	c.Assert(*observed.Using, qt.Equals, "true")
	c.Assert(original.Equal(original.Clone()), qt.IsTrue)
	c.Assert(original.Equal(clone), qt.IsFalse)
}

// TestValues_EqualResolvesNoDefault pins that equality is field equality: an
// omitted value is not the default it requests, and a supplied clause is not
// the absence of one. Comparing a declaration with what a server holds is the
// owner's comparison, not this.
func TestValues_EqualResolvesNoDefault(t *testing.T) {
	predicate := "true"
	tests := []struct {
		name        string
		left, right schemaext.Value
		want        bool
	}{
		{name: "the same declaration", left: &pgpolicy.DesiredPolicy{Using: &predicate}, right: &pgpolicy.DesiredPolicy{Using: &predicate}, want: true},
		{name: "an omitted command and ALL", left: &pgpolicy.DesiredPolicy{}, right: &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll}},
		{name: "omitted roles and PUBLIC", left: &pgpolicy.DesiredPolicy{},
			right: &pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}}},
		{name: "the keyword and a role named after it", left: &pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}},
			right: &pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Name: "PUBLIC"}}}},
		{name: "no WITH CHECK and one", left: &pgpolicy.DesiredPolicy{}, right: &pgpolicy.DesiredPolicy{WithCheck: &predicate}},
		{name: "permissive and restrictive", left: &pgpolicy.DesiredPolicy{Composition: pgpolicy.Permissive},
			right: &pgpolicy.DesiredPolicy{Composition: pgpolicy.Restrictive}},
		{name: "a server's spelling and none", left: &pgpolicy.DesiredPolicy{Using: &predicate},
			right: &pgpolicy.DesiredPolicy{Using: &predicate, Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}, Using: &predicate}}},
		{name: "two spellings that differ", left: &pgpolicy.DesiredPolicy{Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "a"}}}},
			right: &pgpolicy.DesiredPolicy{Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "b"}}}}},
		{name: "two spellings listing roles in another order",
			left:  &pgpolicy.DesiredPolicy{Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "a"}, {Name: "b"}}}},
			right: &pgpolicy.DesiredPolicy{Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "b"}, {Name: "a"}}}}, want: true},
		{name: "enabled and forced", left: &pgpolicy.ObservedTableState{Enabled: true}, right: &pgpolicy.ObservedTableState{Forced: true}},
		{name: "two typed nil values", left: (*pgpolicy.DesiredPolicy)(nil), right: (*pgpolicy.DesiredPolicy)(nil), want: true},
		{name: "a declaration and an observation", left: &pgpolicy.DesiredTableState{}, right: &pgpolicy.ObservedTableState{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.left.Equal(test.right), qt.Equals, test.want)
		})
	}
}

// TestPolicyRef pins the identity rules: the table is part of a policy's
// identity, the parts are kept apart rather than joined, so a dot inside a
// name cannot make two policies one, and no part is trimmed, because
// PostgreSQL keeps a policy named " p" beside one named "p".
func TestPolicyRef(t *testing.T) {
	c := qt.New(t)
	onOrders := pgpolicy.PolicyRef("app", "orders", "tenant")
	onInvoices := pgpolicy.PolicyRef("app", "invoices", "tenant")
	dotted := pgpolicy.PolicyRef("app", "orders.2024", "p")
	split := pgpolicy.PolicyRef("app", "orders", "2024.p")

	c.Assert(onOrders.Key(), qt.Not(qt.Equals), onInvoices.Key())
	c.Assert(dotted.Key(), qt.Not(qt.Equals), split.Key())
	c.Assert(pgpolicy.PolicyRef("app", "orders", " p").Key(), qt.Not(qt.Equals), pgpolicy.PolicyRef("app", "orders", "p").Key())
	c.Assert(pgpolicy.PolicyRef("app", " orders", "p").Key(), qt.Not(qt.Equals), pgpolicy.PolicyRef("app", "orders", "p").Key())
	c.Assert(pgpolicy.ValidatePolicyRef(pgpolicy.PolicyRef("app", "orders", " ")), qt.IsNil)
	c.Assert(pgpolicy.ValidatePolicyRef(onOrders), qt.IsNil)
	c.Assert(pgpolicy.Table(onOrders).Key(), qt.Equals, objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("app", "orders").Key())
	c.Assert(pgpolicy.PolicyRef("", "orders", "tenant").Key(), qt.Equals,
		pgpolicy.PolicyRefWith(identifier.ForDialect("postgres"), "public", "orders", "tenant").Key())
}

func TestPolicyRef_FailurePath(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	tests := []struct {
		name string
		ref  objectidentity.ID
	}{
		{name: "the common policy kind", ref: builder.PolicyParts("app", "orders", "tenant")},
		{name: "no table", ref: objectidentity.ID{Kind: objectidentity.Kind(pgpolicy.PolicyKind), Name: pgpolicy.PolicyRef("", "t", "p").Name}},
		{name: "no schema", ref: pgpolicy.PolicyRefWith(identifier.Semantics{}, "", "t", "p")},
		{name: "an empty name", ref: pgpolicy.PolicyRef("app", "t", "")},
		{name: "a signature", ref: func() objectidentity.ID {
			ref := pgpolicy.PolicyRef("", "t", "p")
			ref.Signature = "(int)"
			return ref
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			object, err := pgpolicy.DesiredPolicyObject(test.ref, pgpolicy.DesiredPolicy{})
			c.Assert(pgpolicy.ValidatePolicyRef(test.ref), qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(object, qt.DeepEquals, schemaext.Object{})
		})
	}
}

// TestPolicyObjects_HappyPath pins that an object holds a copy of the value it
// was built from, under the reference it was given.
func TestPolicyObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	ref := pgpolicy.PolicyRef("app", "orders", "tenant")
	declared := pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Name: "reader"}}}
	observed := pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}, Composition: pgpolicy.Permissive}

	desiredObject := must.Must(pgpolicy.DesiredPolicyObject(ref, declared))
	observedObject := must.Must(pgpolicy.ObservedPolicyObject(ref, observed))
	declared.Roles[0].Name = "writer"

	c.Assert(desiredObject.Ref, qt.Equals, ref)
	c.Assert(desiredObject.Value.(*pgpolicy.DesiredPolicy).Roles[0].Name, qt.Equals, "reader")
	c.Assert(observedObject.Value.Equal(&observed), qt.IsTrue)
}

func TestPolicyObjects_FailurePath(t *testing.T) {
	c := qt.New(t)
	ref := pgpolicy.PolicyRef("app", "orders", "tenant")

	desiredObject, desiredErr := pgpolicy.DesiredPolicyObject(ref, pgpolicy.DesiredPolicy{Composition: "shared"})
	observedObject, observedErr := pgpolicy.ObservedPolicyObject(ref, pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll})

	c.Assert(desiredErr, qt.ErrorMatches, `desired model "ptah.run/pgpolicy/policy": .*unknown policy composition "shared"`)
	var invalid *schemaext.InvalidModelError
	c.Assert(desiredErr, qt.ErrorAs, &invalid, qt.Commentf("the census recognizes the typed error"))
	c.Assert(invalid.Kind, qt.Equals, pgpolicy.PolicyKind)
	c.Assert(desiredObject, qt.DeepEquals, schemaext.Object{})
	c.Assert(observedErr, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(observedObject, qt.DeepEquals, schemaext.Object{})
}

// TestTableState_IsAFacet pins that the switches attach to a table as a facet.
func TestTableState_IsAFacet(t *testing.T) {
	c := qt.New(t)

	facets, err := schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true})

	c.Assert(err, qt.IsNil)
	c.Assert(facets.Kinds(), qt.DeepEquals, []schemaext.Kind{pgpolicy.TableStateKind})
}

// TestCompleteCoverage_ClaimsBothKinds pins the coverage a source records when
// it describes every policy and every table's switches.
func TestCompleteCoverage_ClaimsBothKinds(t *testing.T) {
	c := qt.New(t)

	coverage := must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))

	c.Assert(coverage.Representation(), qt.Equals, schemaext.Desired)
	c.Assert(coverage.Lookup(pgpolicy.PolicyKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.Lookup(pgpolicy.TableStateKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

// TestObservedPolicy_ReadsRelation pins the relations a policy's expressions
// name, which a plan that drops a relation with CASCADE asks about.
func TestObservedPolicy_ReadsRelation(t *testing.T) {
	using := "(tenant_id IN ( SELECT active_tenants.id FROM app.active_tenants))"
	tests := []struct {
		name     string
		policy   pgpolicy.ObservedPolicy
		relation string
		want     bool
	}{
		{name: "the bare name", policy: pgpolicy.ObservedPolicy{Using: &using}, relation: "active_tenants", want: true},
		{name: "the qualified name", policy: pgpolicy.ObservedPolicy{Using: &using}, relation: "app.active_tenants", want: true},
		{name: "the WITH CHECK clause", policy: pgpolicy.ObservedPolicy{WithCheck: &using}, relation: "active_tenants", want: true},
		{name: "another relation", policy: pgpolicy.ObservedPolicy{Using: &using}, relation: "tenants", want: false},
		{name: "no clause", policy: pgpolicy.ObservedPolicy{}, relation: "active_tenants", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.policy.ReadsRelation(test.relation), qt.Equals, test.want)
		})
	}
}
