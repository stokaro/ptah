package chschema_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

// Each policy is encoded through a runtime that registers the codecs and read
// back as the same value. The wire form is the canonical one: keys and role
// lists in byte order and every omitted value off the wire, so a restrictive policy, a
// TO ALL EXCEPT selection and a policy that applies to nobody each keep a form
// of their own.
func TestRowPolicyValuesRoundTripThroughTheRegistry(t *testing.T) {
	registry := must.Must(engine.New(engine.Provider{ID: "ptah.run/clickhouse", Codecs: chschema.RowPolicyCodecs()})).Codecs()
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
		wire           string
	}{
		{"a declaration stating nothing", schemaext.Desired, &chschema.DesiredRowPolicy{}, `{}`},
		{"a declaration stating everything", schemaext.Desired, &chschema.DesiredRowPolicy{
			Filter: new("tenant = currentUser()"), Composition: chschema.Restrictive,
			Roles: chschema.RoleSelection{Names: []string{"reader", "analyst"}}, StructName: "Orders",
		}, `{"composition":"restrictive","filter":"tenant = currentUser()","roles":{"names":["analyst","reader"]},"struct_name":"Orders"}`},
		{"names that look like syntax", schemaext.Desired, &chschema.DesiredRowPolicy{
			Filter: new("1"), Roles: chschema.RoleSelection{Names: []string{"ALL", "my role", "a, b", "x`y"}},
		}, "{\"filter\":\"1\",\"roles\":{\"names\":[\"ALL\",\"a, b\",\"my role\",\"x`y\"]}}"},
		{"TO ALL", schemaext.Desired, &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{All: true}}, `{"roles":{"all":true}}`},
		{"a declaration a server normalized", schemaext.Desired, &chschema.DesiredRowPolicy{Filter: new("tenant=1"), NormalizedFilter: new("tenant = 1")},
			`{"filter":"tenant=1","normalized_filter":"tenant = 1"}`},
		{"TO ALL EXCEPT", schemaext.Desired, &chschema.DesiredRowPolicy{
			Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true, Except: []string{"bob", "admin"}},
		}, `{"composition":"permissive","roles":{"all":true,"except":["admin","bob"]}}`},
		{"an observation that applies to nobody", schemaext.Observed, &chschema.ObservedRowPolicy{
			Filter: new("tenant = 1"), Composition: chschema.Permissive,
		}, `{"composition":"permissive","filter":"tenant = 1","roles":{}}`},
		{"a restrictive observation without a filter", schemaext.Observed, &chschema.ObservedRowPolicy{
			Composition: chschema.Restrictive, Roles: chschema.RoleSelection{Names: []string{"alice"}},
		}, `{"composition":"restrictive","roles":{"names":["alice"]}}`},
		{"an observed TO ALL EXCEPT", schemaext.Observed, &chschema.ObservedRowPolicy{
			Filter: new("1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"carol", "bob"}},
		}, `{"composition":"restrictive","filter":"1","roles":{"all":true,"except":["bob","carol"]}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := registry.Encode(t.Context(), test.representation, []schemaext.Payload{test.value})

			c.Assert(err, qt.IsNil)
			c.Assert(encoded[0].Kind, qt.Equals, chschema.RowPolicyKind)
			c.Assert(string(encoded[0].Payload), qt.Equals, test.wire)
			decoded := must.Must(registry.Decode(t.Context(), encoded))
			value, ok := decoded[0].(schemaext.Value)
			c.Assert(ok, qt.IsTrue)
			c.Assert(value.Equal(test.value), qt.IsTrue, qt.Commentf("decoded %#v", decoded[0]))
		})
	}
}

// Policies that differ only in what decides access encode to different bytes,
// so a fingerprint over the encoding tells them apart, and policies that
// differ only in the order of a role list encode to the same bytes.
func TestRowPolicyEncodingSeparatesWhatDecidesAccess(t *testing.T) {
	c := qt.New(t)
	codec := chschema.RowPolicyCodecs()[1]
	encode := func(policy chschema.ObservedRowPolicy) string {
		return string(must.Must(codec.Canonical(&policy)))
	}
	base := chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{"a", "b"}}}
	variants := []chschema.ObservedRowPolicy{
		base,
		{Filter: base.Filter, Composition: chschema.Restrictive, Roles: base.Roles},
		{Composition: chschema.Permissive, Roles: base.Roles},
		{Filter: base.Filter, Composition: chschema.Permissive},
		{Filter: base.Filter, Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true}},
		{Filter: base.Filter, Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true, Except: []string{"a", "b"}}},
	}
	encodings := make(map[string]bool, len(variants))
	for _, variant := range variants {
		encodings[encode(variant)] = true
	}

	c.Assert(encodings, qt.HasLen, len(variants))
	c.Assert(encode(chschema.ObservedRowPolicy{Filter: base.Filter, Composition: chschema.Permissive,
		Roles: chschema.RoleSelection{Names: []string{"b", "a"}}}), qt.Equals, encode(base))
}

// The codecs are strict: a key the model does not have, a null, a repeated
// key, a value the encoder would have omitted and a selection ClickHouse
// cannot express are all refused, so a document cannot carry a policy the
// owner would read another way.
func TestRowPolicyCodecsRefuseMalformedValues(t *testing.T) {
	for _, test := range []struct {
		name string
		wire string
		// representations is which codecs refuse the wire: a shape both refuse
		// is listed for both.
		representations []schemaext.Representation
	}{
		{"null", `null`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"an unknown key", `{"composition":"permissive","roles":{},"command":"SELECT"}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"a null filter", `{"filter":null,"composition":"permissive","roles":{}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"an empty filter", `{"filter":"","composition":"permissive","roles":{}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"a blank filter", `{"filter":"  ","composition":"permissive","roles":{}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"an unknown composition", `{"composition":"exclusive","roles":{}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"an empty composition", `{"composition":"","roles":{}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"a composition in another case", `{"composition":"RESTRICTIVE","roles":{}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"a repeated key", `{"composition":"permissive","composition":"restrictive","roles":{}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"all set to false", `{"composition":"permissive","roles":{"all":false}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"an empty name list", `{"composition":"permissive","roles":{"names":[]}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"an empty name", `{"composition":"permissive","roles":{"names":[""]}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"a name twice", `{"composition":"permissive","roles":{"names":["a","a"]}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"names beside TO ALL", `{"composition":"permissive","roles":{"all":true,"names":["a"]}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"exceptions without TO ALL", `{"composition":"permissive","roles":{"except":["a"]}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"an unknown selection key", `{"composition":"permissive","roles":{"none":true}}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"a keyword spelled as a string", `{"composition":"permissive","roles":"ALL"}`, []schemaext.Representation{schemaext.Desired, schemaext.Observed}},
		{"a declared empty selection", `{"roles":{}}`, []schemaext.Representation{schemaext.Desired}},
		{"an empty struct name", `{"struct_name":""}`, []schemaext.Representation{schemaext.Desired}},
		{"an observation without a composition", `{"roles":{}}`, []schemaext.Representation{schemaext.Observed}},
		{"an observation without roles", `{"composition":"permissive"}`, []schemaext.Representation{schemaext.Observed}},
		{"an observation with a struct name", `{"composition":"permissive","roles":{},"struct_name":"Orders"}`, []schemaext.Representation{schemaext.Observed}},
		{"an empty normalized filter", `{"filter":"a","normalized_filter":""}`, []schemaext.Representation{schemaext.Desired}},
		{"a normalized filter without a filter", `{"normalized_filter":"a"}`, []schemaext.Representation{schemaext.Desired}},
		{"an observation with a normalized filter", `{"composition":"permissive","roles":{},"filter":"a","normalized_filter":"a"}`, []schemaext.Representation{schemaext.Observed}},
	} {
		for _, representation := range test.representations {
			t.Run(test.name+"/"+string(representation), func(t *testing.T) {
				c := qt.New(t)
				codec := rowPolicyCodec(representation)

				decoded, err := codec.Decode(json.RawMessage(test.wire))

				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(decoded, qt.IsNil)
			})
		}
	}
}

// The values the strict codecs refuse in one representation are the ones
// that representation spells by omission, so the other accepts them.
func TestRowPolicyCodecsAcceptEachRepresentationsOwnSpelling(t *testing.T) {
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		wire           string
	}{
		{"an observation that applies to nobody", schemaext.Observed, `{"composition":"permissive","roles":{}}`},
		{"a declaration that omits its selection", schemaext.Desired, `{"composition":"permissive"}`},
		{"a declaration stating nothing", schemaext.Desired, `{}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := rowPolicyCodec(test.representation).Decode(json.RawMessage(test.wire))

			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.IsNotNil)
		})
	}
}

func rowPolicyCodec(representation schemaext.Representation) schemaext.Codec {
	codecs := chschema.RowPolicyCodecs()
	return map[schemaext.Representation]schemaext.Codec{schemaext.Desired: codecs[0], schemaext.Observed: codecs[1]}[representation]
}

// Validation refuses what ClickHouse cannot hold or would read another way,
// in both representations, and names the model it refused.
func TestValidateRowPolicy_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		desired  *chschema.DesiredRowPolicy
		observed *chschema.ObservedRowPolicy
		want     string
	}{
		{"nil", nil, nil, `.*nil row policy.*`},
		{"an empty filter", &chschema.DesiredRowPolicy{Filter: new("")}, &chschema.ObservedRowPolicy{Filter: new(""), Composition: chschema.Permissive},
			`.*filter cannot be empty.*`},
		{"a filter holding a NUL", &chschema.DesiredRowPolicy{Filter: new("a\x00")}, &chschema.ObservedRowPolicy{Filter: new("a\x00"), Composition: chschema.Permissive},
			`.*filter contains a NUL byte.*`},
		{"names beside TO ALL", &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{All: true, Names: []string{"a"}}},
			&chschema.ObservedRowPolicy{Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true, Names: []string{"a"}}}, `.*TO ALL or to named users and roles, not both.*`},
		{"exceptions without TO ALL", &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Except: []string{"a"}}},
			&chschema.ObservedRowPolicy{Composition: chschema.Permissive, Roles: chschema.RoleSelection{Except: []string{"a"}}}, `.*exceptions only beside TO ALL.*`},
		{"a name twice", &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Names: []string{"a", "a"}}},
			&chschema.ObservedRowPolicy{Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{"a", "a"}}}, `.*TO list names "a" twice.*`},
		{"an exception twice", &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{All: true, Except: []string{"a", "a"}}},
			&chschema.ObservedRowPolicy{Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true, Except: []string{"a", "a"}}}, `.*ALL EXCEPT list names "a" twice.*`},
		{"an empty name", &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Names: []string{""}}},
			&chschema.ObservedRowPolicy{Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{""}}}, `.*holds an empty name.*`},
		{"an unknown composition", &chschema.DesiredRowPolicy{Composition: "exclusive"}, &chschema.ObservedRowPolicy{Composition: "exclusive"},
			`.*unknown row policy composition "exclusive".*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			desiredErr := chschema.ValidateDesiredRowPolicy(test.desired)
			observedErr := chschema.ValidateObservedRowPolicy(test.observed)

			for _, err := range []error{desiredErr, observedErr} {
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(err, qt.ErrorMatches, test.want)
				var invalid *schemaext.InvalidModelError
				c.Assert(err, qt.ErrorAs, &invalid)
				c.Assert(invalid.Kind, qt.Equals, chschema.RowPolicyKind)
			}
		})
	}
}

// An observation is definite: it states its composition, where a declaration
// may leave it to ClickHouse's default.
func TestValidateObservedRowPolicy_RequiresAComposition(t *testing.T) {
	c := qt.New(t)

	desiredErr := chschema.ValidateDesiredRowPolicy(&chschema.DesiredRowPolicy{})
	observedErr := chschema.ValidateObservedRowPolicy(&chschema.ObservedRowPolicy{})

	c.Assert(desiredErr, qt.IsNil)
	c.Assert(observedErr, qt.ErrorMatches, `.*unknown row policy composition "".*`)
}

// A reference names the table and the policy, and the database when the
// source states one; an empty database resolves to the connection's under
// the semantics a comparison uses.
func TestRowPolicyRef_HappyPath(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("clickhouse")
	semantics.DefaultSchema = "analytics"

	qualified := chschema.RowPolicyRef("analytics", "orders", "tenant")
	defaulted := chschema.RowPolicyRefWith(semantics, "", "orders", "tenant")
	spaced := chschema.RowPolicyRef("analytics", "orders", " tenant")

	c.Assert(chschema.ValidateRowPolicyRef(qualified), qt.IsNil)
	c.Assert(chschema.ValidateRowPolicyRef(chschema.RowPolicyRef("", "orders", "tenant")), qt.IsNil)
	c.Assert(qualified.Kind, qt.Equals, objectidentity.Kind(chschema.RowPolicyKind))
	c.Assert(qualified.Parent.Source, qt.Equals, "orders")
	c.Assert(qualified.Name.Source, qt.Equals, "tenant")
	c.Assert(defaulted.Key(), qt.Equals, qualified.Key())
	c.Assert(defaulted.Schema.Defaulted, qt.IsTrue)
	c.Assert(spaced.Key(), qt.Not(qt.Equals), qualified.Key())
	c.Assert(chschema.RowPolicyRef("analytics", "orders", "Tenant").Key(), qt.Not(qt.Equals), qualified.Key())
	c.Assert(chschema.RowPolicyRef("analytics", "customers", "tenant").Key(), qt.Not(qt.Equals), qualified.Key())
	c.Assert(chschema.RowPolicyTable(qualified), qt.DeepEquals, objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TablePartsVerbatim("analytics", "orders"))
}

// A reference without a table is a database-wide policy, which this model
// does not hold; it is refused by name rather than read as a table policy.
func TestValidateRowPolicyRef_FailurePath(t *testing.T) {
	withCatalog := chschema.RowPolicyRef("analytics", "orders", "tenant")
	withCatalog.Catalog = objectidentity.Part{Source: "cluster", Normalized: "cluster"}
	for _, test := range []struct {
		name string
		ref  objectidentity.ID
		want string
	}{
		{"a database-wide policy", chschema.RowPolicyRef("analytics", "", "tenant"), `.*row policy "tenant" has no table; a database-wide policy \(ON db\.\*\) is not supported.*`},
		{"no name", chschema.RowPolicyRef("analytics", "orders", ""), `.*requires a name.*`},
		{"another kind", objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TablePartsVerbatim("analytics", "orders"), `.*has kind "ptah.run/clickhouse/row-policy".*`},
		{"a catalog", withCatalog, `.*no catalog or signature.*`},
		{"a name holding a NUL", chschema.RowPolicyRef("analytics", "orders", "t\x00"), `.*row policy name contains a NUL byte.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := chschema.ValidateRowPolicyRef(test.ref)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// The object constructors refuse an invalid reference or value, and store a
// copy the caller cannot change afterwards.
func TestRowPolicyObjects(t *testing.T) {
	c := qt.New(t)
	ref := chschema.RowPolicyRef("analytics", "orders", "tenant")
	declared := chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Roles: chschema.RoleSelection{Names: []string{"reader"}}}

	desired, err := chschema.DesiredRowPolicyObject(ref, declared)
	declared.Roles.Names[0] = "writer"
	*declared.Filter = "1"

	c.Assert(err, qt.IsNil)
	c.Assert(desired.Value.Equal(&chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Roles: chschema.RoleSelection{Names: []string{"reader"}}}), qt.IsTrue)
	_, err = chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("analytics", "", "tenant"), chschema.DesiredRowPolicy{})
	c.Assert(err, qt.ErrorMatches, `.*database-wide policy.*`)
	_, err = chschema.ObservedRowPolicyObject(ref, chschema.ObservedRowPolicy{})
	c.Assert(err, qt.ErrorMatches, `.*unknown row policy composition.*`)
	observed, err := chschema.ObservedRowPolicyObject(ref, chschema.ObservedRowPolicy{Composition: chschema.Restrictive})
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Ref, qt.DeepEquals, ref)
}

// A clone shares no list or pointer with its source, and a typed nil stays a
// typed nil.
func TestRowPolicyClonesAreIndependent(t *testing.T) {
	c := qt.New(t)
	desired := &chschema.DesiredRowPolicy{Filter: new("a"), NormalizedFilter: new("a"), Roles: chschema.RoleSelection{All: true, Except: []string{"x"}}}
	observed := &chschema.ObservedRowPolicy{Filter: new("a"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{"x"}}}

	desiredClone, ok := desired.Clone().(*chschema.DesiredRowPolicy)
	c.Assert(ok, qt.IsTrue)
	observedClone, ok := observed.Clone().(*chschema.ObservedRowPolicy)
	c.Assert(ok, qt.IsTrue)
	*desired.Filter, *desired.NormalizedFilter, desired.Roles.Except[0] = "b", "b", "y"
	*observed.Filter, observed.Roles.Names[0] = "b", "y"

	c.Assert(*desiredClone.Filter, qt.Equals, "a")
	c.Assert(*desiredClone.NormalizedFilter, qt.Equals, "a")
	c.Assert(desiredClone.Roles.Except, qt.DeepEquals, []string{"x"})
	c.Assert(*observedClone.Filter, qt.Equals, "a")
	c.Assert(observedClone.Roles.Names, qt.DeepEquals, []string{"x"})
	c.Assert((*chschema.DesiredRowPolicy)(nil).Clone(), qt.Equals, schemaext.Value((*chschema.DesiredRowPolicy)(nil)))
	c.Assert((*chschema.ObservedRowPolicy)(nil).Clone(), qt.Equals, schemaext.Value((*chschema.ObservedRowPolicy)(nil)))
}

// Equality reads role lists as sets and keeps everything else exact: an
// omitted composition is not permissive, a missing filter is not an empty one,
// and TO ALL is not TO ALL EXCEPT.
func TestRowPolicyEquality(t *testing.T) {
	for _, test := range []struct {
		name        string
		left, right schemaext.Value
		want        bool
	}{
		{"the same names in another order",
			&chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Names: []string{"a", "b"}}},
			&chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Names: []string{"b", "a"}}}, true},
		{"a nil and an empty list",
			&chschema.ObservedRowPolicy{Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: make([]string, 0)}},
			&chschema.ObservedRowPolicy{Composition: chschema.Permissive}, true},
		{"an omitted composition and permissive",
			&chschema.DesiredRowPolicy{}, &chschema.DesiredRowPolicy{Composition: chschema.Permissive}, false},
		{"permissive and restrictive",
			&chschema.ObservedRowPolicy{Composition: chschema.Permissive}, &chschema.ObservedRowPolicy{Composition: chschema.Restrictive}, false},
		{"no filter and a filter",
			&chschema.DesiredRowPolicy{}, &chschema.DesiredRowPolicy{Filter: new("1")}, false},
		{"TO ALL and TO ALL EXCEPT",
			&chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{All: true}},
			&chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{All: true, Except: []string{"a"}}}, false},
		{"TO ALL and nobody",
			&chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{All: true}}, &chschema.DesiredRowPolicy{}, false},
		{"another struct name",
			&chschema.DesiredRowPolicy{StructName: "A"}, &chschema.DesiredRowPolicy{StructName: "B"}, false},
		{"a normalization and none",
			&chschema.DesiredRowPolicy{Filter: new("a"), NormalizedFilter: new("a")}, &chschema.DesiredRowPolicy{Filter: new("a")}, false},
		{"a declaration and an observation",
			&chschema.DesiredRowPolicy{Composition: chschema.Permissive}, &chschema.ObservedRowPolicy{Composition: chschema.Permissive}, false},
		{"two typed nils", (*chschema.ObservedRowPolicy)(nil), (*chschema.ObservedRowPolicy)(nil), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.left.Equal(test.right), qt.Equals, test.want)
			c.Assert(test.right.Equal(test.left), qt.Equals, test.want)
		})
	}
}

// A declaration projects as the policy the server holds once it is created,
// with the composition ClickHouse defaults to, and an observation captures as
// the declaration that states every default.
func TestRowPolicyProjections(t *testing.T) {
	c := qt.New(t)
	declared := &chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}

	observed, err := declared.Observed()

	c.Assert(err, qt.IsNil)
	c.Assert(observed.Equal(&chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive,
		Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}), qt.IsTrue)
	c.Assert(observed.Desired().Equal(&chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive,
		Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}), qt.IsTrue)
	c.Assert((*chschema.ObservedRowPolicy)(nil).Desired(), qt.IsNil)
	normalized := must.Must((&chschema.DesiredRowPolicy{Filter: new("tenant=1"), NormalizedFilter: new("tenant = 1")}).Observed())
	c.Assert(*normalized.Filter, qt.Equals, "tenant = 1")
	_, err = (*chschema.DesiredRowPolicy)(nil).Observed()
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}

// Coverage names the model's exact definition, and a subject override
// records a policy the source could not describe without changing the claim
// for the rest of the kind.
func TestRowPolicyCoverage(t *testing.T) {
	c := qt.New(t)
	ref := chschema.RowPolicyRef("analytics", "orders", "tenant")
	other := chschema.RowPolicyRef("analytics", "orders", "audit")
	limit := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "defined in users.xml"}

	coverage, err := chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind, Subject: ref, Knowledge: limit}})

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(chschema.RowPolicyKind, ref), qt.DeepEquals, limit)
	c.Assert(coverage.Lookup(chschema.RowPolicyKind, other).State, qt.Equals, schemaext.Complete)
	_, err = chschema.RowPolicyCoverage(schemaext.Change, schemaext.Knowledge{State: schemaext.Complete}, nil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}

// A normalized filter is the server's spelling of a declared one, so it is
// refused without the filter it normalizes, and empty like any filter.
func TestValidateDesiredRowPolicy_NormalizedFilter_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy *chschema.DesiredRowPolicy
		want   string
	}{
		{"without a filter", &chschema.DesiredRowPolicy{NormalizedFilter: new("a")}, `.*normalized row policy filter needs the declared filter it normalizes.*`},
		{"empty", &chschema.DesiredRowPolicy{Filter: new("a"), NormalizedFilter: new(" ")}, `.*filter cannot be empty.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := chschema.ValidateDesiredRowPolicy(test.policy)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
