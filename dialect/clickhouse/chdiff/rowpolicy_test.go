package chdiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func observedPolicy(composition chschema.Composition, filter string, names ...string) *chschema.ObservedRowPolicy {
	return &chschema.ObservedRowPolicy{Filter: new(filter), Composition: composition, Roles: chschema.RoleSelection{Names: names}}
}

func desiredPolicy(composition chschema.Composition, filter string, names ...string) *chschema.DesiredRowPolicy {
	return &chschema.DesiredRowPolicy{Filter: new(filter), Composition: composition, Roles: chschema.RoleSelection{Names: names}}
}

// The owner establishes a direction only where the combination rule decides
// it: a composition flip that keeps the filter and the users. Every other
// transition depends on the sibling policies and on server configuration, and
// is reported as unknown with the parts that changed.
func TestAssessRowPolicyAccess(t *testing.T) {
	normalized := desiredPolicy(chschema.Restrictive, "tenant=1", "alice")
	normalized.NormalizedFilter = new("tenant = 1")
	for _, test := range []struct {
		name   string
		before *chschema.ObservedRowPolicy
		after  *chschema.DesiredRowPolicy
		access schemaext.Access
		reason string
	}{
		{"a creation", nil, desiredPolicy("", "tenant = 1", "alice"), schemaext.AccessUnknown, `creating a row policy changes the rows its users see; whether .*`},
		{"a drop", observedPolicy(chschema.Permissive, "tenant = 1", "alice"), nil, schemaext.AccessUnknown, `dropping a row policy changes .*`},
		{"permissive made restrictive", observedPolicy(chschema.Permissive, "tenant = 1", "alice"),
			desiredPolicy(chschema.Restrictive, "tenant = 1", "alice"), schemaext.AccessNarrows, `a permissive policy made restrictive .*`},
		{"restrictive made permissive by its default", observedPolicy(chschema.Restrictive, "tenant = 1", "alice"),
			desiredPolicy("", "tenant = 1", "alice"), schemaext.AccessWidens, `a restrictive policy made permissive .*`},
		{"a flip read through the server's spelling", observedPolicy(chschema.Permissive, "tenant = 1", "alice"),
			normalized, schemaext.AccessNarrows, `a permissive policy made restrictive .*`},
		{"a flip that also changes the filter", observedPolicy(chschema.Permissive, "tenant = 1", "alice"),
			desiredPolicy(chschema.Restrictive, "tenant = 2", "alice"), schemaext.AccessUnknown, `changing a row policy's filter and composition changes .*`},
		{"a flip that also changes the users", observedPolicy(chschema.Permissive, "tenant = 1", "alice"),
			desiredPolicy(chschema.Restrictive, "tenant = 1", "bob"), schemaext.AccessUnknown, `changing a row policy's composition and users and roles changes .*`},
		{"a filter change", observedPolicy(chschema.Restrictive, "tenant = 1", "alice"),
			desiredPolicy(chschema.Restrictive, "tenant = 2", "alice"), schemaext.AccessUnknown, `changing a row policy's filter changes .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			effect := chdiff.AssessRowPolicyAccess(test.before, test.after)

			c.Assert(effect.Access, qt.Equals, test.access)
			c.Assert(effect.Reason, qt.Matches, test.reason)
			c.Assert(effect.Validate(), qt.IsNil)
		})
	}
}

// A change is encoded as both operands and its access assessment through the
// owner's codec, and reads back as the same change, assessment included.
func TestRowPolicyChangesRoundTripThroughTheRegistry(t *testing.T) {
	registry := must.Must(engine.New(engine.Provider{ID: "ptah.run/clickhouse",
		Codecs: append(chschema.RowPolicyCodecs(), chdiff.RowPolicyCodecs()...)})).Codecs()
	for _, test := range []struct {
		name   string
		change *chdiff.RowPolicy
	}{
		{"a creation", chdiff.NewRowPolicy(nil, desiredPolicy("", "tenant = 1", "alice"))},
		{"a change in place", chdiff.NewRowPolicy(observedPolicy(chschema.Permissive, "tenant = 1", "alice"), desiredPolicy(chschema.Restrictive, "tenant = 1", "alice"))},
		{"a drop", chdiff.NewRowPolicy(&chschema.ObservedRowPolicy{Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}, nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := registry.Encode(t.Context(), schemaext.Change, []schemaext.Payload{test.change})

			c.Assert(err, qt.IsNil)
			decoded := must.Must(registry.Decode(t.Context(), encoded))
			c.Assert(decoded[0], qt.DeepEquals, schemaext.Payload(test.change))
		})
	}
}

// The codec refuses a change it could not read back: no operand, a missing or
// invalid assessment, an extra key, and an operand the model refuses.
func TestRowPolicyChangeCodecRefusesMalformedChanges(t *testing.T) {
	codec := chdiff.RowPolicyCodecs()[0]
	access := `{"access":"unknown","reason":"r"}`
	for _, test := range []struct {
		name string
		wire string
	}{
		{"no operand", `{"before":null,"after":null,"access":` + access + `}`},
		{"no assessment", `{"before":null,"after":{}}`},
		{"a null assessment", `{"before":null,"after":{},"access":null}`},
		{"an assessment without a reason", `{"before":null,"after":{},"access":{"access":"unknown","reason":""}}`},
		{"an unknown assessment", `{"before":null,"after":{},"access":{"access":"sideways","reason":"r"}}`},
		{"an extra key", `{"before":null,"after":{},"access":` + access + `,"note":"x"}`},
		{"an invalid operand", `{"before":null,"after":{"composition":"exclusive"},"access":` + access + `}`},
		{"an observation on the desired side", `{"before":null,"after":{"composition":"permissive","roles":{}},"access":` + access + `}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := codec.Decode(json.RawMessage(test.wire))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			var invalid *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &invalid)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// A change without an assessment cannot be encoded or validated, so a safety
// report never reads an absent verdict as a safe one.
func TestValidateRowPolicy_RequiresAnAssessment(t *testing.T) {
	c := qt.New(t)
	change := &chdiff.RowPolicy{After: desiredPolicy("", "tenant = 1", "alice")}

	err := chdiff.ValidateRowPolicy(change)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	_, err = chdiff.RowPolicyCodecs()[0].Encode(change)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}

// The lifecycle effect separates removing a protection from changing one.
func TestRowPolicyChangeEffect(t *testing.T) {
	for _, test := range []struct {
		name   string
		change *chdiff.RowPolicy
		impact schemaext.Impact
	}{
		{"a creation", chdiff.NewRowPolicy(nil, desiredPolicy("", "1")), schemaext.Behavioral},
		{"a change in place", chdiff.NewRowPolicy(observedPolicy(chschema.Permissive, "1"), desiredPolicy("", "2")), schemaext.Behavioral},
		{"a drop", chdiff.NewRowPolicy(observedPolicy(chschema.Permissive, "1"), nil), schemaext.Destructive},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.change.Effect().Impact, qt.Equals, test.impact)
		})
	}
}

// String names both sides in the server's terms, the default composition
// stated, and none for an absent policy.
func TestRowPolicyChangeString(t *testing.T) {
	c := qt.New(t)
	change := chdiff.NewRowPolicy(observedPolicy(chschema.Permissive, "tenant = 1", "alice"), &chschema.DesiredRowPolicy{
		Roles: chschema.RoleSelection{All: true, Except: []string{"b`t"}}})

	c.Assert(change.String(), qt.Equals, "permissive TO `alice` USING tenant = 1 -> permissive TO ALL EXCEPT `b``t`")
	c.Assert(chdiff.NewRowPolicy(nil, &chschema.DesiredRowPolicy{}).String(), qt.Equals, "none -> permissive TO NONE")
}

// RoleClause writes every selection the model can hold, each name quoted.
func TestRoleClause(t *testing.T) {
	for _, test := range []struct {
		name  string
		roles chschema.RoleSelection
		want  string
	}{
		{"nobody", chschema.RoleSelection{}, "NONE"},
		{"every user", chschema.RoleSelection{All: true}, "ALL"},
		{"every user but some", chschema.RoleSelection{All: true, Except: []string{"bob", "my role"}}, "ALL EXCEPT `bob`, `my role`"},
		{"names that look like syntax", chschema.RoleSelection{Names: []string{"ALL", "a, b", "x`y"}}, "`ALL`, `a, b`, `x``y`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chdiff.RoleClause(test.roles), qt.Equals, test.want)
		})
	}
}

// A change encodes each operand in its canonical form, so two changes whose
// operands list the same users in another order encode to the same bytes.
func TestRowPolicyChangeEncodingIsCanonical(t *testing.T) {
	c := qt.New(t)
	codec := chdiff.RowPolicyCodecs()[0]
	encode := func(names ...string) string {
		change := chdiff.NewRowPolicy(observedPolicy(chschema.Permissive, "tenant = 1", names...), desiredPolicy(chschema.Restrictive, "tenant = 1", names...))
		return string(must.Must(codec.Encode(change)))
	}

	c.Assert(encode("bob", "alice"), qt.Equals, encode("alice", "bob"))
}
