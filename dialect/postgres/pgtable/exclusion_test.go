package pgtable_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

func exclusionDefinition() pgtable.Exclusion {
	return pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&"}
}

func exclusionDependency(name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.KindExtension, Name: objectidentity.Part{Source: name, Normalized: name}}
}

func TestExclusionCodecs_HappyPath(t *testing.T) {
	for _, codec := range pgtable.ExclusionCodecs() {
		for _, wire := range []string{
			`{"using_method":"gist","elements":"during WITH &&"}`,
			`{"using_method":"gist","elements":"during WITH &&","where_condition":"active","deferrable":true,"initially":"deferred","comment":"No overlap"}`,
			`{"using_method":"gist","elements":"room WITH =","requires":[{"kind":"extension","name":{"source":"btree_gist","normalized":"btree_gist"}}]}`,
		} {
			t.Run(string(codec.Representation)+wire, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(wire))
				c.Assert(err, qt.IsNil)
				encoded, err := codec.Encode(value)
				c.Assert(err, qt.IsNil)
				c.Assert(string(encoded), qt.Equals, strings.ReplaceAll(wire, "&", `\u0026`))
				cloned, err := codec.Clone(value)
				c.Assert(err, qt.IsNil)
				c.Assert(cloned, qt.DeepEquals, value)
				c.Assert(cloned, qt.Not(qt.Equals), value)
			})
		}
	}
}

func TestExclusionCodecs_FailurePath(t *testing.T) {
	for _, codec := range pgtable.ExclusionCodecs() {
		for _, wire := range []string{
			`null`, `{}`, `{"using_method":"gist"}`,
			`{"using_method":"gist","elements":""}`,
			`{"using_method":" ","elements":"x WITH ="}`,
			`{"using_method":"gist","elements":"x WITH =","extra":1}`,
			`{"using_method":"gist","elements":"x WITH =","elements":"y WITH ="}`,
			`{"using_method":"gist","elements":"x WITH =","initially":"deferred"}`,
			`{"using_method":"gist","elements":"x WITH =","initially":"later"}`,
			`{"using_method":"gist","elements":"x WITH =","deferrable":false}`,
			`{"using_method":"gist","elements":"x WITH =","where_condition":null}`,
			`{"using_method":"gist","elements":"x WITH =","comment":""}`,
			`{"using_method":"gist","elements":"x WITH =","requires":[]}`,
			`{"using_method":"gist","elements":"x WITH =","requires":[{}]}`,
			`{"using_method":"gist","elements":"x WITH =","requires":[{"kind":"extension","name":{"source":"x","normalized":"x","extra":1}}]}`,
			`{"using_method":"gist","elements":"x WITH =","requires":[{"kind":"extension","name":{"source":"x","normalized":"x","quoted":false}}]}`,
			`{"using_method":"gist","elements":"x WITH =","requires":[{"kind":"extension","name":{"source":"x","normalized":"x"},"schema":{}}]}`,
			`{"using_method":"gist","elements":"x WITH =","requires":[{"kind":"extension","name":{"source":"x","normalized":"x"}},{"kind":"extension","name":{"source":"x","normalized":"x"}}]}`,
		} {
			t.Run(string(codec.Representation)+wire, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(wire))
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(value, qt.IsNil)
			})
		}
	}
}

func TestExclusionCodecs_RepresentationLimits(t *testing.T) {
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		wire           string
	}{
		{"observation requires timing", schemaext.Observed, `{"using_method":"gist","elements":"x WITH =","deferrable":true}`},
		{"observation forbids redundant timing", schemaext.Observed, `{"using_method":"gist","elements":"x WITH =","initially":"immediate"}`},
		{"observation has no source struct", schemaext.Observed, `{"using_method":"gist","elements":"x WITH =","struct_name":"Room"}`},
		{"stale normalization", schemaext.Desired, `{"using_method":"gist","elements":"x WITH =","normalized":{"source_key":"wrong","elements":"x WITH ="}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := exclusionCodec(test.representation)
			value, err := codec.Decode(json.RawMessage(test.wire))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

func exclusionCodec(representation schemaext.Representation) schemaext.Codec {
	for _, codec := range pgtable.ExclusionCodecs() {
		if codec.Representation == representation {
			return codec
		}
	}
	panic("unknown exclusion representation")
}

func TestExclusionCloneAndCanonicalDependencies(t *testing.T) {
	c := qt.New(t)
	value := &pgtable.DesiredExclusion{Exclusion: exclusionDefinition(), StructName: "Room"}
	value.Dependencies = []objectidentity.ID{exclusionDependency("z"), exclusionDependency("a")}
	value.Normalized = &pgtable.NormalizedExclusion{SourceKey: pgtable.ExclusionComparisonKey(value.Exclusion), Elements: value.Elements}
	clone := value.Clone().(*pgtable.DesiredExclusion)
	c.Assert(clone.Equal(value), qt.IsTrue)
	clone.Dependencies[0] = exclusionDependency("changed")
	clone.Normalized.Elements = "different"
	c.Assert(value.Dependencies[0].Name.Source, qt.Equals, "z")
	c.Assert(value.Normalized.Elements, qt.Equals, value.Elements)
	reordered := value.Clone().(*pgtable.DesiredExclusion)
	reordered.Dependencies[0], reordered.Dependencies[1] = reordered.Dependencies[1], reordered.Dependencies[0]
	c.Assert(reordered.Equal(value), qt.IsTrue)
	codec := exclusionCodec(schemaext.Desired)
	before, err := codec.Encode(value)
	c.Assert(err, qt.IsNil)
	after, err := codec.Encode(reordered)
	c.Assert(err, qt.IsNil)
	c.Assert(string(before), qt.Equals, string(after))
	c.Assert(value.Dependencies[0].Name.Source, qt.Equals, "z")
}

func TestExclusionComparisonKey_EquivalentSpellings(t *testing.T) {
	for _, test := range []struct {
		name string
		a, b pgtable.Exclusion
	}{
		{"spacing", pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&"}, pgtable.Exclusion{UsingMethod: "gist", Elements: " room  WITH  = , during WITH && "}},
		{"unquoted names", pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH ="}, pgtable.Exclusion{UsingMethod: "GIST", Elements: "ROOM WITH ="}},
		{"default timing", pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =", Deferrable: true}, pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =", Deferrable: true, Initially: "immediate"}},
		{"comment", pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =", Comment: "first"}, pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =", Comment: "second"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtable.ExclusionComparisonKey(test.a), qt.Equals, pgtable.ExclusionComparisonKey(test.b))
		})
	}
}

func TestExclusionComparisonKey_DifferentDefinitions(t *testing.T) {
	for _, test := range []struct {
		name string
		a, b pgtable.Exclusion
	}{
		{"operator", pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH ="}, pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH <>"}},
		{"quoted name", pgtable.Exclusion{UsingMethod: "gist", Elements: `"Room" WITH =`}, pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH ="}},
		{"predicate literal", pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =", WhereCondition: "state = 'OPEN'"}, pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =", WhereCondition: "state = 'open'"}},
		{"initial timing", pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =", Deferrable: true}, pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =", Deferrable: true, Initially: "deferred"}},
		{"deferral", pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH ="}, pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =", Deferrable: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtable.ExclusionComparisonKey(test.a), qt.Not(qt.Equals), pgtable.ExclusionComparisonKey(test.b))
		})
	}
}

func TestExclusionConversion_DefaultsAndIsolation(t *testing.T) {
	for _, test := range []struct {
		name            string
		deferrable      bool
		initially, want string
	}{
		{"not deferrable", false, "", ""}, {"immediate default", true, "", "immediate"}, {"deferred", true, "deferred", "deferred"}, {"redundant immediate", false, "immediate", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declaration := &pgtable.DesiredExclusion{Exclusion: exclusionDefinition(), StructName: "Room"}
			declaration.Deferrable, declaration.Initially = test.deferrable, test.initially
			declaration.Dependencies = []objectidentity.ID{exclusionDependency("btree_gist")}
			declaration.Normalized = &pgtable.NormalizedExclusion{SourceKey: pgtable.ExclusionComparisonKey(declaration.Exclusion), Elements: declaration.Elements}
			service := pgtable.ExclusionService{}
			got, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{declaration}})
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, 1)
			observed := got[0].(*pgtable.ObservedExclusion)
			c.Assert(observed.Initially, qt.Equals, test.want)
			c.Assert(pgtable.ValidateObservedExclusion(observed), qt.IsNil)
			restored, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Desired, Values: got})
			c.Assert(err, qt.IsNil)
			declared := restored[0].(*pgtable.DesiredExclusion)
			c.Assert(declared.StructName, qt.Equals, "")
			c.Assert(declared.Normalized, qt.IsNil)
			observed.Dependencies[0] = exclusionDependency("changed")
			c.Assert(declaration.Dependencies[0].Name.Source, qt.Equals, "btree_gist")
			c.Assert(declared.Dependencies[0].Name.Source, qt.Equals, "btree_gist")
		})
	}
}

func TestExclusionConversion_RefusesPartialBatch(t *testing.T) {
	c := qt.New(t)
	got, err := (pgtable.ExclusionService{}).ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{&pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}, &pgtable.DesiredExclusion{}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(got, qt.IsNil)
}

func TestExclusionConversion_UnsupportedAndCanceled(t *testing.T) {
	c := qt.New(t)
	service := pgtable.ExclusionService{}
	got, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "unknown", From: schemaext.Desired, To: schemaext.Observed})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(got, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err = service.ConvertFeatures(ctx, schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(got, qt.IsNil)
}

func TestExclusionRef_QuotedScope(t *testing.T) {
	c := qt.New(t)
	ref := pgtable.ExclusionRef(`"App.One"."Room.Two"`, `"No.Overlap"`, identifier.ForDialect("postgres"))
	c.Assert(ref.Kind, qt.Equals, objectidentity.Kind(pgtable.ExclusionKind))
	c.Assert(ref.Schema.Normalized, qt.Equals, `"App.One"`)
	c.Assert(ref.Parent.Normalized, qt.Equals, `"Room.Two"`)
	c.Assert(ref.Name.Normalized, qt.Equals, `"No.Overlap"`)
	c.Assert(ref.Name.Quoted, qt.IsTrue)
}

func TestExclusionCoverage_PreservesLimits(t *testing.T) {
	c := qt.New(t)
	ref := pgtable.ExclusionRef("rooms", "no_overlap", identifier.ForDialect("postgres"))
	records := []schemaext.SubjectCoverage{{Kind: pgtable.ExclusionKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unread operator"}}}
	coverage, err := pgtable.ExclusionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, records)
	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(pgtable.ExclusionKind, ref), qt.DeepEquals, records[0].Knowledge)
	c.Assert(coverage.Lookup(pgtable.ExclusionKind, pgtable.ExclusionRef("rooms", "other", identifier.ForDialect("postgres"))).State, qt.Equals, schemaext.Complete)
	var owned []schemaext.OwnedCodec
	for _, codec := range pgtable.ExclusionCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: pgtable.Owner, Codec: codec})
	}
	registry := must.Must(schemaext.NewRegistry(owned...))
	wire, err := registry.EncodeCoverage(t.Context(), schemaext.Observed, coverage)
	c.Assert(err, qt.IsNil)
	c.Assert(string(must.Must(json.Marshal(wire))), qt.Contains, "unread operator")
}
