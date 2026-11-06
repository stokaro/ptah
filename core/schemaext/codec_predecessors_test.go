package schemaext_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// formerKind is the kind a former owner gave the widget model.
const formerKind schemaext.Kind = "example.org/builtin/widget"

// formerRegistry is the former owner's registration of the widget model, and
// formerIdentity the identity its documents name it under.
func formerRegistry() schemaext.Registry {
	return must.Must(schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "example.org/builtin", Codec: widgetCodec(formerKind, schemaext.Desired)}))
}

func formerIdentity() schemaext.CodecIdentity { return formerRegistry().Definitions()[0] }

// succeedingRegistry owns the widget model under its current identity and
// names the former owner's identity as its predecessor.
func succeedingRegistry() schemaext.Registry {
	return must.Must(schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "example.org/provider", Codec: widgetCodec(widgetKind, schemaext.Desired),
		Predecessors: []schemaext.CodecIdentity{formerIdentity()}}))
}

// formerRef is the subject a former owner's document names: the widget
// events of records, of the former kind.
func formerRef() objectidentity.ID {
	ref := widgetRef("records", "events")
	ref.Kind = objectidentity.Kind(formerKind)
	return ref
}

// formerHeader is the header the former owner wrote: every widget known, but
// the events of records not described.
func formerHeader() string {
	registry := formerRegistry()
	known := must.Must(schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{
		Model: registry.Definitions()[0], Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete}),
	}}, []schemaext.SubjectCoverage{{Kind: formerKind, Subject: formerRef(), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "left unmanaged"}}}))
	return must.Must(registry.EncodeCoverageHeader(context.Background(), schemaext.Desired, known))
}

// A document captured under a predecessor makes the same claim of the
// current model: the kind is known completely, the subject the document
// left undescribed stays undescribed under the current kind, and the claim is
// written again under the current identity.
func TestCoverageHeaderUnderAPredecessor_HappyPath(t *testing.T) {
	c := qt.New(t)
	registry := succeedingRegistry()

	decoded, found, err := registry.DecodeCoverageHeader(t.Context(), schemaext.Desired, "// "+formerHeader()+"\n")

	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(decoded.KindRecords(), qt.HasLen, 1)
	c.Assert(decoded.KindRecords()[0].Model, qt.Equals, registry.Definitions()[0])
	c.Assert(decoded.Lookup(widgetKind, widgetRef("records", "events")), qt.Equals, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "left unmanaged"})
	c.Assert(decoded.Lookup(widgetKind, widgetRef("records", "other")).State, qt.Equals, schemaext.Complete)
	restated, err := registry.EncodeCoverageHeader(t.Context(), schemaext.Desired, decoded)
	c.Assert(err, qt.IsNil)
	c.Assert(restated, qt.Contains, `"kind":"`+string(widgetKind)+`"`)
	c.Assert(restated, qt.Not(qt.Contains), string(formerKind))
}

// A document is read under a predecessor only when it names that exact
// identity, once.
func TestCoverageHeaderUnderAPredecessor_FailurePath(t *testing.T) {
	header := formerHeader()
	current := succeedingRegistry().Definitions()[0]
	both := strings.Replace(header, `"kinds":[`, `"kinds":[{"model":{"owner":"`+current.Owner+`","kind":"`+string(current.Kind)+
		`","representation":"desired","version":1,"definition":"`+current.Definition+`"},"knowledge":[{"outside":{"state":"complete"}}]},`, 1)
	for _, test := range []struct {
		name, source, want string
	}{
		{"both identities", both, `duplicate feature identity: coverage for kind "example\.org/widget"`},
		{"another version", strings.Replace(header, `"version":1`, `"version":2`, 1), `unknown feature codec: "example\.org/builtin/widget"/desired`},
		{"another owner", strings.Replace(header, `"owner":"example.org/builtin"`, `"owner":"example.org/elsewhere"`, 1),
			`unknown feature codec: "example\.org/builtin/widget"/desired`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, found, err := succeedingRegistry().DecodeCoverageHeader(t.Context(), schemaext.Desired, "// "+test.source+"\n")
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(found, qt.IsFalse)
			c.Assert(decoded.IsZero(), qt.IsTrue)
		})
	}
}

func TestRegistryPredecessors_HappyPath(t *testing.T) {
	c := qt.New(t)
	earlier := formerIdentity()
	earlier.Version = 2
	registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "example.org/provider", Codec: widgetCodec(widgetKind, schemaext.Desired),
		Predecessors: []schemaext.CodecIdentity{formerIdentity(), earlier}})
	c.Assert(err, qt.IsNil)
	c.Assert(registry.Definitions(), qt.HasLen, 1)
}

func TestRegistryPredecessors_FailurePath(t *testing.T) {
	current := must.Must(schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))).Definitions()[0]
	with := func(change func(*schemaext.CodecIdentity)) []schemaext.CodecIdentity {
		identity := formerIdentity()
		change(&identity)
		return []schemaext.CodecIdentity{identity}
	}
	for _, test := range []struct {
		name         string
		predecessors []schemaext.CodecIdentity
		others       []schemaext.OwnedCodec
		want         string
	}{
		{"another representation", with(func(i *schemaext.CodecIdentity) { i.Representation = schemaext.Observed }), nil, `.*needs an owner, a kind.*`},
		{"no version", with(func(i *schemaext.CodecIdentity) { i.Version = 0 }), nil, `.*needs an owner, a kind.*`},
		{"an owner that is not namespaced", with(func(i *schemaext.CodecIdentity) { i.Owner = "builtin" }), nil, `.*needs an owner, a kind.*`},
		{"a kind that is not namespaced", with(func(i *schemaext.CodecIdentity) { i.Kind = "widget" }), nil, `.*needs an owner, a kind.*`},
		{"a digest without its prefix", with(func(i *schemaext.CodecIdentity) { i.Definition = strings.TrimPrefix(i.Definition, "sha256:") }), nil,
			`.*needs an owner, a kind.*`},
		{"an uppercase digest", with(func(i *schemaext.CodecIdentity) { i.Definition = strings.ToUpper(i.Definition) }), nil, `.*needs an owner, a kind.*`},
		{"the current identity", []schemaext.CodecIdentity{current}, nil, `.*is its current identity`},
		{"a kind another codec owns", with(func(i *schemaext.CodecIdentity) { i.Kind = otherKind }),
			[]schemaext.OwnedCodec{owned(widgetCodec(otherKind, schemaext.Desired))}, `.*names "example\.org/other", which another codec owns`},
		{"a predecessor of two codecs", []schemaext.CodecIdentity{formerIdentity()},
			[]schemaext.OwnedCodec{{Owner: "example.org/provider", Codec: widgetCodec(otherKind, schemaext.Desired), Predecessors: []schemaext.CodecIdentity{formerIdentity()}}},
			`.*predecessor "example\.org/builtin/widget"/desired is named by .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registrations := append([]schemaext.OwnedCodec{{Owner: "example.org/provider", Codec: widgetCodec(widgetKind, schemaext.Desired),
				Predecessors: test.predecessors}}, test.others...)
			registry, err := schemaext.NewRegistry(registrations...)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(registry.Definitions(), qt.HasLen, 0)
		})
	}
}
