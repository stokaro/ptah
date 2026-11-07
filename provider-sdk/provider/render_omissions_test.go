package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// typeModifierOmission is what a render reports for a declared length its
// engine's type does not keep.
func typeModifierOmission() *protocol.RenderOmission {
	return &protocol.RenderOmission{Reason: "unsupported", Kind: "column", Name: "accounts.email", Property: "type modifier", Detail: "VARCHAR(255): length 255"}
}

// omissionExchange is a render under negotiated omissions: the provider and
// the engine list the feature, the request names it among the target's
// features, and the complete reply reports omissions beside its units.
func omissionExchange(omissions ...*protocol.RenderOmission) (*protocol.Description, *protocol.RenderRequest, *protocol.RenderResult) {
	d, request := operationRender()
	d.Features = append(d.Features, provider.RenderOmissionsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.RenderOmissionsFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.RenderOmissionsFeature)
	result := &protocol.RenderResult{Complete: true, Omissions: omissions}
	for _, change := range request.Changes {
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.Id, Body: &protocol.Unit_Sql{Sql: "CREATE TABLE accounts (email Utf8);"}})
	}
	return d, request, result
}

func TestRenderOmissions_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		omissions []*protocol.RenderOmission
	}{
		{"no omission", nil},
		{"a lost property", []*protocol.RenderOmission{typeModifierOmission()}},
		{"a lost object with a remedy", []*protocol.RenderOmission{{Reason: "unsupported", Kind: "trigger", Name: "audit", Remedy: "declare it in a migration"}}},
		{"two omissions of one column", []*protocol.RenderOmission{typeModifierOmission(),
			{Reason: "unsupported", Kind: "column", Name: "accounts.email", Property: "collation", Detail: "NOCASE"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := omissionExchange(test.omissions...)

			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateRenderedBatch(d, request, result), qt.IsNil)
		})
	}
}

// Each omission names why, what kind of object and which one, and every field
// is one line; a reason is a lower-case token, and one omission is reported
// once.
func TestRenderOmission_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name      string
		omissions []*protocol.RenderOmission
		want      string
	}{
		{"no reason", []*protocol.RenderOmission{{Kind: "column", Name: "accounts.email"}}, `.*an omission's reason is a lower-case token`},
		{"an upper-case reason", []*protocol.RenderOmission{{Reason: "Unsupported", Kind: "column", Name: "accounts.email"}}, `.*an omission's reason is a lower-case token`},
		{"a reason of two words", []*protocol.RenderOmission{{Reason: "not supported", Kind: "column", Name: "accounts.email"}}, `.*an omission's reason is a lower-case token`},
		{"a reason opening with a digit", []*protocol.RenderOmission{{Reason: "2big", Kind: "column", Name: "accounts.email"}}, `.*an omission's reason is a lower-case token`},
		{"no kind", []*protocol.RenderOmission{{Reason: "unsupported", Name: "accounts.email"}}, `.*an omission names the kind and the name of its object`},
		{"no name", []*protocol.RenderOmission{{Reason: "unsupported", Kind: "column"}}, `.*an omission names the kind and the name of its object`},
		{"a name over two lines", []*protocol.RenderOmission{{Reason: "unsupported", Kind: "column", Name: "accounts.\nemail"}},
			`.*an omission names the kind and the name of its object`},
		{"a detail over two lines", []*protocol.RenderOmission{{Reason: "unsupported", Kind: "column", Name: "accounts.email", Detail: "VARCHAR(255):\nlength 255"}},
			`.*an omission's property, detail and remedy are each one line of text`},
		{"a control character in the remedy", []*protocol.RenderOmission{{Reason: "unsupported", Kind: "column", Name: "accounts.email", Remedy: "use\x07Utf8"}},
			`.*an omission's property, detail and remedy are each one line of text`},
		{"a property with surrounding white space", []*protocol.RenderOmission{{Reason: "unsupported", Kind: "column", Name: "accounts.email", Property: " type modifier"}},
			`.*an omission's property, detail and remedy are each one line of text`},
		{"one omission twice", []*protocol.RenderOmission{typeModifierOmission(), typeModifierOmission()}, `.*a render reports one omission twice`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := omissionExchange(test.omissions...)

			err := provider.ValidateRenderedBatch(d, request, result)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// Omissions travel only in a complete render under the negotiated feature:
// a provider or a target that does not list it, an incomplete result and a
// refusal carry none.
func TestRenderOmissionsReply_FailurePath(t *testing.T) {
	t.Run("a target that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := omissionExchange(typeModifierOmission())
		request.TargetFeatures = slices.DeleteFunc(request.TargetFeatures, func(feature string) bool { return feature == provider.RenderOmissionsFeature })

		err := provider.ValidateRenderedBatch(d, request, result)

		c.Assert(err, qt.ErrorMatches, `.*render.omissions was not negotiated`)
	})
	t.Run("a host that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := omissionExchange(typeModifierOmission())
		negotiated := provider.NegotiatedDescription(d, slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool {
			return feature == provider.RenderOmissionsFeature
		}))
		// Such a host names the target's features as it negotiated them.
		request.TargetFeatures = negotiated.Engines[0].Features

		err := provider.ValidateRenderedBatch(negotiated, request, result)

		c.Assert(err, qt.ErrorMatches, `.*render.omissions was not negotiated`)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.RenderOmissionsFeature)
	})
	t.Run("an incomplete render", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := omissionExchange(typeModifierOmission())
		result.Complete = false

		err := provider.ValidateRenderedBatch(d, request, result)

		c.Assert(err, qt.ErrorMatches, `.*only a complete render reports omissions`)
	})
	t.Run("a refusal of a change without a text form", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := omissionExchange()
		d.Features = append(d.Features, provider.TextUnitsFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.TextUnitsFeature)
		request.Form = protocol.RenderRequest_TEXT
		result := &protocol.RenderResult{Omissions: []*protocol.RenderOmission{typeModifierOmission()},
			Refusal: &protocol.RenderRefusal{Reason: protocol.RenderRefusal_NO_TEXT_FORM, ChangeId: request.Changes[0].Id, Message: "native only"}}

		err := provider.ValidateRenderedBatch(d, request, result)

		c.Assert(err, qt.ErrorMatches, `.*only a complete render reports omissions`)
	})
}

// The engine's feature needs the provider's, and a provider that renders.
func TestRenderOmissionsDescription_FailurePath(t *testing.T) {
	t.Run("no provider feature", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := omissionExchange()
		d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.RenderOmissionsFeature })

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*render omissions require the provider's feature and rendering`)
	})
	t.Run("a provider that does not render", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := omissionExchange()
		d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.RenderFeature })

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*render omissions require the provider's feature and rendering`)
	})
}

// A row write render reports no omission: only Render does.
func TestRowWriteOmissions_FailurePath(t *testing.T) {
	c := qt.New(t)
	request, result := rowWritesExchange()
	result.Omissions = []*protocol.RenderOmission{typeModifierOmission()}

	err := provider.ValidateRenderedRowWrites(dataDescription(), request, result)

	c.Assert(err, qt.ErrorMatches, `.*a row write render reports no omissions; only Render does`)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}
