package providerhost

// White-box testing required: the Go encoder turns the provider's parts into
// the writer's directives in private functions, and the fixture provider
// writes no directive of the document; where each placement lands is only
// observable here.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
)

// documentFragment is a provider's own Go directive of the document with one
// text attribute.
func documentFragment() *protocol.SourceFragment {
	return &protocol.SourceFragment{
		GoDirective: &protocol.GoSourceDirective{Scope: protocol.GoSourceDirective_DOCUMENT, Directive: "ptah:fixture:pool", Selector: "pool"},
		Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Value: &protocol.SourceValue_Mapping{Mapping: &protocol.SourceMapping{
			Entries: []*protocol.SourceEntry{{Key: "name", Value: &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: "batch"}}}}}}}},
	}
}

// TestGoEncodingPlacesADirectiveOfTheDocument writes a directive in the group
// its part names, and after the views when it names none: without the
// placement the built-in YDB engine's pools and coordination nodes would stand
// before the grants it wrote them after.
func TestGoEncodingPlacesADirectiveOfTheDocument(t *testing.T) {
	for _, row := range []struct {
		placement protocol.EncodedFragment_Placement
		want      annotation.Placement
	}{
		{protocol.EncodedFragment_PLACEMENT_UNSPECIFIED, annotation.AfterViews},
		{protocol.EncodedFragment_GO_AFTER_SCHEMAS, annotation.AfterSchemas},
		{protocol.EncodedFragment_GO_AFTER_VIEWS, annotation.AfterViews},
		{protocol.EncodedFragment_GO_AFTER_PRIVILEGES, annotation.AfterPrivileges},
	} {
		t.Run(row.placement.String(), func(t *testing.T) {
			c := qt.New(t)
			encoding := &goEncoding{}

			err := encoding.fragment(documentFragment(), row.placement)

			c.Assert(err, qt.IsNil)
			c.Assert(encoding.result.Directives, qt.DeepEquals, []annotation.EncodedDirective{{Directive: "ptah:fixture:pool", Placement: row.want,
				Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "batch"}}}})
		})
	}
}

// TestGoEncodingRefusesAPlacementItHasNoGroupFor keeps an HCL placement, which
// the SDK refuses on a Go part, from landing in a default group here.
func TestGoEncodingRefusesAPlacementItHasNoGroupFor(t *testing.T) {
	c := qt.New(t)
	encoding := &goEncoding{}

	err := encoding.fragment(documentFragment(), protocol.EncodedFragment_AFTER_TABLES)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*the provider placed its DOCUMENT directive ptah:fixture:pool, which the Go writer has no place for`)
	c.Assert(encoding.result.Directives, qt.HasLen, 0)
}
