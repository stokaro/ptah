package providerhost

// White-box testing required: the provider's private inspection adapter is
// where a whole-kind coverage limit becomes a reportable omitted kind.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	protocol "ptah.run/provider-sdk/protocol/experimental"
)

func TestNotDescribedReportsProviderOnlyKinds(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{}
	notDescribed(database, &protocol.Schema{NotDescribed: []*protocol.SchemaCoverageLimit{
		{Kind: "change_stream", Reason: "unsupported", Provenance: "derived-from-target"},
		{Kind: "role", Reason: "not-inspected", Provenance: "observed"},
	}})

	c.Assert(database.NotDescribed.Objects, qt.HasLen, 2)
	c.Assert(database.NotDescribed.Describes(coverage.Kind("change_stream")), qt.IsFalse)
	c.Assert(database.UnmodeledObjects, qt.DeepEquals, []catalog.UnmodeledObject{{
		Kind: "change stream", Reason: "the provider does not describe this object kind",
	}})
}
