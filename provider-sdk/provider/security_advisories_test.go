package provider_test

import (
	"bytes"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func advisoryDescription() *protocol.Description {
	description := dataDescription()
	description.Features = append(description.Features, provider.VulnerabilityScanFeature, provider.SecurityAdvisorySnapshotFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.VulnerabilityScanFeature, provider.SecurityAdvisorySnapshotFeature)
	engine.AdvisorySnapshotJson = []byte(`{"schema_version":2,"product":"rows"}`)
	return description
}

func TestSecurityAdvisorySnapshotNegotiation(t *testing.T) {
	c := qt.New(t)
	description := advisoryDescription()
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	old := provider.NegotiatedDescription(description, nil)
	c.Assert(old.Engines[0].GetAdvisorySnapshotJson(), qt.HasLen, 0)
	c.Assert(provider.ValidateDescription(old, nil), qt.IsNil)
	negotiated := provider.NegotiatedDescription(description, []string{provider.SecurityAdvisorySnapshotFeature})
	c.Assert(negotiated.Engines[0].GetAdvisorySnapshotJson(), qt.DeepEquals, description.Engines[0].GetAdvisorySnapshotJson())
	c.Assert(proto.Equal(description, advisoryDescription()), qt.IsTrue)
}

func TestSecurityAdvisorySnapshotRejectsIncompleteClaims(t *testing.T) {
	tests := []struct {
		name   string
		change func(*protocol.Description)
	}{
		{"provider does not advertise", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.SecurityAdvisorySnapshotFeature })
		}},
		{"engine cannot scan", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.VulnerabilityScanFeature })
		}},
		{"missing data", func(d *protocol.Description) { d.Engines[0].AdvisorySnapshotJson = nil }},
		{"invalid JSON", func(d *protocol.Description) { d.Engines[0].AdvisorySnapshotJson = []byte("{") }},
		{"oversized data", func(d *protocol.Description) {
			d.Engines[0].AdvisorySnapshotJson = append([]byte(`{"x":"`), append(bytes.Repeat([]byte("a"), 1<<20), []byte(`"}`)...)...)
		}},
		{"data without feature", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.SecurityAdvisorySnapshotFeature })
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			description := advisoryDescription()
			test.change(description)
			qt.New(t).Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
