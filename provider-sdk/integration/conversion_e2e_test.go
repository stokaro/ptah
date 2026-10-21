//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func TestIndependentProviderConvertsWithoutSession(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "provider")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	client := start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.ConvertValuesFeature}})
	definition := client.Description().Engines[0].ValueModels[0].DefinitionSha256
	inputs := &protocol.NormalizeValuesRequest{Engine: "fixture", Values: []*protocol.ValueInput{
		retentionInput(definition, "default", `{}`), retentionInput(definition, "exact", `{"seconds":9007199254740993}`), retentionInput(definition, "zero", `{"seconds":0}`),
	}}
	source, err := client.NormalizeValues(c.Context(), inputs)
	c.Assert(err, qt.IsNil)
	expected := proto.CloneOf(source)
	request := &protocol.ConvertValuesRequest{Engine: "fixture", From: protocol.ValueInput_DESIRED, To: protocol.ValueInput_OBSERVED, Values: source.Values}
	// Captured projections cannot make an unvalidated payload authoritative.
	request.Values[0].Value.Payload = []byte(`{}`)
	request.Values[0].CanonicalProjection = []byte(`{"seconds":999}`)
	original := proto.CloneOf(request)
	observed, err := client.ConvertValues(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
	c.Assert(observed.Values, qt.HasLen, 3)
	c.Assert(string(observed.Values[0].Value.Payload), qt.Equals, `{"seconds":60}`)
	c.Assert(string(observed.Values[1].Value.Payload), qt.Equals, `{"seconds":9007199254740993}`)
	c.Assert(string(observed.Values[2].Value.Payload), qt.Equals, `{"seconds":0}`)
	reverse := &protocol.ConvertValuesRequest{Engine: "fixture", From: protocol.ValueInput_OBSERVED, To: protocol.ValueInput_DESIRED, Values: observed.Values}
	desired, err := client.ConvertValues(c.Context(), reverse)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(desired, expected), qt.IsTrue)
	c.Assert(string(desired.Values[0].CanonicalProjection), qt.Equals, `{"seconds":60}`)
	for i, value := range desired.Values {
		c.Assert(proto.Equal(value.Value.Owner, inputs.Values[i].Value.Owner), qt.IsTrue)
		c.Assert(proto.Equal(value.NormalizedOwner, inputs.Values[i].Value.Owner), qt.IsTrue)
	}
	for _, payload := range []string{`{}`, `{"seconds":null}`, `{"seconds":1,"unknown":2}`} {
		t.Run(payload, func(t *testing.T) {
			c := qt.New(t)
			malformed := proto.CloneOf(reverse)
			malformed.Values[1].Value.Payload = []byte(payload)
			refused, err := client.ConvertValues(c.Context(), malformed)
			c.Assert(err, qt.IsNotNil)
			c.Assert(refused, qt.IsNil)
		})
	}
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	refused, err := client.ConvertValues(canceled, reverse)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(refused, qt.IsNil)
}
