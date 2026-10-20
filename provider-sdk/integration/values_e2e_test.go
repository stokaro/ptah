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

func TestIndependentProviderNormalizesValuesWithoutSession(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "provider")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	client := start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.NormalizeValuesFeature}})
	definition := client.Description().GetEngines()[0].GetValueModels()[0].GetDefinitionSha256()
	request := &protocol.NormalizeValuesRequest{Engine: "fixture", Values: []*protocol.ValueInput{
		retentionInput(definition, "seconds", `{"seconds":60}`), retentionInput(definition, "minutes", `{"minutes":1}`),
		retentionInput(definition, "default", `{}`), retentionInput(definition, "zero", `{"seconds":0}`),
		retentionInput(definition, "exact", `{"seconds":9007199254740993}`),
	}}
	original := proto.CloneOf(request)
	result, err := client.NormalizeValues(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Values, qt.HasLen, 5)
	c.Assert(proto.Equal(original, request), qt.IsTrue)
	c.Assert(result.Values[0].CanonicalProjection, qt.DeepEquals, result.Values[1].CanonicalProjection)
	c.Assert(result.Values[0].CanonicalProjection, qt.DeepEquals, result.Values[2].CanonicalProjection)
	c.Assert(string(result.Values[3].CanonicalProjection), qt.Equals, `{"seconds":0}`)
	c.Assert(string(result.Values[4].CanonicalProjection), qt.Equals, `{"seconds":9007199254740993}`)
	for i, value := range result.Values {
		c.Assert(proto.Equal(value.Value.Owner, request.Values[i].Value.Owner), qt.IsTrue)
		c.Assert(proto.Equal(value.NormalizedOwner, request.Values[i].Value.Owner), qt.IsTrue)
	}
	again, err := client.NormalizeValues(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, again), qt.IsTrue)

	for _, payload := range []string{
		`{"seconds":null}`, `{"seconds":-1}`, `{"seconds":1.5}`, `{"seconds":18446744073709551616}`,
		`{"minutes":18446744073709551615}`, `{"seconds":1,"seconds":1}`, `{"seconds":1,"unknown":2}`,
		`{"minutes":1,"seconds":60}`, `{"seconds":1} {}`, `[]`, `{"seconds":`,
	} {
		t.Run(payload, func(t *testing.T) {
			c := qt.New(t)
			refused, err := client.NormalizeValues(c.Context(), &protocol.NormalizeValuesRequest{Engine: "fixture", Values: []*protocol.ValueInput{
				retentionInput(definition, "valid-prefix", `{"seconds":60}`), retentionInput(definition, "bad", payload),
			}})
			c.Assert(err, qt.IsNotNil)
			c.Assert(refused, qt.IsNil)
		})
	}
	observed := retentionInput(definition, "incomplete", `{}`)
	observed.Representation = protocol.ValueInput_OBSERVED
	refused, err := client.NormalizeValues(c.Context(), &protocol.NormalizeValuesRequest{Engine: "fixture", Values: []*protocol.ValueInput{observed}})
	c.Assert(err, qt.IsNotNil)
	c.Assert(refused, qt.IsNil)
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	refused, err = client.NormalizeValues(canceled, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(refused, qt.IsNil)
}

func retentionInput(definition, id, payload string) *protocol.ValueInput {
	return &protocol.ValueInput{Id: id, Representation: protocol.ValueInput_DESIRED, Value: &protocol.Envelope{
		DefinitionSha256: definition, ProviderId: "example.org/fixture", Kind: "example.org/fixture/retention", SchemaVersion: 1, Encoding: "json", Payload: []byte(payload),
		Owner: &protocol.Reference{Engine: "fixture", Database: "db", Catalog: "catalog", Schema: "a.b", Path: []string{"a/b", "c"}, Kind: "table", Name: "C", Overload: []string{"integer"}, DisplayName: "Original"},
	}}
}
