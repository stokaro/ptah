//go:build integration

package providercli_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/providerhost"
)

func TestHostResolvesProviderModelsInSharedRegistry(t *testing.T) {
	c := qt.New(t)
	executable := independentCLIProvider(c)
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	host, err := providerhost.Start(c.Context(), providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"})
	c.Assert(err, qt.IsNil)
	t.Cleanup(host.Close)
	descriptor := host.Provider()
	runtime, err := engine.New(descriptor)
	c.Assert(err, qt.IsNil)
	registry := runtime.Codecs()
	definitions := registry.Definitions()
	c.Assert(definitions, qt.HasLen, 2)
	desired, observed := definitions[0], definitions[1]
	c.Assert(desired.Representation, qt.Equals, schemaext.Desired)
	c.Assert(observed.Representation, qt.Equals, schemaext.Observed)
	inputs := []schemaext.Envelope{
		providerModelEnvelope(c, desired, `{"minutes":1}`), providerModelEnvelope(c, desired, `{"seconds":60}`),
		providerModelEnvelope(c, desired, `{}`), providerModelEnvelope(c, desired, `{"seconds":9007199254740993}`),
		providerModelEnvelope(c, observed, `{"seconds":60}`),
	}
	frozen, err := json.Marshal(inputs)
	c.Assert(err, qt.IsNil)
	values, err := registry.Decode(c.Context(), inputs)
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 5)
	after, err := json.Marshal(inputs)
	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.DeepEquals, frozen)
	equal, err := schemaext.EqualValues(values[0].(schemaext.Value), values[1].(schemaext.Value))
	c.Assert(err, qt.IsNil)
	c.Assert(equal, qt.IsTrue)
	equal, err = schemaext.EqualValues(values[0].(schemaext.Value), values[2].(schemaext.Value))
	c.Assert(err, qt.IsNil)
	c.Assert(equal, qt.IsTrue)
	equal, err = schemaext.EqualValues(values[0].(schemaext.Value), values[4].(schemaext.Value))
	c.Assert(err, qt.IsNil)
	c.Assert(equal, qt.IsFalse)
	minutes, err := registry.Fingerprint(c.Context(), schemaext.Desired, []schemaext.Payload{values[0]})
	c.Assert(err, qt.IsNil)
	seconds, err := registry.Fingerprint(c.Context(), schemaext.Desired, []schemaext.Payload{values[1]})
	c.Assert(err, qt.IsNil)
	c.Assert(minutes, qt.Equals, seconds)
	encoded, err := registry.Encode(c.Context(), schemaext.Desired, values[:4])
	c.Assert(err, qt.IsNil)
	var record struct {
		Payload []byte `json:"payload"`
	}
	c.Assert(json.Unmarshal(encoded[3].Payload, &record), qt.IsNil)
	c.Assert(string(record.Payload), qt.Equals, `{"seconds":9007199254740993}`)
	roundtrip, err := registry.Decode(c.Context(), encoded)
	c.Assert(err, qt.IsNil)
	equal, err = schemaext.EqualValues(roundtrip[0].(schemaext.Value), values[0].(schemaext.Value))
	c.Assert(err, qt.IsNil)
	c.Assert(equal, qt.IsTrue)
	for _, payload := range []string{`{"seconds":null}`, `{"seconds":60,"unknown":false}`, `{"seconds":18446744073709551616}`} {
		t.Run(payload, func(t *testing.T) {
			c := qt.New(t)
			result, err := registry.Decode(c.Context(), []schemaext.Envelope{inputs[0], providerModelEnvelope(c, desired, payload)})
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.IsNil)
		})
	}
	incomplete, err := registry.Decode(c.Context(), []schemaext.Envelope{providerModelEnvelope(c, observed, `{}`)})
	c.Assert(err, qt.IsNotNil)
	c.Assert(incomplete, qt.IsNil)
	// Once resolved, copying, encoding, and fingerprinting need no provider.
	host.Close()
	copied, err := schemaext.CloneValue(values[0].(schemaext.Value))
	c.Assert(err, qt.IsNil)
	equal, err = schemaext.EqualValues(copied, values[1].(schemaext.Value))
	c.Assert(err, qt.IsNil)
	c.Assert(equal, qt.IsTrue)
	offline, err := registry.Fingerprint(c.Context(), schemaext.Desired, []schemaext.Payload{copied})
	c.Assert(err, qt.IsNil)
	c.Assert(offline, qt.Equals, minutes)
	_, err = registry.Encode(c.Context(), schemaext.Desired, []schemaext.Payload{copied})
	c.Assert(err, qt.IsNil)
	unresolved, err := registry.Decode(c.Context(), encoded)
	c.Assert(err, qt.IsNotNil)
	c.Assert(unresolved, qt.IsNil)
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	unresolved, err = registry.Decode(canceled, inputs)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(unresolved, qt.IsNil)
}

func providerModelEnvelope(c *qt.C, definition schemaext.CodecIdentity, payload string) schemaext.Envelope {
	c.Helper()
	wire := map[string]any{"format": 1, "engine": "fixture", "features": make([]string, 0), "required_features": make([]string, 0), "payload": []byte(payload), "subject": map[string]any{"engine": "fixture", "kind": "table", "name": "records", "schema": "a.b", "path": []string{"c/d", "e"}}}
	data, err := json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	return schemaext.Envelope{Format: 1, Owner: definition.Owner, Kind: definition.Kind, Representation: definition.Representation, Version: definition.Version, Definition: definition.Definition, Payload: data}
}
