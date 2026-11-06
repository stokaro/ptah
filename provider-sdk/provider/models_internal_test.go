package provider

// White-box testing required: a definition validated in other bytes is
// accepted either way, so only the set of checked definitions shows whether
// it was checked again.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// A definition validated once is not checked again when it arrives in other
// bytes, as it does from a provider that builds its description for each
// call: the set holds one copy per digest. Keyed by the address of the bytes,
// the set filled up and every later validation hashed each model again.
func TestModelDefinitionIsCheckedOncePerDigest(t *testing.T) {
	c := qt.New(t)
	model := &protocol.ValueModel{Kind: "fixture.items", Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "application/json",
		CanonicalFormat: "fixture.items.v1", DefinitionJson: []byte(`{"type":"object","verified-once":true}`)}
	sum := sha256.Sum256(model.DefinitionJson)
	model.DefinitionSha256 = hex.EncodeToString(sum[:])
	c.Assert(validateModelDefinition(model), qt.IsNil)
	verifiedDefinitions.RLock()
	before := len(verifiedDefinitions.held)
	verifiedDefinitions.RUnlock()

	for range 3 * maxCanonicalDefinitions {
		copied := proto.CloneOf(model)
		c.Assert(validateModelDefinition(copied), qt.IsNil)
	}

	verifiedDefinitions.RLock()
	defer verifiedDefinitions.RUnlock()
	c.Assert(verifiedDefinitions.held, qt.HasLen, before)
	c.Assert(bytes.Equal(verifiedDefinitions.held[model.DefinitionSha256], model.DefinitionJson), qt.IsTrue)
}
