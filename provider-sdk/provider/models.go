package provider

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validateValueModels(models []*protocol.ValueModel) error {
	type identity struct {
		kind           string
		representation protocol.ValueInput_Representation
	}
	seen := make(map[identity]bool, len(models))
	for _, model := range models {
		if model == nil || strings.TrimSpace(model.GetKind()) == "" || model.GetSchemaVersion() == 0 ||
			strings.TrimSpace(model.GetEncoding()) == "" || strings.TrimSpace(model.GetCanonicalFormat()) == "" {
			return fmt.Errorf("%w: incomplete value model definition", ErrProtocol)
		}
		switch model.GetRepresentation() {
		case protocol.ValueInput_DESIRED, protocol.ValueInput_OBSERVED, protocol.ValueInput_OPERATION, protocol.ValueInput_CHANGE:
		default:
			return fmt.Errorf("%w: unknown model representation", ErrProtocol)
		}
		if err := validateModelDisplayName(model); err != nil {
			return err
		}
		if err := validateModelSourceFormats(model); err != nil {
			return err
		}
		if !lowerHexDigest(model.GetDefinitionSha256()) {
			return fmt.Errorf("%w: model definition requires a lowercase SHA-256 digest", ErrProtocol)
		}
		if err := validateModelDefinition(model); err != nil {
			return err
		}
		key := identity{kind: model.GetKind(), representation: model.GetRepresentation()}
		if seen[key] {
			return fmt.Errorf("%w: duplicate model representation", ErrProtocol)
		}
		seen[key] = true
	}
	return nil
}

func findValueModel(description *protocol.Description, target, kind string, representation protocol.ValueInput_Representation) *protocol.ValueModel {
	for _, engine := range description.GetEngines() {
		if engine.GetId() != target {
			continue
		}
		for _, model := range engine.GetValueModels() {
			if model.GetKind() == kind && model.GetRepresentation() == representation {
				return model
			}
		}
	}
	return nil
}

func envelopeMatchesModel(value *protocol.Envelope, model *protocol.ValueModel) bool {
	return value != nil && model != nil && value.GetKind() == model.GetKind() && value.GetSchemaVersion() == model.GetSchemaVersion() &&
		value.GetEncoding() == model.GetEncoding() && value.GetDefinitionSha256() == model.GetDefinitionSha256()
}

func validateModelSourceFormats(model *protocol.ValueModel) error {
	if model.GetRepresentation() != protocol.ValueInput_DESIRED && len(model.GetSourceFormats()) != 0 {
		return fmt.Errorf("%w: only desired models declare source formats", ErrProtocol)
	}
	seen := make(map[protocol.SourceDocument_Format]bool)
	for _, format := range model.GetSourceFormats() {
		switch format {
		case protocol.SourceDocument_SQL, protocol.SourceDocument_YAML, protocol.SourceDocument_HCL, protocol.SourceDocument_GO_ANNOTATIONS:
		default:
			return fmt.Errorf("%w: unknown model source format", ErrProtocol)
		}
		if seen[format] {
			return fmt.Errorf("%w: duplicate model source format", ErrProtocol)
		}
		seen[format] = true
	}
	return nil
}

// canonicalDefinitions holds the digests of model definitions found to be
// canonical nonempty JSON objects, so that each definition is canonicalized
// once per process. Every request a peer validates names the engine whose
// models it checks, and canonicalizing all of them again made up most of the
// validation work. The set is keyed by the digest of the bytes themselves, so
// a changed definition is checked again, and it stops growing at
// maxCanonicalDefinitions.
var canonicalDefinitions = struct {
	sync.RWMutex
	digests map[[sha256.Size]byte]struct{}
}{digests: make(map[[sha256.Size]byte]struct{})}

const maxCanonicalDefinitions = 4096

// verifiedDefinitions holds the definitions found to be canonical and to
// match the digest their model claims, by the address and length of their
// bytes and that digest. A request names the engine whose models it checks,
// and a validator reaches that engine for every table of a request, so
// hashing each definition again on every visit made a request of 500 tables
// spend seconds in SHA-256 alone. A description is validated once per
// process for each copy of its bytes; a model whose bytes were replaced, or
// that claims another digest, is checked again. The set holds the bytes it
// names alive and stops growing at maxCanonicalDefinitions.
var verifiedDefinitions = struct {
	sync.RWMutex
	held map[verifiedDefinition]struct{}
}{held: make(map[verifiedDefinition]struct{})}

type verifiedDefinition struct {
	data   *byte
	length int
	digest string
}

func validateModelDefinition(model *protocol.ValueModel) error {
	definition := model.GetDefinitionJson()
	var verified verifiedDefinition
	if len(definition) != 0 {
		verified = verifiedDefinition{data: &definition[0], length: len(definition), digest: model.GetDefinitionSha256()}
		verifiedDefinitions.RLock()
		_, held := verifiedDefinitions.held[verified]
		verifiedDefinitions.RUnlock()
		if held {
			return nil
		}
	}
	if err := checkModelDefinition(model, definition); err != nil {
		return err
	}
	if verified.data != nil {
		verifiedDefinitions.Lock()
		if len(verifiedDefinitions.held) < maxCanonicalDefinitions {
			verifiedDefinitions.held[verified] = struct{}{}
		}
		verifiedDefinitions.Unlock()
	}
	return nil
}

// checkModelDefinition checks that definition is a canonical nonempty JSON
// object whose digest is the one model claims.
func checkModelDefinition(model *protocol.ValueModel, definition []byte) error {
	digest := sha256.Sum256(definition)
	canonicalDefinitions.RLock()
	_, known := canonicalDefinitions.digests[digest]
	canonicalDefinitions.RUnlock()
	if !known {
		canonical, err := modeljson.Canonical(definition)
		if err != nil || len(canonical) == 0 || canonical[0] != '{' || string(canonical) == "{}" || !bytes.Equal(canonical, definition) {
			return fmt.Errorf("%w: model definition must be a canonical nonempty JSON object", ErrProtocol)
		}
		canonicalDefinitions.Lock()
		if len(canonicalDefinitions.digests) < maxCanonicalDefinitions {
			canonicalDefinitions.digests[digest] = struct{}{}
		}
		canonicalDefinitions.Unlock()
	}
	if hex.EncodeToString(digest[:]) != model.GetDefinitionSha256() {
		return fmt.Errorf("%w: model definition digest differs from its content", ErrProtocol)
	}
	return nil
}

func validateModelDisplayName(model *protocol.ValueModel) error {
	label := model.GetDisplayName()
	if label == "" {
		return nil
	}
	if (model.GetRepresentation() != protocol.ValueInput_DESIRED && model.GetRepresentation() != protocol.ValueInput_OBSERVED) ||
		!utf8.ValidString(label) || strings.TrimSpace(label) != label || strings.ContainsFunc(label, func(r rune) bool {
		return unicode.IsControl(r) || r == '\u2028' || r == '\u2029'
	}) {
		return fmt.Errorf("%w: model display name requires a single-line desired or observed label", ErrProtocol)
	}
	return nil
}

// lowerHexDigest reports a SHA-256 digest written as 64 lowercase hexadecimal
// digits. It allocates nothing: a request's validation checks the models of
// its engine for each table it carries, and decoding and lowering each digest
// made that a second of a plan of 500 tables.
func lowerHexDigest(digest string) bool {
	if len(digest) != 2*sha256.Size {
		return false
	}
	for i := range len(digest) {
		if c := digest[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
