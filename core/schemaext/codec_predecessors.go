package schemaext

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
)

// registerPredecessors records the earlier identities each registration of
// owned names, once r holds every registration. It refuses an identity that
// is incomplete, of another representation, the codec's current identity, of
// a kind another registration owns, or named by two registrations.
func (r *Registry) registerPredecessors(owned []OwnedCodec) error {
	for _, declaration := range owned {
		key := codecKey{kind: declaration.Codec.Prototype.Kind(), representation: declaration.Codec.Representation}
		for _, predecessor := range declaration.Predecessors {
			if !Kind(predecessor.Owner).Valid() || !predecessor.Kind.Valid() || predecessor.Representation != key.representation ||
				predecessor.Version == 0 || !definitionDigest(predecessor.Definition) {
				return fmt.Errorf("%w: predecessor of %q/%s needs an owner, a kind, the codec's representation, a version and a sha256 definition",
					ErrInvalidCodec, key.kind, key.representation)
			}
			if predecessor == r.codecs[key].identity(key) {
				return fmt.Errorf("%w: predecessor of %q/%s is its current identity", ErrInvalidCodec, key.kind, key.representation)
			}
			if other := (codecKey{kind: predecessor.Kind, representation: key.representation}); other != key {
				if _, owned := r.codecs[other]; owned {
					return fmt.Errorf("%w: predecessor of %q/%s names %q, which another codec owns", ErrInvalidCodec, key.kind, key.representation, predecessor.Kind)
				}
			}
			if previous, named := r.predecessors[predecessor]; named {
				return fmt.Errorf("%w: predecessor %q/%s is named by %q and %q", ErrInvalidCodec, predecessor.Kind, predecessor.Representation, previous.kind, key.kind)
			}
			if r.predecessors == nil {
				r.predecessors = make(map[CodecIdentity]codecKey)
			}
			r.predecessors[predecessor] = key
		}
	}
	return nil
}

// definitionDigest reports a definition digest as an identity names it:
// "sha256:" and 64 lowercase hexadecimal digits.
func definitionDigest(definition string) bool {
	text, found := strings.CutPrefix(definition, "sha256:")
	decoded, err := hex.DecodeString(text)
	return found && err == nil && len(decoded) == 32 && text == strings.ToLower(text)
}

// succeed returns document with each kind record named under a registered
// predecessor named under its codec's current identity, and each subject
// record of the predecessor's kind, and its subject's kind, renamed to the
// codec's kind. The knowledge of every record is kept. A document that names
// no predecessor is returned as it is.
func (r Registry) succeed(document CoverageDocument) CoverageDocument {
	renamed := make(map[Kind]Kind)
	kinds := slices.Clone(document.Kinds)
	for i, record := range kinds {
		key, found := r.predecessors[record.Model]
		if !found {
			continue
		}
		renamed[record.Model.Kind] = key.kind
		kinds[i].Model = r.codecs[key].identity(key)
	}
	if len(renamed) == 0 {
		return document
	}
	subjects := slices.Clone(document.Subjects)
	for i, record := range subjects {
		current, found := renamed[record.Kind]
		if !found {
			continue
		}
		subjects[i].Kind = current
		if record.Subject.Kind == objectidentity.Kind(record.Kind) {
			subjects[i].Subject.Kind = objectidentity.Kind(current)
		}
	}
	document.Kinds, document.Subjects = kinds, subjects
	return document
}
