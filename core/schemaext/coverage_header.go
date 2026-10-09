package schemaext

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/coverage"
)

// CoverageHeaderMarker introduces a versioned coverage document in a schema's
// leading comment header. The body includes exact model identities, so an empty
// namespace cannot acquire a newer model's meaning when read by another build.
const CoverageHeaderMarker = "ptah:feature-coverage"

// EncodeCoverageHeader serializes captured claims as one comment body. An
// empty account emits no directive: absence means unknown, never complete.
// The caller adds its format's line-comment prefix.
func (r Registry) EncodeCoverageHeader(ctx context.Context, representation Representation, known Coverage) (string, error) {
	document, err := r.EncodeCoverage(ctx, representation, known)
	if err != nil {
		return "", err
	}
	if known.IsZero() {
		return "", nil
	}
	data, err := json.Marshal(document)
	if err != nil {
		return "", err
	}
	return CoverageHeaderMarker + " " + string(data), nil
}

// DecodeCoverageHeader restores one exact account from a leading header. The
// boolean reports whether the document supplied a directive. Duplicate records,
// malformed JSON, unknown models, and a different representation are refused.
// Without a directive it returns an empty account for the requested direction.
func (r Registry) DecodeCoverageHeader(ctx context.Context, representation Representation, document string) (Coverage, bool, error) {
	if err := codecContext(ctx); err != nil {
		return Coverage{}, false, err
	}
	known, err := NewCoverage(representation, nil, nil)
	if err != nil {
		return Coverage{}, false, err
	}
	found := false
	for body := range coverage.HeaderComments(document) {
		data, recognized := strings.CutPrefix(body, CoverageHeaderMarker)
		if !recognized {
			continue
		}
		if found {
			return Coverage{}, false, fmt.Errorf("%w: duplicate feature coverage header", ErrDuplicate)
		}
		wire, err := decodeCoverageHeaderDocument(json.RawMessage(strings.TrimSpace(data)))
		if err != nil {
			return Coverage{}, false, err
		}
		if wire.Representation != representation {
			return Coverage{}, false, fmt.Errorf("%w: feature coverage header direction differs from the source", ErrIncompatibleCodec)
		}
		known, err = r.DecodeCoverage(ctx, wire)
		if err != nil {
			return Coverage{}, false, err
		}
		found = true
	}
	return known, found, nil
}

func decodeCoverageHeaderDocument(data json.RawMessage) (CoverageDocument, error) {
	shape, err := DecodeJSON[any](data)
	if err != nil {
		return CoverageDocument{}, err
	}
	if err := validateCoverageHeaderAliases(shape); err != nil {
		return CoverageDocument{}, err
	}
	return DecodeJSON[CoverageDocument](data)
}

// encoding/json accepts case aliases, which would let Version and version
// overwrite each other even though they are distinct JSON keys. Reject those
// duplicate meanings before decoding the concrete coverage document.
func validateCoverageHeaderAliases(value any) error {
	switch data := value.(type) {
	case map[string]any:
		seen := make(map[string]bool, len(data))
		for _, key := range slices.Sorted(maps.Keys(data)) {
			folded := strings.ToLower(key)
			if seen[folded] {
				return fmt.Errorf("%w: duplicate coverage header field %q", ErrDuplicate, key)
			}
			seen[folded] = true
			if err := validateCoverageHeaderAliases(data[key]); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range data {
			if err := validateCoverageHeaderAliases(child); err != nil {
				return err
			}
		}
	}
	return nil
}
